/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package solutions.a2.cdc.oracle;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.jmx.OraCdcInitialLoad;
import solutions.a2.cdc.oracle.jmx.OraCdcMgmtBase;
import solutions.a2.cdc.oracle.schema.FileUtils;
import solutions.a2.cdc.oracle.utils.Version;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.utils.ExceptionUtils;

import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_RESTORE;
import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.INITIAL_LOAD_COMPLETED;
import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.INITIAL_LOAD_EXECUTE;
import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.INITIAL_LOAD_IGNORE;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public abstract class OraCdcTaskBase extends SourceTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTaskBase.class);
	static final int WAIT_FOR_WORKER_MILLIS = 50;

	static final AtomicBoolean state = new AtomicBoolean(true);
	static final AtomicInteger taskId = new AtomicInteger(0);

	int batchSize;
	int pollInterval;
	int schemaType;
	OraRdbmsInfo rdbmsInfo;
	String connectorName;
	OraCdcSourceConnectorConfig config;
	boolean useChronicleQueue = true;
	boolean processLobs = false;
	CountDownLatch runLatch;
	AtomicBoolean isPollRunning;
	OraConnectionObjects oraConnections;
	Map<String, Object> offset;
	Map<Long, OraTable> tablesInProcessing;
	Set<Long> tablesOutOfScope;
	BlockingQueue<OraCdcTransaction> committedTransactions;
	OraCdcTransaction transaction;
	OraCdcWorkerThreadBase worker;
	OraCdcDictionaryChecker checker;
	boolean lastStatementInTransaction = true;
	final List<SourceRecord> result = new ArrayList<>();
	final AtomicLong taskThreadId = new AtomicLong(0);

	long lastProcessedCommitScn = 0;
	long lastInProgressCommitScn = 0;
	long lastInProgressScn = 0;
	RedoByteAddress lastInProgressRba = null;
	long lastInProgressSubScn = 0;
	boolean restoreIncompleteRecord = false;

	boolean execInitialLoad = false;
	String initialLoadStatus = INITIAL_LOAD_IGNORE;
	OraCdcInitialLoadThread initialLoadWorker;
	BlockingQueue<OraTable4InitialLoad> tablesQueue;
	OraTable4InitialLoad table4InitialLoad;
	boolean lastRecordInTable = true;
	OraCdcInitialLoad initialLoadMetrics;
	private String fldCommitScnInProgress = null;
	private String fldCommitScnCompleted = null;
	private String fldScnInProgress = null;
	private String fldRbaInProgress = null;
	private String fldSubScnInProgress = null;
	private String fldScnStart = null;
	private String fldRbaStart = null;
	private String fldSubScnStart = null;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		connectorName = props.get("name");
		try {
			config = new OraCdcSourceConnectorConfig(props);
		} catch (ConfigException ce) {
			throw new ConnectException("Couldn't start oracdc due to coniguration error", ce);
		}
		config.setConnectorName(connectorName);
		batchSize = config.getInt(ConnectorParams.BATCH_SIZE_PARAM);
		pollInterval = config.pollIntervalMs();
		schemaType = config.schemaType();
		restoreIncompleteRecord = config.getIncompleteDataTolerance() == INCOMPLETE_REDO_INT_RESTORE;

		useChronicleQueue = Strings.CI.equals(
				config.getString(ParamConstants.ORA_TRANSACTION_IMPL_PARAM),
				ParamConstants.ORA_TRANSACTION_IMPL_CHRONICLE);
		processLobs = config.processLobs();
		if (processLobs) {
			if (!useChronicleQueue) {
				LOGGER.error(
						"""
						
						=====================
						LOB processing is only possible if a2.transaction.implementation is set to ChronicleQueue!
						Please set a2.process.lobs to false if a2.transaction.implementation is set to ConcurrentLinkedQueue
						and restart connector!!!
						=====================
						
						""");
				throw new ConnectException("LOB processing is only possible if a2.transaction.implementation is set to ChronicleQueue!");
			}
		}
		runLatch = new CountDownLatch(1);
		isPollRunning = new AtomicBoolean(false);

		final boolean useRac = config.useRac();
		final boolean useStandby = config.activateStandby();
		final boolean dg4RacSingleInst = useStandby &&
				config.dg4RacThreads() != null && config.dg4RacThreads().size() > 1;
		int threadNo = 1;
		if (dg4RacSingleInst) {
			// Single instance DataGuard for RAC
			state.set(true);
			taskId.set(0);
			final List<String> standbyThreads = config.dg4RacThreads();
			while (!state.compareAndSet(true, false)) {
				try {Thread.sleep(1);} catch (InterruptedException e) {}
			}
			final int index = taskId.getAndAdd(1);
			if (index > (standbyThreads.size() - 1)) {
				LOGGER.error("Errors while processing following array of Oracle Signgle Instance DataGuard for RAC threads:");
				standbyThreads.forEach(v -> LOGGER.error("\t{}", v));
				LOGGER.error("Size equals {}, but current index equals {} !", standbyThreads.size(), index);
				throw new ConnectException("Unable to properly assign Kafka tasks to Oracle Single Instance DataGuard for RAC!");
			} else if (index == (standbyThreads.size() - 1)) {
				// Last element - reset back to 0
				taskId.set(0);
			}
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Processing redo thread array element {} with value {}.",
						index, standbyThreads.get(index));
			}
			threadNo = Integer.parseInt(standbyThreads.get(index));
			state.set(true);
		}
		try {
			if (StringUtils.isNotBlank(config.walletLocation())) {
				if (useRac) {
					oraConnections = OraConnectionObjects.get4OraWallet(
							connectorName, config.racUrls(), config.walletLocation());
				} else {
					oraConnections = OraConnectionObjects.get4OraWallet(
							connectorName,
							config.getString(ConnectorParams.CONNECTION_URL_PARAM), 
							config.walletLocation());
				}
			} else if (StringUtils.isNotBlank(config.getString(ConnectorParams.CONNECTION_USER_PARAM)) &&
					StringUtils.isNotBlank(config.getPassword(ConnectorParams.CONNECTION_PASSWORD_PARAM).value())) {
				if (useRac) {
					oraConnections = OraConnectionObjects.get4UserPassword(
							connectorName,
							config.racUrls(),
							config.getString(ConnectorParams.CONNECTION_USER_PARAM),
							config.getPassword(ConnectorParams.CONNECTION_PASSWORD_PARAM).value());					
				} else {
					oraConnections = OraConnectionObjects.get4UserPassword(
							connectorName,
							config.getString(ConnectorParams.CONNECTION_URL_PARAM),
							config.getString(ConnectorParams.CONNECTION_USER_PARAM),
							config.getPassword(ConnectorParams.CONNECTION_PASSWORD_PARAM).value());
				}
			} else {
				throw new SQLException("Wrong connection parameters!");
			}
		} catch(SQLException sqle) {
			LOGGER.error("Unable to connect to RDBMS for connector '{}'!",
					connectorName);
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			LOGGER.error("Stopping connector '{}'", connectorName);
			throw new ConnectException("Unable to connect to RDBMS");
		}

		if (config.useOracdcSchemas()) {
			LOGGER.info("oracdc will use own schemas for Oracle NUMBER and TIMESTAMP WITH [LOCAL] TIMEZONE datatypes");
		}

		if (offset == null) offset = new ConcurrentHashMap<>();
		if (tablesInProcessing == null) tablesInProcessing = new HashMap<>();
		if (tablesOutOfScope == null) tablesOutOfScope = new HashSet<>();
		if (committedTransactions == null) committedTransactions = new LinkedBlockingQueue<>();

		try (Connection connDictionary = oraConnections.getConnection()) {
			rdbmsInfo = new OraRdbmsInfo(connDictionary);
			if (dg4RacSingleInst) {
				rdbmsInfo.setRedoThread(threadNo);
			}
			config.topicPartition(rdbmsInfo.getRedoThread() - 1);
			if (useRac) {
				int redoThread = rdbmsInfo.getRedoThread();
				fldCommitScnInProgress = "COMMIT_SCN/" + redoThread;
				fldCommitScnCompleted = "C:COMMIT_SCN/" + redoThread;
				fldScnInProgress = "SCN/" + redoThread;
				fldRbaInProgress = "RBA/" + redoThread;
				fldSubScnInProgress = "SSN/" + redoThread;
				fldScnStart = "S:SCN/" + redoThread;
				fldRbaStart = "S:RBA/" + redoThread;
				fldSubScnStart = "S:SSN/" + redoThread;
			} else {
				fldCommitScnInProgress = "COMMIT_SCN";
				fldCommitScnCompleted = "C:COMMIT_SCN";
				fldScnInProgress = "SCN";
				fldRbaInProgress = "RS_ID";
				fldSubScnInProgress = "SSN";
				fldScnStart = "S:SCN";
				fldRbaStart = "S:RS_ID";
				fldSubScnStart = "S:SSN";
			}

			LOGGER.info(
					"""
					
					=====================
					Connector {} connected to {}, {}\n\t$ORACLE_SID={}, THREAD#={} running on {}, OS {}.
					=====================
					
					""",
						connectorName,
						rdbmsInfo.getRdbmsEdition(), rdbmsInfo.getVersionString(),
						rdbmsInfo.getInstanceName(), rdbmsInfo.getRedoThread(),
						rdbmsInfo.getHostName(), rdbmsInfo.getPlatformName());

			if (rdbmsInfo.isCdb() && !rdbmsInfo.isCdbRoot() && !rdbmsInfo.isPdbConnectionAllowed()) {
				LOGGER.error(
						"Connector {} must be connected to CDB$ROOT while using oracdc for mining data using LogMiner!",
						connectorName);
				throw new ConnectException("Unable to run oracdc without connection to CDB$ROOT!");
			} else {
				LOGGER.debug("Oracle connection information:\n{}", rdbmsInfo.toString());
			}
			if (rdbmsInfo.isCdb() && rdbmsInfo.isPdbConnectionAllowed()) {
				LOGGER.info(
						"""
						
						=====================
						Connected to PDB {} (RDBMS 19.10+ Feature)
						=====================
						""",
							rdbmsInfo.getPdbName());
			}

			if (useStandby) {
				oraConnections.addStandbyConnection(
						config.getString(ParamConstants.STANDBY_URL_PARAM),
						config.getString(ParamConstants.STANDBY_WALLET_PARAM));
				LOGGER.info(
						"""
						
						=====================
						Connector {} will use connection to PHYSICAL STANDBY for LogMiner calls
						=====================
						""",
							connectorName);
			}
			if (config.getBoolean(ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM)) {
				oraConnections.addDistributedConnection(
						config.getString(ParamConstants.DISTRIBUTED_URL_PARAM),
						config.getString(ParamConstants.DISTRIBUTED_WALLET_PARAM));
				LOGGER.info(
						"""
						
						=====================
						Connector {} will use remote database in distributed configuration for LogMiner calls
						=====================
						""",
							connectorName);
			}

			if (Strings.CI.equals(rdbmsInfo.getSupplementalLogDataAll(), "YES")) {
				LOGGER.info(
						"""
						
						=====================
						V$DATABASE.SUPPLEMENTAL_LOG_DATA_ALL is set to 'YES'.
						    No additional checks for supplemental logging will performed at the table level.
						=====================
						""");
			} else {
				if (Strings.CI.equals(rdbmsInfo.getSupplementalLogDataMin(), "NO")) {
					LOGGER.error(
							"""
							
							=====================
							Both V$DATABASE.SUPPLEMENTAL_LOG_DATA_ALL and V$DATABASE.SUPPLEMENTAL_LOG_DATA_MIN are set to 'NO'!
							For the connector to work properly, you need to set connecting Oracle RDBMS as SYSDBA:
							alter database add supplemental log data (ALL) columns;
							OR recommended but more time consuming settings
							alter database add supplemental log data;
							and then enable supplemental only for required tables:
							alter table <OWNER>.<TABLE_NAME> add supplemental log data (ALL) columns;
							=====================
							""");
					throw new ConnectException("Must set SUPPLEMENTAL LOGGING settings!");
				} else {
					LOGGER.info(
							"""
							
							=====================
							V$DATABASE.SUPPLEMENTAL_LOG_DATA_ALL is set to 'NO'.
							V$DATABASE.SUPPLEMENTAL_LOG_DATA_MIN is set to '{}'. 
							    Additional checks for supplemental logging will performed at the table level.
							=====================
							""",
								rdbmsInfo.getSupplementalLogDataMin());
				}
			}

			// Initial load
			if (Strings.CI.equals(
					INITIAL_LOAD_EXECUTE,
					config.initialLoad())) {
				execInitialLoad = true;
				initialLoadStatus = INITIAL_LOAD_EXECUTE;
				final Map<String, Object> offsetFromKafka = context.offsetStorageReader().offset(rdbmsInfo.partition());
				if (offsetFromKafka != null &&
						Strings.CI.equals(
								INITIAL_LOAD_COMPLETED,
								(String) offsetFromKafka.get("I"))) {
					execInitialLoad = false;
					initialLoadStatus = INITIAL_LOAD_COMPLETED;
					offset.put("I", INITIAL_LOAD_COMPLETED);
					LOGGER.info("Initial load set to {} (value from offset)", INITIAL_LOAD_COMPLETED);
				}
			}
			// End - initial load analysis...

		} catch (SQLException sqle) {
			LOGGER.error("Unable to prepare to start oracdc task!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			throw new ConnectException(sqle);
		}
	}

	void stop(boolean stopWorker) {
		LOGGER.info("Stopping oracdc source task");
		if (runLatch != null ) {
			// We can stop before runLatch initialization due to invalid parameters
			runLatch.countDown();
			if (stopWorker && worker != null) {
				worker.shutdown();
				while (worker.isRunning()) {
					try {
						LOGGER.debug("Waiting {} ms for worker thread to stop...", WAIT_FOR_WORKER_MILLIS);
						Thread.sleep(WAIT_FOR_WORKER_MILLIS);
					} catch (InterruptedException e) {}
				}
			} else {
				while (isPollRunning.get()) {
					try {
						LOGGER.debug("Waiting {} ms for connector task to stop...", WAIT_FOR_WORKER_MILLIS);
						Thread.sleep(WAIT_FOR_WORKER_MILLIS);
					} catch (InterruptedException e) {}
				}
			}
		}
	}

	void stopEpilogue() {
		if (useChronicleQueue && committedTransactions!= null && !committedTransactions.isEmpty()) {
			// Clean only when we use ChronicleQueue
			committedTransactions.forEach(transaction -> {
				if (isPollRunning.get()) {
					LOGGER.error("Unable to remove directory {}, please remove it manually",
							((OraCdcTransactionChronicleQueue) transaction).getPath().toString());
				} else {
					transaction.close();
				}
			});
		}
		if (oraConnections != null) {
			try {
				oraConnections.destroy();
			} catch (SQLException sqle) {
				LOGGER.error("Unable to close all RDBMS connections!");
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			}
		}
	}

	void processStoredSchemas(final OraCdcMgmtBase metrics) {
		if (config.useOracdcSchemas()) {
			// Use stored schema only in this mode
			final String schemaFileName = config.getString(ParamConstants.DICTIONARY_FILE_PARAM);
			if (StringUtils.isNotBlank(schemaFileName)) {
				try {
					LOGGER.info("Loading stored schema definitions from file {}.", schemaFileName);
					tablesInProcessing = FileUtils.readDictionaryFile(schemaFileName, schemaType, config.transformLobsImpl(), rdbmsInfo);
					LOGGER.info("{} table schema definitions loaded from file {}.",
							tablesInProcessing.size(), schemaFileName);
					tablesInProcessing.forEach((key, table) -> {
						table.setTopicDecoderPartition(config, rdbmsInfo.partition());
						metrics.addTableInProcessing(table.fqn());
					});
				} catch (IOException ioe) {
					LOGGER.warn("Unable to read stored definition from {}.", schemaFileName);
					LOGGER.warn(ExceptionUtils.getExceptionStackTrace(ioe));
				}
			}
		}
	}

	boolean startPosition(MutableTriple<Long, RedoByteAddress, Long> coords) throws SQLException {
		boolean rewind = false;
		final long firstAvailableScn = rdbmsInfo.firstScnFromArchivedLogs(
				oraConnections.getLogMinerConnection(),
				!(config.activateStandby() ||  rdbmsInfo.isStandby()));
		long firstScn = firstAvailableScn;
		RedoByteAddress firstRba = null;
		long firstSubScn = -1;
		final long firstScnFromProps = config.startScn();
		final boolean startScnFromProps = Long.compareUnsigned(firstScnFromProps, 0) > 0;
		final Map<String, Object> offsetFromKafka;
		if (context != null && context.offsetStorageReader() != null) {
			offsetFromKafka = context.offsetStorageReader().offset(rdbmsInfo.partition());
		} else {
			offsetFromKafka = null;
		}

		if (offsetFromKafka != null && offsetFromKafka.containsKey(fldCommitScnCompleted) && !config.ignoreStoredOffset()) {
			// Use stored offset values for SCN and related from storage offset
			if (startScnFromProps) {
				LOGGER.info("Ignoring the value of parameter a2.first.change={}, since the offset is already present in the connector offset data!",
						firstScnFromProps);
			}
			firstScn = (long) offsetFromKafka.get(fldScnStart);
			firstRba = RedoByteAddress.fromLogmnrContentsRs_Id((String) offsetFromKafka.get(fldRbaStart));
			firstSubScn = (long) offsetFromKafka.get(fldSubScnStart);
			LOGGER.info("Point in time from offset data to start reading reading from SCN={}, RS_ID (RBA)='{}', SSN={}",
						firstScn, firstRba, firstSubScn);
			lastProcessedCommitScn = (long) offsetFromKafka.get(fldCommitScnCompleted);
			lastInProgressCommitScn = (long) offsetFromKafka.get(fldCommitScnInProgress);
			if (lastProcessedCommitScn == lastInProgressCommitScn) {
				// Rewind not required, reset back lastInProgressCommitScn
				lastInProgressCommitScn = 0;
			} else {
				lastInProgressScn = (long) offsetFromKafka.get(fldScnInProgress);
				lastInProgressRba = RedoByteAddress.fromLogmnrContentsRs_Id((String) offsetFromKafka.get(fldRbaInProgress));
				lastInProgressSubScn = (long) offsetFromKafka.get(fldSubScnInProgress);
				LOGGER.info("Last sent SCN={}, RBA={}, SSN={} for  transaction with incomplete send",
							lastInProgressScn, lastInProgressRba, lastInProgressSubScn);
			}
			if (firstScn < firstAvailableScn) {
				LOGGER.warn(
						"""
						
						=====================
						Ignoring Point in time {}:{}:{} from offset, and setting it to first available SCN in V$ARCHIVED_LOG {}.
						=====================
						""",
							firstScn, firstRba, firstSubScn, firstAvailableScn);
				firstScn = firstAvailableScn;
			} else {
				rewind = true;
			}
		} else {
			LOGGER.info(config.ignoreStoredOffset()
					? "Ignoring data in connector offset storage for {}:{}"
					: "No data present in connector's offset storage for {}:{}",
					rdbmsInfo.sourcePartitionName(), rdbmsInfo.getDbId());
			if (startScnFromProps) {
				// a2.first.change set in connector properties, restart data are not present
				firstScn = config.startScn();
				LOGGER.info("{}={} is set in connector properties, previous offset data is not available.",
						config.startScnParam(), firstScn);
				if (firstScn < firstAvailableScn) {
					LOGGER.warn(
							"Ignoring {}={} in connector properties, and setting {} to first available SCN in V$ARCHIVED_LOG {}.",
							config.startScnParam(), firstScn, config.startScnParam(), firstAvailableScn);
					firstScn = firstAvailableScn;
				} else {
					// We need to rewind, potentially
					rewind = true;
				}
			} else {
				// No previous offset, no start scn - just use first available from V$ARCHIVED_LOG  
				LOGGER.info("oracdc will start from minimum available SCN in V$ARCHIVED_LOG = {}.",
						firstAvailableScn);
				firstScn = firstAvailableScn;
			}
		}
		coords.setLeft(firstScn);
		coords.setMiddle(firstRba);
		coords.setRight(firstSubScn);
		return rewind;
	}

	OraConnectionObjects oraConnections() {
		return oraConnections;
	}

	CountDownLatch runLatch() {
		return runLatch;
	}

	OraCdcSourceConnectorConfig config() {
		return config;
	}

	OraRdbmsInfo rdbmsInfo() {
		return rdbmsInfo;
	}

	void putReadRestartScn(final Triple<Long, RedoByteAddress, Long> transData) {
		offset.put(fldScnStart, transData.getLeft());
		offset.put(fldRbaStart, transData.getMiddle().toString());
		offset.put(fldSubScnStart, transData.getRight());
	}

	void putTableVersion(final long combinedDataObjectId, final int version) {
		offset.put(Long.toString(combinedDataObjectId), Integer.toString(version));
	}

	int getTableVersion(final long combinedDataObjectId) {
		if (context != null && context.offsetStorageReader() != null) {
			final Map<String, Object> offsetFromKafka = context.offsetStorageReader().offset(rdbmsInfo.partition());
			if (offsetFromKafka != null && offsetFromKafka.containsKey(Long.toString(combinedDataObjectId))) {
				return Integer.parseInt((String) offsetFromKafka.get(Long.toString(combinedDataObjectId)));
			}
		}
		return 1;
	}

	void prepareInitialLoadWorker(final String initialLoadSql, final long scn) throws SQLException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Initial load table list SQL {}", initialLoadSql);
		try (Connection connection = oraConnections.getConnection();
				PreparedStatement statement = connection.prepareStatement(initialLoadSql);
				ResultSet resultSet = statement.executeQuery()) {
			final boolean isCdb = rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed();
			while (resultSet.next()) {
				final long objectId = resultSet.getLong("OBJECT_ID");
				final long conId = isCdb ? resultSet.getLong("CON_ID") : 0L;
				final long combinedDataObjectId = (conId << 32) | (objectId & 0xFFFFFFFFL);
				final String tableName = resultSet.getString("TABLE_NAME");
				if (!tablesInProcessing.containsKey(combinedDataObjectId)
						&& !Strings.CS.startsWith(tableName, "MLOG$_")) {
					OraTable oraTable;
					if (config.logMiner())
						oraTable = new OraTable4LogMiner(
								isCdb ? resultSet.getString("PDB_NAME") : null,
								isCdb ? (short) conId : -1,
								resultSet.getString("OWNER"), tableName,
								Strings.CI.equals("ENABLED", resultSet.getString("DEPENDENCIES")),
								config, rdbmsInfo, connection, getTableVersion(combinedDataObjectId));
					else
						oraTable = new OraTable4RedoMiner(
								isCdb ? resultSet.getString("PDB_NAME") : null,
								isCdb ? (short) conId : -1,
								resultSet.getString("OWNER"), tableName,
								Strings.CI.equals("ENABLED", resultSet.getString("DEPENDENCIES")),
								config, rdbmsInfo, connection, getTableVersion(combinedDataObjectId));
					tablesInProcessing.put(combinedDataObjectId, oraTable);
				}
			}
		} catch (SQLException sqle) {
			throw new SQLException(sqle);
		}
		tablesQueue = new LinkedBlockingQueue<>();
		initialLoadMetrics = new OraCdcInitialLoad(rdbmsInfo, connectorName);
		initialLoadWorker = new OraCdcInitialLoadThread(
				WAIT_FOR_WORKER_MILLIS,
				scn,
				tablesInProcessing,
				config,
				rdbmsInfo,
				initialLoadMetrics,
				tablesQueue,
				oraConnections);
	}

	boolean executeInitialLoad() throws InterruptedException {
		// Execute initial load...
		if (!initialLoadWorker.isRunning() && tablesQueue.isEmpty() && table4InitialLoad == null) {
			Thread.sleep(WAIT_FOR_WORKER_MILLIS);
			if (tablesQueue.isEmpty()) {
				LOGGER.info("Initial load completed");
				execInitialLoad = false;
				initialLoadStatus = INITIAL_LOAD_COMPLETED;
				offset.put("I", INITIAL_LOAD_COMPLETED);
				return true;
			}
		}
		int recordCount = 0;
		while (recordCount < batchSize) {
			if (lastRecordInTable) {
				//First table or end of table reached, need to poll new
				table4InitialLoad = tablesQueue.poll();
				if (table4InitialLoad != null) {
					initialLoadMetrics.startSendTable(table4InitialLoad.fqn());
					LOGGER.info("Table {} initial load (send to Kafka phase) started.",
							table4InitialLoad.fqn());
				}
			}
			if (table4InitialLoad == null) {
				LOGGER.debug("Waiting {} ms for initial load data...", pollInterval);
				Thread.sleep(pollInterval);
				break;
			} else {
				lastRecordInTable = false;
				// Processing.......
				SourceRecord record = table4InitialLoad.getSourceRecord();
				if (record == null) {
					initialLoadMetrics.finishSendTable(table4InitialLoad.fqn());
					LOGGER.info("Table {} initial load (send to Kafka phase) completed.",
							table4InitialLoad.fqn());
					lastRecordInTable = true;
					table4InitialLoad.close();
					table4InitialLoad = null;
				} else {
					result.add(record);
					recordCount++;
				}
			}
		}
		return false;
	}

	void putInProgressOffsets(final OraCdcStatementBase stmt) {
		offset.put(fldScnInProgress, stmt.getScn());
		offset.put(fldRbaInProgress, stmt.getRba().toString());
		offset.put(fldSubScnInProgress, stmt.getSsn());
		offset.put(fldCommitScnInProgress, transaction.getCommitScn());
	}

	void putCompletedOffset() {
		offset.put(fldCommitScnCompleted, transaction.getCommitScn());
	}
}