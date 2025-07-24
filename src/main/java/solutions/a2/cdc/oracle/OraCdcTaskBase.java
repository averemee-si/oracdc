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

import solutions.a2.cdc.oracle.jmx.OraCdcMgmtBase;
import solutions.a2.cdc.oracle.schema.FileUtils;
import solutions.a2.cdc.oracle.utils.Version;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.utils.ExceptionUtils;

import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_RESTORE;

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
	Map<Long, OraTable4LogMiner> tablesInProcessing;
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
						"\n" +
						"=====================\n" +
						"LOB processing is only possible if a2.transaction.implementation is set to ChronicleQueue!\n" +
						"Please set a2.process.lobs to false if a2.transaction.implementation is set to ConcurrentLinkedQueue\n" +
						"and restart connector!!!\n" +
						"=====================");
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

			LOGGER.info(
					"\n" +
					"=====================\n" +
					"Connector {} connected to {}, {}\n\t$ORACLE_SID={}, running on {}, OS {}.\n" +
					"=====================",
					connectorName,
					rdbmsInfo.getRdbmsEdition(), rdbmsInfo.getVersionString(),
					rdbmsInfo.getInstanceName(), rdbmsInfo.getHostName(), rdbmsInfo.getPlatformName());

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
						"\n" +
						"=====================\n" +
						"Connected to PDB {} (RDBMS 19.10+ Feature)\n" +
						"=====================",
						rdbmsInfo.getPdbName());
			}

			if (useStandby) {
				oraConnections.addStandbyConnection(
						config.getString(ParamConstants.STANDBY_URL_PARAM),
						config.getString(ParamConstants.STANDBY_WALLET_PARAM));
				LOGGER.info(
						"\n" +
						"=====================\n" +
						"Connector {} will use connection to PHYSICAL STANDBY for LogMiner calls\n" +
						"=====================",
						connectorName);
			}
			if (config.getBoolean(ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM)) {
				oraConnections.addDistributedConnection(
						config.getString(ParamConstants.DISTRIBUTED_URL_PARAM),
						config.getString(ParamConstants.DISTRIBUTED_WALLET_PARAM));
				LOGGER.info(
						"\n" +
						"=====================\n" +
						"Connector {} will use remote database in distributed configuration for LogMiner calls\n" +
						"=====================",
						connectorName);
			}

			if (Strings.CI.equals(rdbmsInfo.getSupplementalLogDataAll(), "YES")) {
				LOGGER.info(
						"\n" +
						"=====================\n" +
						"V$DATABASE.SUPPLEMENTAL_LOG_DATA_ALL is set to 'YES'.\n" +
						"\tNo additional checks for supplemental logging will performed at the table level.\n" +
						"=====================");
			} else {
				if (Strings.CI.equals(rdbmsInfo.getSupplementalLogDataMin(), "NO")) {
					LOGGER.error(
							"\n" +
							"=====================\n" +
							"Both V$DATABASE.SUPPLEMENTAL_LOG_DATA_ALL and V$DATABASE.SUPPLEMENTAL_LOG_DATA_MIN are set to 'NO'!\n" +
							"For the connector to work properly, you need to set connecting Oracle RDBMS as SYSDBA:\n" +
							"alter database add supplemental log data (ALL) columns;\n" +
							"OR recommended but more time consuming settings\n" +
							"alter database add supplemental log data;\n" +
							"and then enable supplemental only for required tables:\n" +
							"alter table <OWNER>.<TABLE_NAME> add supplemental log data (ALL) columns;\n" +
							"=====================");
					throw new ConnectException("Must set SUPPLEMENTAL LOGGING settings!");
				} else {
					LOGGER.info(
							"\n" +
							"=====================\n" +
							"V$DATABASE.SUPPLEMENTAL_LOG_DATA_ALL is set to 'NO'.\n" +
							"V$DATABASE.SUPPLEMENTAL_LOG_DATA_MIN is set to '{}'.\n" + 
							"\tAdditional checks for supplemental logging will performed at the table level.\n" +
							"=====================",
							rdbmsInfo.getSupplementalLogDataMin());
				}
			}

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
						table.setTopicDecoderPartition(config, rdbmsInfo.odd(), rdbmsInfo.partition());
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

		// New resiliency model
		if (offsetFromKafka != null && offsetFromKafka.containsKey("C:COMMIT_SCN")) {
			// Use stored offset values for SCN and related from storage offset
			if (startScnFromProps) {
				LOGGER.info("Ignoring the value of parameter a2.first.change={}, since the offset is already present in the connector offset data!",
						firstScnFromProps);
			}
			firstScn = (long) offsetFromKafka.get("S:SCN");
			firstRba = RedoByteAddress.fromLogmnrContentsRs_Id((String) offsetFromKafka.get("S:RS_ID"));
			firstSubScn = (long) offsetFromKafka.get("S:SSN");
			LOGGER.info("Point in time from offset data to start reading reading from SCN={}, RS_ID (RBA)='{}', SSN={}",
						firstScn, firstRba, firstSubScn);
			lastProcessedCommitScn = (long) offsetFromKafka.get("C:COMMIT_SCN");
			lastInProgressCommitScn = (long) offsetFromKafka.get("COMMIT_SCN");
			if (lastProcessedCommitScn == lastInProgressCommitScn) {
				// Rewind not required, reset back lastInProgressCommitScn
				lastInProgressCommitScn = 0;
			} else {
				lastInProgressScn = (long) offsetFromKafka.get("SCN");
				lastInProgressRba = RedoByteAddress.fromLogmnrContentsRs_Id((String) offsetFromKafka.get("RS_ID"));
				lastInProgressSubScn = (long) offsetFromKafka.get("SSN");
				LOGGER.info("Last sent SCN={}, RBA={}, SSN={} for  transaction with incomplete send",
							lastInProgressScn, lastInProgressRba, lastInProgressSubScn);
			}
			if (firstScn < firstAvailableScn) {
				LOGGER.warn(
						"\n" +
						"=====================\n" +
						"Ignoring Point in time {}:{}:{} from offset, and setting it to first available SCN in V$ARCHIVED_LOG {}.\n" +
						"=====================",
							firstScn, firstRba, firstSubScn, firstAvailableScn);
				firstScn = firstAvailableScn;
			} else {
				rewind = true;
			}
		} else {
			LOGGER.info("No data present in connector's offset storage for {}:{}",
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
		offset.put("S:SCN", transData.getLeft());
		offset.put("S:RS_ID", transData.getMiddle().toString());
		offset.put("S:SSN", transData.getRight());
	}

	void putTableAndVersion(final long combinedDataObjectId, final int version) {
		offset.put(Long.toString(combinedDataObjectId), Integer.toString(version));
	}


}