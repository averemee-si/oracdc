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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.jmx.OraCdcInitialLoad;
import solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmt;
import solutions.a2.cdc.oracle.schema.FileUtils;
import solutions.a2.cdc.oracle.utils.OraSqlUtils;
import solutions.a2.cdc.oracle.utils.Version;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcLogMinerTask extends SourceTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerTask.class);
	private static final int WAIT_FOR_WORKER_MILLIS = 50;

	private static final AtomicBoolean state = new AtomicBoolean(true);
	private static final AtomicInteger taskId = new AtomicInteger(0);

	private int batchSize;
	private int pollInterval;
	private int schemaType;
	private String stateFileName;
	private OraRdbmsInfo rdbmsInfo;
	private OraCdcLogMinerMgmt metrics;
	private Map<Long, OraTable4LogMiner> tablesInProcessing;
	private Set<Long> tablesOutOfScope;
	private Map<String, OraCdcTransaction> activeTransactions;
	private BlockingQueue<OraCdcTransaction> committedTransactions;
	private OraCdcLogMinerWorkerThread worker;
	private OraCdcTransaction transaction;
	private boolean lastStatementInTransaction = true;
	private boolean processLobs = false;
	private boolean useChronicleQueue = true;
	private CountDownLatch runLatch;
	private AtomicBoolean isPollRunning;
	private boolean execInitialLoad = false;
	private String initialLoadStatus = ParamConstants.INITIAL_LOAD_IGNORE;
	private OraCdcInitialLoadThread initialLoadWorker;
	private BlockingQueue<OraTable4InitialLoad> tablesQueue;
	private OraTable4InitialLoad table4InitialLoad;
	private boolean lastRecordInTable = true;
	private OraCdcInitialLoad initialLoadMetrics;
	private String connectorName; 
	private OraConnectionObjects oraConnections;
	private Map<String, Object> offset;
	private long lastProcessedCommitScn = 0;
	private long lastInProgressCommitScn = 0;
	private long lastInProgressScn = 0;
	private RedoByteAddress lastInProgressRsId = null;
	private long lastInProgressSsn = 0;
	private OraCdcSourceConnectorConfig config;
	private int topicPartition;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		connectorName = props.get("name");
		LOGGER.info("Starting oracdc logminer source task for connector {}.", connectorName);

		try {
			config = new OraCdcSourceConnectorConfig(props);
		} catch (ConfigException ce) {
			throw new ConnectException("Couldn't start oracdc due to coniguration error", ce);
		}
		// pass connectorName to config container
		config.setConnectorName(connectorName);
		final boolean useRac = config.useRac();
		final boolean useStandby = config.activateStandby();
		final boolean dg4RacSingleInst = useStandby &&
				config.dg4RacThreads() != null && config.dg4RacThreads().size() > 1;
		int threadNo = 1;
		if (dg4RacSingleInst) {
			// Single instance DataGuard for RAC
			final List<String> standbyThreads = config.dg4RacThreads();
			while (!state.compareAndSet(true, false)) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
				}
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
			LOGGER.debug("Processing redo thread array element {} with value {}.",
					index, standbyThreads.get(index));
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

		batchSize = config.getInt(ConnectorParams.BATCH_SIZE_PARAM);
		pollInterval = config.pollIntervalMs();
		if (config.useOracdcSchemas()) {
			LOGGER.info("oracdc will use own schemas for Oracle NUMBER and TIMESTAMP WITH [LOCAL] TIMEZONE datatypes");
		}

		schemaType = config.schemaType();
		useChronicleQueue = StringUtils.equalsIgnoreCase(
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
		offset = new ConcurrentHashMap<>();
		try (Connection connDictionary = oraConnections.getConnection()) {
			rdbmsInfo = new OraRdbmsInfo(connDictionary);
			if (dg4RacSingleInst) {
				rdbmsInfo.setRedoThread(threadNo);
			}
			if (useRac || dg4RacSingleInst) {
				topicPartition = rdbmsInfo.getRedoThread() - 1;
			} else {
				topicPartition = config.getShort(ParamConstants.TOPIC_PARTITION_PARAM);
			}

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

			if (StringUtils.equalsIgnoreCase(rdbmsInfo.getSupplementalLogDataAll(), "YES")) {
				LOGGER.info(
						"\n" +
						"=====================\n" +
						"V$DATABASE.SUPPLEMENTAL_LOG_DATA_ALL is set to 'YES'.\n" +
						"\tNo additional checks for supplemental logging will performed at the table level.\n" +
						"=====================");
			} else {
				if (StringUtils.equalsIgnoreCase(rdbmsInfo.getSupplementalLogDataMin(), "NO")) {
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

			metrics = new OraCdcLogMinerMgmt(rdbmsInfo, connectorName, this);
			OraCdcPseudoColumnsProcessor pseudoColumns = config.pseudoColumnsProcessor();

			List<String> excludeList = config.excludeObj();
			if (excludeList.size() < 1)
				excludeList = null;
			List<String> includeList = config.includeObj();
			if (includeList.size() < 1)
				includeList = null;
			final boolean tableListGenerationStatic;
			if (StringUtils.equalsIgnoreCase(
					ParamConstants.TABLE_LIST_STYLE_STATIC, config.getString(ParamConstants.TABLE_LIST_STYLE_PARAM))) {
				// ParamConstants.TABLE_LIST_STYLE_STATIC
				tableListGenerationStatic = true;
			} else {
				// TABLE_LIST_STYLE_DYNAMIC
				tableListGenerationStatic = false;
			}

			final Path queuesRoot = FileSystems.getDefault().getPath(
					config.getString(ParamConstants.TEMP_DIR_PARAM));

			if (config.useOracdcSchemas()) {
				// Use stored schema only in this mode
				final String schemaFileName = config.getString(ParamConstants.DICTIONARY_FILE_PARAM);
				if (StringUtils.isNotBlank(schemaFileName)) {
					try {
						LOGGER.info("Loading stored schema definitions from file {}.", schemaFileName);
						tablesInProcessing = FileUtils.readDictionaryFile(
								schemaFileName, schemaType, config.transformLobsImpl(), rdbmsInfo);
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
			if (tablesInProcessing == null) {
				tablesInProcessing = new ConcurrentHashMap<>();
			}
			tablesOutOfScope = new HashSet<>();
			activeTransactions = new HashMap<>();
			committedTransactions = new LinkedBlockingQueue<>();

			boolean rewind = false;
			final long firstAvailableScn = rdbmsInfo.firstScnFromArchivedLogs(
					oraConnections.getLogMinerConnection(),
					!(useStandby ||  rdbmsInfo.isStandby()));
			long firstScn = firstAvailableScn;
			RedoByteAddress firstRsId = null;
			long firstSsn = -1;
			final boolean startScnFromProps = props.containsKey(ParamConstants.LGMNR_START_SCN_PARAM) &&
									config.getLong(ParamConstants.LGMNR_START_SCN_PARAM) > 0;
			// Initial load
			if (StringUtils.equalsIgnoreCase(
					ParamConstants.INITIAL_LOAD_EXECUTE,
					config.getString(ParamConstants.INITIAL_LOAD_PARAM))) {
				execInitialLoad = true;
				initialLoadStatus = ParamConstants.INITIAL_LOAD_EXECUTE;
			}
			Map<String, Object> offsetFromKafka = context.offsetStorageReader().offset(rdbmsInfo.partition());

			// New resiliency model
			// Begin - initial load analysis...
			if (execInitialLoad) {
				// Need to check value from offset
				if (offsetFromKafka != null &&
						StringUtils.equalsIgnoreCase(
								ParamConstants.INITIAL_LOAD_COMPLETED,
								(String) offsetFromKafka.get("I"))) {
					execInitialLoad = false;
					initialLoadStatus = ParamConstants.INITIAL_LOAD_COMPLETED;
					offset.put("I", ParamConstants.INITIAL_LOAD_COMPLETED);
					LOGGER.info("Initial load set to {} (value from offset)", ParamConstants.INITIAL_LOAD_COMPLETED);
				}
			}
			// End - initial load analysis...
			if (offsetFromKafka != null && offsetFromKafka.containsKey("C:COMMIT_SCN")) {
				if (startScnFromProps) {
					// a2.first.change set in connector properties, ignore stored offsets values
					// for restart...
					firstScn = Long.parseLong(props.get(ParamConstants.LGMNR_START_SCN_PARAM));
					LOGGER.info("{}={} is set in connector properties, ignoring SCN related restart data from connector offset storage.",
							ParamConstants.LGMNR_START_SCN_PARAM, firstScn);
					if (firstScn < firstAvailableScn) {
						LOGGER.warn(
								"Ignoring {}={} in connector properties, and setting {} to first available SCN in V$ARCHIVED_LOG {}.",
								ParamConstants.LGMNR_START_SCN_PARAM, firstScn, ParamConstants.LGMNR_START_SCN_PARAM, firstAvailableScn);
						firstScn = firstAvailableScn;
					} else {
						// We need to rewind, potentially
						rewind = true;
					}
				} else {
					// Use stored offset values for SCN and related from storage offset
					firstScn = (long) offsetFromKafka.get("S:SCN");
					firstRsId = RedoByteAddress.fromLogmnrContentsRs_Id((String) offsetFromKafka.get("S:RS_ID"));
					firstSsn = (long) offsetFromKafka.get("S:SSN");
					LOGGER.info("Point in time from offset data to start reading reading from SCN={}, RS_ID (RBA)='{}', SSN={}",
							firstScn, firstRsId, firstSsn);
					lastProcessedCommitScn = (long) offsetFromKafka.get("C:COMMIT_SCN");
					lastInProgressCommitScn = (long) offsetFromKafka.get("COMMIT_SCN");
					if (lastProcessedCommitScn == lastInProgressCommitScn) {
						// Rewind not required, reset back lastInProgressCommitScn
						lastInProgressCommitScn = 0;
					} else {
						lastInProgressScn = (long) offsetFromKafka.get("SCN");
						lastInProgressRsId = RedoByteAddress.fromLogmnrContentsRs_Id((String) offsetFromKafka.get("RS_ID"));
						lastInProgressSsn = (long) offsetFromKafka.get("SSN");
						LOGGER.info("Last sent SCN={}, RBA={}, SSN={} for  transaction with incomplete send",
								lastInProgressScn, lastInProgressRsId, lastInProgressSsn);
					}
					if (firstScn < firstAvailableScn) {
						LOGGER.warn(
								"\n" +
								"=====================\n" +
								"Ignoring Point in time {}:{}:{} from offset, and setting it to first available SCN in V$ARCHIVED_LOG {}.\n" +
								"=====================",
								firstScn, firstRsId, firstSsn, firstAvailableScn);
						firstScn = firstAvailableScn;
					} else {
						rewind = true;
					}
				}
			} else {
				LOGGER.info("No data present in connector's offset storage for {}:{}",
						rdbmsInfo.sourcePartitionName(), rdbmsInfo.getDbId());
				if (startScnFromProps) {
					// a2.first.change set in connector properties, restart data are not present
					firstScn = Long.parseLong(props.get(ParamConstants.LGMNR_START_SCN_PARAM));
					LOGGER.info("{}={} is set in connector properties, previous offset data is not available.",
							ParamConstants.LGMNR_START_SCN_PARAM, firstScn);
					if (firstScn < firstAvailableScn) {
						LOGGER.warn(
								"Ignoring {}={} in connector properties, and setting {} to first available SCN in V$ARCHIVED_LOG {}.",
								ParamConstants.LGMNR_START_SCN_PARAM, firstScn, ParamConstants.LGMNR_START_SCN_PARAM, firstAvailableScn);
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

			String checkTableSql = null;
			String mineDataSql = null;
			String initialLoadSql = null;
			if (rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed()) {
				mineDataSql = pseudoColumns.isAuditNeeded() ?
						OraDictSqlTexts.MINE_DATA_CDB_AUD :
						OraDictSqlTexts.MINE_DATA_CDB;
				checkTableSql = OraDictSqlTexts.CHECK_TABLE_CDB + OraDictSqlTexts.CHECK_TABLE_CDB_WHERE_PARAM;
				if (execInitialLoad) {
					initialLoadSql = OraDictSqlTexts.INITIAL_LOAD_LIST_CDB;
				}
			} else {
				mineDataSql = pseudoColumns.isAuditNeeded() ? 
						OraDictSqlTexts.MINE_DATA_NON_CDB_AUD :
						OraDictSqlTexts.MINE_DATA_NON_CDB;
				checkTableSql = OraDictSqlTexts.CHECK_TABLE_NON_CDB + OraDictSqlTexts.CHECK_TABLE_NON_CDB_WHERE_PARAM;
				if (execInitialLoad) {
					initialLoadSql = OraDictSqlTexts.INITIAL_LOAD_LIST_NON_CDB;
				}
			}
			if (includeList != null) {
				final String tableList = OraSqlUtils.parseTableSchemaList(
						false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, includeList);
				if (tableListGenerationStatic) {
					// static build list of tables/partitions
					final String objectList = rdbmsInfo.getMineObjectsIds(
						connDictionary, false, tableList);
					if (StringUtils.contains(objectList, "()")) {
						// and DATA_OBJ# in ()
						LOGGER.error("a2.include parameter set to {} but there are no tables matching this condition.\nExiting.",
								StringUtils.join(config.includeObj(), ","));
						throw new ConnectException("Please check value of a2.include parameter or remove it from configuration!");
					}
					/*
					 1 - INSERT
					 2 - DELETE
					 3 - UPDATE
					 5 - DDL
					 9 - SELECT_LOB_LOCATOR
					68 - XML DOC BEGIN
					70 - XML DOC WRITE
					*/
					if (processLobs) {
						mineDataSql += "where ((OPERATION_CODE in (1,2,3,5,9,68,70) " +  objectList + ")";
					} else {
						mineDataSql += "where ((OPERATION_CODE in (1,2,3,5) " +  objectList + ")";
					}
				}
				checkTableSql += tableList;
				if (execInitialLoad) {
					initialLoadSql += tableList;
				}
			}
			if (excludeList != null) {
				if (tableListGenerationStatic) {
					// for static list
					/*
					 1 - INSERT
					 2 - DELETE
					 3 - UPDATE
					 5 - DDL
					 9 - SELECT_LOB_LOCATOR
					68 - XML DOC BEGIN
					70 - XML DOC WRITE
					*/
					if (includeList != null) {
						if (processLobs) {
							mineDataSql += " and (OPERATION_CODE in (1,2,3,5,9,68,70) ";
						} else {
							mineDataSql += " and (OPERATION_CODE in (1,2,3,5) ";
						}
					} else {
						if (processLobs) {
							mineDataSql += " where ((OPERATION_CODE in (1,2,3,5,9,68,70) ";
						} else {
							mineDataSql += " where ((OPERATION_CODE in (1,2,3,5) ";
						}
					}
					final String objectList = rdbmsInfo.getMineObjectsIds(connDictionary, true,
							OraSqlUtils.parseTableSchemaList(false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, excludeList));
					if (StringUtils.contains(objectList, "()")) {
						// and DATA_OBJ# not in ()
						LOGGER.error("a2.exclude parameter set to {} but there are no tables matching this condition.\nExiting.",
								StringUtils.join(config.excludeObj(), ","));
						throw new ConnectException("Please check value of a2.exclude parameter or remove it from configuration!");
					}
					mineDataSql += objectList + ")";
				}
				final String tableList = OraSqlUtils.parseTableSchemaList(true, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, excludeList);
				checkTableSql += tableList;
				if (execInitialLoad) {
					initialLoadSql += tableList;
				}
			}
			if (tableListGenerationStatic) {
				// for static list only!!!
				if (includeList == null && excludeList == null) {
					/*
					 1 - INSERT
					 2 - DELETE
					 3 - UPDATE
					 5 - DDL
					 9 - SELECT_LOB_LOCATOR
					68 - XML DOC BEGIN
					70 - XML DOC WRITE
					*/
					if (processLobs) {
						mineDataSql += "where (OPERATION_CODE in (1,2,3,5,9,68,70) ";
					} else {
						mineDataSql += "where (OPERATION_CODE in (1,2,3,5) ";
					}
				}
				// Finally - COMMIT and ROLLBACK
				if ((includeList != null && excludeList != null) || excludeList != null)  {
					/*
					 7 - COMMIT 
					36 - ROLLBACK
					*/
					if (processLobs) {
						mineDataSql += " or OPERATION_CODE in (7,36) or (OPERATION_CODE=0 and DATA_OBJ#=DATA_OBJD# and DATA_OBJ#!=0)";
					} else {
						mineDataSql += " or OPERATION_CODE in (7,36)";
					}
				} else {
					if (processLobs) {
						mineDataSql += " or OPERATION_CODE in (7,36)) or (OPERATION_CODE=0 and DATA_OBJ#=DATA_OBJD# and DATA_OBJ#!=0)";
					} else {
						mineDataSql += " or OPERATION_CODE in (7,36))";
					}
				}
			} else {
				// for dynamic list
				/*
				 1 - INSERT
				 2 - DELETE
				 3 - UPDATE
				 5 - DDL
				 7 - COMMIT
				36 - ROLLBACK
				 9 - SELECT_LOB_LOCATOR
				68 - XML DOC BEGIN
				70 - XML DOC WRITE
				*/
				if (processLobs) {
					mineDataSql += "where OPERATION_CODE in (1,2,3,5,7,36,9,68,70) or (OPERATION_CODE=0 and DATA_OBJ#=DATA_OBJD# and DATA_OBJ#!=0)";
				} else {
					mineDataSql += "where OPERATION_CODE in (1,2,3,5,7,36) ";
				}
			}
			if (rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed()) {
				// Do not process objects from CDB$ROOT and PDB$SEED
				mineDataSql += rdbmsInfo.getConUidsList(oraConnections.getLogMinerConnection());
			}
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Mining SQL = {}", mineDataSql);
				LOGGER.debug("Dictionary check SQL = {}", checkTableSql);
			}
			worker = new OraCdcLogMinerWorkerThread(
					this,
					firstScn,
					mineDataSql,
					checkTableSql,
					tablesInProcessing,
					tablesOutOfScope,
					topicPartition,
					queuesRoot,
					activeTransactions,
					committedTransactions,
					metrics,
					config,
					rdbmsInfo,
					oraConnections);
			if (rewind) {
				worker.rewind(firstScn, firstRsId, firstSsn);
			}

			if (execInitialLoad) {
				LOGGER.debug("Initial load table list SQL {}", initialLoadSql);
				tablesQueue = new LinkedBlockingQueue<>();
				buildInitialLoadTableList(initialLoadSql);
				initialLoadMetrics = new OraCdcInitialLoad(rdbmsInfo, connectorName);
				initialLoadWorker = new OraCdcInitialLoadThread(
						WAIT_FOR_WORKER_MILLIS,
						firstScn,
						tablesInProcessing,
						queuesRoot,
						rdbmsInfo,
						initialLoadMetrics,
						tablesQueue,
						oraConnections);
			}


		} catch (SQLException | InvalidPathException e) {
			LOGGER.error("Unable to start oracdc logminer task!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
		LOGGER.trace("Starting worker thread.");
		if (execInitialLoad) {
			initialLoadWorker.start();
		}
		worker.start();
		runLatch = new CountDownLatch(1);
		isPollRunning = new AtomicBoolean(false);
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		LOGGER.trace("BEGIN: poll()");
		if (runLatch.getCount() < 1) {
			LOGGER.trace("Returning from poll() -> processing stopped");
			isPollRunning.set(false);
			return null;
		}
		isPollRunning.set(true);
		List<SourceRecord> result = new ArrayList<>();
		if (execInitialLoad) {
			// Execute initial load...
			if (!initialLoadWorker.isRunning() && tablesQueue.isEmpty() && table4InitialLoad == null) {
				Thread.sleep(WAIT_FOR_WORKER_MILLIS);
				if (tablesQueue.isEmpty()) {
					LOGGER.info("Initial load completed");
					execInitialLoad = false;
					initialLoadStatus = ParamConstants.INITIAL_LOAD_COMPLETED;
					offset.put("I", ParamConstants.INITIAL_LOAD_COMPLETED);
					return null;
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
		} else {
			// Load data from archived redo...
			try (Connection connDictionary = oraConnections.getConnection()) {
				final OraCdcLogMinerStatement stmt = new OraCdcLogMinerStatement();
				final List<OraCdcLargeObjectHolder> lobs = new ArrayList<>();
				int recordCount = 0;
				int parseTime = 0;
				while (recordCount < batchSize) {
					if (lastStatementInTransaction) {
						// End of transaction, need to poll new
						transaction = committedTransactions.poll();
					}
					if (transaction == null) {
						// No more records produced by LogMiner worker
						break;
					} else {
						lastStatementInTransaction = false;
						boolean processTransaction = true;
						if (transaction.getCommitScn() <= lastProcessedCommitScn) {
							LOGGER.warn(
										"Transaction '{}' with COMMIT_SCN {} is skipped because transaction with {} COMMIT_SCN {} was already sent to Kafka broker",
										transaction.getXid(),
										transaction.getCommitScn(),
										transaction.getCommitScn() == lastProcessedCommitScn ? "same" : "greater",
										lastProcessedCommitScn);
							// Force poll new transaction
							lastStatementInTransaction = true;
							transaction.close();
							transaction = null;
							continue;
						} else if (transaction.getCommitScn() == lastInProgressCommitScn) {
							while (true) {
								processTransaction = transaction.getStatement(stmt);
								if (processLobs && processTransaction && stmt.getLobCount() > 0) {
									lobs.clear();
									((OraCdcTransactionChronicleQueue) transaction).getLobs(stmt.getLobCount(), lobs);
								}
								lastStatementInTransaction = !processTransaction;
								if (stmt.getScn() == lastInProgressScn &&
										lastInProgressRsId.equals(stmt.getRba()) &&
										stmt.getSsn() == lastInProgressSsn) {
									// Rewind completed
									break;
								}
								if (!processTransaction) {
									LOGGER.error("Unable to rewind transaction {} with COMMIT_SCN={} till requested {}:'{}':{}!",
											transaction.getXid(), transaction.getCommitScn(), lastInProgressScn, lastInProgressRsId, lastInProgressSsn);
									throw new ConnectException("Data corruption while restarting oracdc task!");
								}
							}
						}
						// Prepare records...
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("Start of processing transaction XID {}, first change {}, commit SCN {}.",
								transaction.getXid(), transaction.getFirstChange(), transaction.getCommitScn());
						}
						do {
							processTransaction = transaction.getStatement(stmt);
							if (processLobs && processTransaction && stmt.getLobCount() > 0) {
								lobs.clear();
								((OraCdcTransactionChronicleQueue) transaction).getLobs(stmt.getLobCount(), lobs);
							}
							lastStatementInTransaction = !processTransaction;

							if (processTransaction) {
								final OraTable4LogMiner oraTable = tablesInProcessing.get(stmt.getTableId());
								if (oraTable == null) {
									LOGGER.error("Strange consistency issue for DATA_OBJ# {}, transaction XID {}, statement SCN={}, RS_ID='{}', SSN={}.\n Exiting.",
											stmt.getTableId(), transaction.getXid(), stmt.getScn(), stmt.getRba(), stmt.getSsn());
									isPollRunning.set(false);
									throw new ConnectException("Strange consistency issue!!!");
								} else {
									try {
										if (stmt.getOperation() == OraCdcV$LogmnrContents.DDL) {
											final long ddlStartTs = System.currentTimeMillis();
											final int changedColumnCount = 
													oraTable.processDdl(stmt, transaction.getXid(), transaction.getCommitScn());
											putTableAndVersion(stmt.getTableId(), oraTable.getVersion());
											metrics.addDdlMetrics(changedColumnCount, (System.currentTimeMillis() - ddlStartTs));
										} else {
											final long startParseTs = System.currentTimeMillis();
											offset.put("SCN", stmt.getScn());
											offset.put("RS_ID", stmt.getRba().toString());
											offset.put("SSN", stmt.getSsn());
											offset.put("COMMIT_SCN", transaction.getCommitScn());
											final SourceRecord record = oraTable.parseRedoRecord(
													stmt, lobs,
													transaction,
													offset,
													connDictionary);
											if (record != null) {
												result.add(record);
												recordCount++;
											}
											parseTime += (System.currentTimeMillis() - startParseTs);
										}
									} catch (SQLException e) {
										LOGGER.error(e.getMessage());
										LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
										isPollRunning.set(false);
										throw new ConnectException(e);
									}
								}
							}
						} while (processTransaction && recordCount < batchSize);
						if (lastStatementInTransaction) {
							// close Cronicle queue only when all statements are processed
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug("End of processing transaction XID {}, first change {}, commit SCN {}.",
									transaction.getXid(), transaction.getFirstChange(), transaction.getCommitScn());
							}
							// Store last successfully processed COMMIT_SCN to offset
							offset.put("C:COMMIT_SCN", transaction.getCommitScn());
							transaction.close();
							transaction = null;
						}
					}
				}
				if (recordCount == 0) {
					synchronized (this) {
						LOGGER.debug("Waiting {} ms", pollInterval);
						Thread.sleep(pollInterval);
					}
				} else {
					metrics.addSentRecords(result.size(), parseTime);
				}
			} catch (SQLException sqle) {
				if (!isPollRunning.get() || runLatch.getCount() == 0) {
					LOGGER.warn("Caught SQLException {} while stopping oracdc task.",
							sqle.getMessage());
				} else {
					LOGGER.error(sqle.getMessage());
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
					isPollRunning.set(false);
					throw new ConnectException(sqle);
				}
			}
		}
		isPollRunning.set(false);
		LOGGER.trace("END: poll()");
		return result;
	}

	@Override
	public void stop() {
		stop(true);
	}

	public void stop(boolean stopWorker) {
		LOGGER.info("Stopping oracdc logminer source task.");
		if (runLatch != null ) {
			// We can stop before runLatch initialization due to invalid parameters
			runLatch.countDown();
			if (stopWorker) {
				worker.shutdown();
				while (worker.isRunning()) {
					try {
						LOGGER.debug("Waiting {} ms for worker thread to stop...", WAIT_FOR_WORKER_MILLIS);
						Thread.sleep(WAIT_FOR_WORKER_MILLIS);
					} catch (InterruptedException e) {
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
					}
				}
			} else {
				while (isPollRunning.get()) {
					try {
						LOGGER.debug("Waiting {} ms for connector task to stop...", WAIT_FOR_WORKER_MILLIS);
						Thread.sleep(WAIT_FOR_WORKER_MILLIS);
					} catch (InterruptedException e) {
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
					}
				}
			}
		}
		if (initialLoadWorker != null && initialLoadWorker.isRunning()) {
			initialLoadWorker.shutdown();
		}
		if (activeTransactions != null && activeTransactions.isEmpty()) {
			if (worker != null && worker.getLastRsId() != null && worker.getLastScn() > 0) {
				putReadRestartScn(Triple.of(
						worker.getLastScn(),
						worker.getLastRsId(),
						worker.getLastSsn()));
			}
		}
		if (activeTransactions != null && !activeTransactions.isEmpty()) {
			// Clean it!
			activeTransactions.forEach((name, transaction) -> {
				LOGGER.warn("Removing uncompleted transaction {}", name);
				transaction.close();
			});
		}
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

	/**
	 * 
	 * @param saveFinalState     when set to true performs full save, when set to false only
	 *                           in-progress transactions are saved
	 * @throws IOException
	 */
	public void saveState(boolean saveFinalState) throws IOException {
		final long saveStarted = System.currentTimeMillis();
		final String fileName = saveFinalState ?
				stateFileName : (stateFileName + "-jmx-" + System.currentTimeMillis());
		LOGGER.info("Saving oracdc state to {} file...", fileName);
		OraCdcPersistentState ops = new OraCdcPersistentState();
		ops.setDbId(rdbmsInfo.getDbId());
		ops.setInstanceName(rdbmsInfo.getInstanceName());
		ops.setHostName(rdbmsInfo.getHostName());
		ops.setLastOpTsMillis(System.currentTimeMillis());
		ops.setLastScn(worker.getLastScn());
		ops.setLastRsId(worker.getLastRsId());
		ops.setLastSsn(worker.getLastSsn());
		ops.setInitialLoad(initialLoadStatus);
		if (saveFinalState && useChronicleQueue) {
			if (transaction != null) {
				ops.setCurrentTransaction(((OraCdcTransactionChronicleQueue) transaction).attrsAsMap());
				LOGGER.debug("Added to state file transaction {}", transaction.toString());
			}
			if (!committedTransactions.isEmpty()) {
				final List<Map<String, Object>> committed = new ArrayList<>();
				committedTransactions.stream().forEach(trans -> {
					committed.add(((OraCdcTransactionChronicleQueue) trans).attrsAsMap());
					LOGGER.debug("Added to state file committed transaction {}", trans.toString());
				});
				ops.setCommittedTransactions(committed);
			}
		}
		if (!activeTransactions.isEmpty() && useChronicleQueue) {
			final List<Map<String, Object>> wip = new ArrayList<>();
			activeTransactions.forEach((xid, trans) -> {
				wip.add(((OraCdcTransactionChronicleQueue) trans).attrsAsMap());
				LOGGER.debug("Added to state file in progress transaction {}", trans.toString());
			});
			ops.setInProgressTransactions(wip);
		}
		if (!tablesInProcessing.isEmpty()) {
			final List<String> wipTables = new ArrayList<>();
			tablesInProcessing.forEach((combinedId, table) -> {
				wipTables.add(
						combinedId + 
						OraCdcPersistentState.TABLE_VERSION_SEPARATOR +
						table.getVersion());
				if (LOGGER.isDebugEnabled()) {
					final int tableId = (int) ((long) combinedId);
					final int conId = (int) (combinedId >> 32);
					LOGGER.debug("Added to state file in process table OBJECT_ID {} from CON_ID {}", tableId, conId);
				}
			});
			ops.setProcessedTablesIdsWithVersion(wipTables);
		}
		if (!tablesOutOfScope.isEmpty()) {
			final List<Long> oosTables = new ArrayList<>();
			tablesOutOfScope.forEach(combinedId -> {
				oosTables.add(combinedId);
				metrics.addTableOutOfScope();
				if (LOGGER.isDebugEnabled()) {
					final int tableId = (int) ((long) combinedId);
					final int conId = (int) (combinedId >> 32);
					LOGGER.debug("Added to state file in out of scope table OBJECT_ID {} from CON_ID {}", tableId, conId);
				}
			});
			ops.setOutOfScopeTablesIds(oosTables);
		}
		try {
			ops.toFile(fileName);
		} catch (Exception e) {
			LOGGER.error("Unable to save state file with contents:\n{}", ops.toString());
			throw new IOException(e);
		}
		LOGGER.info("oracdc state saved to {} file, elapsed {} ms",
				fileName, (System.currentTimeMillis() - saveStarted));
		LOGGER.debug("State file contents:\n{}", ops.toString());
	}

	public void saveTablesSchema() throws IOException {
		String schemaFileName = null;
		try {
			schemaFileName = stateFileName.substring(0, stateFileName.lastIndexOf(File.separator));
		} catch (Exception e) {
			LOGGER.error("Unable to detect parent directory for {} using {} separator.",
					stateFileName, File.separator);
			schemaFileName = System.getProperty("java.io.tmpdir");
		}
		schemaFileName += File.separator + "oracdc.schemas-" + System.currentTimeMillis();

		FileUtils.writeDictionaryFile(tablesInProcessing, schemaFileName);
	}

	private void buildInitialLoadTableList(final String initialLoadSql) throws SQLException {
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
						&& !StringUtils.startsWith(tableName, "MLOG$_")) {
					OraTable4LogMiner oraTable = new OraTable4LogMiner(
							isCdb ? resultSet.getString("PDB_NAME") : null,
							isCdb ? (short) conId : -1,
							resultSet.getString("OWNER"), tableName,
							StringUtils.equalsIgnoreCase("ENABLED", resultSet.getString("DEPENDENCIES")),
							config, topicPartition, 
							rdbmsInfo, connection);
					tablesInProcessing.put(combinedDataObjectId, oraTable);
				}
			}
		} catch (SQLException sqle) {
			throw new SQLException(sqle);
		}
	}

	protected void putReadRestartScn(final Triple<Long, RedoByteAddress, Long> transData) {
		offset.put("S:SCN", transData.getLeft());
		offset.put("S:RS_ID", transData.getMiddle().toString());
		offset.put("S:SSN", transData.getRight());
	}

	protected void putTableAndVersion(final long combinedDataObjectId, final int version) {
		offset.put(Long.toString(combinedDataObjectId), Integer.toString(version));
	}

}