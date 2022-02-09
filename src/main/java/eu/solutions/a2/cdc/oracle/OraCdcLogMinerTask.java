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

package eu.solutions.a2.cdc.oracle;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import eu.solutions.a2.cdc.oracle.jmx.OraCdcInitialLoad;
import eu.solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmt;
import eu.solutions.a2.cdc.oracle.schema.FileUtils;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.OraSqlUtils;
import eu.solutions.a2.cdc.oracle.utils.Version;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcLogMinerTask extends SourceTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerTask.class);
	private static final int WAIT_FOR_WORKER_MILLIS = 50;

	private int batchSize;
	private int pollInterval;
	private Map<String, String> partition;
	private int schemaType;
	private String topic;
	private int topicNameStyle;
	private String topicNameDelimiter;
	private String stateFileName;
	private OraRdbmsInfo rdbmsInfo;
	private OraCdcLogMinerMgmt metrics;
	private OraDumpDecoder odd;
	private Map<Long, OraTable4LogMiner> tablesInProcessing;
	private Set<Long> tablesOutOfScope;
	private Map<String, OraCdcTransaction> activeTransactions;
	private BlockingQueue<OraCdcTransaction> committedTransactions;
	private OraCdcLogMinerWorkerThread worker;
	private OraCdcTransaction transaction;
	private boolean lastStatementInTransaction = true;
	private boolean needToStoreState = false;
	private boolean useOracdcSchemas = false;
	private boolean processLobs = false;
	private CountDownLatch runLatch;
	private AtomicBoolean isPollRunning;
	private boolean execInitialLoad = false;
	private String initialLoadStatus = ParamConstants.INITIAL_LOAD_IGNORE;
	private OraCdcInitialLoadThread initialLoadWorker;
	private BlockingQueue<OraTable4InitialLoad> tablesQueue;
	private OraTable4InitialLoad table4InitialLoad;
	private boolean lastRecordInTable = true;
	private OraCdcInitialLoad initialLoadMetrics;
	private OraCdcLobTransformationsIntf transformLobs;
	private String connectorName; 
	private OraConnectionObjects oraConnections;
	private Map<String, Object> offset;
	private boolean legacyResiliencyModel;
	private long lastProcessedCommitScn = 0;
	private long lastInProgressCommitScn = 0;
	private long lastInProgressScn = 0;
	private String lastInProgressRsId = null;
	private long lastInProgressSsn = 0;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		connectorName = props.get("name");
		LOGGER.info("Starting oracdc logminer source task for connector {}.", connectorName);

		try {
			if (StringUtils.isNotBlank(props.get(ParamConstants.CONNECTION_URL_PARAM))) {
				oraConnections = OraConnectionObjects.get4UserPassword(
					connectorName,
					props.get(ParamConstants.CONNECTION_URL_PARAM),
					props.get(ParamConstants.CONNECTION_USER_PARAM),
					props.get(ParamConstants.CONNECTION_PASSWORD_PARAM));
			}
			if (StringUtils.isNotBlank(props.get(ParamConstants.CONNECTION_WALLET_PARAM))) {
				oraConnections = OraConnectionObjects.get4OraWallet(
						connectorName,
						props.get(ParamConstants.CONNECTION_WALLET_PARAM),
						props.get(ParamConstants.CONNECTION_TNS_ADMIN_PARAM),
						props.get(ParamConstants.CONNECTION_TNS_ALIAS_PARAM));
			}
				
		} catch(SQLException sqle) {
			LOGGER.error("Unable to connect to RDBMS for connector '{}'!",
					connectorName);
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			LOGGER.error("Stopping connector '{}'", connectorName);
			throw new ConnectException("Unable to connect to RDBMS");
		}

		batchSize = Integer.parseInt(props.get(ParamConstants.BATCH_SIZE_PARAM));
		pollInterval = Integer.parseInt(props.get(ParamConstants.POLL_INTERVAL_MS_PARAM));
		useOracdcSchemas = Boolean.parseBoolean(props.get(ParamConstants.ORACDC_SCHEMAS_PARAM));
		if (useOracdcSchemas) {
			LOGGER.info("oracdc will use own schemas for Oracle NUMBER and TIMESTAMP WITH [LOCAL] TIMEZONE datatypes");
		}

		if (StringUtils.equalsIgnoreCase(
				props.get(ParamConstants.SCHEMA_TYPE_PARAM),
				ParamConstants.SCHEMA_TYPE_DEBEZIUM)) {
			schemaType = ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM;
			topic = props.get(OraCdcSourceConnectorConfig.KAFKA_TOPIC_PARAM);
		} else {
			schemaType = ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD;
			topic = props.get(OraCdcSourceConnectorConfig.TOPIC_PREFIX_PARAM);
			switch (props.get(ParamConstants.TOPIC_NAME_STYLE_PARAM)) {
			case ParamConstants.TOPIC_NAME_STYLE_TABLE:
				topicNameStyle = ParamConstants.TOPIC_NAME_STYLE_INT_TABLE;
				break;
			case ParamConstants.TOPIC_NAME_STYLE_SCHEMA_TABLE:
				topicNameStyle = ParamConstants.TOPIC_NAME_STYLE_INT_SCHEMA_TABLE;
				break;
			case ParamConstants.TOPIC_NAME_STYLE_PDB_SCHEMA_TABLE:
				topicNameStyle = ParamConstants.TOPIC_NAME_STYLE_INT_PDB_SCHEMA_TABLE;
				break;
			}
			topicNameDelimiter = props.get(ParamConstants.TOPIC_NAME_DELIMITER_PARAM);
		}
		processLobs = Boolean.parseBoolean(props.get(ParamConstants.PROCESS_LOBS_PARAM));
		if (processLobs) {
			final String transformLobsImplClass = props.get(ParamConstants.LOB_TRANSFORM_CLASS_PARAM);
			LOGGER.info("oracdc will process Oracle LOBs using {} LOB transformations implementation",
					transformLobsImplClass);
			try {
				final Class<?> classTransformLobs = Class.forName(transformLobsImplClass);
				final Constructor<?> constructor = classTransformLobs.getConstructor();
				transformLobs = (OraCdcLobTransformationsIntf) constructor.newInstance();
			} catch (ClassNotFoundException nfe) {
				LOGGER.error("ClassNotFoundException while instantiating {}", transformLobsImplClass);
				throw new ConnectException("ClassNotFoundException while instantiating " + transformLobsImplClass, nfe);
			} catch (NoSuchMethodException nme) {
				LOGGER.error("NoSuchMethodException while instantiating {}", transformLobsImplClass);
				throw new ConnectException("NoSuchMethodException while instantiating " + transformLobsImplClass, nme);
			} catch (SecurityException se) {
				LOGGER.error("SecurityException while instantiating {}", transformLobsImplClass);
				throw new ConnectException("SecurityException while instantiating " + transformLobsImplClass, se);
			} catch (InvocationTargetException ite) {
				LOGGER.error("InvocationTargetException while instantiating {}", transformLobsImplClass);
				throw new ConnectException("InvocationTargetException while instantiating " + transformLobsImplClass, ite);
			} catch (IllegalAccessException iae) {
				LOGGER.error("IllegalAccessException while instantiating {}", transformLobsImplClass);
				throw new ConnectException("IllegalAccessException while instantiating " + transformLobsImplClass, iae);
			} catch (InstantiationException ie) {
				LOGGER.error("InstantiationException while instantiating {}", transformLobsImplClass);
				throw new ConnectException("InstantiationException while instantiating " + transformLobsImplClass, ie);
			}
		}
		if (StringUtils.equalsIgnoreCase(
				props.get(ParamConstants.RESILIENCY_TYPE_PARAM),
				ParamConstants.RESILIENCY_TYPE_LEGACY)) {
			legacyResiliencyModel = true;
			offset = new HashMap<>();
		} else {
			legacyResiliencyModel = false;
			offset = new ConcurrentHashMap<>();
		}
		try (Connection connDictionary = oraConnections.getConnection()) {
			rdbmsInfo = new OraRdbmsInfo(connDictionary);

			LOGGER.info("Connector {} connected to {}, {}\n\t$ORACLE_SID={}, running on {}, OS {}.",
					connectorName,
					rdbmsInfo.getRdbmsEdition(), rdbmsInfo.getVersionString(),
					rdbmsInfo.getInstanceName(), rdbmsInfo.getHostName(), rdbmsInfo.getPlatformName());

			if (rdbmsInfo.isCdb() && !rdbmsInfo.isCdbRoot() && !rdbmsInfo.isPdbConnectionAllowed()) {
				LOGGER.error(
						"Connector {} must be connected to CDB$ROOT while using oracdc for mining data using LogMiner!",
						connectorName);
				throw new ConnectException("Unable to run oracdc without connection to CDB$ROOT!");
			} else {
				LOGGER.trace("Oracle connection information:\n{}", rdbmsInfo.toString());
			}
			if (rdbmsInfo.isCdb() && rdbmsInfo.isPdbConnectionAllowed()) {
				LOGGER.info("Connected to PDB {} (RDBMS 19.10+ Feature)", rdbmsInfo.getPdbName());
			}

			if (Boolean.parseBoolean(props.get(ParamConstants.MAKE_STANDBY_ACTIVE_PARAM))) {
				oraConnections.addStandbyConnection(
						props.get(ParamConstants.STANDBY_WALLET_PARAM),
						props.get(ParamConstants.STANDBY_TNS_ADMIN_PARAM),
						props.get(ParamConstants.STANDBY_TNS_ALIAS_PARAM));
				LOGGER.info(
						"Connector {} will use connection to PHYSICAL STANDBY for LogMiner calls",
						connectorName);
			}
			if (Boolean.parseBoolean(props.get(ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM))) {
				oraConnections.addStandbyConnection(
						props.get(ParamConstants.DISTRIBUTED_WALLET_PARAM),
						props.get(ParamConstants.DISTRIBUTED_TNS_ADMIN_PARAM),
						props.get(ParamConstants.DISTRIBUTED_TNS_ALIAS_PARAM));
				LOGGER.info(
						"Connector {} will use remote database in distributed configuration for LogMiner calls",
						connectorName);
			}

			odd = new OraDumpDecoder(rdbmsInfo.getDbCharset(), rdbmsInfo.getDbNCharCharset());
			metrics = new OraCdcLogMinerMgmt(rdbmsInfo, props.get("name"), this);

			final String sourcePartitionName = rdbmsInfo.getInstanceName() + "_" + rdbmsInfo.getHostName();
			LOGGER.debug("Source Partition {} set to {}.", sourcePartitionName, rdbmsInfo.getDbId());
			partition = Collections.singletonMap(sourcePartitionName, ((Long)rdbmsInfo.getDbId()).toString());

			List<String> excludeList = null;
			List<String> includeList = null;
			if (props.containsKey(ParamConstants.TABLE_EXCLUDE_PARAM)) {
				excludeList =
						Arrays.asList(props.get(ParamConstants.TABLE_EXCLUDE_PARAM).split("\\s*,\\s*"));
			}
			if (props.containsKey(ParamConstants.TABLE_INCLUDE_PARAM)) {
				includeList =
						Arrays.asList(props.get(ParamConstants.TABLE_INCLUDE_PARAM).split("\\s*,\\s*"));
			}
			final boolean tableListGenerationStatic;
			if (ParamConstants.TABLE_LIST_STYLE_STATIC.equalsIgnoreCase(props.get(ParamConstants.TABLE_LIST_STYLE_PARAM))) {
				// ParamConstants.TABLE_LIST_STYLE_STATIC
				tableListGenerationStatic = true;
			} else {
				// TABLE_LIST_STYLE_DYNAMIC
				tableListGenerationStatic = false;
			}

			final Path queuesRoot = FileSystems.getDefault().getPath(
					props.get(ParamConstants.TEMP_DIR_PARAM));

			if (useOracdcSchemas) {
				// Use stored schema only in this mode
				final String schemaFileName = props.get(ParamConstants.DICTIONARY_FILE_PARAM);
				if (!StringUtils.isEmpty(schemaFileName)) {
					try {
						LOGGER.info("Loading stored schema definitions from file {}.", schemaFileName);
						tablesInProcessing = FileUtils.readDictionaryFile(schemaFileName, schemaType, transformLobs, rdbmsInfo);
						LOGGER.info("{} table schema definitions loaded from file {}.",
								tablesInProcessing.size(), schemaFileName);
						tablesInProcessing.forEach((key, table) -> {
							table.setTopicDecoderPartition(
									topic, topicNameStyle, topicNameDelimiter, odd, partition);
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
			final long firstAvailableScn = OraRdbmsInfo.firstScnFromArchivedLogs(oraConnections.getLogMinerConnection());
			long firstScn = firstAvailableScn;
			String firstRsId = null;
			long firstSsn = -1;
			final boolean startScnFromProps = props.containsKey(ParamConstants.LGMNR_START_SCN_PARAM) &&
									Long.parseLong(props.get(ParamConstants.LGMNR_START_SCN_PARAM)) > 0;
			// Initial load
			if (StringUtils.equalsIgnoreCase(
					ParamConstants.INITIAL_LOAD_EXECUTE,
					props.get(ParamConstants.INITIAL_LOAD_PARAM))) {
				execInitialLoad = true;
				initialLoadStatus = ParamConstants.INITIAL_LOAD_EXECUTE;
			}
			Map<String, Object> offsetFromKafka = context.offsetStorageReader().offset(partition);

			if (legacyResiliencyModel) {
				// Legacy code to restore state from state file
				stateFileName = props.get(ParamConstants.PERSISTENT_STATE_FILE_PARAM);
				final Path stateFilePath = Paths.get(stateFileName);
				if (stateFilePath.toFile().exists()) {
					// File with stored state exists
					final long restoreStarted = System.currentTimeMillis();
					OraCdcPersistentState persistentState = OraCdcPersistentState.fromFile(stateFileName);
					LOGGER.info("Will start processing using stored persistent state file {} dated {}.",
							stateFileName,
							LocalDateTime.ofInstant(
									Instant.ofEpochMilli(persistentState.getLastOpTsMillis()), ZoneId.systemDefault()
								).format(DateTimeFormatter.ISO_DATE_TIME));
					if (rdbmsInfo.getDbId() != persistentState.getDbId()) {
						LOGGER.error("DBID from stored state file {} and from connection {} are different!",
								persistentState.getDbId(), rdbmsInfo.getDbId());
						LOGGER.error("Exiting.");
						throw new ConnectException("Unable to use stored file for database with different DBID!!!");
					}
					LOGGER.debug(persistentState.toString());
					// Begin - initial load analysis...
					if (execInitialLoad) {
						// Need to check state file value
						final String initialLoadFromStateFile = persistentState.getInitialLoad();
						if (StringUtils.equalsIgnoreCase(ParamConstants.INITIAL_LOAD_COMPLETED, initialLoadFromStateFile)) {
							execInitialLoad = false;
							initialLoadStatus = ParamConstants.INITIAL_LOAD_COMPLETED;
							LOGGER.info("Initial load set to {} (value from state file)", ParamConstants.INITIAL_LOAD_COMPLETED);
						}
					}
					// End - initial load analysis...

					if (startScnFromProps) {
						// a2.first.change set in parameters, ignore stored state, rename file
						firstScn = Long.parseLong(props.get(ParamConstants.LGMNR_START_SCN_PARAM));
						if (firstScn < firstAvailableScn) {
							LOGGER.warn(
									"Ignoring {}={} in connector properties, and setting {} to first available SCN in V$ARCHIVED_LOG {}.",
									ParamConstants.LGMNR_START_SCN_PARAM, firstScn, ParamConstants.LGMNR_START_SCN_PARAM, firstAvailableScn);
							firstScn = firstAvailableScn;
						} else {
							LOGGER.info("Ignoring last processed SCN value from stored state file {} and setting it to {} from connector properties",
								stateFileName, firstScn);
						}
					} else {
						firstScn = persistentState.getLastScn();
						if (firstScn < firstAvailableScn) {
							LOGGER.warn(
									"Ignoring {}={} in oracdc state file '{}', and setting {} to first available SCN in V$ARCHIVED_LOG {}.",
									ParamConstants.LGMNR_START_SCN_PARAM, firstScn, stateFileName, ParamConstants.LGMNR_START_SCN_PARAM, firstAvailableScn);
							firstScn = firstAvailableScn;
						} else {
							firstRsId = persistentState.getLastRsId();
							firstSsn = persistentState.getLastSsn();

							if (offsetFromKafka != null && offsetFromKafka.size() > 0) {
								LOGGER.info("Last read SCN={}, RS_ID (RBA)='{}', SSN={}",
										firstScn, firstRsId, firstSsn);
								LOGGER.info("Last sent SCN={}, RS_ID (RBA)='{}', SSN={}",
										offsetFromKafka.get("SCN"), offsetFromKafka.get("RS_ID"), offsetFromKafka.get("SSN"));
							}
							
							if (persistentState.getCurrentTransaction() != null) {
								transaction = OraCdcTransaction.restoreFromMap(persistentState.getCurrentTransaction());
								// To prevent committedTransactions.poll() in this.poll()
								lastStatementInTransaction = false;
								LOGGER.debug("Restored current transaction {}", transaction.toString());
							}
							if (persistentState.getCommittedTransactions() != null) {
								for (int i = 0; i < persistentState.getCommittedTransactions().size(); i++) {
									final OraCdcTransaction oct = OraCdcTransaction.restoreFromMap(
											persistentState.getCommittedTransactions().get(i));
									committedTransactions.add(oct);
									LOGGER.debug("Restored committed transaction {}", oct.toString());
								}
							}
							if (persistentState.getInProgressTransactions() != null) {
								for (int i = 0; i < persistentState.getInProgressTransactions().size(); i++) {
									final OraCdcTransaction oct = OraCdcTransaction.restoreFromMap(
											persistentState.getInProgressTransactions().get(i));
									activeTransactions.put(oct.getXid(), oct);
									LOGGER.debug("Restored in progress transaction {}", oct.toString());
								}
							}
							// Restore table's, its versions and other related information
							restoreTableInfoFromDictionary(persistentState);
							LOGGER.info("Restore persistent state {} ms", (System.currentTimeMillis() - restoreStarted));
							rewind = true;
						}
					}
					final String savedStateFile = stateFileName + "." + System.currentTimeMillis(); 
					Files.copy(stateFilePath, Paths.get(savedStateFile), StandardCopyOption.REPLACE_EXISTING);
					LOGGER.info("Stored state file {} copied to {}", stateFileName, savedStateFile);
				} else {
					// Check Kafka offset
					if (offsetFromKafka != null && offsetFromKafka.size() > 0) {
						firstScn = (long) offsetFromKafka.get("SCN");
						if (firstScn < firstAvailableScn) {
							LOGGER.warn(
									"Ignoring {}={} in connect.offsets, and setting {} to first available SCN in V$ARCHIVED_LOG {}.",
									ParamConstants.LGMNR_START_SCN_PARAM, firstScn, ParamConstants.LGMNR_START_SCN_PARAM, firstAvailableScn);
							firstScn = firstAvailableScn;
						} else {
							firstRsId = (String) offsetFromKafka.get("RS_ID");
							firstSsn = (long) offsetFromKafka.get("SSN");
							rewind = true;
							LOGGER.warn("Persistent state file {} not found!", stateFileName);
							LOGGER.warn("oracdc will use offset from Kafka cluster: SCN={}, RS_ID(RBA)='{}', SSN={}",
									firstScn, firstRsId, firstSsn);
						}
					} else if (startScnFromProps) {
						firstScn = Long.parseLong(props.get(ParamConstants.LGMNR_START_SCN_PARAM));
						if (firstScn < firstAvailableScn) {
							LOGGER.warn(
									"Ignoring {}={} in connector properties, and setting {} to first available SCN in V$ARCHIVED_LOG {}.",
									ParamConstants.LGMNR_START_SCN_PARAM, firstScn, ParamConstants.LGMNR_START_SCN_PARAM, firstAvailableScn);
							firstScn = firstAvailableScn;
						} else {
							LOGGER.info("Using first SCN value {} from connector properties.", firstScn);
						}
					} else {
						firstScn = firstAvailableScn;
						LOGGER.info("Using min(FIRST_CHANGE#) from V$ARCHIVED_LOG = {} as first SCN value.", firstScn);
					}
				}
			} else {
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
						firstRsId = (String) offsetFromKafka.get("S:RS_ID");
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
							lastInProgressRsId = (String) offsetFromKafka.get("RS_ID");
							lastInProgressSsn = (long) offsetFromKafka.get("SSN");
							LOGGER.info("Last sent SCN={}, RS_ID (RBA)='{}', SSN={} for  transaction with incomplete send",
									lastInProgressScn, lastInProgressRsId, lastInProgressSsn);
						}
						if (firstScn < firstAvailableScn) {
							LOGGER.warn(
									"Ignoring Point in time {}:{}:{} from offset, and setting it {} to first available SCN in V$ARCHIVED_LOG {}.",
									firstScn, firstRsId, firstSsn, firstAvailableScn);
							firstScn = firstAvailableScn;
						} else {
							rewind = true;
						}
						restoreTableInfoFromDictionary(offsetFromKafka);
					}
				} else {
					LOGGER.info("No data present in connector's offset storage for {}:{}",
							sourcePartitionName, rdbmsInfo.getDbId());
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
			}

			String checkTableSql = null;
			String mineDataSql = null;
			String initialLoadSql = null;
			if (rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed()) {
				mineDataSql = OraDictSqlTexts.MINE_DATA_CDB;
				checkTableSql = OraDictSqlTexts.CHECK_TABLE_CDB + OraDictSqlTexts.CHECK_TABLE_CDB_WHERE_PARAM;
				if (execInitialLoad) {
					initialLoadSql = OraDictSqlTexts.INITIAL_LOAD_LIST_CDB;
				}
			} else {
				mineDataSql = OraDictSqlTexts.MINE_DATA_NON_CDB;
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
						LOGGER.error("{} parameter set to {} but there are no tables matching this condition.\nExiting.",
							ParamConstants.TABLE_INCLUDE_PARAM, props.get(ParamConstants.TABLE_INCLUDE_PARAM));
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
						LOGGER.error("{} parameter set to {} but there are no tables matching this condition.\nExiting.",
								ParamConstants.TABLE_EXCLUDE_PARAM, props.get(ParamConstants.TABLE_EXCLUDE_PARAM));
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
					partition,
					firstScn,
					mineDataSql,
					checkTableSql,
					tablesInProcessing,
					tablesOutOfScope,
					schemaType,
					topic,
					odd,
					queuesRoot,
					activeTransactions,
					committedTransactions,
					metrics,
					topicNameStyle,
					props,
					transformLobs,
					rdbmsInfo,
					oraConnections);
			if (rewind) {
				worker.rewind(firstScn, firstRsId, firstSsn);
			}

			if (execInitialLoad) {
				LOGGER.debug("Initial load table list SQL {}", initialLoadSql);
				tablesQueue = new LinkedBlockingQueue<>();
				buildInitialLoadTableList(initialLoadSql);
				initialLoadMetrics = new OraCdcInitialLoad(rdbmsInfo, props.get("name"));
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


		} catch (SQLException | InvalidPathException | IOException e) {
			LOGGER.error("Unable to start oracdc logminer task!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
		LOGGER.trace("Starting worker thread.");
		if (execInitialLoad) {
			initialLoadWorker.start();
		}
		worker.start();
		needToStoreState = true;
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
					if (!legacyResiliencyModel) {
						offset.put("I", ParamConstants.INITIAL_LOAD_COMPLETED);
					}
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
						if (!legacyResiliencyModel) {
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
										transaction.getLobs(stmt.getLobCount(), lobs);
									}
									lastStatementInTransaction = !processTransaction;
									if (stmt.getScn() == lastInProgressScn &&
											StringUtils.equals(stmt.getRsId(), lastInProgressRsId) &&
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
								transaction.getLobs(stmt.getLobCount(), lobs);
							}
							lastStatementInTransaction = !processTransaction;

							if (processTransaction) {
								final OraTable4LogMiner oraTable = tablesInProcessing.get(stmt.getTableId());
								if (oraTable == null) {
									LOGGER.error("Strange consistency issue for DATA_OBJ# {}, transaction XID {}, statement SCN={}, RS_ID='{}', SSN={}.\n Exiting.",
											stmt.getTableId(), transaction.getXid(), stmt.getScn(), stmt.getRsId(), stmt.getSsn());
									isPollRunning.set(false);
									throw new ConnectException("Strange consistency issue!!!");
								} else {
									try {
										if (stmt.getOperation() == OraCdcV$LogmnrContents.DDL) {
											final long ddlStartTs = System.currentTimeMillis();
											final int changedColumnCount = 
													oraTable.processDdl(useOracdcSchemas, stmt, transaction.getXid(), transaction.getCommitScn());
											if (!legacyResiliencyModel) {
												putTableAndVersion(stmt.getTableId(), oraTable.getVersion());
											}
											metrics.addDdlMetrics(changedColumnCount, (System.currentTimeMillis() - ddlStartTs));
										} else {
											final long startParseTs = System.currentTimeMillis();
											offset.put("SCN", stmt.getScn());
											offset.put("RS_ID", stmt.getRsId());
											offset.put("SSN", stmt.getSsn());
											if (!legacyResiliencyModel) {
												offset.put("COMMIT_SCN", transaction.getCommitScn());
											}
											final SourceRecord record = oraTable.parseRedoRecord(
													stmt, lobs,
													transaction.getXid(),
													transaction.getCommitScn(),
													offset,
													connDictionary);
											result.add(record);
											recordCount++;
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
							if (!legacyResiliencyModel) {
								// Store last successfully processed COMMIT_SCN to offset
								offset.put("C:COMMIT_SCN", transaction.getCommitScn());
							}
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
			if (legacyResiliencyModel && needToStoreState) {
				// We need state file only when legacyResilencyModel == true
				try {
					saveState(true);
				} catch(IOException ioe) {
					LOGGER.error("Unable to save state to file " + stateFileName + "!");
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
					throw new ConnectException("Unable to save state to file " + stateFileName + "!");
				}
			} else if (legacyResiliencyModel && !needToStoreState) {
				LOGGER.info("Do not need to run store state procedures.");
				LOGGER.info("Check Connect log files for errors.");
			}
		}
		if (!legacyResiliencyModel && activeTransactions.isEmpty()) {
			putReadRestartScn(Triple.of(
					worker.getLastScn(),
					worker.getLastRsId(),
					worker.getLastSsn()));
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
		if (saveFinalState) {
			if (transaction != null) {
				ops.setCurrentTransaction(transaction.attrsAsMap());
				LOGGER.debug("Added to state file transaction {}", transaction.toString());
			}
			if (!committedTransactions.isEmpty()) {
				final List<Map<String, Object>> committed = new ArrayList<>();
				committedTransactions.stream().forEach(trans -> {
					committed.add(trans.attrsAsMap());
					LOGGER.debug("Added to state file committed transaction {}", trans.toString());
				});
				ops.setCommittedTransactions(committed);
			}
		}
		if (!activeTransactions.isEmpty()) {
			final List<Map<String, Object>> wip = new ArrayList<>();
			activeTransactions.forEach((xid, trans) -> {
				wip.add(trans.attrsAsMap());
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

	private void restoreTableInfoFromDictionary(final OraCdcPersistentState ops) throws SQLException {
//		final List<Long> processedTablesIds = ops.getProcessedTablesIds();
		final boolean useVersion;
		final int tableCount;
		if (ops.getProcessedTablesIdsWithVersion() != null) {
			useVersion = true;
			tableCount = ops.getProcessedTablesIdsWithVersion().size();
		} else if (ops.getProcessedTablesIds() != null) {
			useVersion = false;
			tableCount = ops.getProcessedTablesIds().size();
		} else {
			return;
		}
		final Connection connection = oraConnections.getConnection();
		final PreparedStatement psCheckTable;
		final boolean isCdb = rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed();
		if (isCdb) {
			psCheckTable = connection.prepareStatement(
					OraDictSqlTexts.CHECK_TABLE_CDB + OraDictSqlTexts.CHECK_TABLE_CDB_WHERE_PARAM,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		} else {
			psCheckTable = connection.prepareStatement(
					OraDictSqlTexts.CHECK_TABLE_NON_CDB + OraDictSqlTexts.CHECK_TABLE_NON_CDB_WHERE_PARAM,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		}
		for (int i = 0; i < tableCount; i++) {
			final long combinedDataObjectId;
			final int version;
			if (useVersion) {
				final String[] versionWithId = StringUtils.split(
						ops.getProcessedTablesIdsWithVersion().get(i),
						OraCdcPersistentState.TABLE_VERSION_SEPARATOR);
				combinedDataObjectId = Long.parseLong(versionWithId[0]);
				version = Integer.parseInt(versionWithId[1]);
			} else {
				combinedDataObjectId = ops.getProcessedTablesIds().get(i);
				version = 1;
			}
			if (!tablesInProcessing.containsKey(combinedDataObjectId)) {
				final int tableId = (int) combinedDataObjectId;
				final int conId = (int) (combinedDataObjectId >> 32);
				psCheckTable.setInt(1, tableId);
				if (isCdb) {
					psCheckTable.setInt(2, conId);
				}
				LOGGER.debug("Adding from database dictionary for internal id {}: OBJECT_ID = {}, CON_ID = {}",
						combinedDataObjectId, tableId, conId);
				final ResultSet rsCheckTable = psCheckTable.executeQuery();
				if (rsCheckTable.next()) {
					final String tableName = rsCheckTable.getString("TABLE_NAME");
					final String tableOwner = rsCheckTable.getString("OWNER");
					OraTable4LogMiner oraTable = new OraTable4LogMiner(
							isCdb ? rsCheckTable.getString("PDB_NAME") : null,
							isCdb ? (short) conId : -1,
							tableOwner, tableName,
							StringUtils.equalsIgnoreCase("ENABLED", rsCheckTable.getString("DEPENDENCIES")),
							schemaType, useOracdcSchemas,
							processLobs, transformLobs,
							isCdb, odd, partition, topic, topicNameStyle, topicNameDelimiter,
							rdbmsInfo, connection);
					oraTable.setVersion(version);
					tablesInProcessing.put(combinedDataObjectId, oraTable);
					metrics.addTableInProcessing(oraTable.fqn());
					LOGGER.debug("Restored metadata for table {}, OBJECT_ID={}, CON_ID={}",
							oraTable.fqn(), tableId, conId);
				} else {
					throw new SQLException("Data corruption detected!\n" +
							"OBJECT_ID=" + tableId + ", CON_ID=" + conId + 
							" exist in stored state but not in database!!!");
				}
				rsCheckTable.close();
				psCheckTable.clearParameters();
			}
		}
		psCheckTable.close();
		connection.close();

		if (ops.getOutOfScopeTablesIds() != null) {
			ops.getOutOfScopeTablesIds().forEach(combinedId -> {
				tablesOutOfScope.add(combinedId);
				if (LOGGER.isDebugEnabled()) {
					final int tableId = (int) ((long) combinedId);
					final int conId = (int) (combinedId >> 32);
					LOGGER.debug("Restored out of scope table OBJECT_ID {} from CON_ID {}", tableId, conId);
				}
			});
		}
	}

	private void restoreTableInfoFromDictionary(final Map<String, Object> offsetFromKafka) throws SQLException {
		final Connection connection = oraConnections.getConnection();
		final PreparedStatement psCheckTable;
		final boolean isCdb = rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed();
		if (isCdb) {
			psCheckTable = connection.prepareStatement(
					OraDictSqlTexts.CHECK_TABLE_CDB + OraDictSqlTexts.CHECK_TABLE_CDB_WHERE_PARAM,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		} else {
			psCheckTable = connection.prepareStatement(
					OraDictSqlTexts.CHECK_TABLE_NON_CDB + OraDictSqlTexts.CHECK_TABLE_NON_CDB_WHERE_PARAM,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		}
		for (String id : offsetFromKafka.keySet()) {
			if (StringUtils.isNumeric(id)) {
				final long combinedDataObjectId = Long.parseLong(id);
				final int version = Integer.parseInt((String) offsetFromKafka.get(id));
				if (!tablesInProcessing.containsKey(combinedDataObjectId)) {
					final int tableId = (int) combinedDataObjectId;
					final int conId = (int) (combinedDataObjectId >> 32);
					psCheckTable.setInt(1, tableId);
					if (isCdb) {
						psCheckTable.setInt(2, conId);
					}
					LOGGER.debug("Adding from database dictionary for internal id {}: OBJECT_ID = {}, CON_ID = {}",
							combinedDataObjectId, tableId, conId);
					final ResultSet rsCheckTable = psCheckTable.executeQuery();
					if (rsCheckTable.next()) {
						final String tableName = rsCheckTable.getString("TABLE_NAME");
						final String tableOwner = rsCheckTable.getString("OWNER");
						OraTable4LogMiner oraTable = new OraTable4LogMiner(
								isCdb ? rsCheckTable.getString("PDB_NAME") : null,
								isCdb ? (short) conId : -1,
								tableOwner, tableName,
								StringUtils.equalsIgnoreCase("ENABLED", rsCheckTable.getString("DEPENDENCIES")),
								schemaType, useOracdcSchemas,
								processLobs, transformLobs,
								isCdb, odd, partition, topic, topicNameStyle, topicNameDelimiter,
								rdbmsInfo, connection);
						oraTable.setVersion(version);
						tablesInProcessing.put(combinedDataObjectId, oraTable);
						metrics.addTableInProcessing(oraTable.fqn());
						LOGGER.debug("Restored metadata for table {}, OBJECT_ID={}, CON_ID={}",
								oraTable.fqn(), tableId, conId);
					} else {
						throw new SQLException("Data corruption detected!\n" +
								"OBJECT_ID=" + tableId + ", CON_ID=" + conId + 
								" exist in offset but not in database!!!");
					}
					rsCheckTable.close();
					psCheckTable.clearParameters();
				}
			}
		}
		psCheckTable.close();
		connection.close();	
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
							schemaType, useOracdcSchemas,
							processLobs, transformLobs,
							isCdb, odd, partition, topic, topicNameStyle, topicNameDelimiter,
							rdbmsInfo, connection);
					tablesInProcessing.put(combinedDataObjectId, oraTable);
				}
			}
		} catch (SQLException sqle) {
			throw new SQLException(sqle);
		}
	}

	protected boolean isLegacyResiliencyModel() {
		return legacyResiliencyModel;
	}

	protected void putReadRestartScn(final Triple<Long, String, Long> transData) {
		offset.put("S:SCN", transData.getLeft());
		offset.put("S:RS_ID", transData.getMiddle());
		offset.put("S:SSN", transData.getRight());
	}

	protected void putTableAndVersion(final long combinedDataObjectId, final int version) {
		offset.put(Long.toString(combinedDataObjectId), Integer.toString(version));
	}

}