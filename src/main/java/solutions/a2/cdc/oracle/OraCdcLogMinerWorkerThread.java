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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleResultSet;
import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmt;
import solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmtIntf;
import solutions.a2.cdc.oracle.utils.Lz4Util;
import solutions.a2.cdc.oracle.utils.OraSqlUtils;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcLogMinerWorkerThread extends Thread {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerWorkerThread.class);
	private static final int ORA_17410 = 17410;
	private static final int ORA_2396 = 2396;
	private static final int ORA_17008 = 17008;
	private static final int ORA_17002 = 17002;
	private static final int MAX_RETRIES = 63;

	private final OraCdcLogMinerTask task;
	private final int pollInterval;
	private final OraRdbmsInfo rdbmsInfo;
	private final OraCdcLogMinerMgmt metrics;
	private final CountDownLatch runLatch;
	private boolean logMinerReady = false;
	private final Map<String, String> partition;
	private final Map<Long, OraTable4LogMiner> tablesInProcessing;
	private final Map<Long, Long> partitionsInProcessing;
	private final Set<Long> tablesOutOfScope;
	private final OraDumpDecoder odd;
	private final OraLogMiner logMiner;
	private Connection connLogMiner;
	private OraclePreparedStatement psLogMiner;
	private PreparedStatement psCheckTable;
	private PreparedStatement psCheckLob;
	private PreparedStatement psIsDataObjLob;
	private OraclePreparedStatement psReadLob;
	private OracleResultSet rsLogMiner;
	private final String mineDataSql;
	private final String checkTableSql;
	private Connection connDictionary;
	private final Path queuesRoot;
	private final Map<String, OraCdcTransaction> activeTransactions;
	private final boolean legacyResiliencyModel;
	private final TreeMap<String, Triple<Long, String, Long>> sortedByFirstScn;
	private final ActiveTransComparator activeTransComparator;
	private final BlockingQueue<OraCdcTransaction> committedTransactions;
	private final OraCdcSourceConnectorConfig config;
	private final boolean useChronicleQueue;
	private long lastScn;
	private String lastRsId;
	private long lastSsn;
	private final AtomicBoolean running;
	private boolean isCdb;
	private final boolean processLobs;
	private final OraCdcLobTransformationsIntf transformLobs;
	private OraCdcLargeObjectWorker lobWorker;
	private final int connectionRetryBackoff;
	private final int fetchSize;
	private final boolean traceSession;
	private final OraConnectionObjects oraConnections;
	private final int topicPartition;

	private boolean fetchRsLogMinerNext;
	private boolean isRsLogMinerRowAvailable;

	private final Set<Long> lobObjects;
	private final Set<Long> nonLobObjects;
	private String lastRealRowId;
	private final long logMinerReconnectIntervalMs;

	public OraCdcLogMinerWorkerThread(
			final OraCdcLogMinerTask task,
			final Map<String, String> partition,
			final long firstScn,
			final String mineDataSql,
			final String checkTableSql,
			final Map<Long, OraTable4LogMiner> tablesInProcessing,
			final Set<Long> tablesOutOfScope,
			final int topicPartition,
			final OraDumpDecoder odd,
			final Path queuesRoot,
			final Map<String, OraCdcTransaction> activeTransactions,
			final BlockingQueue<OraCdcTransaction> committedTransactions,
			final OraCdcLogMinerMgmt metrics,
			final OraCdcSourceConnectorConfig config,
			final OraCdcLobTransformationsIntf transformLobs,
			final OraRdbmsInfo rdbmsInfo,
			final OraConnectionObjects oraConnections) throws SQLException {
		LOGGER.info("Initializing oracdc logminer archivelog worker thread");
		this.setName("OraCdcLogMinerWorkerThread-" + System.nanoTime());
		this.task = task;
		this.config = config;
		this.partition = partition;
		this.mineDataSql = mineDataSql;
		this.checkTableSql = checkTableSql;
		this.tablesInProcessing = tablesInProcessing;
		// We do not need concurrency for this map
		this.partitionsInProcessing = new HashMap<>();
		this.tablesOutOfScope = tablesOutOfScope;
		this.queuesRoot = queuesRoot;
		this.odd = odd;
		this.topicPartition = topicPartition;
		this.activeTransactions = activeTransactions;
		this.committedTransactions = committedTransactions;
		this.metrics = metrics;
		this.processLobs = config.getBoolean(ParamConstants.PROCESS_LOBS_PARAM);
		this.transformLobs = transformLobs;
		this.pollInterval = config.getInt(ParamConstants.POLL_INTERVAL_MS_PARAM);
		this.connectionRetryBackoff = config.getInt(ParamConstants.CONNECTION_BACKOFF_PARAM);
		this.fetchSize = config.getInt(ParamConstants.FETCH_SIZE_PARAM);
		this.traceSession = config.getBoolean(ParamConstants.TRACE_LOGMINER_PARAM);
		this.rdbmsInfo = rdbmsInfo;
		this.oraConnections = oraConnections;
		isCdb = rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed();
		legacyResiliencyModel = task.isLegacyResiliencyModel();
		if (legacyResiliencyModel) {
			activeTransComparator = null;
			sortedByFirstScn = null;
		} else {
			activeTransComparator = new ActiveTransComparator(activeTransactions);
			sortedByFirstScn = new TreeMap<>(activeTransComparator);
		}

		runLatch = new CountDownLatch(1);
		running = new AtomicBoolean(false);

		if (processLobs) {
			lobObjects = new HashSet<>();
			nonLobObjects = new HashSet<>();
		} else {
			lobObjects = null;
			nonLobObjects = null;
		}
		this.useChronicleQueue = StringUtils.equalsIgnoreCase(
				config.getString(ParamConstants.ORA_TRANSACTION_IMPL_PARAM),
				ParamConstants.ORA_TRANSACTION_IMPL_CHRONICLE);
		this.logMinerReconnectIntervalMs = config.getLong(ParamConstants.LM_RECONNECT_INTERVAL_MS_PARAM);

		try {
			connLogMiner = oraConnections.getLogMinerConnection(traceSession);
			connDictionary = oraConnections.getConnection();

			final int negotiatedSDU =  rdbmsInfo.getNegotiatedSDU(connLogMiner);
			if (negotiatedSDU > 0 && negotiatedSDU <= 8192) {
				LOGGER.warn(
						"\n" +
						"=====================\n" +
						"The negotiated SDU between connector and mining instance is set to {}.\n" +
						"\tWe recommend increasing it to achieve better performance.\n" + 
						"\tInstructions on how to do this can be found at\n" +
						"\t\t\thttps://github.com/averemee-si/oracdc#performance-tips\n" +
						"=====================",
						negotiatedSDU);
			}

			final String archivedLogCatalogImplClass = config.getString(ParamConstants.ARCHIVED_LOG_CAT_PARAM);
			try {
				final Class<?> classLogMiner = Class.forName(archivedLogCatalogImplClass);
				final Constructor<?> constructor = classLogMiner.getConstructor(
						Connection.class,
						OraCdcLogMinerMgmtIntf.class,
						long.class,
						OraCdcSourceConnectorConfig.class,
						CountDownLatch.class,
						OraRdbmsInfo.class,
						OraConnectionObjects.class);
				logMiner = (OraLogMiner) constructor.newInstance(
						connLogMiner, metrics, firstScn, config, runLatch, rdbmsInfo, oraConnections);
			} catch (ClassNotFoundException nfe) {
				LOGGER.error("ClassNotFoundException while instantiating {}", archivedLogCatalogImplClass);
				throw new ConnectException("ClassNotFoundException while instantiating " + archivedLogCatalogImplClass, nfe);
			} catch (NoSuchMethodException nme) {
				LOGGER.error(
						"NoSuchMethodException while obtaining " +
						"'{}.(java.sql.Connection, solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmtIntf, long, Integer, Long)'" +
						" constructor", archivedLogCatalogImplClass);
				throw new ConnectException("NoSuchMethodException while obtaining required constructor for " + archivedLogCatalogImplClass, nme);
			} catch (SecurityException se) {
				LOGGER.error(
						"SecurityException while obtaining " +
						"'{}.(java.sql.Connection, solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmtIntf, long, Integer, Long)'" +
						" constructor", archivedLogCatalogImplClass);
				throw new ConnectException("SecurityException while obtaining required constructor for " + archivedLogCatalogImplClass, se);
			} catch (InvocationTargetException ite) {
				LOGGER.error(
						"InvocationTargetException while calling " +
						"'{}.(java.sql.Connection, solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmtIntf, long, Integer, Long)'" +
						" constructor", archivedLogCatalogImplClass);
				throw new ConnectException("InvocationTargetException while calling required constructor for " + archivedLogCatalogImplClass, ite);
			} catch (IllegalAccessException iae) {
				LOGGER.error(
						"IllegalAccessException while calling " +
						"'{}.(java.sql.Connection, solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmtIntf, long, Integer, Long)'" +
						" constructor", archivedLogCatalogImplClass);
				throw new ConnectException("IllegalAccessException while calling required constructor for " + archivedLogCatalogImplClass, iae);
			} catch (InstantiationException ie) {
				LOGGER.error(
						"InstantiationException while calling " +
						"'{}.(java.sql.Connection, solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmtIntf, long, Integer, Long)'" +
						" constructor", archivedLogCatalogImplClass);
				throw new ConnectException("InstantiationException while calling required constructor for " + archivedLogCatalogImplClass, ie);
			} catch (IllegalArgumentException iae2) {
				LOGGER.error(
						"IllegalArgumentException while calling " +
						"'{}.(java.sql.Connection, solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmtIntf, long, Integer, Long)'" +
						" constructor", archivedLogCatalogImplClass);
				throw new ConnectException("IllegalArgumentException while calling required constructor for " + archivedLogCatalogImplClass, iae2);
			}

			if (logMiner.isOracleConnectionRequired()) {
				if (logMiner.getDbId() == rdbmsInfo.getDbId()) {
					LOGGER.debug("Database Id for dictionary and mining connections: {}", logMiner.getDbId());
					if (logMiner.isDictionaryAvailable()) {
						LOGGER.info("Mining database {} is in OPEN mode", logMiner.getDbUniqueName());
						if (logMiner.getDbUniqueName().equals(rdbmsInfo.getDbUniqueName())) {
							LOGGER.info("Same database will be used for dictionary query and mining");
						} else {
							LOGGER.info("Active DataGuard database {} will be used for mining", logMiner.getDbUniqueName());
						}
					} else {
						LOGGER.info("Mining database {} is in MOUNT mode", logMiner.getDbUniqueName());
						LOGGER.info("DataGuard database {} will be used for mining", logMiner.getDbUniqueName());
					}
				} else {
					LOGGER.info("Mining database {} has {} DBID.", logMiner.getDbUniqueName(), logMiner.getDbId());
					LOGGER.info("Source database {} has {} DBID.", rdbmsInfo.getDbUniqueName(), rdbmsInfo.getDbId());
				}
			}

			// Finally - prepare for mining...
			psLogMiner = (OraclePreparedStatement) connLogMiner.prepareStatement(
					mineDataSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			psLogMiner.setRowPrefetch(fetchSize);
			LOGGER.info("RowPrefetch size for accessing V$LOGMNR_CONTENTS set to {}.", fetchSize);
			
			initDictionaryStatements();
			logMinerReady = logMiner.next();
			if (processLobs) {
				psReadLob = (OraclePreparedStatement) connLogMiner.prepareStatement(
						isCdb ? OraDictSqlTexts.MINE_LOB_CDB :
								OraDictSqlTexts.MINE_LOB_NON_CDB,
						ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				psReadLob.setRowPrefetch(fetchSize);
			}

		} catch (SQLException e) {
			LOGGER.error(
					"\n\nUnable to start OraCdcLogMinerWorkerThread !\n" +
					"SQL Error ={}, SQL State = {}, SQL Message = '{}'\n\n",
					e.getErrorCode(), e.getSQLState(), e.getMessage());
			throw e;
		}
	}

	public void rewind(final long firstScn, final String firstRsId, final long firstSsn) throws SQLException {
		if (logMinerReady) {
			LOGGER.info("Rewinding LogMiner ResultSet to first position after SCN = {}, RS_ID = '{}', SSN = {}.",
					firstScn, firstRsId, firstSsn);
			rsLogMiner = (OracleResultSet) psLogMiner.executeQuery();
			int recordCount = 0;
			long rewindElapsed = System.currentTimeMillis();
			boolean rewindNeeded = true;
			lastScn = firstScn;
			lastRsId = firstRsId;
			lastSsn = firstSsn;
			while (rewindNeeded) {
				if (rsLogMiner.next()) {
					lastScn = rsLogMiner.getLong("SCN");
					lastRsId = rsLogMiner.getString("RS_ID");
					lastSsn = rsLogMiner.getLong("SSN");
					if (recordCount == 0 && lastScn > firstScn) {
						// Hit this with 10.2.0.5
						rewindNeeded = false;
						// Need to reopen cursor
						rsLogMiner.close();
						rsLogMiner = (OracleResultSet) psLogMiner.executeQuery();
						break;
					} else {
						recordCount++;
						if (firstScn == lastScn &&
							(firstRsId == null || StringUtils.equals(firstRsId, lastRsId)) &&
							(firstSsn == -1 || firstSsn == lastSsn) &&
							!rsLogMiner.getBoolean("CSF")) {
							rewindNeeded = false;
							break;
						}
					}
				} else {
					LOGGER.error("Incorrect rewind to SCN = {}, RS_ID = '{}', SSN = {}",
							firstScn, firstRsId, firstSsn);
					throw new SQLException("Incorrect rewind operation!!!");
				}
			}
			rewindElapsed = System.currentTimeMillis() - rewindElapsed;
			LOGGER.info("Total records scipped while rewinding: {}, elapsed time ms: {}", recordCount, rewindElapsed);
		} else {
			LOGGER.info("Values from offset (SCN = {}, RS_ID = '{}', SSN = {}) ignored, waiting for new archived log.",
					firstScn, firstRsId, firstSsn);
		}
	}

	@Override
	public void run()  {
		LOGGER.info("BEGIN: OraCdcLogMinerWorkerThread.run()");
		running.set(true);
		boolean firstTransaction = true;
		long logMinerSessionStartMs = System.currentTimeMillis();
		while (runLatch.getCount() > 0) {
			long lastGuaranteedScn = 0;
			String lastGuaranteedRsId = null;
			long lastGuaranteedSsn = 0;
			String xid = null;
			try {
				if (logMinerReady) {
					if (rsLogMiner == null) {
						rsLogMiner = (OracleResultSet) psLogMiner.executeQuery();
					}
					isRsLogMinerRowAvailable = rsLogMiner.next();
					while (isRsLogMinerRowAvailable && runLatch.getCount() > 0) {
						fetchRsLogMinerNext = true;
						final short operation = rsLogMiner.getShort("OPERATION_CODE");
						final boolean partialRollback = rsLogMiner.getInt("ROLLBACK") > 0;
						xid = rsLogMiner.getString("XID");
						lastScn = rsLogMiner.getLong("SCN");
						lastRsId = rsLogMiner.getString("RS_ID");
						lastSsn = rsLogMiner.getLong("SSN");
						OraCdcTransaction transaction = activeTransactions.get(xid);
						switch (operation) {
						case OraCdcV$LogmnrContents.COMMIT:
							if (transaction != null) {
								// SCN of commit
								transaction.setCommitScn(lastScn);
								committedTransactions.add(transaction);
								activeTransactions.remove(xid);
								if (!legacyResiliencyModel) {
									sortedByFirstScn.remove(xid);
									if (!sortedByFirstScn.isEmpty()) {
										task.putReadRestartScn(sortedByFirstScn.firstEntry().getValue());
									} else {
										firstTransaction = true;
									}
								}
								metrics.addCommittedRecords(transaction.length(), transaction.size(),
										committedTransactions.size(), activeTransactions.size());
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Performing commit at SCN {} for transaction XID {}", lastScn, xid);
								}
							} else {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Skipping commit at SCN {} for transaction XID {}", lastScn, xid);
								}
							}
							break;
						case OraCdcV$LogmnrContents.ROLLBACK:
							if (transaction != null) {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Rolling back at SCN transaction XID {} with {} records.",
											lastScn, xid, transaction.length());
								}
								transaction.close();
								activeTransactions.remove(xid);
								if (!legacyResiliencyModel) {
									sortedByFirstScn.remove(xid);
									if (!sortedByFirstScn.isEmpty()) {
										task.putReadRestartScn(sortedByFirstScn.firstEntry().getValue());
									} else {
										firstTransaction = true;
									}
								}
								metrics.addRolledBackRecords(transaction.length(), transaction.size(),
										activeTransactions.size() - 1);
							} else {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Skipping rollback at SCN {} for transaction XID {}", lastScn, xid);
								}
							}
							break;
						case OraCdcV$LogmnrContents.INSERT:
						case OraCdcV$LogmnrContents.DELETE:
						case OraCdcV$LogmnrContents.UPDATE:
						case OraCdcV$LogmnrContents.XML_DOC_BEGIN:
							// Read as long to speed up shift
							final long dataObjectId = rsLogMiner.getLong("DATA_OBJ#");
							long combinedDataObjectId;
							final long conId;
							if (isCdb) {
								conId = rsLogMiner.getInt("CON_ID");
								combinedDataObjectId = (conId << 32) | (dataObjectId & 0xFFFFFFFFL); 
							} else {
								conId = 0;
								combinedDataObjectId = dataObjectId;
							}
							// First check for table definition...
							OraTable4LogMiner oraTable = tablesInProcessing.get(combinedDataObjectId);
							if (oraTable == null && !tablesOutOfScope.contains(combinedDataObjectId)) {
								// Check for partitions
								Long combinedParentTableId = partitionsInProcessing.get(combinedDataObjectId);
								if (combinedParentTableId != null) {
									combinedDataObjectId = combinedParentTableId;
									oraTable = tablesInProcessing.get(combinedDataObjectId);
								} else {
									// Check for object...
									ResultSet rsCheckTable = null;
									boolean wait4CheckTableCursor = true;
									while (runLatch.getCount() > 0 && wait4CheckTableCursor) {
										try {
											psCheckTable.setLong(1, dataObjectId);
											if (isCdb) {
												psCheckTable.setLong(2, conId);
											}
											rsCheckTable = psCheckTable.executeQuery();
											wait4CheckTableCursor = false;
											break;
										} catch (SQLException sqle) {
											if (sqle.getErrorCode() == ORA_2396 ||
													sqle.getErrorCode() == ORA_17002 ||
													sqle.getErrorCode() == ORA_17008 ||
													sqle.getErrorCode() == ORA_17410 || 
													sqle instanceof SQLRecoverableException ||
													(sqle.getCause() != null && sqle.getCause() instanceof SQLRecoverableException)) {
												LOGGER.warn(
														"\n" +
														"=====================\n" +
														"Encontered an 'ORA-{}: {}'\n" +
														"Attempting to reconnect to dictionary...\n" +
														"=====================",
														sqle.getErrorCode(), sqle.getMessage());
												try {
													try {
														connDictionary.close();
														connDictionary = null;
													} catch(SQLException unimportant) {
														LOGGER.warn(
																"\n" +
																"=====================\n" +
																"Unable to close inactive dictionary connection after 'ORA-{}'\n" +
																"=====================",
																sqle.getErrorCode());
													}
													boolean ready = false;
													int retries = 0;
													while (runLatch.getCount() > 0 && !ready) {
														try {
															connDictionary = oraConnections.getConnection();
															initDictionaryStatements();
														} catch(SQLException sqleRestore) {
															if (retries > MAX_RETRIES) {
																LOGGER.error(
																		"\n" +
																		"=====================\n" +
																		"Unable to restore dictionary connection after {} retries!\n" +
																		"=====================",
																		retries);
																throw sqleRestore;
															}
														}
														ready = true;
														if (!ready) {
															long waitTime = (long) Math.pow(2, retries++) + connectionRetryBackoff;
															LOGGER.warn("Waiting {} ms for dictionary connection to restore...", waitTime);
															try {
																this.wait(waitTime);
															} catch (InterruptedException ie) {}
														}
													}
												} catch (SQLException ucpe) {
													LOGGER.error(
															"\n" +
															"=====================\n" +
															"SQL errorCode = {}, SQL state = '{}' while restarting connection to dictionary tables\n" +
															"SQL error message = {}\n" +
															"=====================",
															ucpe.getErrorCode(), ucpe.getSQLState(), ucpe.getMessage());
													throw new SQLException(sqle);
												}
											} else {
												LOGGER.error(
														"\n" +
														"=====================\n" +
														"SQL errorCode = {}, SQL state = '{}' while trying to SELECT from dictionary tables\n" +
														"SQL error message = {}\n" +
														"=====================",
														sqle.getErrorCode(), sqle.getSQLState(), sqle.getMessage());
												throw new SQLException(sqle);
											}
										}
									}
									if (rsCheckTable.next()) {
										//May be this is partition, so just check tablesInProcessing map for table
										boolean needNewTableDefinition = true;
										final boolean isPartition = StringUtils.equals("N", rsCheckTable.getString("IS_TABLE"));
										if (isPartition) {
											final long parentTableId = rsCheckTable.getLong("PARENT_OBJECT_ID");
											combinedParentTableId = isCdb ?
													((conId << 32) | (parentTableId & 0xFFFFFFFFL)) :
													parentTableId;
											oraTable = tablesInProcessing.get(combinedParentTableId);
											if (oraTable != null) {
												needNewTableDefinition = false;
												partitionsInProcessing.put(combinedDataObjectId, combinedParentTableId);
												metrics.addPartitionInProcessing();
												combinedDataObjectId = combinedParentTableId;
											}
										}
										//Get table definition from RDBMS
										if (needNewTableDefinition) {
											final String tableName = rsCheckTable.getString("TABLE_NAME");
											final String tableOwner = rsCheckTable.getString("OWNER");
											oraTable = new OraTable4LogMiner(
												isCdb ? rsCheckTable.getString("PDB_NAME") : null,
												isCdb ? (short) conId : -1,
												tableOwner, tableName,
												"ENABLED".equalsIgnoreCase(rsCheckTable.getString("DEPENDENCIES")),
												config, processLobs,
												transformLobs, isCdb, topicPartition,
												odd, partition, rdbmsInfo, connDictionary);
											if (!legacyResiliencyModel) {
												task.putTableAndVersion(combinedDataObjectId, 1);
											}

											if (isPartition) {
												partitionsInProcessing.put(combinedDataObjectId, combinedParentTableId);
												metrics.addPartitionInProcessing();
												combinedDataObjectId = combinedParentTableId;
											}
											tablesInProcessing.put(combinedDataObjectId, oraTable);
											metrics.addTableInProcessing(oraTable.fqn());
										}
									} else {
										tablesOutOfScope.add(combinedDataObjectId);
										metrics.addTableOutOfScope();
									}
									rsCheckTable.close();
									rsCheckTable = null;
									psCheckTable.clearParameters();
								}
							}

							if (oraTable != null) {
								String sqlRedo = readSqlRedo();
								if (oraTable.isCheckSupplementalLogData()) {
									final long timestamp = rsLogMiner.getDate("TIMESTAMP").getTime();
									final String rowId = rsLogMiner.getString("ROW_ID");
									// squeeze it!
									sqlRedo = StringUtils.replace(sqlRedo, "HEXTORAW(", "");
									if (operation == OraCdcV$LogmnrContents.INSERT) {
										sqlRedo = StringUtils.replace(sqlRedo, "')", "'");
									} else {
										sqlRedo = StringUtils.replace(sqlRedo, ")", "");
									}
									final OraCdcLogMinerStatement lmStmt = new  OraCdcLogMinerStatement(
											combinedDataObjectId, operation, sqlRedo, timestamp,
											lastScn, lastRsId, lastSsn, rowId, partialRollback);

									// Catch the LOBs!!!
									List<OraCdcLargeObjectHolder> lobs = 
											catchTheLob(operation, xid, dataObjectId, oraTable, sqlRedo);

									if (transaction == null) {
										if (LOGGER.isDebugEnabled()) {
											LOGGER.debug("New transaction {} created. Transaction start timestamp {}, first SCN {}.",
													xid, timestamp, lastScn);
										}
										if (useChronicleQueue) {
											transaction = new OraCdcTransactionChronicleQueue(processLobs, queuesRoot, xid);
										} else {
											transaction = new OraCdcTransactionArrayList(xid);
										}
										activeTransactions.put(xid, transaction);
										if (!legacyResiliencyModel) {
											sortedByFirstScn.put(xid,
													Triple.of(lastScn, lastRsId, lastSsn));
											if (firstTransaction) {
												firstTransaction = false;
												task.putReadRestartScn(sortedByFirstScn.firstEntry().getValue());
											}
										}
									}
									if (processLobs) {
										((OraCdcTransactionChronicleQueue) transaction).addStatement(lmStmt, lobs);
									} else {
										transaction.addStatement(lmStmt);
									}
									metrics.addRecord();
								} else {
									LOGGER.error(
											"\n" +
											"=====================\n" +
											"Supplemental logging for table '{}' is not configured correctly!\n" +
											"Please set it according to the oracdc documentation!\n" +
											"Redo record is skipped for OPERATION={}, SCN={}, RBA='{}', XID='{}',\n\tREDO_DATA='{}'\n" +
											"=====================\n",
											oraTable.fqn(), operation, lastScn, StringUtils.trim(lastRsId), xid, sqlRedo);
								}
							}
							break;
						case OraCdcV$LogmnrContents.DDL:
							final long combinedDdlDataObjectId;
							if (isCdb) {
								combinedDdlDataObjectId = (rsLogMiner.getInt("CON_ID") << 32) | (rsLogMiner.getLong("DATA_OBJ#") & 0xFFFFFFFFL); 
							} else {
								combinedDdlDataObjectId = rsLogMiner.getLong("DATA_OBJ#");
							}
							if (tablesInProcessing.containsKey(combinedDdlDataObjectId)) {
								// Handle DDL only for known table
								final long timestamp = rsLogMiner.getDate("TIMESTAMP").getTime();
								final String rowId = rsLogMiner.getString("ROW_ID");
								final String originalDdl = readSqlRedo();
								final String preProcessedDdl = OraSqlUtils.alterTablePreProcessor(originalDdl);
								if (preProcessedDdl != null) {
									final OraCdcLogMinerStatement lmStmt = new  OraCdcLogMinerStatement(
											combinedDdlDataObjectId, operation, 
											preProcessedDdl + "\n" + originalDdl,
											timestamp, lastScn, lastRsId, lastSsn, rowId, false);
									if (transaction == null) {
										if (LOGGER.isDebugEnabled()) {
											LOGGER.debug("New transaction {} created. Transaction start timestamp {}, first SCN {}.",
													xid, timestamp, lastScn);
										}
										if (useChronicleQueue) {
											transaction = new OraCdcTransactionChronicleQueue(processLobs, queuesRoot, xid);
										} else {
											transaction = new OraCdcTransactionArrayList(xid);
										}
										activeTransactions.put(xid, transaction);
										if (!legacyResiliencyModel) {
											sortedByFirstScn.put(xid,
													Triple.of(lastScn, lastRsId, lastSsn));
											if (firstTransaction) {
												firstTransaction = false;
												task.putReadRestartScn(sortedByFirstScn.firstEntry().getValue());
											}
										}
									}
									transaction.addStatement(lmStmt);
									metrics.addRecord();
								} else {
									LOGGER.warn("Unsupported DDL operation '{}' at SCN {} for object ID {}",
											originalDdl, lastScn, rsLogMiner.getLong("DATA_OBJ#"));
								}
							} else {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Skipping DDL operation '{}' at SCN {} for object ID {}",
											rsLogMiner.getString("SQL_REDO"), lastScn, rsLogMiner.getLong("DATA_OBJ#"));
								}
							}
							break;
						case OraCdcV$LogmnrContents.INTERNAL:
							if (processLobs) {
								// Only in this case...
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Skipping internal operation at SCN '{}', RS_ID '{}' for object ID '{}'",
											lastScn, rsLogMiner.getString("RS_ID"), rsLogMiner.getLong("DATA_OBJ#"));
								}
								final long internalOpObjectId = rsLogMiner.getLong("DATA_OBJ#");
								final int containerInternalOpObjectId;
								final long combinedInternalOpObjectId;
								if (isCdb) {
									containerInternalOpObjectId = rsLogMiner.getInt("CON_ID");
									combinedInternalOpObjectId = (containerInternalOpObjectId << 32) | (internalOpObjectId & 0xFFFFFFFFL); 
								} else {
									containerInternalOpObjectId = 0;
									combinedInternalOpObjectId = internalOpObjectId;
								}
								boolean readRowId = false;
								if (!lobObjects.contains(combinedInternalOpObjectId)) {
									if (nonLobObjects.contains(combinedInternalOpObjectId)) {
										readRowId = true;
									} else {
										if (isCdb) {
											psIsDataObjLob.setLong(1, internalOpObjectId);
											psIsDataObjLob.setInt(2, containerInternalOpObjectId);
										} else {
											psIsDataObjLob.setLong(1, internalOpObjectId);
										}
										ResultSet rsIsDataObjLob = psIsDataObjLob.executeQuery();
										if (rsIsDataObjLob.next()) {
											if (rsIsDataObjLob.getBoolean("IS_LOB")) {
												lobObjects.add(combinedInternalOpObjectId);
											} else {
												nonLobObjects.add(combinedInternalOpObjectId);
												readRowId = true;
											}
										} else {
											if (isCdb) {
												LOGGER.error("Data dictionary corruption for LOB with OBJECT_ID '{}', CON_ID = '{}'",
														internalOpObjectId, containerInternalOpObjectId);
											} else {
												LOGGER.error("Data dictionary corruption for LOB with OBJECT_ID '{}'",
														internalOpObjectId);
											}
											if (isRsLogMinerRowAvailable) {
												LOGGER.error("Last read row information: SCN={}, RS_ID='{}', SSN={}, XID='{}'",
														lastScn, lastRsId, lastSsn, xid);
											}
											LOGGER.error("Current query is:\n{}\n", mineDataSql);
										}
										rsIsDataObjLob.close();
										rsIsDataObjLob = null;
									}
								}
								if (readRowId) {
									lastRealRowId = rsLogMiner.getString("ROW_ID");
									if (LOGGER.isDebugEnabled()) {
										LOGGER.debug("ROWID for skipped operaion - '{}'", lastRealRowId);
									}
								}
							}
							break;
						case OraCdcV$LogmnrContents.SELECT_LOB_LOCATOR:
							// SELECT_LOB_LOCATOR is processed in inner loop before,
							// except for LOB_TRIM and LOB_ERASE
							LOGGER.warn("Unexpected SELECT_LOB_LOCATOR operation at SCN {}, RS_ID '{}' for object ID '{}', ROWID '{}', transaction XID '{}'",
									lastScn,
									rsLogMiner.getString("RS_ID"),
									rsLogMiner.getLong("DATA_OBJ#"),
									lastRealRowId == null ? rsLogMiner.getString("ROW_ID") : lastRealRowId,
									rsLogMiner.getString("XID"));
							LOGGER.warn("Possibly ignored LOB_ERASE or LOB_TRIM operation.");
							final String checkSql =
									"\tselect SCN, RS_ID,OPERATION_CODE, DATA_OBJ#, DATA_OBJD#\n" +
									"\tfrom   V$LOGMNR_CONTENTS\n" +
									"\twhere  XID = '" + rsLogMiner.getString("XID") + "' and SCN >= " + lastScn;
							LOGGER.warn("Please check using following SQL:\n{}", checkSql);
							break;
						default:
							// Just for diag purpose...
							LOGGER.error("Unknown operation {} at SCN {}, RS_ID '{}' for object ID '{}', transaction XID '{}'",
									operation, lastScn,
									rsLogMiner.getString("RS_ID"),
									rsLogMiner.getLong("DATA_OBJ#"),
									rsLogMiner.getString("XID"));
							LOGGER.error("Current query is:\n{}\n", mineDataSql);
							throw new SQLException("Unknown operation in OraCdcLogMinerWorkerThread.run()");
						}
						// Copy again, to protect from exception...
						lastGuaranteedScn = lastScn;
						lastGuaranteedRsId = lastRsId;
						lastGuaranteedSsn = lastSsn;
						if (fetchRsLogMinerNext) {
							//TODO
							//TODO Add try-catch block with exponential backoff here!!!
							//TODO hit SQL errorCode = 17002, SQL state = '08006'
							//TODO
							isRsLogMinerRowAvailable = rsLogMiner.next();
						}
					}
					logMiner.stop();
					rsLogMiner.close();
					rsLogMiner = null;
					if (!legacyResiliencyModel && activeTransactions.isEmpty() && lastGuaranteedScn > 0) {
						// Update restart point in time
						task.putReadRestartScn(Triple.of(lastGuaranteedScn, lastGuaranteedRsId, lastGuaranteedSsn));
					}
					if (runLatch.getCount() > 0) {
						try {
							logMinerReady = logMiner.next();
						} catch (SQLException sqle) {
							if (sqle instanceof SQLRecoverableException) {
								restoreOraConnection(sqle);
							} else {
								throw new SQLException(sqle);
							}
						}
					} else {
						LOGGER.debug("Preparing to end LogMiner loop...");
						logMinerReady = false;
						break;
					}
				} else {
					while (!logMinerReady && runLatch.getCount() > 0) {
						synchronized (this) {
							LOGGER.debug("Waiting {} ms", pollInterval);
							try {
								this.wait(pollInterval);
							} catch (InterruptedException ie) {
								LOGGER.error(ie.getMessage());
								LOGGER.error(ExceptionUtils.getExceptionStackTrace(ie));
							}
							try {
								boolean doReconnect = false;
								if (rdbmsInfo.isWindows()) {
									if (LOGGER.isDebugEnabled()) {
										LOGGER.debug("Recreating LogMiner connections to unlock archived log files on Windows.");
									}
									doReconnect = true;
								} else {
									long currentSysMs = System.currentTimeMillis();
									if ((currentSysMs - logMinerSessionStartMs) > logMinerReconnectIntervalMs) {
										if (LOGGER.isDebugEnabled()) {
											LOGGER.debug("Recreating LogMiner connections to prevent PGA growth.");
										}
										logMinerSessionStartMs = currentSysMs;
										doReconnect = true;
									}
								}
								if (doReconnect) {
									closeOraConnection();
									restoreOraConnection();
								}
								logMinerReady = logMiner.next();
							} catch (SQLException sqle) {
								if (sqle instanceof SQLRecoverableException) {
									restoreOraConnection(sqle);
								} else {
									throw new SQLException(sqle);
								}
							}
						}
					}
				}
			} catch (SQLException | IOException e) {
				LOGGER.error(e.getMessage());
				if (e instanceof SQLException) {
					SQLException sqle = (SQLException) e;
					LOGGER.error("SQL errorCode = {}, SQL state = '{}'",
							sqle.getErrorCode(), sqle.getSQLState());
					if (isRsLogMinerRowAvailable) {
						LOGGER.error("Last read row information: SCN={}, RS_ID='{}', SSN={}, XID='{}'",
								lastScn, lastRsId, lastSsn, xid);
					}
					LOGGER.error("Current query is:\n{}\n", mineDataSql);
				}
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
				lastScn = lastGuaranteedScn;
				lastRsId = lastGuaranteedRsId;
				lastSsn = lastGuaranteedSsn;
				running.set(false);
				task.stop(false);
				throw new ConnectException(e);
			}
		}
		LOGGER.debug("End of LogMiner loop...");
		running.set(false);
		LOGGER.info("END: OraCdcLogMinerWorkerThread.run()");
	}

	public long getLastScn() {
		return lastScn;
	}

	public String getLastRsId() {
		return lastRsId;
	}

	public long getLastSsn() {
		return lastSsn;
	}

	public boolean isRunning() {
		return running.get();
	}

	public void shutdown() {
		LOGGER.info("Stopping oracdc logminer archivelog worker thread...");
		while (runLatch.getCount() > 0) {
			runLatch.countDown();
		}
		LOGGER.debug("call to shutdown() completed");
	}

	private void restoreOraConnection(SQLException sqle) {
		LOGGER.error("Error '{}' when waiting for next archived log.", sqle.getMessage());
		LOGGER.error("SQL errorCode = {}, SQL state = '{}'",
				sqle.getErrorCode(), sqle.getSQLState());
		if (sqle.getErrorCode() == ORA_17410 ||
				// SQLSTATE = '08000'
				sqle.getErrorCode() == ORA_17002) { 
				// SQLSTATE = '08006'
			boolean ready = false;
			int retries = 0;
			while (runLatch.getCount() > 0 && !ready) {
				long waitTime = (long) Math.pow(2, retries++) + connectionRetryBackoff;
				LOGGER.warn("Waiting {} ms for LogMiner connection to restore after ORA-{} error...",
						waitTime, sqle.getErrorCode());
				try {
					this.wait(waitTime);
				} catch (InterruptedException ie) {
					LOGGER.error(ie.getMessage());
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(ie));
				}
				try {
					restoreOraConnection();
					ready = true;
				} catch (SQLException getConnException) {
					LOGGER.error("Error '{}' when restoring connection, SQL errorCode = {}, SQL state = '{}'",
							getConnException.getMessage(), getConnException.getErrorCode(), getConnException.getSQLState());
				}
			}
		} else {
			LOGGER.error(
					"\n=====================\n" +
					"Unhandled '{}', SQL errorCode = {}, SQL state = '{}'\n" +
					"in restoreOraConnection(sqle) !\n" +
					"To fix - please send this message to oracle@a2-solutions.eu\n" +
					"=====================\n",
					sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
		}
	}

	private void restoreOraConnection() throws SQLException {
		connLogMiner = oraConnections.getLogMinerConnection(traceSession);
		psLogMiner = (OraclePreparedStatement)connLogMiner.prepareStatement(
				mineDataSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		psLogMiner.setRowPrefetch(fetchSize);
		psCheckTable = connDictionary.prepareStatement(
				checkTableSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		if (processLobs) {
			psReadLob = (OraclePreparedStatement) connLogMiner.prepareStatement(
					isCdb ? OraDictSqlTexts.MINE_LOB_CDB :
							OraDictSqlTexts.MINE_LOB_NON_CDB,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			psReadLob.setRowPrefetch(fetchSize);
		}
		logMiner.createStatements(connLogMiner);
	}

	private void closeOraConnection() throws SQLException {
		if (processLobs) {
			psReadLob.close();
			psReadLob = null;
		}
		psCheckTable.close();
		psCheckTable = null;
		psLogMiner.close();
		psLogMiner = null;
		connLogMiner.close();
		connLogMiner = null;
	}

	private List<OraCdcLargeObjectHolder> catchTheLob(
			final short operation, final String xid,
			final long dataObjectId, final OraTable4LogMiner oraTable,
			final String redoData) throws SQLException {
		List<OraCdcLargeObjectHolder> lobs = null;
		if (processLobs && oraTable.isWithLobs() &&
				(operation == OraCdcV$LogmnrContents.INSERT ||
				operation == OraCdcV$LogmnrContents.UPDATE ||
				operation == OraCdcV$LogmnrContents.XML_DOC_BEGIN)) {
			final String tableOperationRsId = rsLogMiner.getString("RS_ID");
			String lobStartRsId = tableOperationRsId; 
			boolean searchLobObjects = true;
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace(
						"Catch LOB started for table {}, operation {} at RBA '{}'.",
						oraTable.fqn(), operation, tableOperationRsId);
			}
			//TODO
			//TODO Ignore CDB here???
			//TODO
			long lobObjectId = 0;
			while (logMinerReady && searchLobObjects && runLatch.getCount() > 0) {
				searchLobObjects = rsLogMiner.next();
				isRsLogMinerRowAvailable = searchLobObjects;
				if (searchLobObjects) {
					final short catchLobOperation = rsLogMiner.getShort("OPERATION_CODE");
					final String catchLobXid = rsLogMiner.getString("XID");
					if (catchLobOperation == OraCdcV$LogmnrContents.INSERT ||
							catchLobOperation == OraCdcV$LogmnrContents.UPDATE ||
							catchLobOperation == OraCdcV$LogmnrContents.DELETE) {
						// Next INSERT/UPDATE/DELETE for given objects.....
						// Do nothing and don't call next() for rsLogMiner
						fetchRsLogMinerNext = false;
						searchLobObjects = false;
					} else if ((catchLobOperation == OraCdcV$LogmnrContents.COMMIT ||
							catchLobOperation == OraCdcV$LogmnrContents.ROLLBACK) &&
							(catchLobXid.equals(xid) || activeTransactions.containsKey(catchLobXid))) {
						// Do nothing and don't call next() for rsLogMiner
						fetchRsLogMinerNext = false;
						searchLobObjects = false;
					} else if (catchLobOperation == OraCdcV$LogmnrContents.XML_DOC_BEGIN) {
						// Previous operation was OraCdcV$LogmnrContents.INSERT
						// Current position is "XML DOC BEGIN" - need to read column id from SQL_REDO
						// String is:
						// XML DOC BEGIN:  select "COL 2" from "UNKNOWN"."OBJ# 28029"...
						boolean multiLineSql = rsLogMiner.getBoolean("CSF");
						final StringBuilder xmlHexColumnId = new StringBuilder(16000);
						xmlHexColumnId.append(rsLogMiner.getString("SQL_REDO"));
						while (multiLineSql) {
							rsLogMiner.next();
							xmlHexColumnId.append(rsLogMiner.getString("SQL_REDO"));
							multiLineSql = rsLogMiner.getBoolean("CSF");
						}
						final String xmlColumnId = getLobColumnId(xmlHexColumnId.toString());
						// Next row(s) are:
						// XML_REDO := HEXTORAW('.............
						//TODO
						//TODO What if XML DOC WRITE is in more than one redo log?
						//TODO
						final String xmlAsString = readXmlWriteRedoData(xmlColumnId);
						if (lobs == null) {
							lobs = new ArrayList<>();
						}
						lobs.add(new OraCdcLargeObjectHolder(xmlColumnId, Lz4Util.compress(xmlAsString)));
						//TODO
						//TODO BEGIN: Workaround for operation duplication when LogMiner runs
						//TODO BEGIN: without dictionary 
						//TODO
						isRsLogMinerRowAvailable = rsLogMiner.next();
						if (isRsLogMinerRowAvailable) {
							if (dataObjectId == rsLogMiner.getLong("DATA_OBJ#") &&
									OraCdcV$LogmnrContents.UPDATE == rsLogMiner.getShort("OPERATION_CODE") &&
									StringUtils.contains(rsLogMiner.getString("SQL_REDO"), "HEXTORAW('0070") &&
									StringUtils.equals(rsLogMiner.getString("XID"), xid)) {
								// Skip these records
								while (rsLogMiner.getBoolean("CSF")) {
									isRsLogMinerRowAvailable = rsLogMiner.next();
								}
								fetchRsLogMinerNext = true;
								// This update indicates that XML operations with table are done
								break;
							} else {
								fetchRsLogMinerNext = false;
								// There may be more BLOB/CLOB/NCLOB/XMLTYPE records here
								continue;
							}
						}
						//TODO
						//TODO END: Workaround for operation duplication when LogMiner runs
						//TODO END: without dictionary 
						//TODO
					} else if (catchLobOperation == OraCdcV$LogmnrContents.XML_DOC_WRITE) { 
						final String xmlColumnId = getLobColumnId(redoData);
						//TODO
						//TODO What if XML DOC WRITE is in more than one redo log?
						//TODO
						fetchRsLogMinerNext = false;
						final String xmlAsString = readXmlWriteRedoData(xmlColumnId);
						if (lobs == null) {
							lobs = new ArrayList<>();
						}
						lobs.add(new OraCdcLargeObjectHolder(xmlColumnId, Lz4Util.compress(xmlAsString)));
						//TODO
						//TODO BEGIN: Workaround for operation duplication when LogMiner runs
						//TODO BEGIN: without dictionary 
						//TODO
						isRsLogMinerRowAvailable = rsLogMiner.next();
						if (isRsLogMinerRowAvailable) {
							if (dataObjectId == rsLogMiner.getLong("DATA_OBJ#") &&
									OraCdcV$LogmnrContents.UPDATE == rsLogMiner.getShort("OPERATION_CODE") &&
									StringUtils.contains(rsLogMiner.getString("SQL_REDO"), "HEXTORAW('0070") &&
									StringUtils.equals(rsLogMiner.getString("XID"), xid)) {
								// Skip these records
								while (rsLogMiner.getBoolean("CSF")) {
									isRsLogMinerRowAvailable = rsLogMiner.next();
								}
								fetchRsLogMinerNext = true;
								// This update indicates that XML operations with table are done
								break;
							} else {
								fetchRsLogMinerNext = false;
								// There may be more BLOB/CLOB/NCLOB/XMLTYPE records here
								continue;
							}
						}
						//TODO
						//TODO END: Workaround for operation duplication when LogMiner runs
						//TODO END: without dictionary 
						//TODO
					} else {
						// Check for RS_ID of INSERT
						// Previous row contains: DATA_OBJ# = DATA_OBJD# = LOB_ID
						//                        RS_ID to call readLob!!!
						final String lobRsId = rsLogMiner.getString("RS_ID");
						if (LOGGER.isTraceEnabled()) {
							LOGGER.trace(
									"Inside loop for LOB search with 'catch' operation {} and transaction Id {}, table op RBA {}, LOB op RBA {}",
									catchLobOperation, catchLobXid, tableOperationRsId, lobRsId);
						}
						
						if (lobRsId.equals(tableOperationRsId)) {
							final long lobSsn = rsLogMiner.getLong("SSN");
							final long lobScn = rsLogMiner.getLong("SCN");
							if (lobWorker == null) {
								lobWorker = new OraCdcLargeObjectWorker(this,
										isCdb, logMiner, psReadLob, runLatch,
										pollInterval);
							}
							if (lobs == null) {
								lobs = new ArrayList<>();
							}
							lobs.add(lobWorker.readLobData(
									lobScn,
									lobStartRsId,
									tableOperationRsId,
									dataObjectId,
									lobObjectId,
									catchLobXid,
									oraTable.getLobColumn(lobObjectId, psCheckLob),
									isCdb ? rsLogMiner.getNUMBER("SRC_CON_UID") : null));
							if (lobWorker.isLogMinerExtended()) {
								//TODO
								//TODO Add SCN>= to MineSql!!!
								//TODO
								rsLogMiner = (OracleResultSet) psLogMiner.executeQuery();
								while(rsLogMiner.next()) {
									if (rsLogMiner.getLong("SCN") == lobScn &&
										StringUtils.equals(rsLogMiner.getString("RS_ID"), lobRsId) &&
										rsLogMiner.getLong("SSN") == lobSsn) {
										break;
									}
								}
								fetchRsLogMinerNext = true;
							}
						} else {
							lobObjectId = rsLogMiner.getInt("DATA_OBJ#");
							lobStartRsId = rsLogMiner.getString("RS_ID");
							if (catchLobOperation == OraCdcV$LogmnrContents.XML_DOC_BEGIN) {
								// Perform XML document processing here
							}
						}
					}
				} else {
					//Switch to next archived log
					logMinerReady = false;
					logMiner.stop();
					rsLogMiner.close();
					rsLogMiner = null;
					while (!logMinerReady && runLatch.getCount() > 0) {
						try {
							logMinerReady = logMiner.next();
						} catch (SQLException sqle) {
							if (sqle instanceof SQLRecoverableException) {
								restoreOraConnection(sqle);
							} else {
								throw new SQLException(sqle);
							}
						}
						if (logMinerReady) {
							rsLogMiner = (OracleResultSet) psLogMiner.executeQuery();
							//Exit from next archived log loop
							break;
						} else if (runLatch.getCount() > 0) {
							//Wait for next archived log
							synchronized (this) {
								LOGGER.debug("Waiting {} ms", pollInterval);
								try {
									this.wait(pollInterval);
								} catch (InterruptedException ie) {
									LOGGER.error(ie.getMessage());
									LOGGER.error(ExceptionUtils.getExceptionStackTrace(ie));
								}
							}
						} else {
							//Stop processing
							break;
						}
					}
				}
			}
		}
		return lobs;
	}

	private String getLobColumnId(final String xmlHexColumnId)  {
		return
			"\"" + StringUtils.substringBetween(xmlHexColumnId, "\"") + "\"";
	}

	private String readXmlWriteRedoData(final String xmlColumnId) throws SQLException {
		boolean multiLineSql = true;
		final StringBuilder xmlHexData = new StringBuilder(65000);
		while (multiLineSql) {
			if (fetchRsLogMinerNext) {
				isRsLogMinerRowAvailable = rsLogMiner.next();
			} else {
				fetchRsLogMinerNext = true;
			}
			if (rsLogMiner.getShort("OPERATION_CODE") != OraCdcV$LogmnrContents.XML_DOC_WRITE) {
				LOGGER.error("Unexpected operation with code {} at SCN {} RBA '{}'",
						rsLogMiner.getShort("OPERATION_CODE"), rsLogMiner.getLong("SCN"), rsLogMiner.getString("RS_ID"));
				throw new SQLException("Unexpected operation!!!");
			}
			multiLineSql = rsLogMiner.getBoolean("CSF");
			xmlHexData.append(rsLogMiner.getString("SQL_REDO"));
		}
		final String xmlAsString = new String(
				OraDumpDecoder.toByteArray(
						StringUtils.substringBetween(xmlHexData.toString(), "HEXTORAW('", ")'")));
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("{} column {} content:\n{}",
					xmlColumnId, xmlAsString);
		}
		fetchRsLogMinerNext = true;
		return xmlAsString;
	}

	private String readSqlRedo() throws SQLException {
		final boolean multiLineSql = rsLogMiner.getBoolean("CSF");
		if (multiLineSql) {
			final StringBuilder sb = new StringBuilder(16000);
			boolean moreRedoLines = multiLineSql;
			while (moreRedoLines) {
				sb.append(rsLogMiner.getString("SQL_REDO"));
				moreRedoLines = rsLogMiner.getBoolean("CSF");
				if (moreRedoLines) { 
					rsLogMiner.next();
				}
			}
			return sb.toString();
		} else {
			return rsLogMiner.getString("SQL_REDO");
		}
	}

	private void initDictionaryStatements() throws SQLException {
		psCheckTable = connDictionary.prepareStatement(
				checkTableSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		if (processLobs) {
			psCheckLob = connDictionary.prepareStatement(
					isCdb ? OraDictSqlTexts.MAP_DATAOBJ_TO_COLUMN_CDB :
						OraDictSqlTexts.MAP_DATAOBJ_TO_COLUMN_NON_CDB,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			psIsDataObjLob = connDictionary.prepareStatement(
					isCdb ? OraDictSqlTexts.LOB_CHECK_CDB :
						OraDictSqlTexts.LOB_CHECK_NON_CDB,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		}
	}

	private static class ActiveTransComparator implements Comparator<String> {

		private final Map<String, OraCdcTransaction> activeTransactions;

		ActiveTransComparator(final Map<String, OraCdcTransaction> activeTransactions) {
			this.activeTransactions = activeTransactions;
		}

		@Override
		public int compare(String first, String second) {
			if (StringUtils.equals(first, second)) {
				// A transaction ID is unique to a transaction and represents the undo segment number, slot, and sequence number.
				// https://docs.oracle.com/en/database/oracle/oracle-database/21/cncpt/transactions.html#GUID-E3FB3DC3-3317-4589-BADD-D89A3547F87D
				return 0;
			}

			OraCdcTransaction firstOraTran = activeTransactions.get(first);
			OraCdcTransaction secondOraTran = activeTransactions.get(second);
			if (firstOraTran != null && secondOraTran != null && firstOraTran.getFirstChange() >= secondOraTran.getFirstChange()) {
				return 1;
			}

			return -1;
		}

	}

}
