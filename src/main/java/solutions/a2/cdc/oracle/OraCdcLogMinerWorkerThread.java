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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleResultSet;
import oracle.sql.CHAR;
import solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmt;
import solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmtIntf;
import solutions.a2.cdc.oracle.utils.OraSqlUtils;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;
import solutions.a2.oracle.utils.BinaryUtils;
import solutions.a2.utils.ExceptionUtils;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static solutions.a2.cdc.oracle.OraCdcStatementBase.APPROXIMATE_SIZE;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcLogMinerWorkerThread extends OraCdcWorkerThreadBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerWorkerThread.class);
	private static final int MAX_RETRIES = 63;
	// XIDUSN || XIDSLT
	private static final int TRANS_PREFIX = 8;
	private static final byte[] SQUEEZE_PATTERN = "HEXTORAW(".getBytes(US_ASCII);

	private final OraCdcLogMinerTask task;
	private final OraCdcLogMinerMgmt metrics;
	private boolean logMinerReady = false;
	private final OraLogMiner logMiner;
	private Connection connLogMiner;
	private OraclePreparedStatement psLogMiner;
	private PreparedStatement psCheckLob;
	private PreparedStatement psIsDataObjLob;
	private OraclePreparedStatement psReadLob;
	private OracleResultSet rsLogMiner;
	private final String mineDataSql;
	private Connection connDictionary;
	private final Map<String, OraCdcTransaction> activeTransactions;
	private final Map<String, String> prefixedTransactions;
	private final TreeMap<String, Triple<Long, RedoByteAddress, Long>> sortedByFirstScn;
	private final ActiveTransComparator activeTransComparator;
	private OraCdcLargeObjectWorker lobWorker;
	private final int connectionRetryBackoff;
	private final int fetchSize;
	private final boolean traceSession;
	private final Set<Long> lobObjects;
	private final Set<Long> nonLobObjects;
	private final OraCdcDictionaryChecker checker;

	private boolean fetchRsLogMinerNext;
	private boolean isRsLogMinerRowAvailable;

	private RowId lastRealRowId;
	private final long logMinerReconnectIntervalMs;

	public OraCdcLogMinerWorkerThread(
			final OraCdcLogMinerTask task,
			final OraCdcDictionaryChecker checker,
			final long firstScn,
			final String mineDataSql,
			final Map<String, OraCdcTransaction> activeTransactions,
			final BlockingQueue<OraCdcTransaction> committedTransactions,
			final OraCdcLogMinerMgmt metrics) throws SQLException {
		super(task.runLatch(), task.rdbmsInfo(), task.config(), 
				task.oraConnections(), committedTransactions);
		LOGGER.info("Initializing oracdc LogMiner archivelog worker thread");
		this.setName("OraCdcLogMinerWorkerThread-" + System.nanoTime());
		this.task = task;
		this.checker = checker;
		this.mineDataSql = mineDataSql;
		this.activeTransactions = activeTransactions;
		this.metrics = metrics;
		this.connectionRetryBackoff = config.connectionRetryBackoff();
		this.fetchSize = config.getInt(ParamConstants.FETCH_SIZE_PARAM);
		this.traceSession = config.getBoolean(ParamConstants.TRACE_LOGMINER_PARAM);
		activeTransComparator = new ActiveTransComparator(activeTransactions);
		sortedByFirstScn = new TreeMap<>(activeTransComparator);
		prefixedTransactions = new HashMap<>();
		this.logMinerReconnectIntervalMs = config.logMinerReconnectIntervalMs();
		if (processLobs) {
			lobObjects = new HashSet<>();
			nonLobObjects = new HashSet<>();
		} else {
			lobObjects = null;
			nonLobObjects = null;
		}

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

	@Override
	public void rewind(final long firstScn, final RedoByteAddress firstRsId, final long firstSsn) throws SQLException {
		if (logMinerReady) {
			LOGGER.info("Rewinding LogMiner ResultSet to first position after SCN= {}, RBA={}, SSN={}.",
					firstScn, firstRsId, firstSsn);
			rsLogMiner = (OracleResultSet) psLogMiner.executeQuery();
			int recordCount = 0;
			long rewindElapsed = System.currentTimeMillis();
			boolean rewindNeeded = true;
			lastScn = firstScn;
			lastRba = firstRsId;
			lastSubScn = firstSsn;
			int errorCount = 0;
			while (rewindNeeded) {
				if (rsLogMiner.next()) {
					lastScn = rsLogMiner.getLong("SCN");
					lastRba = RedoByteAddress.fromLogmnrContentsRs_Id(rsLogMiner.getCHAR("RS_ID").getBytes());
					lastSubScn = rsLogMiner.getLong("SSN");
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
							(firstRsId == null || firstRsId.equals(lastRba)) &&
							(firstSsn == -1 || firstSsn == lastSubScn) &&
							!rsLogMiner.getBoolean("CSF")) {
							rewindNeeded = false;
							break;
						}
					}
				} else {
					if (errorCount < MAX_RETRIES) {
						LOGGER.warn("Unable to rewind to SCN = {}, RBA ={}, SSN = {}, empty ResultSet!",
								firstScn, firstRsId, firstSsn);
						rsLogMiner.close();
						//TODO - do we need to re-initialize LogMiner here?
						rsLogMiner = (OracleResultSet) psLogMiner.executeQuery();
						errorCount++;
					} else {
						LOGGER.error("Incorrect rewind to SCN = {}, RBA = {}, SSN = {}",
								firstScn, firstRsId, firstSsn);
						throw new SQLException("Incorrect rewind operation!!!");
					}
				}
			}
			rewindElapsed = System.currentTimeMillis() - rewindElapsed;
			LOGGER.info("Total records skipped while rewinding: {}, elapsed time ms: {}", recordCount, rewindElapsed);
		} else {
			LOGGER.info("Values from offset (SCN = {}, RS_ID = '{}', SSN = {}) ignored, waiting for new archived log.",
					firstScn, firstRsId, firstSsn);
		}
	}

	@Override
	public void run()  {
		LOGGER.info("BEGIN: OraCdcLogMinerWorkerThread.run()");
		running.set(true);
		OraCdcPseudoColumnsProcessor pseudoColumns = config.pseudoColumnsProcessor();
		boolean firstTransaction = true;
		long logMinerSessionStartMs = System.currentTimeMillis();
		while (runLatch.getCount() > 0) {
			long lastGuaranteedScn = 0;
			RedoByteAddress lastGuaranteedRsId = null;
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
						final boolean partialRollback = rsLogMiner.getBoolean("ROLLBACK");
						xid = rsLogMiner.getString("XID");
						lastScn = rsLogMiner.getLong("SCN");
						lastRba = RedoByteAddress.fromLogmnrContentsRs_Id(rsLogMiner.getString("RS_ID"));
						lastSubScn = rsLogMiner.getLong("SSN");
						OraCdcTransaction transaction = activeTransactions.get(xid);
						switch (operation) {
						case OraCdcV$LogmnrContents.COMMIT:
							if (transaction != null) {
								// SCN of commit
								transaction.setCommitScn(lastScn, pseudoColumns, rsLogMiner);
								committedTransactions.add(transaction);
								activeTransactions.remove(xid);
								prefixedTransactions.remove(StringUtils.left(xid, TRANS_PREFIX));
								sortedByFirstScn.remove(xid);
								if (!sortedByFirstScn.isEmpty()) {
									task.putReadRestartScn(sortedByFirstScn.firstEntry().getValue());
								} else {
									firstTransaction = true;
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
								prefixedTransactions.remove(StringUtils.left(xid, TRANS_PREFIX));
								sortedByFirstScn.remove(xid);
								if (!sortedByFirstScn.isEmpty()) {
									task.putReadRestartScn(sortedByFirstScn.firstEntry().getValue());
								} else {
									firstTransaction = true;
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
							if (transaction == null && partialRollback) {
								final String substitutedXid = prefixedTransactions.get(StringUtils.left(xid, TRANS_PREFIX));
								if (Strings.CS.endsWith(xid, "FFFFFFFF") && substitutedXid != null) {
									LOGGER.warn(
											"\n======================\n" +
											"Suspicious XID {} is changed to {} for operation {} at SCN={}, RBA(RS_ID)={}, SSN={}\n" +
											"LogMiner START_SCN={}, END_SCN={}.\n" +
											"======================\n",
											xid, substitutedXid, operation, lastScn, lastRba, lastSubScn,
											logMiner.getFirstChange(), logMiner.getNextChange());
									transaction = activeTransactions.get(substitutedXid);
									transaction.setSuspicious();
								} else {
									LOGGER.error(
											"\n=====================\n\n" +
											"The transaction with XID='{}' starts with with the PARTIAL ROLLBACK flag!\n" +
											"Operation code is {}, SCN={}, RBA(RS_ID)={}, SSN={}\n" +
											"LogMiner START_SCN={}, END_SCN={}.\n" +
											"A possible reason for this could be that more than one oracdc instance connected to the same database\n" + 
											"instance/service/PDB using the same credentials are running in the same JVM, i.e.\n" +
											"sharing the same Kafka Connect process/cluster specified by the group.id parameter.\n" +
											"In this case, you need to use different Kafka Connect processes/clusters with unique grop.id's\n" +
											"for the same connections to the same database instance/service/PDB using the same credentials.\n\n" +
											"Another possible reason is that the connector's starting point is incorrect and the transaction '{}'\n" +
											"starts with an SCN that splits the transaction into two parts, and the initial operations of the transaction,\n" +
											"which are in the first part of the transaction, are not available to the connector.\n" + 
											"In this case, you need to set the connector's 'a2.first.change' parameter to the correct value\n" +
											"and make sure that the necessary archive logs are available.\n\n" +
											"If you have questions or need more information, please write to us at oracle@a2.solutions\n\n" +
											"\n=====================\n",
											xid, operation, lastScn, lastRba, lastSubScn,
											logMiner.getFirstChange(), logMiner.getNextChange());
								}
							}
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
							OraTable4LogMiner oraTable = checker.getTable(combinedDataObjectId, dataObjectId, conId);

							if (oraTable != null) {
								final byte[] redoBytes = readSqlRedo();
								if (oraTable.isCheckSupplementalLogData()) {
									final long timestamp = rsLogMiner.getDate("TIMESTAMP").getTime();
									final OraCdcLogMinerStatement lmStmt = new  OraCdcLogMinerStatement(
											combinedDataObjectId, operation, redoBytes, timestamp,
											lastScn, lastRba, lastSubScn, getRowId(), partialRollback);

									// Catch the LOBs!!!
									List<OraCdcLargeObjectHolder> lobs = 
											catchTheLob(operation, xid, dataObjectId, oraTable, redoBytes);

									if (transaction == null) {
										if (LOGGER.isDebugEnabled()) {
											LOGGER.debug("New transaction {} created. Transaction start timestamp {}, first SCN {}.",
													xid, timestamp, lastScn);
										}
										transaction = createTransaction(xid, lastScn, activeTransactions.size());
										activeTransactions.put(xid, transaction);
										createTransactionPrefix(xid);
										sortedByFirstScn.put(xid,
													Triple.of(lastScn, lastRba, lastSubScn));
										if (firstTransaction) {
											firstTransaction = false;
											task.putReadRestartScn(sortedByFirstScn.firstEntry().getValue());
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
											"\n=====================\n" +
											"Supplemental logging for table '{}' is not configured correctly!\n" +
											"Please set it according to the oracdc documentation!\n" +
											"Redo record is skipped for OPERATION={}, SCN={}, RBA={}, XID='{}',\n\tREDO_DATA='{}'\n" +
											"=====================\n",
											oraTable.fqn(), operation, lastScn, lastRba, xid, new String(redoBytes, US_ASCII));
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
							if (checker.containsTable(combinedDdlDataObjectId)) {
								// Handle DDL only for known table
								final long timestamp = rsLogMiner.getDate("TIMESTAMP").getTime();
								final byte[] ddlBytes = readSqlRedo();
								final String originalDdl = new String(ddlBytes, US_ASCII);
								final String preProcessedDdl = OraSqlUtils.alterTablePreProcessor(originalDdl);
								if (preProcessedDdl != null) {
									final OraCdcLogMinerStatement lmStmt = new  OraCdcLogMinerStatement(
											combinedDdlDataObjectId, operation, 
											(preProcessedDdl + "\n" + originalDdl).getBytes(US_ASCII),
											timestamp, lastScn, lastRba, lastSubScn, getRowId(), false);
									if (transaction == null) {
										if (LOGGER.isDebugEnabled()) {
											LOGGER.debug("New transaction {} created. Transaction start timestamp {}, first SCN {}.",
													xid, timestamp, lastScn);
										}
										transaction = createTransaction(xid, lastScn, activeTransactions.size());
										activeTransactions.put(xid, transaction);
										createTransactionPrefix(xid);
										sortedByFirstScn.put(xid,
													Triple.of(lastScn, lastRba, lastSubScn));
										if (firstTransaction) {
											firstTransaction = false;
											task.putReadRestartScn(sortedByFirstScn.firstEntry().getValue());
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
											if (LOGGER.isDebugEnabled())
												LOGGER.debug(
													isCdb 
														? "Unable to find in dictionary LOB with DATA_OBJ#={}, CON_ID={} using SQL:\n{}\nLast read row information: SCN={}, RBA='{}', SSN={}, XID='{}'"
														: "Unable to find in dictionary LOB with DATA_OBJ#={}{} using SQL:\\n{}\nLast read row information: SCN={}, RBA='{}', SSN={}, XID='{}'",
														internalOpObjectId,
														isCdb ? Integer.toString(containerInternalOpObjectId) : "",
														mineDataSql, lastScn, lastRba, lastSubScn, xid);
										}
										rsIsDataObjLob.close();
										rsIsDataObjLob = null;
									}
								}
								if (readRowId) {
									lastRealRowId = getRowId();
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
									lastRealRowId == null ? getRowId() : lastRealRowId,
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
						} // switch(operation)
						// Copy again, to protect from exception...
						lastGuaranteedScn = lastScn;
						lastGuaranteedRsId = lastRba;
						lastGuaranteedSsn = lastSubScn;
						if (fetchRsLogMinerNext) {
							try {
								isRsLogMinerRowAvailable = rsLogMiner.next();
							} catch (SQLException sqle) {
								if (sqle.getErrorCode() == OraRdbmsInfo.ORA_310) {
									// SQLState = 64000
									isRsLogMinerRowAvailable = false;
									logMiner.setFirstChange(lastScn);
									LOGGER.error(
											"\n=====================\n" +
											"{}\n" +
											"\twhile executing 'isRsLogMinerRowAvailable = rsLogMiner.next();'\n" +
											"\tFull errorstack:\n{}\n" +
											"=====================\n",
											sqle.getMessage(), ExceptionUtils.getExceptionStackTrace(sqle));
									//TODO
									//TODO Do we need to rewind to <lastScn, lastRba, lastSubScn>? 
									//TODO
								} else {
									//TODO
									//TODO Add try-catch block with exponential backoff here!!!
									//TODO hit SQL errorCode = 17002, SQL state = '08006'
									//TODO
									throw sqle;
								}
							}
						}
					} // while (isRsLogMinerRowAvailable && runLatch.getCount() > 0)
					try {
						logMiner.stop();
						rsLogMiner.close();
					} catch (SQLException sqle) {
						LOGGER.error(
								"\n=====================\n" +
								"{}\n" +
								"\twhile executing 'logMiner.stop(); rsLogMiner.close();'\n" +
								"\tFull errorstack:\n{}\n" +
								"=====================\n",
								sqle.getMessage(), ExceptionUtils.getExceptionStackTrace(sqle));
					}
					rsLogMiner = null;
					if (activeTransactions.isEmpty() && lastGuaranteedScn > 0) {
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
									final long tsReconnect = System.currentTimeMillis();
									closeOraConnection();
									restoreOraConnection();
									LOGGER.info(
											"Reconnection to RDBMS completed in {} ms.",
											(System.currentTimeMillis() - tsReconnect));
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
			} catch (Exception e) {
				LOGGER.error(e.getMessage());
				if (e instanceof SQLException) {
					SQLException sqle = (SQLException) e;
					LOGGER.error("SQL errorCode = {}, SQL state = '{}'",
							sqle.getErrorCode(), sqle.getSQLState());
					if (isRsLogMinerRowAvailable) {
						LOGGER.error("Last read row information: SCN={}, RS_ID='{}', SSN={}, XID='{}'",
								lastScn, lastRba, lastSubScn, xid);
					}
					LOGGER.error("Current query is:\n{}\n", mineDataSql);
				}
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
				lastScn = lastGuaranteedScn;
				lastRba = lastGuaranteedRsId;
				lastSubScn = lastGuaranteedSsn;
				running.set(false);
				task.stop(false);
				throw new ConnectException(e);
			}
		}
		LOGGER.debug("End of LogMiner loop...");
		running.set(false);
		LOGGER.info("END: OraCdcLogMinerWorkerThread.run()");
	}

	private void restoreOraConnection(SQLException sqle) {
		LOGGER.error("Error '{}' when waiting for next archived log.", sqle.getMessage());
		LOGGER.error("SQL errorCode = {}, SQL state = '{}'",
				sqle.getErrorCode(), sqle.getSQLState());
		if (sqle.getErrorCode() == OraRdbmsInfo.ORA_17410 ||
				// SQLSTATE = '08000'
				sqle.getErrorCode() == OraRdbmsInfo.ORA_17002 || 
				// SQLSTATE = '08006'
				sqle.getErrorCode() == OraRdbmsInfo.ORA_1089) {
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
				if (retries == MAX_RETRIES) {
					LOGGER.error(
							"\n=====================\n" +
							"Unable to restore connections after {} retries.\n" +
							"Original error: '{}',\n\tSQL errorCode = {}, SQL state = '{}'\n" +
							"\n=====================\n",
							retries, sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
					throw new ConnectException(sqle);
				}
			}
		} else {
			LOGGER.error(
					"\n=====================\n" +
					"Unhandled '{}', SQL errorCode = {}, SQL state = '{}'\n" +
					"in restoreOraConnection(sqle) !\n" +
					"To fix - please send this message to oracle@a2.solutions\n" +
					"=====================\n",
					sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
			throw new ConnectException(sqle);
		}
	}

	private void restoreOraConnection() throws SQLException {
		connLogMiner = oraConnections.getLogMinerConnection(traceSession);
		psLogMiner = (OraclePreparedStatement)connLogMiner.prepareStatement(
				mineDataSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		psLogMiner.setRowPrefetch(fetchSize);
		checker.initStatements();
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
		psLogMiner.close();
		psLogMiner = null;
		oraConnections.closeLogMinerConnection(connLogMiner);
		connLogMiner = null;
	}

	private List<OraCdcLargeObjectHolder> catchTheLob(
			final short operation, final String xid,
			final long dataObjectId, final OraTable4LogMiner oraTable,
			final byte[] redoBytes) throws SQLException {
		List<OraCdcLargeObjectHolder> lobs = null;
		if (processLobs && oraTable.isWithLobs() &&
				(operation == OraCdcV$LogmnrContents.INSERT ||
				operation == OraCdcV$LogmnrContents.UPDATE ||
				operation == OraCdcV$LogmnrContents.XML_DOC_BEGIN)) {
			final String redoData = new String(redoBytes, US_ASCII);
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
						final StringBuilder xmlHexColumnId = new StringBuilder(APPROXIMATE_SIZE);
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
						lobs.add(new OraCdcLargeObjectHolder(xmlColumnId, xmlAsString.getBytes()));
						//TODO
						//TODO BEGIN: Workaround for operation duplication when LogMiner runs
						//TODO BEGIN: without dictionary 
						//TODO
						isRsLogMinerRowAvailable = rsLogMiner.next();
						if (isRsLogMinerRowAvailable) {
							if (dataObjectId == rsLogMiner.getLong("DATA_OBJ#") &&
									OraCdcV$LogmnrContents.UPDATE == rsLogMiner.getShort("OPERATION_CODE") &&
									Strings.CS.contains(rsLogMiner.getString("SQL_REDO"), "HEXTORAW('0070") &&
									Strings.CS.equals(rsLogMiner.getString("XID"), xid)) {
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
						lobs.add(new OraCdcLargeObjectHolder(xmlColumnId, xmlAsString.getBytes()));
						//TODO
						//TODO BEGIN: Workaround for operation duplication when LogMiner runs
						//TODO BEGIN: without dictionary 
						//TODO
						isRsLogMinerRowAvailable = rsLogMiner.next();
						if (isRsLogMinerRowAvailable) {
							if (dataObjectId == rsLogMiner.getLong("DATA_OBJ#") &&
									OraCdcV$LogmnrContents.UPDATE == rsLogMiner.getShort("OPERATION_CODE") &&
									Strings.CS.contains(rsLogMiner.getString("SQL_REDO"), "HEXTORAW('0070") &&
									Strings.CS.equals(rsLogMiner.getString("XID"), xid)) {
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
										Strings.CS.equals(rsLogMiner.getString("RS_ID"), lobRsId) &&
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
		if (Strings.CS.equals(xmlColumnId, "\"UNKNOWN\"")) {
			LOGGER.error("UNKNOWN columnId!");
			return "";
		}
		boolean multiLineSql = true;
		final StringBuilder xmlHexData = new StringBuilder(65000);
		boolean withHexToRaw = false;
		while (multiLineSql) {
			if (fetchRsLogMinerNext) {
				isRsLogMinerRowAvailable = rsLogMiner.next();
			} else {
				fetchRsLogMinerNext = true;
			}
			final short operation = rsLogMiner.getShort("OPERATION_CODE");
			if (operation != OraCdcV$LogmnrContents.XML_DOC_WRITE &&
					operation != OraCdcV$LogmnrContents.XML_DOC_BEGIN &&
					operation != OraCdcV$LogmnrContents.XML_DOC_END	) {
				LOGGER.error("Unexpected operation with code {} at SCN {} RBA '{}'",
						rsLogMiner.getShort("OPERATION_CODE"), rsLogMiner.getLong("SCN"), rsLogMiner.getString("RS_ID"));
				throw new SQLException("Unexpected operation!!!");
			}
			if (operation == OraCdcV$LogmnrContents.XML_DOC_WRITE) {
				multiLineSql = rsLogMiner.getBoolean("CSF");
				final String currentLine = rsLogMiner.getString("SQL_REDO");
				if (!withHexToRaw)
					withHexToRaw = Strings.CS.contains(currentLine, "HEXTORAW");
				xmlHexData.append(currentLine);
			}
		}
		final String xmlAsString = new String(
				OraDumpDecoder.hexToRaw(withHexToRaw
						? StringUtils.substringBetween(xmlHexData.toString(), "HEXTORAW('", ")'")
						: xmlHexData.toString()));
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("{} column {} content:\n{}",
					xmlColumnId, xmlAsString);
		}
		fetchRsLogMinerNext = true;
		return xmlAsString;
	}

	private byte[] readSqlRedo() throws SQLException {
		final byte[] array;
		if (rsLogMiner.getBoolean("CSF")) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream(APPROXIMATE_SIZE);
			while (true) {
				try {
					final CHAR oraChar = rsLogMiner.getCHAR("SQL_REDO");
					if (!rsLogMiner.wasNull())
						baos.write(oraChar.getBytes());
				} catch (IOException ioe) {
					throw new SQLException(ioe);
				}
				if (rsLogMiner.getBoolean("CSF")) { 
					rsLogMiner.next();
				} else {
					break;
				}
			}
			array = baos.toByteArray();
		} else {
			array = rsLogMiner.getCHAR("SQL_REDO").getBytes();
		}
		if (array == null || array.length == 0) {
			throw new SQLException("Unexpected NULL while reading SQL_REDO column!");
		} else {
			int start = 0;
			int srcPos = 0;
			int destPos = 0;
			int currLength = 0;
			byte[] squeezedArray = new byte[array.length];
			while ((srcPos = BinaryUtils.indexOf(array, start, SQUEEZE_PATTERN)) > -1) {
				currLength = srcPos - start;
				System.arraycopy(array, start, squeezedArray, destPos, currLength);
				start = srcPos + SQUEEZE_PATTERN.length;
				destPos += currLength;

				if ((srcPos = BinaryUtils.indexOf(array, start, (byte)0x29)) > -1) {
					currLength = srcPos - start;
					System.arraycopy(array, start, squeezedArray, destPos, currLength);
					start = srcPos + 1;
					destPos += currLength;
				}
			}
			if (start < array.length) {
				currLength = array.length - start;
				System.arraycopy(array, start, squeezedArray, destPos, currLength);
				destPos += currLength;
			}

			if (destPos < squeezedArray.length) {
				final byte[] result = new byte[destPos];
				System.arraycopy(squeezedArray, 0, result, 0, destPos);
				squeezedArray = null;
				return result;
			} else {
				return squeezedArray;
			}
		}
	}

	private RowId getRowId() throws SQLException {
		final CHAR c = rsLogMiner.getCHAR("ROW_ID");
		return rsLogMiner.wasNull() ? RowId.ZERO : RowId.fromLogmnrContents(c.getBytes());
	}

	private void initDictionaryStatements() throws SQLException {
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
			if (Strings.CS.equals(first, second)) {
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

	private void createTransactionPrefix(final String xid) {
		final String prefix = StringUtils.left(xid, TRANS_PREFIX);
		final String prevXid = prefixedTransactions.put(prefix, xid);
		if (prevXid != null) {
			LOGGER.warn(
					"\n=====================\n" +
					"Transaction prefix {} binding changed from {} to {}.\n" +
					"=====================\n",
					prefix, prevXid, xid);
		}
	}

}
