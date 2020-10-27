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

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmt;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleResultSet;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcLogMinerWorkerThread extends Thread {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerWorkerThread.class);
	private static final int ORA_17410 = 17410;

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
	private final int schemaType;
	private final String topic;
	private final OraDumpDecoder odd;
	private final OraLogMiner logMiner;
	private Connection connLogMiner;
	private PreparedStatement psLogMiner;
	private OraclePreparedStatement psReadLob;
	private OracleResultSet rsLogMiner;
	private final String mineDataSql;
	private final Connection connDictionary;
	private final PreparedStatement psCheckTable;
	private final Path queuesRoot;
	private final Map<String, OraCdcTransaction> activeTransactions;
	private final BlockingQueue<OraCdcTransaction> committedTransactions;
	private final boolean useOracdcSchemas;
	private long lastScn;
	private String lastRsId;
	private long lastSsn;
	private final AtomicBoolean running;
	private boolean isCdb;
	private final boolean processLobs;
	private final int topicNameStyle;
	private final String topicNameDelimiter;
	private OraCdcLargeObjectWorker lobWorker;

	public OraCdcLogMinerWorkerThread(
			final OraCdcLogMinerTask task,
			final int pollInterval,
			final Map<String, String> partition,
			final long firstScn,
			final String mineDataSql,
			final String checkTableSql,
			final Long redoSizeThreshold,
			final Integer redoFilesCount,
			final Map<Long, OraTable4LogMiner> tablesInProcessing,
			final Set<Long> tablesOutOfScope,
			final int schemaType,
			final boolean useOracdcSchemas,
			final boolean processLobs,
			final String topic,
			final OraDumpDecoder odd,
			final Path queuesRoot,
			final Map<String, OraCdcTransaction> activeTransactions,
			final BlockingQueue<OraCdcTransaction> committedTransactions,
			final OraCdcLogMinerMgmt metrics,
			final int topicNameStyle,
			final String topicNameDelimiter) throws SQLException {
		LOGGER.info("Initializing oracdc logminer archivelog worker thread");
		this.setName("OraCdcLogMinerWorkerThread-" + System.nanoTime());
		this.task = task;
		this.pollInterval = pollInterval;
		this.partition = partition;
		this.mineDataSql = mineDataSql;
		this.tablesInProcessing = tablesInProcessing;
		// We do not need concurrency for this map
		this.partitionsInProcessing = new HashMap<>();
		this.tablesOutOfScope = tablesOutOfScope;
		this.queuesRoot = queuesRoot;
		this.odd = odd;
		this.schemaType = schemaType;
		this.useOracdcSchemas = useOracdcSchemas;
		this.processLobs = processLobs;
		this.topic = topic;
		this.activeTransactions = activeTransactions;
		this.committedTransactions = committedTransactions;
		this.metrics = metrics;
		this.topicNameStyle = topicNameStyle;
		this.topicNameDelimiter = topicNameDelimiter;
		runLatch = new CountDownLatch(1);
		running = new AtomicBoolean(false);
		try {
			connLogMiner = OraPoolConnectionFactory.getLogMinerConnection();
			connDictionary = OraPoolConnectionFactory.getConnection();

			rdbmsInfo = OraRdbmsInfo.getInstance();
			isCdb = rdbmsInfo.isCdb();

			if (redoSizeThreshold != null) {
				logMiner = new OraLogMiner(connLogMiner, metrics, firstScn, redoSizeThreshold);
			} else {
				logMiner = new OraLogMiner(connLogMiner, metrics, firstScn, redoFilesCount);
			}
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
				throw new SQLException("Unable to mine data from databases with different DBID!!!");
			}

			// Finally - prepare for mining...
			psLogMiner = connLogMiner.prepareStatement(
					mineDataSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			psCheckTable = connDictionary.prepareStatement(
					checkTableSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			logMinerReady = logMiner.next();
			if (processLobs) {
				psReadLob = (OraclePreparedStatement) connLogMiner.prepareStatement(
						isCdb ? OraDictSqlTexts.MINE_LOB_CDB :
								OraDictSqlTexts.MINE_LOB_NON_CDB,
						ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			}

		} catch (SQLException e) {
			LOGGER.error("Unable to start logminer archivelog worker thread!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new SQLException(e);
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
					final long scn = rsLogMiner.getLong("SCN");
					if (recordCount == 0 && scn > firstScn) {
						// Hit this with 10.2.0.5
						rewindNeeded = false;
						// Need to reopen cursor
						rsLogMiner.close();
						rsLogMiner = (OracleResultSet) psLogMiner.executeQuery();
					} else {
						recordCount++;
						if (firstScn == scn &&
							firstRsId.equals(rsLogMiner.getString("RS_ID")) &&
							firstSsn == rsLogMiner.getLong("SSN") &&
							!rsLogMiner.getBoolean("CSF")) {
							rewindNeeded = false;
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
		while (runLatch.getCount() > 0) {
			long lastGuaranteedScn = 0;
			String lastGuaranteedRsId = null;
			long lastGuaranteedSsn = 0;
			try {
				if (logMinerReady) {
					if (rsLogMiner == null) {
						rsLogMiner = (OracleResultSet) psLogMiner.executeQuery();
					}
					boolean isRsLogMinerRowAvailable = rsLogMiner.next();
					while (isRsLogMinerRowAvailable) {
						boolean fetchRsLogMinerNext = true;
						final short operation = rsLogMiner.getShort("OPERATION_CODE");
						final String xid = rsLogMiner.getString("XID");
						lastScn = rsLogMiner.getLong("SCN");
						lastRsId = rsLogMiner.getString("RS_ID");
						lastSsn = rsLogMiner.getLong("SSN");
						OraCdcTransaction transaction = activeTransactions.get(xid);
						switch (operation) {
						case OraLogMiner.V$LOGMNR_CONTENTS_COMMIT:
							if (transaction != null) {
								// SCN of commit
								transaction.setCommitScn(lastScn);
								committedTransactions.add(transaction);
								activeTransactions.remove(xid);
								metrics.addCommittedRecords(transaction.length());
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Performing commit at SCN {} for transaction XID {}", lastScn, xid);
								}
							} else {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Skipping commit at SCN {} for transaction XID {}", lastScn, xid);
								}
							}
							break;
						case OraLogMiner.V$LOGMNR_CONTENTS_ROLLBACK:
							if (transaction != null) {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Rolling back at SCN transaction XID {} with {} records.",
											lastScn, xid, transaction.length());
								}
								metrics.addRolledBackRecords(transaction.length());
								transaction.close();
								activeTransactions.remove(xid);
							} else {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Skipping rollback at SCN {} for transaction XID {}", lastScn, xid);
								}
							}
							break;
						case OraLogMiner.V$LOGMNR_CONTENTS_INSERT:
						case OraLogMiner.V$LOGMNR_CONTENTS_DELETE:
						case OraLogMiner.V$LOGMNR_CONTENTS_UPDATE:
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
									psCheckTable.setLong(1, dataObjectId);
									if (isCdb) {
										psCheckTable.setLong(2, conId);
									}
									ResultSet rsCheckTable = psCheckTable.executeQuery();
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
												isCdb ? (short) conId : null,
												tableOwner, tableName,
												"ENABLED".equalsIgnoreCase(rsCheckTable.getString("DEPENDENCIES")),
												schemaType, useOracdcSchemas, processLobs,
												isCdb, odd, partition, topic, topicNameStyle, topicNameDelimiter);
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
								final boolean multiLineSql = rsLogMiner.getBoolean("CSF");
								final long timestamp = rsLogMiner.getDate("TIMESTAMP").getTime();
								final String rowId = rsLogMiner.getString("ROW_ID");
								String sqlRedo;
								if (multiLineSql) {
									StringBuilder sb = new StringBuilder(16000);
									boolean moreRedoLines = multiLineSql;
									while (moreRedoLines) {
										sb.append(rsLogMiner.getString("SQL_REDO"));
										moreRedoLines = rsLogMiner.getBoolean("CSF");
										if (moreRedoLines) { 
											rsLogMiner.next();
										}
									}
									sqlRedo = sb.toString();
									sb = null;
								} else {
									sqlRedo = rsLogMiner.getString("SQL_REDO");
								}
								// squeeze it!
								sqlRedo = StringUtils.replace(sqlRedo, "HEXTORAW(", "");
								if (operation == OraLogMiner.V$LOGMNR_CONTENTS_INSERT) {
									sqlRedo = StringUtils.replace(sqlRedo, "')", "'");
								} else {
									sqlRedo = StringUtils.replace(sqlRedo, ")", "");
								}
								final OraCdcLogMinerStatement lmStmt = new  OraCdcLogMinerStatement(
										combinedDataObjectId, operation, sqlRedo, timestamp, lastScn, lastRsId, lastSsn, rowId);

								//BEGIN: Catch the LOB!!!
								List<OraCdcLargeObjectHolder> lobs = null;
								if (processLobs && oraTable.isWithLobs() &&
										(operation == OraLogMiner.V$LOGMNR_CONTENTS_INSERT ||
										operation == OraLogMiner.V$LOGMNR_CONTENTS_UPDATE)) {
									final String tableOperationRsId = rsLogMiner.getString("RS_ID");
									String lobStartRsId = tableOperationRsId; 
									boolean searchLobObjects = true;
									//TODO
									//TODO Ignore CDB here???
									//TODO
									Integer lobObjectId = 0;
									while (logMinerReady && searchLobObjects && runLatch.getCount() > 0) {
										searchLobObjects = rsLogMiner.next();
										isRsLogMinerRowAvailable = searchLobObjects;
										if (searchLobObjects) {
											final short catchLobOperation = rsLogMiner.getShort("OPERATION_CODE");
											final String catchLobXid = rsLogMiner.getString("XID");
											if (catchLobOperation == OraLogMiner.V$LOGMNR_CONTENTS_INSERT ||
													catchLobOperation == OraLogMiner.V$LOGMNR_CONTENTS_UPDATE ||
													catchLobOperation == OraLogMiner.V$LOGMNR_CONTENTS_DELETE) {
												// Next INSERT/UPDATE/DELETE for given objects.....
												// Do nothing and don't call next() for rsLogMiner
												fetchRsLogMinerNext = false;
												searchLobObjects = false;
											} else if ((catchLobOperation == OraLogMiner.V$LOGMNR_CONTENTS_COMMIT ||
													catchLobOperation == OraLogMiner.V$LOGMNR_CONTENTS_ROLLBACK) &&
													(catchLobXid.equals(xid) || activeTransactions.containsKey(catchLobXid))) {
												// Do nothing and don't call next() for rsLogMiner
												fetchRsLogMinerNext = false;
												searchLobObjects = false;
											} else {
												// Check for RS_ID of INSERT
												// Previous row contains: DATA_OBJ# = DATA_OBJD# = LOB_ID
												//                        RS_ID to call readLob!!!
												final String lobRsId = rsLogMiner.getString("RS_ID");
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
															catchLobXid,
															oraTable.getLobColumns().get(lobObjectId),
															isCdb ? rsLogMiner.getNUMBER("SRC_CON_UID") : null));
													if (lobWorker.isLogMinerExtended()) {
														//TODO
														//TODO Add SCN>= to MineSql!!!
														//TODO
														rsLogMiner = (OracleResultSet) psLogMiner.executeQuery();
														boolean rewind = true;
														while(rewind && rsLogMiner.next()) {
															if (rsLogMiner.getLong("SCN") == lobScn &&
																StringUtils.equals(rsLogMiner.getString("RS_ID"), lobRsId) &&
																rsLogMiner.getLong("SSN") == lobSsn) {
																fetchRsLogMinerNext = true;
																break;
															}
														}
													}
												} else {
													lobObjectId = rsLogMiner.getInt("DATA_OBJ#");
													lobStartRsId = rsLogMiner.getString("RS_ID");
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
								if (!(runLatch.getCount() > 0)) {
									LOGGER.debug("Breaking cycle in 'Catch the LOB!!!'");
									break;
								}
								//END: Catch the LOB!!!

								if (transaction == null) {
									if (LOGGER.isDebugEnabled()) {
										LOGGER.debug("New transaction {} created. Transaction start timestamp {}, first SCN {}.",
												xid, timestamp, lastScn);
									}
									transaction = new OraCdcTransaction(processLobs, queuesRoot, xid);
									activeTransactions.put(xid, transaction);
								}
								if (processLobs) {
									transaction.addStatement(lmStmt, lobs);
								} else {
									transaction.addStatement(lmStmt);
								}
								metrics.addRecord();
							}
							break;
						case OraLogMiner.V$LOGMNR_CONTENTS_INTERNAL:
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug("Skipping internal operation at SCN {} for object ID {}",
										lastScn, rsLogMiner.getLong("DATA_OBJ#"));
							}
							break;
						default:
							// SELECT_LOB_LOCATOR must be processed in inner loop before!!!
							LOGGER.error("Unknown operation {} at SCN {}, RS_ID '{}' for object ID {}",
									operation, lastScn, rsLogMiner.getString("RS_ID"), rsLogMiner.getLong("DATA_OBJ#"));
							throw new SQLException("Unknown operation in OraCdcLogMinerWorkerThread.run()");
						}
						// Copy again, to protect from exception...
						lastGuaranteedScn = lastScn;
						lastGuaranteedRsId = lastRsId;
						lastGuaranteedSsn = lastSsn;
						if (fetchRsLogMinerNext) {
							isRsLogMinerRowAvailable = rsLogMiner.next();
						}
					}
					logMiner.stop();
					rsLogMiner.close();
					rsLogMiner = null;
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
		runLatch.countDown();
		LOGGER.debug("call to shutdown() completed");
	}

	private void restoreOraConnection(SQLException sqle) {
		LOGGER.error("Error '{}' when waiting for next archived log.", sqle.getMessage());
		LOGGER.error("SQL errorCode = {}, SQL state = '{}'",
				sqle.getErrorCode(), sqle.getSQLState());
		if (sqle.getErrorCode() == ORA_17410) {
			// SQLSTATE = '08000'
			LOGGER.error("ORA-17410: No more data to read from socket");
			boolean ready = false;
			while (runLatch.getCount() > 0 && !ready) {
				LOGGER.debug("Waiting {} ms for RDBMS connection restore...", pollInterval);
				try {
					this.wait(pollInterval);
				} catch (InterruptedException ie) {
					LOGGER.error(ie.getMessage());
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(ie));
				}
				try {
					connLogMiner = OraPoolConnectionFactory.getLogMinerConnection();
					psLogMiner = connLogMiner.prepareStatement(
							mineDataSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
					if (processLobs) {
						psReadLob = (OraclePreparedStatement) connLogMiner.prepareStatement(
								isCdb ? OraDictSqlTexts.MINE_LOB_CDB :
										OraDictSqlTexts.MINE_LOB_NON_CDB,
								ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
					}
					logMiner.createStatements(connLogMiner);
					ready = true;
				} catch (SQLException getConnException) {
					LOGGER.error("Error '{}' when restoring connection, SQL errorCode = {}, SQL state = '{}'",
							sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
				}
			}
		}
	}

}