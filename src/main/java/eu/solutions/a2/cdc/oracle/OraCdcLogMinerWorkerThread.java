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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMBeanServer;
import eu.solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmt;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.OraSqlUtils;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcLogMinerWorkerThread extends Thread {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerWorkerThread.class);

	private final int pollInterval;
	private final OraRdbmsInfo rdbmsInfo;
	private final OraCdcLogMinerMgmt metrics;
	private final CountDownLatch runLatch;
	private boolean logMinerReady = false;
	private final Map<String, String> partition;
	private final Map<Integer, OraTable> tablesInProcessing;
	private final Set<Integer> tablesOutOfScope;
	private final int schemaType;
	private final String topic;
	private final OraDumpDecoder odd;
	private final OraLogMiner logMiner;
	private final Connection connLogMiner;
	private final PreparedStatement psLogMiner;
	private ResultSet rsLogMiner;
	private final Connection connDictionary;
	private final PreparedStatement psCheckTable;
	private final Path queuesRoot;
	private final Map<String, OraCdcTransaction> activeTransactions;
	private final BlockingQueue<OraCdcTransaction> committedTransactions;

	public OraCdcLogMinerWorkerThread(
			final int pollInterval,
			final Map<String, String> partition,
			final SourceTaskContext taskContext,
			final List<String> includeList,
			final List<String> excludeList,
			final Long redoSizeThreshold,
			final Integer redoFilesCount,
			final Map<Integer, OraTable> tablesInProcessing,
			final int schemaType,
			final String topic,
			final OraDumpDecoder odd,
			final Path queuesRoot,
			final BlockingQueue<OraCdcTransaction> committedTransactions) {
		LOGGER.info("Initializing oracdc logminer archivelog worker thread");
		this.setName("OraCdcLogMinerWorkerThread-" + System.nanoTime());
		this.pollInterval = pollInterval;
		this.partition = partition;
		this.tablesInProcessing = tablesInProcessing;
		this.queuesRoot = queuesRoot;
		this.odd = odd;
		this.schemaType = schemaType;
		this.topic = topic;
		this.committedTransactions = committedTransactions;
		runLatch = new CountDownLatch(1);
		tablesOutOfScope = new HashSet<>();
		activeTransactions = new HashMap<>();
		try {
			connLogMiner = OraPoolConnectionFactory.getLogMinerConnection();
			connDictionary = OraPoolConnectionFactory.getConnection();

			rdbmsInfo = OraRdbmsInfo.getInstance();
			metrics = OraCdcLogMinerMBeanServer.getInstance().getMbean();

			LOGGER.trace("Checking for stored offset...");
			final Map<String, Object> offset = taskContext.offsetStorageReader().offset(partition);
			final Map<String, String> props = taskContext.configs();
			/*final*/ long firstScn;
			boolean rewind = false;
			String rsId = null;
			long ssn = -1;
			if (offset != null) {
				if (props.containsKey(ParamConstants.LGMNR_START_SCN_PARAM)) {
					firstScn = Long.parseLong(props.get(ParamConstants.LGMNR_START_SCN_PARAM));
					LOGGER.info("Ignoring SCN value from offset file and setting it to {} from connector properties", firstScn);
				} else {
					rewind = true;
					firstScn = (long) offset.get("SCN");
					rsId = (String) offset.get("RS_ID");
					ssn = (long) offset.get("SSN");
					LOGGER.info("Using values from offset file: SCN = {}, RS_ID = '{}', SSN = {} .",
							firstScn, rsId, ssn);
				}
			} else {
				if (props.containsKey(ParamConstants.LGMNR_START_SCN_PARAM)) {
					firstScn = Long.parseLong(props.get(ParamConstants.LGMNR_START_SCN_PARAM));
					LOGGER.info("Using first SCN value {} from connector properties.", firstScn);
				} else {
					firstScn = OraRdbmsInfo.firstScnFromArchivedLogs(connLogMiner);
					LOGGER.info("Using min(FIRST_CHANGE#) from V$ARCHIVED_LOG = {} as first SCN value.", firstScn);
				}
			}

			if (redoSizeThreshold != null) {
				logMiner = new OraLogMiner(connLogMiner, metrics, firstScn, redoSizeThreshold);
			} else {
				logMiner = new OraLogMiner(connLogMiner, metrics, firstScn, redoFilesCount);
			}
			if (logMiner.getDbId() == rdbmsInfo.getDbId()) {
				LOGGER.debug("Database Id for dictionary and mining connections: {}", logMiner.getDbId());
				if (logMiner.isDictionaryAvailable()) {
					LOGGER.trace("Mining database {} is in OPEN mode", logMiner.getDbUniqueName());
					if (logMiner.getDbUniqueName().equals(rdbmsInfo.getDbUniqueName())) {
						LOGGER.trace("Same database will be used for dictionary query and mining");
					} else {
						LOGGER.trace("Active DataGuard database {} will be used for mining", logMiner.getDbUniqueName());
					}
				} else {
					LOGGER.trace("Mining database {} is in MOUNT mode", logMiner.getDbUniqueName());
					LOGGER.trace("DataGuard database {} will be used for mining", logMiner.getDbUniqueName());
				}
			} else {
				throw new ConnectException("Unable to mine data from databases with different DBID!!!");
			}

			String mineDataSql = null;
			String checkTableSql = null;
			if (rdbmsInfo.isCdb()) {
				mineDataSql = OraDictSqlTexts.MINE_DATA_CDB;
				checkTableSql = OraDictSqlTexts.CHECK_TABLE_CDB;
			} else {
				mineDataSql = OraDictSqlTexts.MINE_DATA_NON_CDB;
				checkTableSql = OraDictSqlTexts.CHECK_TABLE_NON_CDB;
			}
			if (includeList != null) {
				mineDataSql += "where (OPERATION_CODE in (1,2,3) " + 
						OraRdbmsInfo.getMineObjectsIds(connDictionary, false,
								OraSqlUtils.parseTableSchemaList(false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, includeList)) +
						")";
				checkTableSql += OraSqlUtils.parseTableSchemaList(false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, includeList);
			}
			if (excludeList != null) {
				if (includeList != null) {
					mineDataSql += " and ";
				} else {
					mineDataSql += " where ";
				}
				mineDataSql += "(OPERATION_CODE in (1,2,3) " +
						OraRdbmsInfo.getMineObjectsIds(connDictionary, true,
								OraSqlUtils.parseTableSchemaList(false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, excludeList)) +
						")";
				checkTableSql += OraSqlUtils.parseTableSchemaList(true, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, excludeList);

			}
			if (includeList == null && excludeList == null) {
				mineDataSql += "where (OPERATION_CODE in (1,2,3) ";
			}
			// Finally - COMMIT and ROLLBACK
			mineDataSql += " or OPERATION_CODE in (7,36)";
			LOGGER.debug("Mining SQL = {}", mineDataSql);
			LOGGER.debug("Dictionary check SQL = {}", checkTableSql);

			// Finally - prepare for mining...
			psLogMiner = connLogMiner.prepareStatement(
					mineDataSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			psCheckTable = connDictionary.prepareStatement(
					checkTableSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			logMinerReady = logMiner.next();
			if (rewind) {
				if (logMinerReady) {
					LOGGER.info("Rewinding LogMiner ResultSet to first position after CSN = {}, RS_ID = '{}', SSN = {}.",
							firstScn, rsId, ssn);
					rsLogMiner = psLogMiner.executeQuery();
					int recordCount = 0;
					long rewindElapsed = System.currentTimeMillis();
					while (rewind) {
						if (rsLogMiner.next()) {
							recordCount++;
							if (firstScn == rsLogMiner.getLong("SCN") &&
									rsId.equals(rsLogMiner.getString("RS_ID")) &&
									ssn == rsLogMiner.getLong("SSN") &&
									!rsLogMiner.getBoolean("CSF")) {
								rewind = false;
							}
						} else {
							LOGGER.error("Incorrect rewind to CSN = {}, RS_ID = '{}', SSN = {}",
									firstScn, rsId, ssn);
							throw new ConnectException("Incorrect rewind operation!!!");
						}
					}
					rewindElapsed = System.currentTimeMillis() - rewindElapsed;
					LOGGER.info("Total records scipped while rewinding: {}, elapsed time ms: {}", recordCount, rewindElapsed);
				} else {
					LOGGER.info("Values from offset (CSN = {}, RS_ID = '{}', SSN = {}) ignored, waiting for new archived log.",
							firstScn, rsId, ssn);
				}
			}
		} catch (SQLException | SecurityException e) {
			LOGGER.error("Unable to start logminer archivelog worker thread!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
	}

	@Override
	public void run()  {
		LOGGER.trace("BEGIN: run()");
		while (runLatch.getCount() > 0) {
			try {
			if (logMinerReady) {
				if (rsLogMiner == null) {
					rsLogMiner = psLogMiner.executeQuery();
				}
				final long readStartMillis = System.currentTimeMillis(); 
				while (rsLogMiner.next()) {
					final short operation = rsLogMiner.getShort("OPERATION_CODE");
					final String xid = rsLogMiner.getString("XID");
					OraCdcTransaction transaction = activeTransactions.get(xid);
					if (operation == OraLogMiner.V$LOGMNR_CONTENTS_COMMIT) {
						if (transaction != null) {
							transaction.setCommitScn(rsLogMiner.getLong("COMMIT_SCN"));
							committedTransactions.add(transaction);
							activeTransactions.remove(xid);
							metrics.addCommittedRecords(transaction.length());
						} else {
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug("Skipping commit with transaction XID {}", xid);
							}
						}
					} else if (operation == OraLogMiner.V$LOGMNR_CONTENTS_ROLLBACK) {
						if (transaction != null) {
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug("Rolling back transaction {} with {} records.", xid, transaction.length());
							}
							metrics.addRolledBackRecords(transaction.length());
							transaction.close();
							activeTransactions.remove(xid);
						}
					} else {
						final int dataObjectId = rsLogMiner.getInt("DATA_OBJ#");
						OraTable oraTable = tablesInProcessing.get(dataObjectId);
						if (oraTable == null && !tablesOutOfScope.contains(dataObjectId)) {
							psCheckTable.setInt(1, dataObjectId);
							if (rdbmsInfo.isCdb()) {
								psCheckTable.setShort(2, rsLogMiner.getShort("CON_ID"));
							}
							ResultSet rsCheckTable = psCheckTable.executeQuery();
							if (rsCheckTable.next()) {
								final String tableName = rsCheckTable.getString("TABLE_NAME");
								final String tableOwner = rsCheckTable.getString("OWNER");
								final String tableFqn = tableOwner + "." + tableName;
								final String tableTopic;
								if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
									if (topic == null || "".equals(topic)) {
										tableTopic = tableName;
									} else {
										tableTopic = topic + "_" + tableName;
									}
								} else {
									// ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM
									tableTopic = topic;
								}
								if (rdbmsInfo.isCdb()) {
									final String pdbName = rsCheckTable.getString("PDB_NAME");
									oraTable = new OraTable(
											pdbName, rsLogMiner.getShort("CON_ID"),
											tableOwner, tableName,
											schemaType, rdbmsInfo.isCdb(), odd, partition, tableTopic);
										tablesInProcessing.put(dataObjectId, oraTable);
										metrics.addTableInProcessing(pdbName + ":" + tableFqn);
								} else {
									oraTable = new OraTable(
										null, null,
										tableOwner, tableName,
										schemaType, rdbmsInfo.isCdb(), odd, partition, tableTopic);
									tablesInProcessing.put(dataObjectId, oraTable);
									metrics.addTableInProcessing(tableFqn);
								}
							} else {
								tablesOutOfScope.add(dataObjectId);
								metrics.addTableOutOfScope();
							}
							rsCheckTable.close();
							rsCheckTable = null;
							psCheckTable.clearParameters();
						}

						if (oraTable != null) {
							final boolean multiLineSql = rsLogMiner.getBoolean("CSF");
							final long scn = rsLogMiner.getLong("SCN");
							final long timestamp = rsLogMiner.getDate("TIMESTAMP").getTime();
							final String rsId = rsLogMiner.getString("RS_ID");
							final int ssn = rsLogMiner.getInt("SSN");
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
									dataObjectId, operation, sqlRedo, timestamp, scn, rsId, ssn, rowId);

							if (transaction == null) {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("New transaction {} created. Transaction start timestamp {}, first SCN {}.",
											xid, timestamp, scn);
								}
								transaction = new OraCdcTransaction(queuesRoot, xid, lmStmt);
								activeTransactions.put(xid, transaction);
							} else {
								transaction.addStatement(lmStmt);
							}
							metrics.addRecord();
						}
					}

				}
				logMiner.stop();
				rsLogMiner.close();
				rsLogMiner = null;
				// Count archived redo log(s) read time
				metrics.addRedoReadMillis(System.currentTimeMillis() - readStartMillis);
				logMinerReady = logMiner.next();
			} else {
				while (!logMinerReady) {
					synchronized (this) {
						LOGGER.trace("Waiting {} ms", pollInterval);
						try {
							this.wait(pollInterval);
						} catch (InterruptedException ie) {
							LOGGER.error(ie.getMessage());
							LOGGER.error(ExceptionUtils.getExceptionStackTrace(ie));
						}
						logMinerReady = logMiner.next();
					}
				}
			}
			} catch (SQLException | IOException e) {
				LOGGER.error(e.getMessage());
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
				throw new ConnectException(e);
			}
		}
	}

	public void shutdown() {
		LOGGER.info("Stopping oracdc logminer archivelog worker thread.");
		runLatch.countDown();
		boolean throwError = false;
		if (committedTransactions.size() > 0) {
			LOGGER.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			LOGGER.error("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			LOGGER.error("Unprocessed committed transactions for processing!!!");
			LOGGER.error("Please check information below and set a2.first.change to appropriate value!!!");
			LOGGER.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			OraCdcTransaction transaction = null;
			do {
				transaction = committedTransactions.poll();
				if (transaction != null) {
					LOGGER.error("\tUnprocessed committed transaction XID {}, first change {}, commit SCN {}, number of rows {}.",
							transaction.getXid(), transaction.getFirstChange(), transaction.getCommitScn(), transaction.length());
					//TODO
					//TODO persistence???
					//TODO
					transaction.close();
				}
			} while (transaction != null);
			LOGGER.error("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			throwError = true;
		}
		if (!activeTransactions.isEmpty()) {
			LOGGER.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			LOGGER.error("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			LOGGER.error("Unprocessed transactions found!!!");
			LOGGER.error("Please check information below and set a2.first.change to appropriate value!!!");
			LOGGER.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			activeTransactions.forEach((xid, transaction) -> {
				LOGGER.error("\tIn process transaction XID {}, first change {}, current number of rows {}.",
						transaction.getXid(), transaction.getFirstChange(), transaction.length());
				//TODO
				//TODO persistence???
				//TODO
				transaction.close();
			});
			LOGGER.error("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			throwError = true;
		}
		if (throwError) {
			throw new ConnectException("Unprocessed transactions left!!!\n" + 
										"Please check connector log files!!!");
		}
	}

}