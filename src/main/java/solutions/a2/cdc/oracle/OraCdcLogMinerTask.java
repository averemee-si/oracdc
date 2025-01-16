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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.jmx.OraCdcInitialLoad;
import solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmt;
import solutions.a2.cdc.oracle.schema.FileUtils;
import solutions.a2.cdc.oracle.utils.OraSqlUtils;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcLogMinerTask extends OraCdcTaskBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerTask.class);

	private String stateFileName;
	private OraCdcLogMinerMgmt metrics;
	private Map<String, OraCdcTransaction> activeTransactions;
	private OraCdcTransaction transaction;
	private boolean lastStatementInTransaction = true;
	private boolean execInitialLoad = false;
	private String initialLoadStatus = ParamConstants.INITIAL_LOAD_IGNORE;
	private OraCdcInitialLoadThread initialLoadWorker;
	private BlockingQueue<OraTable4InitialLoad> tablesQueue;
	private OraTable4InitialLoad table4InitialLoad;
	private boolean lastRecordInTable = true;
	private OraCdcInitialLoad initialLoadMetrics;
	private OraCdcDictionaryChecker checker;

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc logminer source task for connector {}.", connectorName);
		super.start(props);

		try (Connection connDictionary = oraConnections.getConnection()) {

			metrics = new OraCdcLogMinerMgmt(rdbmsInfo, connectorName, this);
			OraCdcPseudoColumnsProcessor pseudoColumns = config.pseudoColumnsProcessor();
			processStoredSchemas(metrics);

			// Initial load
			if (StringUtils.equalsIgnoreCase(
					ParamConstants.INITIAL_LOAD_EXECUTE,
					config.getString(ParamConstants.INITIAL_LOAD_PARAM))) {
				execInitialLoad = true;
				initialLoadStatus = ParamConstants.INITIAL_LOAD_EXECUTE;
				final Map<String, Object> offsetFromKafka = context.offsetStorageReader().offset(rdbmsInfo.partition());
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

			List<String> excludeList = config.excludeObj();
			if (excludeList.size() < 1)
				excludeList = null;
			List<String> includeList = config.includeObj();
			if (includeList.size() < 1)
				includeList = null;
			final boolean tableListGenerationStatic = config.staticObjIds();
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
			MutableTriple<Long, RedoByteAddress, Long> coords = new MutableTriple<>();
			boolean rewind = startPosition(coords);
			activeTransactions = new HashMap<>();

			checker = new OraCdcDictionaryChecker(this,
					tablesInProcessing, tablesOutOfScope, checkTableSql, metrics);

			worker = new OraCdcLogMinerWorkerThread(this,
					checker, coords.getLeft(), mineDataSql, activeTransactions,
					committedTransactions, metrics);
			if (rewind) {
				worker.rewind(coords.getLeft(), coords.getMiddle(), coords.getRight());
			}

			if (execInitialLoad) {
				LOGGER.debug("Initial load table list SQL {}", initialLoadSql);
				tablesQueue = new LinkedBlockingQueue<>();
				buildInitialLoadTableList(initialLoadSql);
				initialLoadMetrics = new OraCdcInitialLoad(rdbmsInfo, connectorName);
				initialLoadWorker = new OraCdcInitialLoadThread(
						WAIT_FOR_WORKER_MILLIS,
						coords.getLeft(),
						tablesInProcessing,
						config,
						rdbmsInfo,
						initialLoadMetrics,
						tablesQueue,
						oraConnections);
			}


		} catch (SQLException e) {
			LOGGER.error("Unable to start oracdc logminer task!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
		LOGGER.trace("Starting worker thread.");
		if (execInitialLoad) {
			initialLoadWorker.start();
		}
		worker.start();
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
										lastInProgressRba.equals(stmt.getRba()) &&
										stmt.getSsn() == lastInProgressSubScn) {
									// Rewind completed
									break;
								}
								if (!processTransaction) {
									LOGGER.error("Unable to rewind transaction {} with COMMIT_SCN={} till requested {}:'{}':{}!",
											transaction.getXid(), transaction.getCommitScn(), lastInProgressScn, lastInProgressRba, lastInProgressSubScn);
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
								final OraTable4LogMiner oraTable = checker.getTable(stmt.getTableId());
								if (oraTable == null) {
									checker.printConsistencyError(transaction, stmt);
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
		LOGGER.info("Stopping oracdc logminer source task.");
		super.stop(true);
		if (initialLoadWorker != null && initialLoadWorker.isRunning()) {
			initialLoadWorker.shutdown();
		}
		if (activeTransactions != null && activeTransactions.isEmpty()) {
			if (worker != null && worker.lastRba() != null && worker.lastScn() > 0) {
				putReadRestartScn(Triple.of(
						worker.lastScn(),
						worker.lastRba(),
						worker.lastSubScn()));
			}
		}
		if (activeTransactions != null && !activeTransactions.isEmpty()) {
			// Clean it!
			activeTransactions.forEach((name, transaction) -> {
				LOGGER.warn("Removing uncompleted transaction {}", name);
				transaction.close();
			});
		}
		super.stopEpilogue();
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
		ops.setLastScn(worker.lastScn());
		ops.setLastRsId(worker.lastRba());
		ops.setLastSsn(worker.lastSubScn());
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
							config, rdbmsInfo, connection);
					tablesInProcessing.put(combinedDataObjectId, oraTable);
				}
			}
		} catch (SQLException sqle) {
			throw new SQLException(sqle);
		}
	}

}