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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.jmx.OraCdcSourceConnMgmt;
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

	private OraCdcSourceConnMgmt metrics;
	private Map<String, OraCdcTransaction> activeTransactions;
	private final OraCdcLogMinerStatement stmt = new OraCdcLogMinerStatement();
	private final List<OraCdcLargeObjectHolder> lobs = new ArrayList<>();


	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc logminer source task for connector {}.", connectorName);
		super.start(props);

		try (Connection connDictionary = oraConnections.getConnection()) {

			OraCdcPseudoColumnsProcessor pseudoColumns = config.pseudoColumnsProcessor();

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
					if (Strings.CS.contains(objectList, "()")) {
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
					if (Strings.CS.contains(objectList, "()")) {
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

			metrics = new OraCdcSourceConnMgmt(rdbmsInfo, connectorName, "LogMiner-metrics");
			checker = new OraCdcDictionaryChecker(this,
					tablesInProcessing, tablesOutOfScope, checkTableSql, metrics);

			worker = new OraCdcLogMinerWorkerThread(this,
					checker, coords.getLeft(), mineDataSql, activeTransactions,
					committedTransactions, metrics);
			if (rewind) {
				worker.rewind(coords.getLeft(), coords.getMiddle(), coords.getRight());
			}

			if (execInitialLoad) {
				prepareInitialLoadWorker(initialLoadSql, coords.getLeft());
			}

		} catch (SQLException e) {
			LOGGER.error("Unable to start oracdc logminer task!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
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
		result.clear();
		if (processLobs) {
			lobs.clear();
		}
		if (execInitialLoad) {
			if (executeInitialLoad())
				return null;
		} else {
			// Load data from archived redo...
			try (Connection connDictionary = oraConnections.getConnection()) {
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
								final OraTable4LogMiner oraTable = (OraTable4LogMiner) checker.getTable(stmt.getTableId());
								if (oraTable == null) {
									checker.printConsistencyError(transaction, stmt);
									isPollRunning.set(false);
									throw new ConnectException("Strange consistency issue!!!");
								} else {
									try {
										if (stmt.getOperation() == OraCdcV$LogmnrContents.DDL) {
											final long ddlStartTs = System.currentTimeMillis();
											final Connection connection = oraConnections.getConnection();
											final int changedColumnCount = 
													oraTable.processDdl(connection, stmt, transaction.getXid(), transaction.getCommitScn());
											connection.close();
											putTableVersion(stmt.getTableId(), oraTable.version());
											metrics.addDdlMetrics(changedColumnCount, (System.currentTimeMillis() - ddlStartTs));
										} else {
											final long startParseTs = System.currentTimeMillis();
											putInProgressOffsets(stmt);
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
							putCompletedOffset();
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

}