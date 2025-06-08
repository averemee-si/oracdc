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

import java.nio.file.InvalidPathException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.jmx.OraCdcRedoMinerMgmt;
import solutions.a2.cdc.oracle.utils.OraSqlUtils;
import solutions.a2.oracle.internals.LobId;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.Xid;
import solutions.a2.utils.ExceptionUtils;

import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_1013;
import static solutions.a2.cdc.oracle.OraCdcSourceBaseConfig.TABLE_EXCLUDE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcSourceBaseConfig.TABLE_INCLUDE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.TABLE_LIST_STYLE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.TABLE_LIST_STYLE_STATIC;
import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.TABLE_LIST_STYLE_DYNAMIC;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcRedoMinerTask extends OraCdcTaskBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoMinerTask.class);

	private OraCdcRedoMinerMgmt metrics;
	private Map<Xid, OraCdcTransaction> activeTransactions;
	private String checkTableSql;
	private final OraCdcRedoMinerStatement stmt = new OraCdcRedoMinerStatement();

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc Redo Miner source task for connector {}.", connectorName);
		super.start(props);
		config.logMiner(false);

		try (Connection connDictionary = oraConnections.getConnection()) {
			metrics = new OraCdcRedoMinerMgmt(rdbmsInfo, connectorName);

			List<String> excludeList = config.excludeObj();
			List<String> includeList = config.includeObj();

			processStoredSchemas(metrics);

			final int[] conUids;
			if (rdbmsInfo.isCdb()) {
				if (rdbmsInfo.isCdbRoot()) {
					checkTableSql = OraDictSqlTexts.CHECK_TABLE_CDB + OraDictSqlTexts.CHECK_TABLE_CDB_WHERE_PARAM;
					conUids = rdbmsInfo.getConUidsArray(connDictionary);
				} else {
					checkTableSql = OraDictSqlTexts.CHECK_TABLE_NON_CDB + OraDictSqlTexts.CHECK_TABLE_NON_CDB_WHERE_PARAM;
					conUids = new int[1];
					conUids[0] = rdbmsInfo.conUid();
				}
			} else {
				checkTableSql = OraDictSqlTexts.CHECK_TABLE_NON_CDB + OraDictSqlTexts.CHECK_TABLE_NON_CDB_WHERE_PARAM;
				conUids = null;
			}
			int[] includeObjIds = null;
			if (includeList != null && includeList.size() > 0) {
				final String tableList = OraSqlUtils.parseTableSchemaList(false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, includeList);
				includeObjIds = rdbmsInfo.getMineObjectsIds(
						false, tableList, connDictionary, processLobs);
				if (includeObjIds == null || includeObjIds.length == 0) {
						LOGGER.error("a2.include parameter set to {} but there are no tables matching this condition.\nExiting.",
								StringUtils.join(config.includeObj(), ","));
						throw new ConnectException("Please check value of a2.include parameter or remove it from configuration!");
				}
				checkTableSql += tableList;
			}
			int[] excludeObjIds = null;
			if (excludeList != null && excludeList.size() > 0) {
				excludeObjIds = rdbmsInfo.getMineObjectsIds(true,
							OraSqlUtils.parseTableSchemaList(false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, excludeList),
							connDictionary, processLobs);
				if (excludeObjIds == null || excludeObjIds.length == 0) {
						LOGGER.error("a2.exclude parameter set to {} but there are no tables matching this condition.\nExiting.",
								StringUtils.join(config.excludeObj(), ","));
						throw new ConnectException("Please check value of a2.exclude parameter or remove it from configuration!");
				}
				final String tableList = OraSqlUtils.parseTableSchemaList(true, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, excludeList);
				checkTableSql += tableList;
			}
			if (config.staticObjIds() &&
					(includeObjIds == null || includeObjIds.length == 0)) {
				LOGGER.error(
						"\n=====================\n" +
						"Parameter {} is set to '{}' (default value) and parameter {} is not set.\n" + 
						"Set the desired value for parameter {} and, if desired, set parameter {},\n" +
						"or set parameter {} to '{}'" +
						"\n=====================\n",
						TABLE_LIST_STYLE_PARAM, TABLE_LIST_STYLE_STATIC, TABLE_INCLUDE_PARAM,
						TABLE_INCLUDE_PARAM, TABLE_EXCLUDE_PARAM,
						TABLE_LIST_STYLE_PARAM, TABLE_LIST_STYLE_DYNAMIC);
				throw new ConnectException("Check oracdc parameters!");
			}
			MutableTriple<Long, RedoByteAddress, Long> coords = new MutableTriple<>();
			boolean rewind = startPosition(coords);
			activeTransactions = new HashMap<>();
			checker = new OraCdcDictionaryChecker(this,
					tablesInProcessing, tablesOutOfScope, checkTableSql, metrics);
			worker = new OraCdcRedoMinerWorkerThread(
					this,
					rewind ? coords : new ImmutableTriple<>(coords.getLeft(), null, -1l),
					includeObjIds,
					excludeObjIds,
					conUids,
					checker,
					activeTransactions,
					committedTransactions,
					metrics,
					rewind);

		} catch (SQLException | InvalidPathException e) {
			LOGGER.error("Unable to start oracdc logminer task!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
		LOGGER.trace("Starting worker thread.");
		worker.start();
		taskThreadId.set(Thread.currentThread().getId());
		return;
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

		try {
			Connection connDictionary;
			if (restoreIncompleteRecord)
				connDictionary = oraConnections.getConnection();
			else
				connDictionary = null;
			int recordCount = 0;
			int parseTime = 0;
			while (recordCount < batchSize) {
				if (lastStatementInTransaction) {
					// End of transaction, need to poll new
					transaction = committedTransactions.poll();
				}
				if (transaction == null) {
					// No more records produced by RedoMiner worker
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
					final Set<LobId> lobIds = useChronicleQueue
								? ((OraCdcTransactionChronicleQueue)transaction).lobIds(false)
								: null;
					do {
						processTransaction = transaction.getStatement(stmt);
						lastStatementInTransaction = !processTransaction;

						if (processTransaction && runLatch.getCount() > 0) {
							OraTable4LogMiner oraTable = checker.getTable(stmt.getTableId());
							if (oraTable == null) {
								checker.printConsistencyError(transaction, stmt);
								isPollRunning.set(false);
								runLatch.countDown();
								throw new ConnectException("Strange consistency issue!!!");
							}
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
											stmt, transaction, lobIds, offset, connDictionary);
									if (record != null) {
										result.add(record);
										recordCount++;
									}
									parseTime += (System.currentTimeMillis() - startParseTs);
								}
							} catch (SQLException sqle) {
								isPollRunning.set(false);
								if (sqle.getErrorCode() != ORA_1013) {
									LOGGER.error(sqle.getMessage());
									LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
									throw new ConnectException(sqle);
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
				try {
					LOGGER.debug("Waiting {} ms", pollInterval);
					Thread.sleep(pollInterval);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
				}
			} else {
				metrics.addSentRecords(result.size(), parseTime);
			}
			if (restoreIncompleteRecord) {
				connDictionary.close();
				connDictionary = null;
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
		isPollRunning.set(false);
		LOGGER.trace("END: poll()");
		return result;
	}

	@Override
	public void stop() {
		LOGGER.info("Stopping oracdc Redo Miner source task.");
		if (taskThreadId.longValue() == Thread.currentThread().getId()) {
			super.stop(true);
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
					LOGGER.warn("Removing uncompleted transaction {} with size {}, first SCN {}",
							name, transaction.size(), transaction.getFirstChange());
					transaction.close();
				});
			}
			super.stopEpilogue();
		}
	}

}