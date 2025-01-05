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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.Xid;
import solutions.a2.utils.ExceptionUtils;

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
	private OraCdcTransaction transaction;
	private boolean lastStatementInTransaction = true;
	private boolean staticObjIds;
	private OraCdcDictionaryChecker checker;

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc Redo Miner source task for connector {}.", connectorName);
		super.start(props);
		config.logMiner(false);

		try (Connection connDictionary = oraConnections.getConnection()) {
			metrics = new OraCdcRedoMinerMgmt(rdbmsInfo, connectorName);

			//TODO
			staticObjIds = true;
			List<String> excludeList = config.excludeObj();
			List<String> includeList = config.includeObj();

			processStoredSchemas(metrics);

			if (rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed()) {
				checkTableSql = OraDictSqlTexts.CHECK_TABLE_CDB + OraDictSqlTexts.CHECK_TABLE_CDB_WHERE_PARAM;
			} else {
				checkTableSql = OraDictSqlTexts.CHECK_TABLE_NON_CDB + OraDictSqlTexts.CHECK_TABLE_NON_CDB_WHERE_PARAM;
			}
			int[] includeObjIds = null;
			if (includeList != null && includeList.size() > 0) {
				final String tableList = OraSqlUtils.parseTableSchemaList(false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, includeList);
				includeObjIds = rdbmsInfo.getMineObjectsIds(
						false, tableList, connDictionary);
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
							connDictionary);
				if (excludeObjIds == null || excludeObjIds.length == 0) {
						LOGGER.error("a2.exclude parameter set to {} but there are no tables matching this condition.\nExiting.",
								StringUtils.join(config.excludeObj(), ","));
						throw new ConnectException("Please check value of a2.exclude parameter or remove it from configuration!");
				}
				final String tableList = OraSqlUtils.parseTableSchemaList(true, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, excludeList);
				checkTableSql += tableList;
			}
			MutableTriple<Long, RedoByteAddress, Long> coords = new MutableTriple<>();
			boolean rewind = startPosition(coords);
			activeTransactions = new HashMap<>();
			checker = new OraCdcDictionaryChecker(this,
					tablesInProcessing, tablesOutOfScope, checkTableSql, metrics);
			//TODO - we din't pass checker to worker thread (temporary!)
			worker = new OraCdcRedoMinerWorkerThread(
					this,
					rewind ? coords : new ImmutableTriple<>(coords.getLeft(), null, -1l),
					includeObjIds,
					excludeObjIds,
					activeTransactions,
					committedTransactions,
					metrics);
			if (rewind) {
				worker.rewind(coords.getLeft(), coords.getMiddle(), coords.getRight());
			}

		} catch (SQLException | InvalidPathException e) {
			LOGGER.error("Unable to start oracdc logminer task!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
		LOGGER.trace("Starting worker thread.");
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
			// Load data from archived redo...
			try (Connection connDictionary = oraConnections.getConnection()) {
				final OraCdcRedoMinerStatement stmt = new OraCdcRedoMinerStatement();
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
								OraTable4LogMiner oraTable = checker.getTable(stmt.getTableId());
								if (oraTable == null) {
									LOGGER.error(
											"\n=====================\n" +
											"Strange consistency issue for DATA_OBJ# {}, transaction XID {}, statement SCN={}, RS_ID='{}', SSN={}.\nExiting." +
											"\n=====================\n",
											stmt.getTableId(), transaction.getXid(), stmt.getScn(), stmt.getRba(), stmt.getSsn());
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
		isPollRunning.set(false);
		LOGGER.trace("END: poll()");
		return result;
	}

	@Override
	public void stop() {
		LOGGER.info("Stopping oracdc Redo Miner source task.");
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
				LOGGER.warn("Removing uncompleted transaction {}", name);
				transaction.close();
			});
		}
		super.stopEpilogue();
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