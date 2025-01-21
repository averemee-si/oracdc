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
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.internals.OraCdcChange;
import solutions.a2.cdc.oracle.internals.OraCdcChangeRowOp;
import solutions.a2.cdc.oracle.internals.OraCdcChangeUndo;
import solutions.a2.cdc.oracle.internals.OraCdcChangeUndoBlock;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLog;
import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
import solutions.a2.cdc.oracle.jmx.OraCdcRedoMinerMgmt;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;
import solutions.a2.oracle.internals.Xid;
import solutions.a2.oracle.utils.BinaryUtils;
import solutions.a2.utils.ExceptionUtils;

import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UNSUPPORTED;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KCOCODRW;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KDO_ORP_IRP_NULL_POS;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KDO_URP_NULL_POS;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KDO_KDOM2;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_10_SKL;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_11_QMI;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_12_QMD;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_16_LMN;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_2_IRP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_3_DRP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_4_LKR;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_5_URP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_6_ORP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_8_CFA;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgFirstPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgHeadPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgLastPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgNextPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgPrevPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.formatOpCode;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.printFbFlags;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcRedoMinerWorkerThread extends OraCdcWorkerThreadBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoMinerWorkerThread.class);

	private final OraCdcRedoMinerTask task;
	private final OraCdcRedoMinerMgmt metrics;
	private boolean redoMinerReady = false;
	private final OraRedoMiner redoMiner;
	private Connection connDictionary;
	private final Map<Xid, OraCdcTransaction> activeTransactions;
	private final Map<Integer, Xid> prefixedTransactions;
	private final TreeMap<Xid, Triple<Long, RedoByteAddress, Long>> sortedByFirstScn;
	private final ActiveTransComparator activeTransComparator;
	private final BinaryUtils bu;

	private Iterator<OraCdcRedoRecord> miner = null;

	private final Map<Integer, Deque<RowChangeHolder>> halfDone;
	private final boolean staticObjIds;
	private final int[] includeObjIds;
	private final boolean includeFilter;
	private final int[] excludeObjIds;
	private final boolean excludeFilter;
	private final int[] conUids;
	private final boolean conFilter;
	private final OraCdcDictionaryChecker checker;

	public OraCdcRedoMinerWorkerThread(
			final OraCdcRedoMinerTask task,
			final Triple<Long, RedoByteAddress, Long> startFrom,
			final int[] includeObjIds,
			final int[] excludeObjIds,
			final int[] conUids,
			final OraCdcDictionaryChecker checker,
			final Map<Xid, OraCdcTransaction> activeTransactions,
			final BlockingQueue<OraCdcTransaction> committedTransactions,
			final OraCdcRedoMinerMgmt metrics) throws SQLException {
		super(task.runLatch(), task.rdbmsInfo(), task.config(),
				task.oraConnections(), committedTransactions);
		LOGGER.info("Initializing oracdc Redo Miner worker thread");
		this.setName("OraCdcRedoMinerWorkerThread-" + System.nanoTime());
		this.task = task;
		this.activeTransactions = activeTransactions;
		this.metrics = metrics;
		this.halfDone = new HashMap<>();
		this.includeObjIds = includeObjIds;
		this.staticObjIds = config.staticObjIds();
		if (includeObjIds == null || includeObjIds.length == 0)
			includeFilter = false;
		else
			includeFilter = true;
		this.excludeObjIds = excludeObjIds;
		if (excludeObjIds == null || excludeObjIds.length == 0)
			excludeFilter = false;
		else
			excludeFilter = true;
		this.conUids = conUids;
		if (conUids == null || conUids.length == 0)
			conFilter = false;
		else
			conFilter = true;
		this.checker = checker;
		activeTransComparator = new ActiveTransComparator(activeTransactions);
		sortedByFirstScn = new TreeMap<>(activeTransComparator);
		prefixedTransactions = new HashMap<>();
		this.bu = BinaryUtils.get(rdbmsInfo.littleEndian());

		try {
			connDictionary = oraConnections.getConnection();
			redoMiner = new OraRedoMiner(
					connDictionary, metrics, startFrom, config, runLatch, rdbmsInfo, oraConnections, bu);
			// Finally - prepare for mining...
			redoMinerReady = redoMiner.next();

		} catch (SQLException e) {
			LOGGER.error(
					"\n\nUnable to start OraCdcRedoMinerWorkerThread !\n" +
					"SQL Error ={}, SQL State = {}, SQL Message = '{}'\n\n",
					e.getErrorCode(), e.getSQLState(), e.getMessage());
			throw e;
		}
	}

	@Override
	public void rewind(final long firstScn, final RedoByteAddress firstRba, final long firstSubScn) throws SQLException {
		//TODO
		//TODO
		//TODO Must be rewritten to file specific!!!
		//TODO
		//TODO
		if (redoMinerReady) {
			LOGGER.info("Move through file to first position after SCN= {}, RBA={}, SSN={}.",
					firstScn, firstRba, firstSubScn);
			miner = redoMiner.iterator();
			int recordCount = 0;
			long rewindElapsed = System.currentTimeMillis();
			boolean rewindNeeded = true;
			lastScn = firstScn;
			lastRba = firstRba;
			lastSubScn = firstSubScn;
			while (rewindNeeded) {
				if (miner.hasNext()) {
					final OraCdcRedoRecord rr = miner.next();
					lastScn = rr.scn();
					lastRba = rr.rba();
					lastSubScn = rr.subScn();
					recordCount++;
					if (firstScn == lastScn &&
							(firstRba == null || firstRba.equals(lastRba)) &&
							(firstSubScn == -1 || firstSubScn == lastSubScn)) {
						rewindNeeded = false;
						break;
					}
				} else {
					LOGGER.error("Incorrect rewind to SCN = {}, RBA = {}, SSN = {}",
							firstScn, firstRba, firstSubScn);
					throw new SQLException("Incorrect rewind operation!!!");
				}
			}
			rewindElapsed = System.currentTimeMillis() - rewindElapsed;
			LOGGER.info("Total records skipped while rewinding: {}, elapsed time ms: {}", recordCount, rewindElapsed);
		} else {
			LOGGER.info("Values from offset (SCN = {}, RS_ID = '{}', SSN = {}) ignored, waiting for new redo log.",
					firstScn, firstRba, firstSubScn);
		}
	}

	@Override
	public void run()  {
		LOGGER.info("BEGIN: OraCdcRedoMinerWorkerThread.run()");
		running.set(true);
		boolean firstTransaction = true;
		boolean notFirstRecord = false;
		while (runLatch.getCount() > 0) {
			long lastGuaranteedScn = 0;
			RedoByteAddress lastGuaranteedRsId = null;
			long lastGuaranteedSsn = 0;
			Xid xid = null;
			try {
				if (redoMinerReady) {
					miner = redoMiner.iterator();
					boolean firstInMinerSession = true;
					while (miner.hasNext() && runLatch.getCount() > 0) {
						final OraCdcRedoRecord record = miner.next();
						if (record == null) {
							LOGGER.warn("Unexpected termination of redo records stream after RBA {}", lastGuaranteedRsId);
							break;
						}
						if (conFilter && 
								Arrays.binarySearch(conUids, record.conUid()) < 0) {
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug("Skipping RBA {} with CON_UID {}",
										record.rba(), record.conUid());
							}
							continue;
						}
						if (LOGGER.isTraceEnabled()) {
							LOGGER.trace(record.toString());
						}
						if (firstInMinerSession) {
							if (LOGGER.isDebugEnabled()) {
								if (!notFirstRecord) {
									LOGGER.debug("Processing RBA {} after RBA {} in previous session",
											record.rba(), lastRba);
								}
							}
							firstInMinerSession = false;
						}
						if (notFirstRecord) {
							if (record.rba().sqn() < lastRba.sqn()) {
								break;
							}
						} else {
							notFirstRecord = true;
						}
						xid = record.xid();
						lastScn = record.scn();
						lastRba = record.rba();
						lastSubScn = record.subScn();
						OraCdcTransaction transaction = null;
						if (xid != null) {
							transaction = activeTransactions.get(xid);
						}
						//BEGIN: Main decision tree
						if (record.has5_4()) {
							final boolean rollback = record.change5_4().rollback();
							if (transaction == null) {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug(
											"Skipping {} at SCN={}, RBA={} for transaction XID {}",
											rollback ? "ROLLBACK" : "COMMIT",
											lastScn, lastRba, xid);
								}
							} else {
								if (rollback) {
									metrics.addRolledBackRecords(transaction.length(), transaction.size(),
											activeTransactions.size() - 1);
									transaction.close();
									transaction = null;
								} else {
									transaction.setCommitScn(lastScn);
									committedTransactions.add(transaction);
									metrics.addCommittedRecords(transaction.length(), transaction.size(),
											committedTransactions.size(), activeTransactions.size());
								}
								activeTransactions.remove(xid);
								prefixedTransactions.remove(xid.partial());
								sortedByFirstScn.remove(xid);
								if (!sortedByFirstScn.isEmpty()) {
									task.putReadRestartScn(sortedByFirstScn.firstEntry().getValue());
								} else {
									firstTransaction = true;
								}
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Performing {} at SCN={}, RBA={} for transaction XID {}",
											rollback ? "ROLLBACK" : "COMMIT",
											lastScn, lastRba, xid);
								}
							}
						} else if (record.has5_1() && record.has11_x()) {
							if (staticObjIds) {
								if (includeFilter &&
										Arrays.binarySearch(includeObjIds, record.change5_1().obj()) < 0) {
									continue;
								}
								if (excludeFilter &&
										Arrays.binarySearch(excludeObjIds, record.change5_1().obj()) > -1) {
									continue;
								}
							} else {
								if (checker.notNeeded(record.change5_1().obj(), record.change5_1().conId())) {
									continue;
								}
							}
							if (transaction == null) {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("New transaction {} created. Transaction start timestamp {}, first SCN {}.",
											xid, Instant.ofEpochMilli(record.unixMillis()), lastScn);
								}
								if (useChronicleQueue) {
									transaction = getChronicleQueue(xid.toString());
								} else {
									transaction = new OraCdcTransactionArrayList(xid.toString());
								}
								final OraCdcTransaction duplicateXid = activeTransactions.put(xid, transaction);
								if (duplicateXid != null) {
									
									LOGGER.error(
											"\n=====================\n" +
											"Duplicate hash value for '{}' and '{}'!\n" +
											"Please send this message to oracle@a2.solutions" +
											"\n=====================\n",
											xid.toString(), duplicateXid.getXid());
									throw new ConnectException("Duplicate XID/hash function error!");
								}
								createTransactionPrefix(xid, lastRba);
								sortedByFirstScn.put(xid,
											Triple.of(lastScn, lastRba, lastSubScn));
								if (firstTransaction) {
									firstTransaction = false;
									task.putReadRestartScn(sortedByFirstScn.firstEntry().getValue());
								}
							}
							final short operation = record.change11_x().operation();
							switch (operation) {
							case _11_2_IRP:
							case _11_3_DRP:
							case _11_5_URP:
							case _11_6_ORP:
								processRowChange(transaction, record, false);
								break;
							case _11_16_LMN:
								processRowChangeLmnUpdate(transaction, record);
								break;
							case _11_4_LKR:
							case _11_8_CFA:
							case _11_10_SKL:
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Skipping OP:{} at RBA {}", formatOpCode(operation), record.rba());
								}
								break;
							case _11_11_QMI:
							case _11_12_QMD:
								emitMultiRowChange(transaction, record);
								break;
							default:
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Skipping OP:{} at RBA {}", formatOpCode(operation), record.rba());
								}
							}
							if (record.hasAudit()) {
								//TODO
								//TODO Add audit data to trans...
								//TODO
							}
							continue;
						} else if (record.hasPrb() && record.has11_x()) {
							if (staticObjIds) {
								if (includeFilter &&
										Arrays.binarySearch(includeObjIds, record.changePrb().obj()) < 0) {
									continue;
								}
								if (excludeFilter &&
										Arrays.binarySearch(excludeObjIds, record.changePrb().obj()) > -1) {
									continue;
								}
							} else {
								if (checker.notNeeded(record.changePrb().obj(), record.changePrb().conId())) {
									continue;
								}
							}
							boolean suspiciousRecord = false;
							if (transaction == null) {
								final Xid substitutedXid = prefixedTransactions.get(record.xid().partial());
								if (substitutedXid == null) {
									suspiciousRecord = true;
								} else {
									transaction = activeTransactions.get(substitutedXid);
									if (transaction == null) {
										suspiciousRecord = true;
									}
								}
							}
							if (suspiciousRecord) {
								LOGGER.error(
										"\n=====================\n\n" +
										"The transaction with XID='{}' starts with with the record with PARTIAL ROLLBACK flagset to true!\n" +
										"SCN={}, RBA={}, redo Record details:\n{}\n" +
										"If you have questions or need more information, please write to us at oracle@a2.solutions\n\n" +
										"\n=====================\n",
										xid, lastScn, lastRba, record.toString());
							} else {
								final short operation = record.change11_x().operation();
								switch (operation) {
								case _11_2_IRP:
								case _11_3_DRP:
								case _11_5_URP:
								case _11_6_ORP:
									processRowChange(transaction, record, true);
									break;
								default:
									LOGGER.warn("Skipping partial rollback OP:{} at RBA {}", formatOpCode(operation), record.rba());
								}
							}
							continue;
						} else if (record.hasDdl()) {
							//TODO
							//TODO
							//TODO
						} else {
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug("Skipping redo record at RBA {}", record.rba());
							}
						}
						//END: Main decision tree

					} // while (isRsLogMinerRowAvailable && runLatch.getCount() > 0)
					//TODO - do we need to pass lastScn???
					redoMiner.stop(lastRba, lastScn);
					miner = null;
					if (activeTransactions.isEmpty() && lastGuaranteedScn > 0) {
						// Update restart point in time
						task.putReadRestartScn(Triple.of(lastGuaranteedScn, lastGuaranteedRsId, lastGuaranteedSsn));
					}
					redoMinerReady = false;
					while (!redoMinerReady && runLatch.getCount() > 0) {
						redoMinerReady = redoMiner.next();
						if (!redoMinerReady) {
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug("Waiting {} ms", pollInterval);
							}
							synchronized(this) {
								try {wait(pollInterval); } catch (InterruptedException ie) {}
							}
						} else {
							//TODO
							//TODO - rewind here?
							//TODO
						}
					}
				}
			} catch (SQLException | IOException e) {
				final StringBuilder sb = new StringBuilder(0x400);
				sb.append("\n=====================\n");
				sb
					.append("Exception: ")
					.append(e.getMessage());
				if (e instanceof SQLException) {
					SQLException sqle = (SQLException) e;
					sb
						.append("\nSQL errorCode = ")
						.append(sqle.getErrorCode())
						.append(", SQL state = '")
						.append(sqle.getSQLState())
						.append("'");
				}
				sb
					.append("\nLast read row information: SCN=")
					.append(lastScn)
					.append(", RBA=")
					.append(lastRba.toString())
					.append(", SUBSCN=")
					.append(lastSubScn)
					.append(", XID=")
					.append(xid.toString())
					.append("\n=====================\n");
				LOGGER.error(sb.toString());
				lastScn = lastGuaranteedScn;
				lastRba = lastGuaranteedRsId;
				lastSubScn = lastGuaranteedSsn;
				running.set(false);
				task.stop(false);
				throw new ConnectException(e);
			}
		} // while (runLatch.getCount() > 0)
		running.set(false);
		LOGGER.info("END: OraCdcRedoMinerWorkerThread.run()");
	}

	private static class ActiveTransComparator implements Comparator<Xid> {

		private final Map<Xid, OraCdcTransaction> activeTransactions;

		ActiveTransComparator(final Map<Xid, OraCdcTransaction> activeTransactions) {
			this.activeTransactions = activeTransactions;
		}

		@Override
		public int compare(Xid first, Xid second) {
			if (first.equals(second)) {
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

	private void createTransactionPrefix(final Xid xid, final RedoByteAddress rba) {
		final int partial = xid.partial();
		final Xid prevXid = prefixedTransactions.put(partial, xid);
		if (prevXid != null) {
			final StringBuilder sb = new StringBuilder();
			sb
				.append(String.format("0x%04x", partial >> 16))
				.append('.')
				.append(String.format("0x%03x", Short.toUnsignedInt((short)partial)));
			LOGGER.warn(
					"\n=====================\n" +
					"Transaction prefix {} binding changed from {} to {} at RBA {}.\n" +
					"=====================\n",
					sb.toString(), prevXid, xid, rba);
		}
	}

	private void processRowChangeLmnUpdate(final OraCdcTransaction transaction,
			final OraCdcRedoRecord record) {
		final byte fbLmn = record.change11_x().fb();
		if (flgPrevPart(fbLmn) && flgNextPart(fbLmn)) {
			//Just complete previous record
			final int key = record.halfDoneKey();
			Deque<RowChangeHolder> deque =  halfDone.get(key);
			if (deque == null) {
				//TODO - error message
			} else {
				RowChangeHolder halfDoneRow =  deque.pollFirst();
				halfDoneRow.complete = true;
				emitRowChange(transaction, halfDoneRow);
				if (deque.isEmpty()) {
					halfDone.remove(key);
				}
				//TODO debug output
				
			}
		} else if (record.supplementalLogData()) {
			if (flgFirstPart(fbLmn) && !flgNextPart(fbLmn)) {
				processRowChange(transaction, record, false);
			} else if (flgFirstPart(fbLmn) && flgNextPart(fbLmn)) {
				LOGGER.debug("Skipping OP:11.16 record with row flags F and N set at RBA {}, SCN {}, XID {} in file {}",
						record.rba(), record.scn(), record.xid(), record.redoLog().fileName());
			} else if (flgLastPart(fbLmn) && flgNextPart(fbLmn)) {
				LOGGER.debug("Skipping OP:11.16 record with row flags L and N set at RBA {}, SCN {}, XID {} in file {}",
						record.rba(), record.scn(), record.xid(), record.redoLog().fileName());
			} else {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Skipping OP:11.16 record with row flags {} at RBA {}, SCN {}, XID {} in file {}",
							printFbFlags(fbLmn),record.rba(), record.scn(), record.xid(), record.redoLog().fileName());
				}
			}
		} else {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Skipping OP:11.16 record without supplemental log data at RBA {}, SCN {}, XID {} in file {}",
						record.rba(), record.scn(), record.xid(), record.redoLog().fileName());
			}
		}
	}

	private void processRowChange(final OraCdcTransaction transaction,
			final OraCdcRedoRecord record, final boolean partialRollback) {
		RowChangeHolder row = createRowChangeHolder(record, partialRollback);
		if (row.complete) {
			emitRowChange(transaction, row);
		} else {
			final int key = record.halfDoneKey();
			Deque<RowChangeHolder> deque =  halfDone.get(key);
			if (deque == null) {
				row.add(record);
				deque = new ArrayDeque<>();
				deque.addFirst(row);
				halfDone.put(key, deque);
			} else {
				RowChangeHolder halfDoneRow =  deque.peekFirst();
				final OraCdcRedoRecord lastHalfDone = halfDoneRow.last();
				final boolean push;
				if (partialRollback)
					push = lastHalfDone.change11_x().fb() == record.change11_x().fb();
				else
					if (record.change11_x().fb() == 0 && record.change5_1().fb() == 0)
						push = false;
					else
						push = lastHalfDone.change11_x().fb() == record.change11_x().fb() && 
								lastHalfDone.change5_1().fb() == record.change5_1().fb();
				if (push) {
					row.add(record);
					deque.addFirst(row);
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug(
								"An stored incomplete change at RBA {} cannot be merged with an incomplete change in progress at RBA {}",
								lastHalfDone.rba(), record.rba());
					}
				} else {
					halfDoneRow.add(record);
					row = null;
					completeRow(halfDoneRow);
					if (halfDoneRow.complete) {
						emitRowChange(transaction, halfDoneRow);
						deque.removeFirst();
						if (deque.isEmpty()) {
							halfDone.remove(key);
						}
					}
				}
			}

		}
	}

	private RowChangeHolder createRowChangeHolder(final OraCdcRedoRecord record, final boolean partialRollback) {
		final RowChangeHolder row = new RowChangeHolder(partialRollback, record.change11_x().operation());
		if (partialRollback) {
			switch (row.operation) {
			case _11_5_URP:
			case _11_6_ORP:
			case _11_16_LMN:
				row.lmOp = UPDATE;
				if (flgFirstPart(record.change11_x().fb()) && flgLastPart(record.change11_x().fb()))
					row.complete = true;
				else if (!flgHeadPart(record.change11_x().fb()))
					row.oppositeOrder = true;
				if (row.operation == _11_16_LMN) {
					row.needHeadFlag = false;
					if (flgFirstPart(record.change11_x().fb()))
						row.oppositeOrder = true;
				}
				break;
			case _11_2_IRP:
				row.lmOp = INSERT;
				if (flgFirstPart(record.change11_x().fb()) && flgLastPart(record.change11_x().fb()))
					row.complete = true;
				else if (!flgHeadPart(record.change11_x().fb()))
					row.oppositeOrder = true;
				break;
			case _11_3_DRP:
				row.lmOp = DELETE;
				row.complete = true;
				break;
			}
		} else {
			switch (row.operation) {
			case _11_5_URP:
			case _11_16_LMN:
				row.lmOp = UPDATE;
				if (flgFirstPart(record.change5_1().supplementalFb()) && flgLastPart(record.change5_1().supplementalFb()))
						row.complete = true;
				else if (flgFirstPart(record.change5_1().fb()) && flgLastPart(record.change5_1().fb()) &&
						flgFirstPart(record.change11_x().fb()) && flgLastPart(record.change11_x().fb()))
					row.complete = true;
				else if (!flgHeadPart(record.change5_1().fb()))
					row.oppositeOrder = true;
				if (row.operation == _11_16_LMN) {
					row.needHeadFlag = false;
					if (!flgFirstPart(record.change5_1().fb()) || flgFirstPart(record.change11_x().fb()))
						row.oppositeOrder = true;
				}
				break;
			case _11_2_IRP:
			case _11_6_ORP:
				if (row.operation == _11_2_IRP)
					row.lmOp = INSERT;
				else
					row.lmOp = UPDATE;
				if (flgHeadPart(record.change11_x().fb()) && flgFirstPart(record.change11_x().fb()) && flgLastPart(record.change11_x().fb()))
					row.complete = true;
				else if (!flgHeadPart(record.change11_x().fb()))
					row.oppositeOrder = true;
				break;
			case _11_3_DRP:
				row.lmOp = DELETE;
				if (flgHeadPart(record.change5_1().fb()) && flgFirstPart(record.change5_1().fb()) && flgLastPart(record.change5_1().fb()))
					row.complete = true;
				else if (!flgHeadPart(record.change5_1().fb()))
					row.oppositeOrder = true;
				break;
			}
		}
		if (row.complete)
			row.add(record);
		return row;
	}

	private void completeRow(final RowChangeHolder row) {
		if (row.partialRollback) {
			int head = 0;
			int first = 0;
			int last = 0;
			for (int i = 0; i < row.records.size(); i++) {
				final OraCdcRedoRecord rr = row.records.get(i);
				if (rr.has11_x() && rr.hasPrb()) {
					final byte fb = rr.change11_x().fb();
					if (flgHeadPart(fb)) head++;
					if (flgFirstPart(fb)) first++;
					if (flgLastPart(fb)) last++;
				} else {
					LOGGER.warn(
							"\n=====================\n" +
							"Strange redo record without required op codes 5.6/5.11 and 11.x at RBA {} in '{}'.\n" +
							"Redo record information:\n{}" +
							"\n=====================\n",
							rr.rba(), rr.redoLog().fileName(), rr.toString());
				}
			}
			if (head > 0 && first > 0 && last > 0) {
				row.complete = true;
			}
		} else {
			int head = 0;
			int first = 0;
			int last = 0;
			for (int i = 0; i < row.records.size(); i++) {
				final OraCdcRedoRecord rr = row.records.get(i);
				if (rr.has11_x() && rr.has5_1()) {
					if (row.lmOp == INSERT && rr.change11_x().operation() == _11_6_ORP) {
						row.lmOp = UPDATE;
						row.homogeneous = false;
						completeRow(row);
					}
					if (row.lmOp == UPDATE && rr.change11_x().operation() != _11_5_URP && row.homogeneous) {
						row.homogeneous = false;
					}
					if (row.lmOp == DELETE) {
						final byte fb = rr.change5_1().fb();
						if (flgHeadPart(fb)) head++;
						if (flgFirstPart(fb)) first++;
						if (flgLastPart(fb)) last++;
					} else if (row.lmOp == INSERT) {
						final byte fb = rr.change11_x().fb();
						if (flgHeadPart(fb)) head++;
						if (flgFirstPart(fb)) first++;
						if (flgLastPart(fb)) last++;
					} else {
						//UPDATE
						final byte fb5_1 = rr.change5_1().fb();
						if (flgHeadPart(fb5_1)) head++;
						if (flgFirstPart(fb5_1)) first++;
						if (flgLastPart(fb5_1)) last++;
						final byte fb11_x = rr.change11_x().fb();
						if (flgHeadPart(fb11_x)) head++;
						if (flgFirstPart(fb11_x)) first++;
						if (flgLastPart(fb11_x)) last++;
					}
				} else {
					LOGGER.warn(
							"\n=====================\n" +
							"Strange redo record without required op codes 5.1 and 11.x at RBA {} in '{}'.\n" +
							"Redo record information:\n{}" +
							"\n=====================\n",
							rr.rba(), rr.redoLog().fileName(), rr.toString());
				}
			}
			if ((row.lmOp == INSERT || row.lmOp == DELETE) &&
					head > 0 && first > 0 && last > 0)
				row.complete = true;
			else if (row.lmOp == UPDATE && row.needHeadFlag &&
					head > 1 && first > 1 && last > 1)
				row.complete = true;
			else if (row.lmOp == UPDATE && !row.needHeadFlag &&
					first > 1 && last > 1)
				row.complete = true;
			else
				row.complete = false;
		}
		if (LOGGER.isDebugEnabled()) {
			if (row.complete) {
				final StringBuilder sb = new StringBuilder(0x800);
				sb.append("Ready to merge redo records into one row for RBA's");
				for (final OraCdcRedoRecord record : row.records) {
					sb
						.append("\n\tXID:")
						.append(record.xid().toString())
						.append(", SCN:")
						.append(record.scn())
						.append(", RBA:")
						.append(record.rba().toString());
					if (row.partialRollback)
						sb
							.append(", OP:")
							.append(formatOpCode(record.changePrb().operation()))
							.append(" fb:")
							.append(printFbFlags(record.changePrb().fb()))
							.append(", OP:")
							.append(formatOpCode(record.change11_x().operation()))
							.append(" fb:")
							.append(printFbFlags(record.change11_x().fb()));
					else
						sb
							.append(", OP:5.1 fb:")
							.append(printFbFlags(record.change5_1().fb()))
							.append(", supp fb:")
							.append(printFbFlags(record.change5_1().supplementalFb()))
							.append(", OP:")
							.append(formatOpCode(record.change11_x().operation()))
							.append(" fb:")
							.append(printFbFlags(record.change11_x().fb()));
				}
				LOGGER.debug(sb.toString());
			}
		}
	}

	private void emitRowChange(final OraCdcTransaction transaction, final RowChangeHolder row) {
		if (row.reorder) {
			if (LOGGER.isDebugEnabled()) {
				final StringBuilder sb = new StringBuilder(0x100);
				sb.append("Executing row.reorderRecords() for following RBA's: ");
				boolean firstRba = true;
				for (final OraCdcRedoRecord rr : row.records) {
					if (firstRba)
						firstRba = false;
					else
						sb.append(", ");
					sb.append(rr.rba());
				}
				LOGGER.debug(sb.toString());
			}
			row.reorderRecords();
		}
		final OraCdcRedoRecord first = row.first();

		if (row.lmOp == UNSUPPORTED) {
			return;
		}

		final byte[] redoBytes;
		if (row.partialRollback && row.lmOp == DELETE) {
			redoBytes = new byte[2];
			redoBytes[0] = 0;
			redoBytes[1] = 0;
		} else {
			int totalBytes = 0;
			int supplColCount = 0;
			int whereColCount = 0;
			int setOrValColCount = 0;
			for (final OraCdcRedoRecord rr : row.records) {
				totalBytes += rr.len();
				final OraCdcChangeRowOp rowChange = rr.change11_x();
				if (row.homogeneous) {
					// URP, IRP
					setOrValColCount += rowChange.columnCount();
					//TODO Layer 11 operations where the partial rollback also contains
					//TODO supplemental log data but we don't need the contents for partial rollback
					if (!row.partialRollback) {
						final OraCdcChangeUndoBlock change = rr.change5_1();
						// URP, IRP, DRP
						supplColCount += change.supplementalCc();
						// 	URP, DRP
						whereColCount += change.columnCount();
					}
				} else if (!row.partialRollback) {
					final OraCdcChangeUndoBlock change = rr.change5_1();
					if (rowChange.operation() == _11_2_IRP) {
						setOrValColCount += rowChange.columnCount();
						setOrValColCount += change.supplementalCc();
					} else if (rowChange.operation() == _11_6_ORP) {
						whereColCount += change.columnCount();
						whereColCount += change.supplementalCc();
						whereColCount += rowChange.columnCount();
					} else {
						// _11_5_URP
						setOrValColCount += rowChange.columnCount();
						whereColCount += change.columnCount();
						whereColCount += change.supplementalCc();						
					}
				} else {
					//TODO
					LOGGER.error(
							"\n=====================\n" +
							"Unable to properly process the following RBA's with partial rollback\n");
					for (final OraCdcRedoRecord ocrr : row.records) {
						LOGGER.error("\t{}", ocrr.rba());
					}
					LOGGER.error(
							"\nPlease send message above along with the resulting dump of command execution\n\n" +
							"alter system dump logfile '{}' scn min {} scn max {};\n\n" +
							"to oracle@a2.solutions" +
							"\n=====================\n",
							first.redoLog().fileName(), first.scn(), row.last().scn());
					return;
				}
			}
			if (LOGGER.isDebugEnabled()) {
				if (row.lmOp == UPDATE) {
					LOGGER.debug("Number of columns in SET clause {}, number of columns in WHERE clause {}", setOrValColCount, whereColCount);
				} else if (row.lmOp == INSERT) {
					LOGGER.debug("Number of columns in VALUES clause {}", setOrValColCount);
				} else {
					//DELETE
					LOGGER.debug("Number of columns in WHERE clause {}", whereColCount);
				}
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream(totalBytes);
			if (row.lmOp == INSERT || row.lmOp == DELETE ||
					(row.partialRollback && row.lmOp == UPDATE && first.change11_x().operation() == _11_6_ORP)) {
				if (row.partialRollback) {
					writeU16(baos, setOrValColCount);
				} else {
					if (row.lmOp == INSERT)
						writeU16(baos, setOrValColCount);
					else
						writeU16(baos, supplColCount + whereColCount);
				}
				int colNumOffset = 1;
				final int rsiz = row.size();
				int i = 0;
				if (row.oppositeOrder)
					i = rsiz - 1;
				for (;;) {
					final OraCdcRedoRecord rr = row.records.get(i);
					final OraCdcChangeUndoBlock change = rr.change5_1();
					if (rr.has5_1() && row.lmOp != INSERT) {
						if (change.supplementalCc() > 0) {
							writeSupplementalCols(baos, change, change.supplementalCc(), rr.rba());
						}
						if (OraCdcChangeUndoBlock.KDO_POS + 1 + change.columnCount() <= change.coords().length) {
							writeColsWithNulls(
									baos, change, OraCdcChangeUndoBlock.KDO_POS, 0,
									change.suppOffsetUndo() == 0 ? colNumOffset : change.suppOffsetUndo(),
									KDO_ORP_IRP_NULL_POS, change.columnCount());
							colNumOffset += change.columnCount();
						} else {
							LOGGER.warn("Unable to read column data for DELETE at RBA {}, change #{}",
									rr.rba(), change.num());
						}
					} else if (row.lmOp != INSERT) {
						LOGGER.warn("Redo record {} does not contains expected operation 5.1!", rr.rba());
					}
					if (row.lmOp != DELETE) {
						if (rr.has11_x()) {
							final OraCdcChangeRowOp rowChange = rr.change11_x();
							if (OraCdcChangeRowOp.KDO_POS + rowChange.columnCount() < rowChange.coords().length) {
								if (change != null) {
									writeColsWithNulls(
											baos, rowChange, OraCdcChangeRowOp.KDO_POS, 0,
											change.suppOffsetRedo() == 0 ? colNumOffset : change.suppOffsetRedo(),
											KDO_ORP_IRP_NULL_POS, rowChange.columnCount());
								} else if (row.partialRollback) {
									writeColsWithNulls(
											baos, rowChange, OraCdcChangeRowOp.KDO_POS, 0,
											colNumOffset,
											KDO_ORP_IRP_NULL_POS, rowChange.columnCount());
								} else {
									LOGGER.warn("Unable to read column data for INSERT at RBA {}",
											rr.rba());
								}
								colNumOffset += rowChange.columnCount();
							}
						} else {
							LOGGER.warn("Redo record {} does not contains expected row change operation!", rr.rba());
						}
					}
					if (row.oppositeOrder) {
						i--;
						if (i > -1)
							continue;
						else
							break;
					} else {
						i++;
						if (i < rsiz)
							continue;
						else
							break;
					}
				}
				if (row.partialRollback && first.change11_x().operation() == _11_6_ORP) {
					writeU16(baos, 0);
				}
			} else {
				//UPDATE
				int colNumOffsetSet = 1;
				int colNumOffsetWhere = 1;
				if (row.onlyLmn)
					writeU16(baos, whereColCount + supplColCount);
				else
					writeU16(baos, setOrValColCount);
				if (row.homogeneous) {
					for (final OraCdcRedoRecord rr : row.records) {
						final OraCdcChangeUndoBlock change = rr.change5_1();
						final OraCdcChangeRowOp rowChange = rr.change11_x();
						if (rowChange.operation() == _11_5_URP &&
								(rowChange.flags() & KDO_KDOM2) != 0) {
							if (rowChange.coords()[OraCdcChangeRowOp.KDO_POS + 1][1] > 1 &&
									OraCdcChangeRowOp.KDO_POS + 2 < rowChange.coords().length) {
								writeKdoKdom2(baos, rowChange, OraCdcChangeRowOp.KDO_POS);
							} else {
								LOGGER.warn("Not enough data to process KDO_KDOM2 structure at RBA {}, change #{}",
										rr.rba(), rowChange.num());
							}
						} else if (rowChange.operation() == _11_5_URP &&
								OraCdcChangeRowOp.KDO_POS + 1 + rowChange.columnCount() < rowChange.coords().length) {
							writeColsWithNulls(
									baos, rowChange, OraCdcChangeRowOp.KDO_POS, OraCdcChangeRowOp.KDO_POS + 1,
									row.partialRollback ?  colNumOffsetSet : 
										(change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo()),
									KDO_URP_NULL_POS, rowChange.columnCount(), rowChange.columnCountNn());
						} else if (rowChange.operation() == _11_6_ORP &&
								OraCdcChangeRowOp.KDO_POS + rowChange.columnCount() < rowChange.coords().length) {
							writeColsWithNulls(
									baos, rowChange, OraCdcChangeRowOp.KDO_POS, 0,
									row.partialRollback ?  colNumOffsetSet : 
										(change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo()),
									KDO_ORP_IRP_NULL_POS, rowChange.columnCount(), rowChange.columnCountNn());
						} else if (row.onlyLmn && change.supplementalCc() > 0) {
							writeSupplementalCols(baos, change, change.supplementalCc(), rr.rba());
						} else {
							LOGGER.warn("Unable to read column data for UPDATE (SET) at RBA {}, change #{}",
									rr.rba(), rowChange.num());
						}
						colNumOffsetSet += rowChange.columnCount();
					}
					if (row.partialRollback) {
						writeU16(baos, 0);
					} else {
						writeU16(baos, whereColCount + supplColCount);
						for (final OraCdcRedoRecord rr : row.records) {
							final OraCdcChangeUndoBlock change = rr.change5_1();
							if (change.supplementalCc() > 0) {
								writeSupplementalCols(baos, change, change.supplementalCc(), rr.rba());
							}
							if (change.columnCount() > 0) {
								final short selector = (short) ((change.op() & 0x1F) | (KCOCODRW << 0x08));
								if (change.operation() == _11_5_URP &&
										(change.flags() & KDO_KDOM2) != 0) {
									if (change.coords()[OraCdcChangeUndoBlock.KDO_POS + 1][1] > 1 &&
											OraCdcChangeUndoBlock.KDO_POS + 2 < change.coords().length) {
										writeKdoKdom2(baos, change, OraCdcChangeUndoBlock.KDO_POS);
									} else {
										LOGGER.warn("Not enough data to process KDO_KDOM2 structure at RBA {}, change #{}",
												rr.rba(), change.num());
									}
								} else if (selector == _11_5_URP &&
										OraCdcChangeUndoBlock.KDO_POS + 1 + change.columnCountNn() < change.coords().length) {
									writeColsWithNulls(baos, change, OraCdcChangeUndoBlock.KDO_POS, OraCdcChangeUndoBlock.KDO_POS + 1,
											change.suppOffsetUndo() == 0 ? colNumOffsetWhere : change.suppOffsetUndo(),
											KDO_URP_NULL_POS, change.columnCount());
								} else if (selector == _11_6_ORP &&
										OraCdcChangeUndoBlock.KDO_POS + change.columnCountNn() < change.coords().length) {
									writeColsWithNulls(baos, change, OraCdcChangeUndoBlock.KDO_POS, 0,
											change.suppOffsetUndo() == 0 ? colNumOffsetWhere : change.suppOffsetUndo(),
											KDO_ORP_IRP_NULL_POS, change.columnCount());
								} else {
									LOGGER.warn("Unable to read column data for UPDATE(WHERE) at RBA {}, change #{}",
											rr.rba(), change.num());
								}
								colNumOffsetWhere += change.columnCount();
							}
						}
					}
				} else {
					ByteArrayOutputStream baosW = new ByteArrayOutputStream(totalBytes);
					for (final OraCdcRedoRecord rr : row.records) {
						final OraCdcChangeUndoBlock change = rr.change5_1();
						final OraCdcChangeRowOp rowChange = rr.change11_x();
						if (rowChange.operation() == _11_2_IRP) {
							if (change.supplementalCc() > 0) {
								writeSupplementalCols(baos, change, change.supplementalCc(), rr.rba());
							}
							writeColsWithNulls(
									baos, rowChange, OraCdcChangeRowOp.KDO_POS, 0,
									change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo(),
									KDO_ORP_IRP_NULL_POS, rowChange.columnCount());
						} else if (rowChange.operation() == _11_6_ORP) {
							if (change.supplementalCc() > 0) {
								writeSupplementalCols(baosW, change, change.supplementalCc(), rr.rba());
							}
							writeColsWithNulls(
									baosW, change, OraCdcChangeUndoBlock.KDO_POS, 0,
									change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetUndo(),
									KDO_ORP_IRP_NULL_POS, change.columnCount());
							colNumOffsetSet += change.columnCount();
							writeColsWithNulls(
									baosW, rowChange, OraCdcChangeRowOp.KDO_POS, 0,
									change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetRedo(),
									KDO_ORP_IRP_NULL_POS, rowChange.columnCount());
							colNumOffsetSet += rowChange.columnCount();
						} else {
							// _11_5_URP
							if (change.supplementalCc() > 0) {
								writeSupplementalCols(baosW, change, change.supplementalCc(), rr.rba());
							}
							if (OraCdcChangeUndoBlock.KDO_POS + 1 + change.columnCountNn() < change.coords().length) {
								writeColsWithNulls(baosW, change, OraCdcChangeUndoBlock.KDO_POS, OraCdcChangeUndoBlock.KDO_POS + 1,
										change.suppOffsetUndo(), KDO_URP_NULL_POS, change.columnCount());
							}
							if (OraCdcChangeRowOp.KDO_POS + 1 + rowChange.columnCount() < rowChange.coords().length) {
								colNumOffsetSet += writeColsWithNulls(
										baos, rowChange, OraCdcChangeRowOp.KDO_POS, OraCdcChangeRowOp.KDO_POS + 1,
										row.partialRollback ?  colNumOffsetSet : 
											(change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo()),
										KDO_URP_NULL_POS, rowChange.columnCount(), rowChange.columnCountNn());
								colNumOffsetSet += rowChange.ncol(OraCdcChangeRowOp.KDO_POS);
							}
						}
					}
					writeU16(baos, whereColCount);
					try {
						baos.write(baosW.toByteArray());
						baosW.close();
						baosW = null;
					} catch (IOException ioe) {}
				}
			}
			redoBytes = baos.toByteArray();
		}

		final OraCdcRedoRecord last = row.last();
		final OraCdcChangeUndo change;
		if (row.oppositeOrder) {
			if (row.partialRollback) {
				change = last.changePrb();
			} else {
				change = last.change5_1();
			}
		} else {
			if (row.partialRollback) {
				change = first.changePrb();
			} else {
				change = first.change5_1();
			}
		}

		final OraCdcRedoMinerStatement orm;
		if (row.oppositeOrder)
			orm = new OraCdcRedoMinerStatement(
					isCdb ? (((long)change.conId()) << 32) |  (change.obj() & 0xFFFFFFFFL): change.obj(),
					row.lmOp, redoBytes,
					last.unixMillis(), last.scn(), row.rba,
					(long) last.subScn(),
					last.rowid(),
					row.partialRollback);
		else
			orm = new OraCdcRedoMinerStatement(
					isCdb ? (((long)change.conId()) << 32) |  (change.obj() & 0xFFFFFFFFL): change.obj(),
					row.lmOp, redoBytes,
					first.unixMillis(), first.scn(), row.rba,
					(long) first.subScn(),
					first.rowid(),
					row.partialRollback);
		transaction.addStatement(orm);
		metrics.addRecord();
	}

	private void emitMultiRowChange(
			final OraCdcTransaction transaction,
			final OraCdcRedoRecord rr) {
		final OraCdcChangeUndoBlock change = rr.change5_1();
		final OraCdcChangeRowOp rowChange = rr.change11_x();
		final short lmOp;
		final int index;
		final OraCdcChange qmData;
		if (rowChange.operation() == _11_11_QMI) {
			index = OraCdcChangeRowOp.KDO_POS;
			lmOp = INSERT;
			qmData = rowChange;
		} else {
			index = OraCdcChangeUndoBlock.KDO_POS;
			lmOp = DELETE;
			qmData = change;
		}
		final byte[] record = qmData.record();
		final int[][] coords = qmData.coords();
		final OraCdcRedoLog redoLog = qmData.redoLog();
		int rowDiff = 0;
		final int rowCount = Byte.toUnsignedInt(change.qmRowCount());
		for (int row = 0; row < rowCount; ++row) {
			rowDiff += 0x2;
			final int columnCount = Byte.toUnsignedInt(record[coords[index + 2][0] + rowDiff++]);
			if ((qmData.op() & OraCdcChange.FLG_ROWDEPENDENCIES) != 0) {
				// Skip row SCN
				rowDiff += redoLog.bigScn() ? Long.BYTES : (Integer.BYTES + Short.BYTES);
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream(coords[index + 2][1]/rowCount + 0x100);
			writeU16(baos, columnCount);
			for (int col = 0; col < columnCount; col++) {
				writeU16(baos, col + 1);
				int colSize = Byte.toUnsignedInt(record[coords[index +2][0] + rowDiff++]);
				if (colSize ==  0xFE) {
					baos.write(0xFE);
					colSize = Short.toUnsignedInt(bu.getU16(record, coords[index + 2][0] + rowDiff));
					writeU16(baos, colSize);
					rowDiff += Short.BYTES;
				} else  if (colSize == 0xFF) {
					colSize = 0;
					baos.write(0xFF);
				} else {
					baos.write(colSize);
				}
				if (colSize != 0) {
					baos.write(record, coords[index + 2][0] + rowDiff, colSize);
					rowDiff += colSize;
				}
			}
			final RowId rowId = new RowId(
					change.dataObj(),
					change.bdba(),
					bu.getU16(record, coords[index][0] + 0x14 + row * Short.BYTES));
			final OraCdcRedoMinerStatement orm = new OraCdcRedoMinerStatement(
					isCdb ? (((long)change.conId()) << 32) |  (change.obj() & 0xFFFFFFFFL): change.obj(),
					lmOp, baos.toByteArray(), rr.unixMillis(), rr.scn(), rr.rba(),
					(long) rr.subScn(), rowId, false);
			transaction.addStatement(orm);
			metrics.addRecord();
		}
	}

	private void writeColSize(final ByteArrayOutputStream baos, final int colSize) {
		if (colSize < 0xFE) {
			baos.write(colSize);
		} else {
			baos.write(0xFE);
			writeU16(baos, colSize);
		}
	}

	private void writeU16(final ByteArrayOutputStream baos, final int u16) {
		baos.write(u16 >> 8);
		baos.write((byte)u16);
	}

	private int writeColsWithNulls(
			final ByteArrayOutputStream baos, final OraCdcChange change,
			final int index, final int colNumIndex, final int offset,
			final int nullPos, final int count) {
		return writeColsWithNulls(baos, change, index, colNumIndex, offset, nullPos, count, Integer.MAX_VALUE);
	}

	private int writeColsWithNulls(
			final ByteArrayOutputStream baos, final OraCdcChange change,
			final int index, final int colNumIndex, final int offset,
			final int nullPos, final int count, final int colNn) {
		final byte[] record = change.record();
		final int[][] coords = change.coords();
		final int colNumOffset;
		if (colNumIndex > 0) {
			colNumOffset = offset - bu.getU16(record, coords[colNumIndex][0]);
		} else {
			colNumOffset = offset;
		}
		byte mask = 1;
		int diff = nullPos;
		for (int i = 0; i < count; i++) {
			final int colDataIndex = index + i + (colNumIndex > 0 ? 2 : 1);
			final int colNum;
			if (colNumIndex > 0) {
				colNum = bu.getU16(record, coords[colNumIndex][0] + i * Short.BYTES) + colNumOffset;
			} else {
				colNum = i  + colNumOffset;
			}
			writeU16(baos, colNum);
			boolean isNull = false;
			if ((record[coords[index][0] + diff] & mask) != 0) {
				isNull = true;
			}
			mask <<= 1;
			if (mask == 0) {
				mask = 1;
				diff++;
			}
			if (isNull) {
				baos.write(0xFF);
			} else {
				final int colSize = coords[colDataIndex][1];
				writeColSize(baos, colSize);
				if (colSize > 0) {
					baos.write(record, coords[colDataIndex][0], colSize);
				}
			}
		}
		return colNumOffset;
	}

	private void writeSupplementalCols(
			final ByteArrayOutputStream baos, final OraCdcChangeUndoBlock change,
			final int supplColCount, final RedoByteAddress rba) {
		final int supplColCountNn = change.supplementalCcNn();
		final int colNumIndex = change.suppDataStartIndex() + 1;
		final int dataEndPos = supplColCount + change.suppDataStartIndex() + 0x3;
		final byte[] record = change.record();
		final int[][] coords = change.coords();
		int colIndex = 0;
		for (int i = change.suppDataStartIndex() + 0x3; i < dataEndPos; i++) {
			if (i < coords.length) {
				final int colNum = bu.getU16(record, coords[colNumIndex][0] + colIndex * Short.BYTES);
				writeU16(baos, colNum);
				if (colIndex < supplColCountNn) {
					final int colSize = coords[i][1];
					if (colSize == 0) {
						baos.write(0xFF);
					} else {
						writeColSize(baos, colSize);
						baos.write(record, coords[i][0], colSize);
					}
				} else {
					baos.write(0xFF);
				}
				colIndex++;
			} else {
				LOGGER.warn("Incorrect index {} when processing redo record at rba {}",
						i, rba);
				break;
			}
		}
	}

	private void writeKdoKdom2(final ByteArrayOutputStream baos, final OraCdcChange change, final int index) {
		final int[][] coords = change.coords();
		final byte[] record = change.record();
		final short colNumOffset = bu.getU16(record, coords[index + 1][0]);
		int rowDiff = 0;
		for (int i = 0; i < change.columnCount(); i++) {
			writeU16(baos, i + colNumOffset);
			int colSize = Byte.toUnsignedInt(record[coords[index +2][0] + rowDiff++]);
			if (colSize ==  0xFE) {
				baos.write(0xFE);
				colSize = Short.toUnsignedInt(bu.getU16(record, coords[index + 2][0] + rowDiff));
				writeU16(baos, colSize);
				rowDiff += Short.BYTES;
			} else if (colSize == 0xFF) {
				colSize = 0;
				baos.write(0xFF);
			} else {
				baos.write(colSize);
			}
			if (colSize != 0) {
				baos.write(record, coords[index + 2][0] + rowDiff, colSize);
				rowDiff += colSize;
			}									
		}
	}

	private static class RowChangeHolder {
		private final boolean partialRollback;
		private final List<OraCdcRedoRecord> records;
		private final short operation;
		private boolean complete;
		private boolean oppositeOrder;
		private short lmOp;
		private boolean homogeneous;
		private boolean needHeadFlag;
		private boolean reorder;
		private RedoByteAddress rba = RedoByteAddress.MAX_VALUE;
		private boolean onlyLmn;

		RowChangeHolder(final boolean partialRollback, final short operation) {
			this.partialRollback = partialRollback;
			this.operation = operation;
			this.records = new ArrayList<>();
			this.complete = false;
			this.oppositeOrder = false;
			this.lmOp = UNSUPPORTED;
			this.homogeneous = true;
			this.needHeadFlag = true;
			this.reorder = false;
			if (partialRollback)
				onlyLmn = false;
			else
				onlyLmn = true;
		}
		void add(final OraCdcRedoRecord record) {
			records.add(record);
			if (record.rba().compareTo(rba) < 0)
				rba = record.rba();
			if (!partialRollback)
				onlyLmn = onlyLmn && (record.change11_x().operation() == _11_16_LMN);
			if (!complete && !partialRollback && records.size() == 1) {
				final byte fb5_1 = record.change5_1().fb();
				final byte fb11_x = record.change11_x().fb();
				if (!flgFirstPart(fb5_1) && !flgLastPart(fb5_1) && !flgHeadPart(fb5_1) &&
						!flgFirstPart(fb11_x) && !flgLastPart(fb11_x) && !flgHeadPart(fb11_x))
					reorder = true;
			}
			if (LOGGER.isDebugEnabled()) {
				if (partialRollback) {
					LOGGER.debug("Adding XID {}, SCN {}, RBA {}, OP:{} fb:{}, OP:{} fb:{}",
							record.xid(),
							record.scn(),
							record.rba(),
							formatOpCode(record.changePrb().operation()),
							printFbFlags(record.changePrb().fb()),
							formatOpCode(record.change11_x().operation()),
							printFbFlags(record.change11_x().fb()));
				} else {
					LOGGER.debug("Adding XID {}, SCN {}, RBA {}, OP:5.1 fb:{}, supp fb:{}, OP:{} fb:{}",
							record.xid(),
							record.scn(),
							record.rba(),
							printFbFlags(record.change5_1().fb()),
							printFbFlags(record.change5_1().supplementalFb()),
							formatOpCode(record.change11_x().operation()),
							printFbFlags(record.change11_x().fb()));
				}
			}
		}
		int size() {
			return records.size();
		}
		OraCdcRedoRecord first() {
			return records.get(0);
		}
		OraCdcRedoRecord last() {
			return records.get(records.size() - 1);
		}
		void reorderRecords() {
			List<OraCdcRedoRecord> sortedRecs = new ArrayList<>();
			int indexValue = -1;
			int indexHead = -1;
			int indexFirst= -1;
			int indexLast = -1;
			for (int index = 0; index < records.size(); index++) {
				final OraCdcRedoRecord record = records.get(index);
				final byte fb5_1 = record.change5_1().fb();
				final byte fb11_x = record.change11_x().fb();
				if (indexValue == -1)
					switch (lmOp) {
					case UPDATE:
						if (flgFirstPart(fb5_1) || flgLastPart(fb5_1) || flgHeadPart(fb5_1) ||
								flgFirstPart(fb11_x) || flgLastPart(fb11_x) || flgHeadPart(fb11_x)) {
							indexValue = index;
						}
						break;
					case INSERT:
						if (flgFirstPart(fb11_x) || flgLastPart(fb11_x) || flgHeadPart(fb11_x)) {
							indexValue = index;
						}
						break;
					case DELETE:
						if (flgFirstPart(fb5_1) || flgLastPart(fb5_1) || flgHeadPart(fb5_1)) {
							indexValue = index;
						}
						break;
					default:
						LOGGER.error("Unknown lmOp code!");
					}					
				switch (lmOp) {
				case UPDATE:
					if (homogeneous) {
						if (flgFirstPart(fb5_1) && flgFirstPart(fb11_x)) indexFirst = index;
						if (flgLastPart(fb5_1) && flgLastPart(fb11_x)) indexLast = index;	
						if (flgHeadPart(fb5_1) && flgHeadPart(fb11_x)) indexHead = index;
					} else {
						if (flgFirstPart(fb5_1) || flgFirstPart(fb11_x)) indexFirst = index;
						if (flgLastPart(fb5_1) || flgLastPart(fb11_x)) indexLast = index;	
						if (flgHeadPart(fb5_1) || flgHeadPart(fb11_x)) indexHead = index;
					}
					break;
				case INSERT:
					if (flgFirstPart(fb11_x)) indexFirst = index;
					if (flgLastPart(fb11_x)) indexLast = index;	
					if (flgHeadPart(fb11_x)) indexHead = index;
					break;
				case DELETE:
					if (flgFirstPart(fb5_1)) indexFirst = index;
					if (flgLastPart(fb5_1)) indexLast = index;	
					if (flgHeadPart(fb5_1)) indexHead = index;
					break;
				default:
					LOGGER.error("Unknown lmOp code!");
				}
			}
			if (indexHead > -1 && indexHead > indexFirst)
				oppositeOrder = true;
			else
				oppositeOrder = false;
			if (oppositeOrder)
				sortedRecs.add(records.get(indexLast));
			else
				sortedRecs.add(records.get(indexFirst));
			for (int index = 0; index < indexValue; index++)
				sortedRecs.add(records.get(index));
			if (oppositeOrder)
				sortedRecs.add(records.get(indexFirst));
			else
				sortedRecs.add(records.get(indexLast));
			records.clear();
			for (int index = 0; index < sortedRecs.size(); index++)
				records.add(sortedRecs.get(index));
			sortedRecs = null;
		}
	}

}
