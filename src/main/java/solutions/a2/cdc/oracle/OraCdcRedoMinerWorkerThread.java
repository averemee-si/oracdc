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
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
import solutions.a2.cdc.oracle.jmx.OraCdcSourceConnMgmt;
import solutions.a2.oracle.internals.LobId;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.Xid;
import solutions.a2.oracle.utils.BinaryUtils;
import solutions.a2.oracle.utils.FormattingUtils;
import solutions.a2.utils.ExceptionUtils;

import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_2_IRP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_3_DRP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_4_LKR;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_5_URP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_6_ORP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_8_CFA;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_10_SKL;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_11_QMI;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_12_QMD;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_16_LMN;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.formatOpCode;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeKrvXml.TYPE_XML_DOC;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.TYPE_1;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.TYPE_3;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.TYPE_4;
import static solutions.a2.oracle.utils.BinaryUtils.parseTimestamp;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcRedoMinerWorkerThread extends OraCdcWorkerThreadBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoMinerWorkerThread.class);
	private static final String SQL_STATE_REWIND = "RWND00";
	private static final int SMALL_MAGIC_WAIT = 21;

	private final OraCdcRedoMinerTask task;
	private final OraCdcSourceConnMgmt metrics;
	private boolean redoMinerReady = false;
	private final OraRedoMiner redoMiner;
	private final Map<Xid, OraCdcRawTransaction> activeTransactions;
	private final BlockingQueue<OraCdcRawTransaction> rawTransactions;
	private final Map<Integer, Xid> prefixedTransactions;
	private final TreeMap<Xid, Triple<Long, RedoByteAddress, Long>> sortedByFirstScn;
	private final ActiveTransComparator activeTransComparator;
	private final BinaryUtils bu;

	private Iterator<OraCdcRedoRecord> miner = null;

	private final int[] conUids;
	private final boolean conFilter;
	private final OraCdcDictionaryChecker checker;
	private boolean firstTransaction;
	private long lastCommitScn = 0;
	private int lwnTs = 0;
	private final ZoneId dbZoneId; 
	private final TreeMap<Long, List<OraCdcRedoRecord>> halfDoneRcm;
	private final Map<LobId, Xid> transFromLobId;
	private final Map<Xid, List<LobId>> lobIdFromTrans;
	private final Map<Integer, Integer> iotMapping;
	private final OraCdcLobExtras lobExtras;
	private final OraCdcRedoMinerEmitterThread emitter;

	public OraCdcRedoMinerWorkerThread(
			final OraCdcRedoMinerTask task,
			final OraCdcRedoMinerEmitterThread emitter,
			final Triple<Long, RedoByteAddress, Long> startFrom,
			final int[] conUids,
			final OraCdcDictionaryChecker checker,
			final Map<Xid, OraCdcRawTransaction> activeTransactions,
			final BlockingQueue<OraCdcRawTransaction> rawTransactions,
			final Map<Integer, Integer> iotMapping,
			final OraCdcSourceConnMgmt metrics,
			final boolean rewind) throws SQLException {
		super(task);
		LOGGER.info("Initializing oracdc Redo Miner worker thread");
		this.setDaemon(true);
		this.setName("OraCdcRedoMinerWorkerThread-" + System.nanoTime());
		this.task = task;
		this.emitter = emitter;
		this.activeTransactions = activeTransactions;
		this.rawTransactions = rawTransactions;
		this.metrics = metrics;
		this.conUids = conUids;
		if (conUids == null || conUids.length == 0)
			conFilter = false;
		else
			conFilter = true;
		this.checker = checker;
		this.iotMapping = iotMapping;
		activeTransComparator = new ActiveTransComparator(activeTransactions);
		sortedByFirstScn = new TreeMap<>(activeTransComparator);
		prefixedTransactions = new HashMap<>(task.config().transactionsInProcessSize(), .7f);
		this.bu = BinaryUtils.get(rdbmsInfo.littleEndian());
		dbZoneId = rdbmsInfo.getDbTimeZone();
		this.halfDoneRcm  = new TreeMap<>(new Comparator<Long>() {
			@Override
			public int compare(Long l1, Long l2) {
				return Long.compareUnsigned(l1, l2);
			}
		});
		if (processLobs) {
			transFromLobId = new HashMap<>();
			lobIdFromTrans = new HashMap<>();
			lobExtras = new OraCdcLobExtras();
		} else { 
			transFromLobId = null;
			lobIdFromTrans = null;
			lobExtras = null;
		}
		try (Connection connDictionary = oraConnections.getConnection()) {
			rdbmsInfo.initTde(connDictionary, config, bu);
			redoMiner = new OraRedoMiner(metrics, startFrom, config, runLatch, rdbmsInfo, oraConnections, bu);
		} catch (SQLException sqle) {
			LOGGER.error(
					"\n\nUnable to start OraCdcRedoMinerWorkerThread !\n" +
					"SQL Error ={}, SQL State = {}, SQL Message = '{}'\n\n",
					sqle.getErrorCode(), sqle.getSQLState(), sqle.getMessage());
			throw sqle;
		}
		// Finally - prepare for mining...
		redoMinerNext(startFrom.getLeft(), startFrom.getMiddle(), startFrom.getRight(), rewind);
	}

	@Override
	public void rewind(final long firstScn, final RedoByteAddress firstRba, final long firstSubScn) throws SQLException {
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
					} else if (Long.compareUnsigned(lastScn, firstScn) > 0 &&
							firstRba == null) {
						if (lastRba.blk() > 0x2)
							LOGGER.warn("Requested SCN {} not found, starting from SCN {} at RBA {} ", 
									Long.toUnsignedString(firstScn), Long.toUnsignedString(lastScn), lastRba);
						rewindNeeded = false;
						break;
					}
				} else {
					final StringBuilder reason = new StringBuilder(0x80);
					reason
						.append("Incorrect rewind to SCN = ")
						.append(Long.toUnsignedString(firstScn))
						.append(", RBA = ")
						.append(firstRba)
						.append(", SSN = ")
						.append(firstSubScn);
					throw new SQLException(reason.toString(), SQL_STATE_REWIND);
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
		firstTransaction = true;
		boolean notFirstRecord = false;
		while (runLatch.getCount() > 0) {
			try {
				if (redoMinerReady && runLatch.getCount() > 0) {
					miner = redoMiner.iterator();
					boolean firstInMinerSession = true;
					while (runLatch.getCount() > 0 && miner.hasNext()) {
						final OraCdcRedoRecord record = miner.next();
						if (record == null) {
							LOGGER.warn("Unexpected termination of redo records stream after RBA {}", lastRba);
							break;
						}
						lastScn = record.scn();
						lastRba = record.rba();
						lastSubScn = record.subScn();
						if (record.hasLwn()) {
							lwnTs = record.ts();
							while (halfDoneRcm.size() > 0) {
								if (Long.compareUnsigned(halfDoneRcm.firstKey(), lastScn) > 0) {
									break;
								} else {
									final Map.Entry<Long, List<OraCdcRedoRecord>> rcmList = halfDoneRcm.firstEntry();
									for (final OraCdcRedoRecord delayed : rcmList.getValue()) {
										emitRcm(delayed, delayed.xid());
										if (LOGGER.isDebugEnabled()) {
											LOGGER.debug("Emitting delayed OP:5.4 XID {}, Original SCN {}, Original RBA at SCN {}, RBA{}",
													delayed.xid(), delayed.scn(), delayed.rba(), record.scn(), record.rba());
										}
									}
									halfDoneRcm.remove(rcmList.getKey());
								}
							}
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug(
										"Start processing LWN at RBA {} with length {} blocks.",
										record.rba(), record.lwnLen());
							}
						}
						if (conFilter && 
								Arrays.binarySearch(conUids, record.conUid()) < 0) {
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug("Skipping RBA {} with CON_UID {}",
										record.rba(), record.conUid());
							}
							continue;
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
						//BEGIN: Main decision tree
						if (record.has5_4()) {
							if (LOGGER.isDebugEnabled()) {
								final int size = halfDoneRcm.size();
								LOGGER.debug("Adding XID {}, SCN {}, RBA {}  to delayed list with size {}, last delayed commit SCN {}",
										record.xid(), record.scn(), record.rba(), size, size > 0 ? halfDoneRcm.lastKey() : 0);
							}
							List<OraCdcRedoRecord> rcm = halfDoneRcm.get(record.scn());
							if (rcm == null) {
								rcm = new ArrayList<>(0x8);
								halfDoneRcm.put(record.scn(), rcm);
							}
							rcm.add(record);
							if (LOGGER.isDebugEnabled() && rcm.size() > 1) {
								final StringBuilder sb = new StringBuilder(0x400);
								sb
									.append("\nDuplicate commit SCN ")
									.append(record.scn())
									.append(".\nList of transactions at this commit SCN (XID, RBA):");
								rcm.forEach(rr ->
									sb
										.append("\n\t")
										.append(rr.xid().toString())
										.append('\t')
										.append(rr.rba().toString()));
								LOGGER.debug(sb.toString());
							}
							continue;
						}
						if (record.has5_1()) {
							if (record.has11_x()) {
								if (checker.notNeeded(record.change5_1().obj(), record.change5_1().conId()))
									continue;
								final short operation = record.change11_x().operation();
								switch (operation) {
									case _11_2_IRP, _11_3_DRP, _11_5_URP,_11_6_ORP -> {
										iotObjRemap(true, record);
										getTransaction(record).add(record, lwnTs);
									}
									case _11_16_LMN, _11_8_CFA ->
										getTransaction(record).add(record, lwnTs);
									case _11_4_LKR, _11_10_SKL -> {
										if (LOGGER.isDebugEnabled())
											LOGGER.debug("Skipping OP:{} at RBA {}", formatOpCode(operation), record.rba());
									}
									case _11_11_QMI, _11_12_QMD ->
										getTransaction(record).add(record, lwnTs);
									default -> {
										if (LOGGER.isDebugEnabled())
											LOGGER.debug("Skipping OP:{} at RBA {}", formatOpCode(operation), record.rba());
									}
								}
								continue;
							} else if (record.has10_x()) {
								if (checker.notNeeded(record.change5_1().obj(), record.change5_1().conId()))
									continue;
								iotObjRemap(false, record);
								if (record.change5_1().fb() == 0 &&
										record.change10_x().fb() == 0 &&
										record.change5_1().supplementalFb() == 0)
									continue;
								getTransaction(record).add(record, lwnTs);
								continue;
							} else if (processLobs && record.has26_x()) {
								if (checker.notNeeded(record.change5_1().obj(), record.change5_1().conId()))
									continue;
								final var raw = activeTransactions.get(record.xid());
								if (raw != null)
									if (record.change26_x().kdliFillLen() > -1)
										raw.add(record, lwnTs);
									else if (LOGGER.isDebugEnabled()) skippingDebugMsg("change.kdliFillLen() < 0", record.change26_x().operation(), record.rba());
								else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(transaction=null)", record.change26_x().operation(), record.rba());
								continue;
							} else {
								continue;
							}
						} else if (record.hasPrb() && record.has11_x()) {
							if (checker.notNeeded(record.changePrb().obj(), record.changePrb().conId()))
								continue;
							var suspiciousRecord = false;
							var raw = activeTransactions.get(record.xid());
							if (raw == null) {
								final Xid substitutedXid = prefixedTransactions.get(record.xid().partial());
								if (substitutedXid == null) {
									suspiciousRecord = true;
								} else {
									raw = activeTransactions.get(substitutedXid);
									if (raw == null) {
										suspiciousRecord = true;
									}
								}
							}
							final short operation = record.change11_x().operation();
							if (suspiciousRecord) {
								final String suspiciousMsg =
										"""
										
										=====================
										
										The transaction with XID='{}' starts with with the record with PARTIAL ROLLBACK flag set to true!
										SCN={}, RBA={}, redo Record details:
										{}
										
										If you have questions or need more information, please write to us at oracle@a2.solutions
										
										=====================
										
										""";
								if (operation == _11_4_LKR)
									LOGGER.debug(suspiciousMsg,
											record.xid(), lastScn, lastRba, record.toString());
								else
									LOGGER.error(suspiciousMsg,
											record.xid(), lastScn, lastRba, record.toString());
							} else {
								switch (operation) {
									case _11_2_IRP, _11_3_DRP, _11_5_URP,_11_6_ORP ->
										raw.add(record, lwnTs);
									case _11_11_QMI, _11_12_QMD ->
									raw.add(record, lwnTs);
									default -> {
										if (operation == _11_4_LKR) {
											if (LOGGER.isDebugEnabled())
												LOGGER.debug("Skipping partial rollback OP:{} at RBA {}", formatOpCode(operation), record.rba());
										} else
											LOGGER.warn("Skipping partial rollback OP:{} at RBA {}", formatOpCode(operation), record.rba());
									}
								}
							}
							continue;
						} else if (record.hasColb()) {
							final var colb = record.changeColb();
							if (processLobs && colb.longDump()) {
								final var lid = colb.lid();
								if (lid != null) {
									final var xid = transFromLobId.get(lid);
									if (xid != null) {
										final var raw = activeTransactions.get(xid);
										if (raw != null) {
											raw.add(record, lwnTs);
										} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(null transaction)", colb.operation(), record.rba());
									} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(XID=NULL)", colb.operation(), record.rba());
								} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(LOB_ID=NULL)", colb.operation(), record.rba());
							} else if (!colb.longDump() && record.hasKrvDlr10()) {
								if (checker.notNeeded(colb.obj(), colb.conId()))
									continue;
								getTransaction(record).add(record, lwnTs);
							} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("", colb.operation(), record.rba());
							continue;
						} else if (processLobs) {
							if (record.hasLlb()) {
								if (checker.notNeeded(record.changeLlb().obj(), record.changeLlb().conId()))
									continue;
								final var llb = record.changeLlb();
								final var raw = getTransaction(record);
								if (llb.type() == TYPE_1) {
									var xid = record.xid();
									transFromLobId.put(llb.lid(), xid);
									var lobIds = lobIdFromTrans.get(xid);
									if (lobIds == null) {
										lobIds = new ArrayList<>();
										lobIdFromTrans.put(xid, lobIds);
									}
									lobIds.add(llb.lid());
									raw.add(record, lwnTs);
								} else if (llb.type() == TYPE_3) {
									if (lobExtras.intColumnId(llb.obj(), llb.lobCol(), true) == -1)
										raw.add(record, lwnTs);
									else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(TYP:3 for XMLTYPE)", llb.operation(), record.rba());
								} else if (llb.type() == TYPE_4) {
									if (llb.hasXmlType()) {
										lobExtras.buildColMap(llb);
									} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(TYP:4)", llb.operation(), record.rba());
								}
								continue;
							} else if (record.hasKrvXml()) {
								final var xml = record.changeKrvXml();
								if (xml.type() == TYPE_XML_DOC) {
									if (checker.notNeeded(xml.obj(), xml.conId()))
										continue;
									final var raw = getTransaction(record);
									raw.add(record, lwnTs);
								} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(TYP!=1)", xml.operation(), record.rba());
								continue;
							} else if (record.has26_x()) {
								var checkDataObj = true;
								final var change = record.change26_x();
								if (change.obj() != 0) {
									if (checker.notNeeded(change.obj(), change.conId()))
										continue;
									else
										checkDataObj = false;
								}
								if (checkDataObj && change.dataObj() != 0) {
									if (checker.notNeeded(change.dataObj(), change.conId()))
										continue;
								} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(checkDataObj && dataObj=0)", change.operation(), record.rba());
								final var lid = change.lid();
								if (lid != null) {
									Xid xid = transFromLobId.get(lid);
									if (xid != null) {
										if (change.lobBimg()) {
											final var raw = activeTransactions.get(xid);
											if (raw != null) {
												raw.add(record, lwnTs);
											} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(transaction=null)", change.operation(), record.rba());
										} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(lobBimg()=false)", change.operation(), record.rba());
									} else {
											//OP:26.6 without accompanying OP:11.17
										if (change.lobBimg()) {
											getTransaction(record).add(record, lwnTs);
										} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(lobBimg()=false)", change.operation(), record.rba());
									}
								} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(LOB_ID=NULL)", record.change26_x().operation(), record.rba());
								continue;
							}
						} else if (record.hasDdl()) {
							final var ddl = record.changeDdl();
							if (ddl.valid()) {
								if (checker.notNeeded(ddl.obj(), ddl.conId()))
									continue;
								else
									getTransaction(record).add(record, lwnTs);
							} else {
								continue;
							}
						} else if (record.hasBeginTrans()) {
							getTransaction(record);
							continue;
						} else {
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug("Skipping redo record at RBA {}", record.rba());
							}
							continue;
						}
						//END: Main decision tree

					} // while (isRsLogMinerRowAvailable && runLatch.getCount() > 0)
					//TODO - do we need to pass lastScn???
					redoMiner.stop(lastRba, lastScn);
					miner = null;
					if (activeTransactions.isEmpty() && lastScn > 0) {
						// Update restart point in time
						task.putReadRestartScn(Triple.of(lastScn, lastRba, lastSubScn));
					}
					redoMinerNext(lastScn, lastRba, lastSubScn, false);
				}
			} catch (IOException sftpe) {
				redoMinerNext(lastScn, lastRba, lastSubScn, false);
			} catch (Exception e) {
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
					.append("\n=====================\n");
				LOGGER.error(sb.toString());
				running.set(false);
				task.stop(false);
				throw new ConnectException(e);
			}
		} // while (runLatch.getCount() > 0)
		running.set(false);
		LOGGER.info("END: OraCdcRedoMinerWorkerThread.run()");
	}

	private static class ActiveTransComparator implements Comparator<Xid> {

		private final Map<Xid, OraCdcRawTransaction> activeTransactions;

		ActiveTransComparator(final Map<Xid, OraCdcRawTransaction> activeTransactions) {
			this.activeTransactions = activeTransactions;
		}

		@Override
		public int compare(Xid first, Xid second) {
			if (first.equals(second)) {
				// A transaction ID is unique to a transaction and represents the undo segment number, slot, and sequence number.
				// https://docs.oracle.com/en/database/oracle/oracle-database/26/cncpt/transactions.html#GUID-E3FB3DC3-3317-4589-BADD-D89A3547F87D
				return 0;
			}

			OraCdcRawTransaction firstOraTran = activeTransactions.get(first);
			OraCdcRawTransaction secondOraTran = activeTransactions.get(second);

			if (firstOraTran != null && secondOraTran != null && Long.compareUnsigned(firstOraTran.firstChange(), secondOraTran.firstChange()) >= 0) {
				return 1;
			}

			return -1;
		}

	}

	private void createTransactionPrefix(final Xid xid, final RedoByteAddress rba) {
		final int partial = xid.partial();
		final Xid prevXid = prefixedTransactions.put(partial, xid);
		if (prevXid != null) {
			final StringBuilder sb = new StringBuilder(
					(halfDoneRcm.size() + activeTransactions.size()) * 0x80 + 0x200);
			sb
				.append("\n=====================\nTransaction prefix ")
				.append(String.format("0x%04x", partial >> 16))
				.append('.')
				.append(String.format("%03x", Short.toUnsignedInt((short)partial)))
				.append(" binding changed from ")
				.append(prevXid.toString())
				.append(" to ")
				.append(xid.toString())
				.append(" at RBA ")
				.append(rba.toString());
			sb.append(printHalfDoneRcmContents());
			if (activeTransactions.size() > 0) {
				sb.append("\n\nList of transactions in progress (XID, FIRST_CHANGE#, NEXT_CHANGE#, NUMBBER_OF_CHANGES, SIZE_IN_BYTES)");
				final var sbPrefix = new StringBuilder();
				sbPrefix.append("0x");
				FormattingUtils.leftPad(sbPrefix, prevXid.usn(), 4);
				sbPrefix.append('.');
				FormattingUtils.leftPad(sbPrefix, prevXid.slt(), 3);
				for (final var t : activeTransactions.values()) {
					if (Strings.CI.startsWith(t.xid().toString(), sbPrefix.toString()))
						sb
							.append("\n\t")
							.append(t.xid().toString())
							.append('\t')
							.append(t.firstChange())
							.append('\t')
							.append(t.nextChange())
							.append('\t')
							.append(t.length())
							.append('\t')
							.append(t.size());
				}
			}
			sb.append("\n=====================\n");
			LOGGER.warn(sb.toString());
		}
	}

	private void emitRcm(final OraCdcRedoRecord record, final Xid xid) {
		var raw = activeTransactions.get(xid);
		var rollback = record.change5_4().rollback();
		if (raw == null) {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Skipping {} at SCN={}, RBA={} for transaction XID {}",
						rollback ? "ROLLBACK" : "COMMIT", lastScn, lastRba, xid);
		} else {
			if (processLobs) {
				//TODO
				//TODO
				//TODO What if same LOB participate in concurrent transactions?
				//TODO
				//TODO
				var lobIds = lobIdFromTrans.get(xid);
				if (lobIds != null) {
					for (var lid : lobIds) {
						transFromLobId.remove(lid);
					}
					lobIds = null;
					lobIdFromTrans.remove(xid);
				}
			}
			if (rollback) {
				if (LOGGER.isDebugEnabled())
					LOGGER.debug(
							"Rolling back transaction {} at SCN/RBA {}/{}, LWN SCN/RBA={}/{}, FIRST_CHANGE#={} with {} changes and size {} bytes",
							xid, record.scn(), record.rba(), lastScn, lastRba, raw.firstChange(), raw.length(), raw.size());
				metrics.addRolledBackRecords(raw.length(), raw.size(), activeTransactions.size());
			} else {
				final var commitScn = record.scn();
				if (raw.hasRows()) {
					raw.commitScn(commitScn);
					rawTransactions.add(raw);
					metrics.addCommittedRecords(raw.length(), raw.size(), rawTransactions.size(), activeTransactions.size());
					if (LOGGER.isDebugEnabled())
						LOGGER.debug(
								"Committing transaction {} at SCN={}, RBA={}, FIRST_CHANGE#={} with {} changes and size {} bytes",
								xid, record.scn(), record.rba(), raw.firstChange(), raw.length(), raw.size());
				} else {
					rollback = true;
					if (LOGGER.isDebugEnabled())
						LOGGER.debug("Skipping empty transaction {} at COMMIT_SCN={}",
								xid, commitScn);
				}
				if (Long.compareUnsigned(commitScn, lastCommitScn) < 0) {
					LOGGER.warn("Committing transaction {} with a commit SCN {} lower than the previous one {}!",
							xid, commitScn, lastCommitScn);
				} else {
					lastCommitScn = commitScn;
				}
			}
			sortedByFirstScn.remove(xid);
			activeTransactions.remove(xid);
			prefixedTransactions.remove(xid.partial());
			if (!sortedByFirstScn.isEmpty()) {
				task.putReadRestartScn(sortedByFirstScn.firstEntry().getValue());
			} else {
				firstTransaction = true;
			}
			if (rollback) {
				raw.close();
				raw = null;
			}
		}
	}

	@Override
	public void shutdown() {
		if (halfDoneRcm.size() > 0) {
			final StringBuilder sb = new StringBuilder(halfDoneRcm.size() * 0x40 + 0x10);
			sb
				.append("\n=====================\n")
				.append(printHalfDoneRcmContents())
				.append("\n=====================\n");
			LOGGER.warn(sb.toString());
		}
		if (redoMiner != null) {
			try {
				redoMiner.stop(lastRba, lastScn);
			} catch (IOException | SQLException e) {
				LOGGER.error(
						"\n=====================\n" +
						"{} while stopping RedoMiner. Stack trace:\n{}" +
						"\n=====================\n",
						e.getMessage(), ExceptionUtils.getExceptionStackTrace(e));
			}
		}
		super.shutdown();
	}

	private StringBuilder printHalfDoneRcmContents() {
		final StringBuilder sb = new StringBuilder(halfDoneRcm.size() * 0x40 + 0x10);
		if (halfDoneRcm.size() > 0) {
			sb.append("\n\nList of transactions with delayed commit (XID, RBA, FIRST_CHANGE#, COMMIT_SCN#)");
			for (final Map.Entry<Long, List<OraCdcRedoRecord>> entry : halfDoneRcm.entrySet()) {
				for (final OraCdcRedoRecord record : entry.getValue()) {
					if (activeTransactions.containsKey(record.xid())) {
						sb
							.append("\n\t")
							.append(record.xid().toString())
							.append('\t')
							.append(record.rba().toString())
							.append('\t')
							.append(record.hasKrvMisc() ? record.changeKrvMisc().startScn() : "N/A")
							.append('\t')
							.append(entry.getKey());
					}
				}
			}
		}
		return sb;
	}

	private OraCdcRawTransaction getTransaction(final OraCdcRedoRecord record) {
		var xid = record.xid();
		var raw = activeTransactions.get(xid);
		if (raw == null) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("New transaction {} created. Transaction start timestamp {}, first SCN {}.",
						xid, Instant.ofEpochMilli(parseTimestamp(lwnTs).atZone(dbZoneId).toInstant().toEpochMilli()), lastScn);
			}
			while (emitter.coolDown()) {
				try {
					Thread.sleep(reduceLoadMs);
				} catch(InterruptedException ie) {}
			}
			raw = new OraCdcRawTransaction(xid, dbZoneId, initialCapacity, lobExtras);
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"Starting transaction {} at SCN={}, RBA={}",
						xid, record.scn(), record.rba());
			}
			var duplicateXid = activeTransactions.put(xid, raw);
			if (duplicateXid != null) {
				
				LOGGER.error(
						"""
						
						=====================
						Duplicate hash value for '{}' and '{}'!
						Please send this message to oracle@a2.solutions
						=====================
						
						""", xid, duplicateXid.xid());
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
		return raw;
	}

	private void skippingDebugMsg(final String reason, final short operation, final RedoByteAddress rba) {
		LOGGER.debug("Skipping {} OP:{} at RBA {}",
			reason, formatOpCode(operation), rba);

	}

	private void redoMinerNext(final long startScn, final RedoByteAddress startRba, final long startSubScn, final boolean rewind) {
		var attempt = 0;
		final var redoMinerReadyStart = System.currentTimeMillis();
		redoMinerReady = false;
		while (!redoMinerReady && runLatch.getCount() > 0) {
			try {
				if (redoMinerReady = redoMiner.next()) {
					if (attempt > 0 || rewind)
						rewind(startScn, startRba, startSubScn);
					return;
				} else {
					synchronized(this) {
						try {wait(SMALL_MAGIC_WAIT); } catch (InterruptedException ie) {}
					}
					if (LOGGER.isDebugEnabled()) LOGGER.debug("Waiting for readiness {} ms", SMALL_MAGIC_WAIT);
					continue;
				}
			} catch (SQLException sqle) {
				if (LOGGER.isDebugEnabled())
					LOGGER.debug("RedoMiner is not ready due to {}.\nStack trace:\n{}\n",
							sqle.getMessage(), ExceptionUtils.getExceptionStackTrace(sqle));
				if (attempt > Byte.MAX_VALUE && runLatch.getCount() > 0) {
					LOGGER.error(
							"""
							
							=====================
							Unable to execute redoMiner.next() (attempt #{}) after {} ms.
							=====================
							
							""", attempt, System.currentTimeMillis() - redoMinerReadyStart);
					throw new ConnectException(sqle);
				} else if (runLatch.getCount() < 1)
					return;
				else if (Strings.CS.equals(sqle.getSQLState(), SQL_STATE_REWIND)) {
					synchronized(this) {
						try {wait(SMALL_MAGIC_WAIT); } catch (InterruptedException ie) {}
					}
					if (LOGGER.isDebugEnabled()) LOGGER.debug("Waiting for rewind {} ms", SMALL_MAGIC_WAIT);
					continue;
				}
				else {
					if (redoMiner.waitOnError())
						LOGGER.error(
							"""
							
							=====================
							Failed to execute redoMiner.next() (attempt #{}) due to '{}'.
							oracdc will try again to execute redoMiner.next() in {} ms. 
							=====================
							
							""", attempt, sqle.getMessage(), backoffMs);
				}
			}
			if (redoMiner.waitOnError()) {
				if (!redoMinerReady && runLatch.getCount() > 0) {
					if (LOGGER.isDebugEnabled()) LOGGER.debug("Waiting {} ms", backoffMs);
					synchronized(this) {
						try {wait(backoffMs); } catch (InterruptedException ie) {}
					}
					try {
						redoMiner.resetRedoLogFactory(startScn, startRba);
					} catch (SQLException sqle) {
						LOGGER.error(
								"""
								
								=====================
								'{}' while resetting RLF!
								errorCode={}, SQLState='{}'. 
								=====================
								
								""", sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
						if (LOGGER.isDebugEnabled())
							LOGGER.debug("Stack trace for RLF reset:\n{}\n",
									sqle.getMessage(), ExceptionUtils.getExceptionStackTrace(sqle));
					}
					attempt++;
				}
			}
		}
	}

	private void iotObjRemap(final boolean overflow, final OraCdcRedoRecord record) {
		final Integer iotObjId = iotMapping.get(record.change5_1().obj());
		if (iotObjId  != null) {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug(
						"Remapping {} obj {} at RBA {} to parent obj {}",
						overflow ? "OVF" : "IND", record.change5_1().obj(), record.rba(), iotObjId);
			record.change5_1().obj(iotObjId);
			record.change5_1().dataObj(iotObjId);
			if (overflow) {
				record.change11_x().obj(iotObjId);
				record.change11_x().dataObj(iotObjId);
			} else {
				record.change10_x().obj(iotObjId);
				record.change10_x().dataObj(iotObjId);
			}
		}
	}

}
