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

import solutions.a2.cdc.oracle.internals.OraCdcChange;
import solutions.a2.cdc.oracle.internals.OraCdcChangeColb;
import solutions.a2.cdc.oracle.internals.OraCdcChangeDdl;
import solutions.a2.cdc.oracle.internals.OraCdcChangeKrvXml;
import solutions.a2.cdc.oracle.internals.OraCdcChangeLlb;
import solutions.a2.cdc.oracle.internals.OraCdcChangeLobs;
import solutions.a2.cdc.oracle.internals.OraCdcChangeRowOp;
import solutions.a2.cdc.oracle.internals.OraCdcChangeUndo;
import solutions.a2.cdc.oracle.internals.OraCdcChangeUndoBlock;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLog;
import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
import solutions.a2.cdc.oracle.jmx.OraCdcRedoMinerMgmt;
import solutions.a2.oracle.internals.LobId;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;
import solutions.a2.oracle.internals.Xid;
import solutions.a2.oracle.utils.BinaryUtils;
import solutions.a2.utils.ExceptionUtils;

import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DDL;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.utils.OraSqlUtils.alterTablePreProcessor;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.FLG_ROWDEPENDENCIES;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.FLG_KDLI_CMAP;
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
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.TYPE_1;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.TYPE_3;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.TYPE_4;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLobs.LOB_BIMG_INDEX;
import static solutions.a2.oracle.utils.BinaryUtils.parseTimestamp;
import static solutions.a2.oracle.utils.BinaryUtils.putU16;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcRedoMinerWorkerThread extends OraCdcWorkerThreadBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoMinerWorkerThread.class);
	private static final byte[] ZERO_COL_COUNT = {0, 0};
	private static final String SQL_STATE_REWIND = "RWND00";
	private static final int SMALL_MAGIC_WAIT = 21;

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

	private final int[] conUids;
	private final boolean conFilter;
	private final OraCdcDictionaryChecker checker;
	private boolean firstTransaction;
	private long lastCommitScn = 0;
	private long lwnUnixMillis = 0;
	private final TreeMap<Long, List<OraCdcRedoRecord>> halfDoneRcm;
	private final Map<LobId, Xid> transFromLobId;
	private final Map<Integer, Map<Short, Short>[]> intColIdsMap;
	private final Map<Integer, Integer> iotMapping;

	public OraCdcRedoMinerWorkerThread(
			final OraCdcRedoMinerTask task,
			final Triple<Long, RedoByteAddress, Long> startFrom,
			final int[] conUids,
			final OraCdcDictionaryChecker checker,
			final Map<Xid, OraCdcTransaction> activeTransactions,
			final BlockingQueue<OraCdcTransaction> committedTransactions,
			final Map<Integer, Integer> iotMapping,
			final OraCdcRedoMinerMgmt metrics,
			final boolean rewind) throws SQLException {
		super(task.runLatch(), task.rdbmsInfo(), task.config(),
				task.oraConnections(), committedTransactions);
		LOGGER.info("Initializing oracdc Redo Miner worker thread");
		this.setDaemon(true);
		this.setName("OraCdcRedoMinerWorkerThread-" + System.nanoTime());
		this.task = task;
		this.activeTransactions = activeTransactions;
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
		prefixedTransactions = new HashMap<>();
		this.bu = BinaryUtils.get(rdbmsInfo.littleEndian());
		this.halfDoneRcm  = new TreeMap<>(new Comparator<Long>() {
			@Override
			public int compare(Long l1, Long l2) {
				return Long.compareUnsigned(l1, l2);
			}
		});
		if (processLobs) {
			transFromLobId = new HashMap<>();
			intColIdsMap = new HashMap<>();
		} else { 
			transFromLobId = null;
			intColIdsMap = null;
		}
		try {
			connDictionary = oraConnections.getConnection();
			rdbmsInfo.initTde(connDictionary, config, bu);
			redoMiner = new OraRedoMiner(
					connDictionary, metrics, startFrom, config, runLatch, rdbmsInfo, oraConnections, bu);
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

	@SuppressWarnings("unchecked")
	@Override
	public void run()  {
		LOGGER.info("BEGIN: OraCdcRedoMinerWorkerThread.run()");
		running.set(true);
		firstTransaction = true;
		boolean notFirstRecord = false;
		final ZoneId dbZoneId = rdbmsInfo.getDbTimeZone();
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
							lwnUnixMillis = parseTimestamp(record.ts()).atZone(dbZoneId).toInstant().toEpochMilli();
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
						//BEGIN: Main decision tree
						if (record.has5_4()) {
							addToHalfDoneRcm(record);
							continue;
						} else if (record.has5_1() && record.has11_x()) {
							if (checker.notNeeded(record.change5_1().obj(), record.change5_1().conId()))
								continue;
							final short operation = record.change11_x().operation();
							switch (operation) {
							case _11_2_IRP:
							case _11_3_DRP:
							case _11_5_URP:
							case _11_6_ORP:
								iotObjRemap(true, record);
								getTransaction(record).processRowChange(record, false, lwnUnixMillis);
								break;
							case _11_16_LMN:
								getTransaction(record).processRowChangeLmnUpdate(record, lwnUnixMillis);
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
								emitMultiRowChange(getTransaction(record), record, false);
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
							if (checker.notNeeded(record.changePrb().obj(), record.changePrb().conId()))
								continue;
							boolean suspiciousRecord = false;
							final Xid xid = record.xid();
							OraCdcTransaction transaction = activeTransactions.get(xid);
							if (transaction == null) {
								final Xid substitutedXid = prefixedTransactions.get(xid.partial());
								if (substitutedXid == null) {
									suspiciousRecord = true;
								} else {
									transaction = activeTransactions.get(substitutedXid);
									if (transaction == null) {
										suspiciousRecord = true;
									}
								}
							}
							final short operation = record.change11_x().operation();
							if (suspiciousRecord) {
								final String suspiciousMsg =
										"\n=====================\n\n" +
										"The transaction with XID='{}' starts with with the record with PARTIAL ROLLBACK flag set to true!\n" +
										"SCN={}, RBA={}, redo Record details:\n{}\n" +
										"If you have questions or need more information, please write to us at oracle@a2.solutions\n\n" +
										"\n=====================\n";
								if (operation == _11_4_LKR)
									LOGGER.debug(suspiciousMsg,
											xid, lastScn, lastRba, record.toString());
								else
									LOGGER.error(suspiciousMsg,
											xid, lastScn, lastRba, record.toString());
							} else {
								switch (operation) {
								case _11_2_IRP:
								case _11_3_DRP:
								case _11_5_URP:
								case _11_6_ORP:
									transaction.processRowChange(record, true, lwnUnixMillis);
									break;
								case _11_11_QMI:
								case _11_12_QMD:
									emitMultiRowChange(transaction, record, true);
									break;
								default:
									if (operation == _11_4_LKR) {
										if (LOGGER.isDebugEnabled())
											LOGGER.debug("Skipping partial rollback OP:{} at RBA {}", formatOpCode(operation), record.rba());
									} else {
										LOGGER.warn("Skipping partial rollback OP:{} at RBA {}", formatOpCode(operation), record.rba());
									}
								}
							}
							continue;
						} else if (record.has5_1() && record.has10_x()) {
							if (checker.notNeeded(record.change5_1().obj(), record.change5_1().conId()))
								continue;
							iotObjRemap(false, record);
							if (record.change5_1().fb() == 0 &&
									record.change10_x().fb() == 0 &&
									record.change5_1().supplementalFb() == 0)
								continue;
							getTransaction(record).processRowChange(record, false, lwnUnixMillis);
							continue;
						} else if (record.hasColb()) {
							final OraCdcChangeColb colb = record.changeColb();
							if (processLobs && colb.longDump()) {
								final LobId lid = colb.lid();
								if (lid != null) {
									final Xid xid = transFromLobId.get(lid);
									if (xid != null) {
										final OraCdcTransactionChronicleQueue transaction = 
												(OraCdcTransactionChronicleQueue) activeTransactions.get(xid);
										if (transaction != null) {
											transaction.writeLobChunk(lid, colb.record(), colb.coords()[0][0] + OraCdcChangeColb.LONG_DUMP_SIZE, colb.colbSize(), true, false);
										} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(null transaction)", colb.operation(), record.rba());
									} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(XID=NULL)", colb.operation(), record.rba());
								} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(LOB_ID=NULL)", colb.operation(), record.rba());
							} else if (!colb.longDump() && record.hasKrvDlr10()) {
								if (checker.notNeeded(colb.obj(), colb.conId()))
									continue;
								emitDirectBlockChange(getTransaction(record), record, colb);
							} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("", colb.operation(), record.rba());
							continue;
						} else if (processLobs && record.hasLlb()) {
							if (checker.notNeeded(record.changeLlb().obj(), record.changeLlb().conId()))
								continue;
							final OraCdcChangeLlb llb = record.changeLlb();
							final OraCdcTransactionChronicleQueue transaction =
									(OraCdcTransactionChronicleQueue) getTransaction(record);
							if (llb.type() == TYPE_1) {
								transFromLobId.put(llb.lid(), record.xid());
								transaction.openLob(
										llb.lid(), llb.obj(), llb.lobCol(), llb.lobOp(), record.rba(),
										intColumnId(llb.obj(), llb.lobCol(), true) == -1 ? true : false);
							} else if (llb.type() == TYPE_3) {
								if (intColumnId(llb.obj(), llb.lobCol(), true) == -1)
									transaction.closeLob(llb.obj(), llb.lobCol(), llb.fsiz(), record.rba());
								else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(TYP:3 for XMLTYPE)", llb.operation(), record.rba());
							} else if (llb.type() == TYPE_4) {
								if (llb.hasXmlType()) {
									final short[][] columnMap = llb.columnMap();
									Map<Short, Short>[] columns = intColIdsMap.get(llb.obj());
									if (columns == null) {
										columns = (Map<Short, Short>[]) new Map[2];
										columns[0] = new HashMap<>();
										columns[1] = new HashMap<>();
										intColIdsMap.put(llb.obj(), columns);
									}
									for (int i = 0; i < columnMap.length; i++)
										if (!columns[0].containsKey(columnMap[i][0])) {
											columns[0].put(columnMap[i][0], columnMap[i][1]);
											columns[1].put(columnMap[i][1], columnMap[i][0]);
										}
								} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(TYP:4)", llb.operation(), record.rba());
							}
							continue;
						} else if (processLobs && record.hasKrvXml()) {
							final OraCdcChangeKrvXml xml = record.changeKrvXml();
							if (xml.type() == OraCdcChangeKrvXml.TYPE_XML_DOC) {
								if (checker.notNeeded(xml.obj(), xml.conId()))
									continue;
								final OraCdcTransactionChronicleQueue transaction =
										(OraCdcTransactionChronicleQueue) getTransaction(record);
								final short xmlColId = intColumnId(xml.obj(), xml.internalColId(), false);
								if ((xml.status() & OraCdcChangeKrvXml.XML_DOC_BEGIN) != 0)
										transaction.openLob(xml.obj(), xmlColId, record.rba());
								transaction.writeLobChunk(
										xml.obj(), xmlColId, xml.record(), xml.coords()[7][0], xml.coords()[7][1]);
								if ((xml.status() & OraCdcChangeKrvXml.XML_DOC_END) != 0)
									transaction.closeLob(xml.obj(), xmlColId, 0, record.rba());
							} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(TYP!=1)", xml.operation(), record.rba());
							continue;
						} else if (processLobs && record.has5_1() && record.has26_x()) {
							if (checker.notNeeded(record.change5_1().obj(), record.change5_1().conId()))
								continue;
							final LobId lid = record.change26_x().lid();
							final OraCdcTransactionChronicleQueue transaction = 
									(OraCdcTransactionChronicleQueue) activeTransactions.get(record.xid());
							final OraCdcChangeLobs change = record.change26_x();
							if (transaction != null)
								if (change.kdliFillLen() > -1)
									transaction.writeLobChunk(
											lid, change.record(), change.lobDataOffset(), change.kdliFillLen(),
											false, (change.kdli_flg2() & FLG_KDLI_CMAP) > 0);
								else if (LOGGER.isDebugEnabled()) skippingDebugMsg("change.kdliFillLen() < 0", change.operation(), record.rba());
							else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(transaction=null)", change.operation(), record.rba());
							continue;
						} else if (processLobs && record.has26_x()) {
							boolean checkDataObj = true;
							if (record.change26_x().obj() != 0) {
								if (checker.notNeeded(record.change26_x().obj(), record.change26_x().conId()))
									continue;
								else
									checkDataObj = false;
							}
							if (checkDataObj && record.change26_x().dataObj() != 0) {
								if (checker.notNeeded(record.change26_x().dataObj(), record.change26_x().conId()))
									continue;
							} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(checkDataObj && dataObj=0)", record.change26_x().operation(), record.rba());
							final LobId lid = record.change26_x().lid();
							if (lid != null) {
								Xid xid = transFromLobId.get(lid);
								if (xid != null) {
									final OraCdcChangeLobs change = record.change26_x();
									if (change.lobBimg()) {
										final OraCdcTransactionChronicleQueue transaction = 
												(OraCdcTransactionChronicleQueue) activeTransactions.get(xid);
										if (transaction != null) {
											final int[][] coords = change.coords();
											transaction.writeLobChunk(
												lid, change.record(), coords[LOB_BIMG_INDEX][0], coords[LOB_BIMG_INDEX][1],
												false, (change.kdli_flg2() & FLG_KDLI_CMAP) > 0);
										} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(transaction=null)", change.operation(), record.rba());
									} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(lobBimg()=false)", change.operation(), record.rba());
								} else {
										//OP:26.6 without accompanying OP:11.17
									final OraCdcChangeLobs change = record.change26_x();
									if (change.lobBimg()) {
										final OraCdcTransactionChronicleQueue transaction =
												(OraCdcTransactionChronicleQueue) getTransaction(record);
										final int[][] coords = change.coords();
										transaction.writeLobChunk(
												lid, change.dataObj(), change.record(), coords[LOB_BIMG_INDEX][0], coords[LOB_BIMG_INDEX][1],
												(change.kdli_flg2() & FLG_KDLI_CMAP) > 0);
									} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(lobBimg()=false)", change.operation(), record.rba());
								}
							} else if (LOGGER.isDebugEnabled()) skippingDebugMsg("(LOB_ID=NULL)", record.change26_x().operation(), record.rba());
							continue;
						} else if (record.hasDdl()) {
							final OraCdcChangeDdl ddl = record.changeDdl();
							if (ddl.valid()) {
								if (checker.notNeeded(ddl.obj(), ddl.conId()))
									continue;
								else
									emitDdlChange(record);
							} else {
								continue;
							}
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

			if (firstOraTran != null && secondOraTran != null && Long.compareUnsigned(firstOraTran.getFirstChange(), secondOraTran.getFirstChange()) >= 0) {
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
					(halfDoneRcm.size() + committedTransactions.size() + activeTransactions.size()) * 0x80 + 0x200);
			sb
				.append("\n=====================\nTransaction prefix ")
				.append(String.format("0x%04x", partial >> 16))
				.append('.')
				.append(String.format("0x%03x", Short.toUnsignedInt((short)partial)))
				.append(" binding changed from ")
				.append(prevXid.toString())
				.append(" to ")
				.append(xid.toString())
				.append(" at RBA ")
				.append(rba.toString());
			if (committedTransactions.size() > 0) {
				sb.append("\n\nList of transactions ready to be sent to Kafka (XID, FIRST_CHANGE#, COMMIT_SCN#, NUMBBER_OF_CHANGES, SIZE_IN_BYTES)");
				Iterator<OraCdcTransaction> iterator = committedTransactions.iterator();
				while (iterator.hasNext()) {
					final OraCdcTransaction t = iterator.next();
					sb
						.append("\n\t")
						.append(t.getXid().toString())
						.append('\t')
						.append(t.getFirstChange())
						.append('\t')
						.append(t.getCommitScn())
						.append('\t')
						.append(t.length())
						.append('\t')
						.append(t.size());
				}
			}
			sb.append(printHalfDoneRcmContents());
			if (activeTransactions.size() > 0) {
				sb.append("\n\nList of transactions in progress (XID, FIRST_CHANGE#, NEXT_CHANGE#, NUMBBER_OF_CHANGES, SIZE_IN_BYTES)");
				for (final OraCdcTransaction t : activeTransactions.values()) {
					sb
						.append("\n\t")
						.append(t.getXid().toString())
						.append('\t')
						.append(t.getFirstChange())
						.append('\t')
						.append(t.getNextChange())
						.append('\t')
						.append(t.length())
						.append('\t')
						.append(t.size());
				}
			}
			sb.append("\n=====================\n");
			LOGGER.error(sb.toString());
		}
	}

	private void emitMultiRowChange(
			final OraCdcTransaction transaction,
			final OraCdcRedoRecord rr,
			final boolean partialRollback) throws IOException {
		final int index;
		final short lmOp;
		final OraCdcChangeRowOp rowChange = rr.change11_x();
		final OraCdcChangeUndo change;
		if (partialRollback) {
			change = rr.changePrb();
			index = OraCdcChangeRowOp.KDO_POS;
			lmOp = rowChange.operation() == _11_11_QMI ? INSERT : DELETE;
			final byte[] record = rowChange.record();
			final int[][] coords = rowChange.coords();
			for (int row = 0; row < rowChange.qmRowCount(); ++row) {
				final RowId rowId = new RowId(
						change.dataObj(),
						rowChange.bdba(),
						bu.getU16(record, coords[index][0] + 0x14 + row * Short.BYTES));
				final OraCdcRedoMinerStatement orm = new OraCdcRedoMinerStatement(
						isCdb ? (((long)change.conId()) << 32) |  (change.obj() & 0xFFFFFFFFL): change.obj(),
						lmOp, ZERO_COL_COUNT, lwnUnixMillis, rr.scn(), rr.rba(),
						(long) rr.subScn(), rowId, partialRollback);
				transaction.addStatement(orm);
			}
		} else {
			change = rr.change5_1();
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
			final int rowCount = change.qmRowCount();
			for (int row = 0; row < rowCount; ++row) {
				rowDiff += 0x2;
				final int columnCount = Byte.toUnsignedInt(record[coords[index + 2][0] + rowDiff++]);
				if ((qmData.op() & FLG_ROWDEPENDENCIES) != 0) {
					// Skip row SCN
					rowDiff += redoLog.bigScn() ? Long.BYTES : (Integer.BYTES + Short.BYTES);
				}
				ByteArrayOutputStream baos = new ByteArrayOutputStream(coords[index + 2][1]/rowCount + 0x100);
				putU16(baos, columnCount);
				for (int col = 0; col < columnCount; col++) {
					putU16(baos, col + 1);
					int colSize = Byte.toUnsignedInt(record[coords[index +2][0] + rowDiff++]);
					if (colSize ==  0xFE) {
						baos.write(0xFE);
						colSize = Short.toUnsignedInt(bu.getU16(record, coords[index + 2][0] + rowDiff));
						putU16(baos, colSize);
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
						lmOp, baos.toByteArray(), lwnUnixMillis, rr.scn(), rr.rba(),
						(long) rr.subScn(), rowId, partialRollback);
				transaction.addStatement(orm);
			}
		}
	}

	private void emitDirectBlockChange(
			final OraCdcTransaction transaction,
			final OraCdcRedoRecord rr,
			final OraCdcChangeColb colb) throws IOException {
		final byte[] record = colb.record();
		final int[][] coords = colb.coords();
		final OraCdcRedoLog redoLog = colb.redoLog();
		final int startPos = coords[0][0] + colb.lobDataOffset();
		for (int row = 0; row < Short.toUnsignedInt(colb.qmRowCount()); row++) {
			int offset = Short.toUnsignedInt(redoLog.bu().getU16(record, startPos + 0x12 + row * Short.BYTES));
			final int columnCount = Byte.toUnsignedInt(record[startPos + offset + 2]);
			int rowDiff = colb.lobDataOffset() + offset + 3;
			ByteArrayOutputStream baos = new ByteArrayOutputStream(columnCount * 0x80);
			putU16(baos, columnCount);
			for (int col = 0; col < columnCount; col++) {
				putU16(baos, col + 1);
				int colSize = Byte.toUnsignedInt(record[coords[0][0] + rowDiff++]);
				if (colSize ==  0xFE) {
					baos.write(0xFE);
					colSize = Short.toUnsignedInt(bu.getU16(record, coords[0][0] + rowDiff));
					putU16(baos, colSize);
					rowDiff += Short.BYTES;
				} else  if (colSize == 0xFF) {
					colSize = 0;
					baos.write(0xFF);
				} else {
					baos.write(colSize);
				}
				if (colSize != 0) {
					baos.write(record, coords[0][0] + rowDiff, colSize);
					rowDiff += colSize;
				}
			}
			final RowId rowId = new RowId(
				colb.obj(), colb.bdba(), (short)(row + 1));
			final OraCdcRedoMinerStatement orm = new OraCdcRedoMinerStatement(
				isCdb ? (((long)colb.conId()) << 32) |  (colb.obj() & 0xFFFFFFFFL): colb.obj(),
				INSERT, baos.toByteArray(), lwnUnixMillis, rr.scn(), rr.rba(),
				(long) rr.subScn(), rowId, false);
			transaction.addStatement(orm);
		}
	}

	private void emitRcm(final OraCdcRedoRecord record, final Xid xid) {
		OraCdcTransaction transaction = activeTransactions.get(xid);
		final boolean rollback = record.change5_4().rollback();
		if (transaction == null) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"Skipping {} at SCN={}, RBA={} for transaction XID {}",
						rollback ? "ROLLBACK" : "COMMIT",
						lastScn, lastRba, xid);
			}
		} else {
			if (processLobs) {
				//TODO
				//TODO
				//TODO What if same LOB participate in concurrent transactions?
				//TODO
				//TODO
				for (final LobId lobId : ((OraCdcTransactionChronicleQueue)transaction).lobIds(true))
					transFromLobId.remove(lobId);
			}
			if (rollback) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(
							"Rolling back transaction {} at SCN={}, RBA={}, FIRST_CHANGE#={} with {} changes and size {} bytes",
							transaction.getXid(), record.scn(), record.rba(), transaction.getFirstChange(), transaction.length(), transaction.size());
				}
				transaction.close();
			} else {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(
							"Committing transaction {} at SCN={}, RBA={}, FIRST_CHANGE#={} with {} changes and size {} bytes",
							transaction.getXid(), record.scn(), record.rba(), transaction.getFirstChange(), transaction.length(), transaction.size());
				}
				final long commitScn = record.scn();
				transaction.setCommitScn(commitScn);
				committedTransactions.add(transaction);
				if (Long.compareUnsigned(commitScn, lastCommitScn) < 0) {
					LOGGER.warn(
							"Committing transaction {} with a commit SCN {} lower than the previous one {}!",
							transaction.getXid(), commitScn, lastCommitScn);
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
				metrics.addRolledBackRecords(transaction.length(), transaction.size(),
						activeTransactions.size());
				transaction = null;
			}
			else {
				metrics.addCommittedRecords(transaction.length(), transaction.size(),
						committedTransactions.size(), activeTransactions.size());
			}
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Performing {} at SCN={}, RBA={} for transaction XID {}",
						rollback ? "ROLLBACK" : "COMMIT",
						lastScn, lastRba, xid);
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

	private void addToHalfDoneRcm(final OraCdcRedoRecord record) {
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
		prefixedTransactions.remove(record.xid().partial());
		if (LOGGER.isDebugEnabled()) {
			if (rcm.size() > 1) {
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
				
			}
		}
	}

	private OraCdcTransaction getTransaction(final OraCdcRedoRecord record) {
		final Xid xid = record.xid();
		OraCdcTransaction transaction = activeTransactions.get(xid);
		if (transaction == null) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("New transaction {} created. Transaction start timestamp {}, first SCN {}.",
						xid, Instant.ofEpochMilli(lwnUnixMillis), lastScn);
			}
			transaction = createTransaction(xid.toString(), record.scn(), activeTransactions.size());
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"Starting transaction {} at SCN={}, RBA={}",
						xid, record.scn(), record.rba());
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
		return transaction;
	}

	private void skippingDebugMsg(final String reason, final short operation, final RedoByteAddress rba) {
		LOGGER.debug("Skipping {} OP:{} at RBA {}",
			reason, formatOpCode(operation), rba);

	}

	private short intColumnId(final int obj, final short col, final boolean direct) {
		final Map<Short, Short>[] columns = intColIdsMap.get(obj);
		if (columns == null)
			return -1;
		else {
			Short intColumnIdBoxed = columns[direct ? 0 : 1].get(col);
			if (intColumnIdBoxed == null)
				return -1;
			else
				return intColumnIdBoxed;
		}
	}

	private void redoMinerNext(final long startScn, final RedoByteAddress startRba, final long startSubScn, final boolean rewind) {
		int attempt = 0;
		final long redoMinerReadyStart = System.currentTimeMillis();
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
							"\n=====================\n" +
							"Unable to execute redoMiner.next() (attempt #{}) after {} ms.\n" +
							"\n=====================\n",
							attempt, System.currentTimeMillis() - redoMinerReadyStart);
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
				else
					LOGGER.error(
							"\n=====================\n" +
							"Failed to execute redoMiner.next() (attempt #{}) due to '{}'.\n" +
							"oracdc will try again to execute redoMiner.next() in {} ms." + 
							"\n=====================\n",
							attempt, sqle.getMessage(), backofMs);
			}
			if (!redoMinerReady && runLatch.getCount() > 0) {
				if (LOGGER.isDebugEnabled()) LOGGER.debug("Waiting {} ms", backofMs);
				synchronized(this) {
					try {wait(backofMs); } catch (InterruptedException ie) {}
				}
				try {
					redoMiner.resetRedoLogFactory(startScn, startRba);
				} catch (SQLException sqle) {
					LOGGER.error("'{}' while resetting RLF!", sqle.getMessage());
					if (LOGGER.isDebugEnabled())
						LOGGER.debug("Stack trace for RLF reset:\n{}\n",
								sqle.getMessage(), ExceptionUtils.getExceptionStackTrace(sqle));
				}
				attempt++;
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

	private void emitDdlChange(OraCdcRedoRecord record) {
		final OraCdcChangeDdl ddl = record.changeDdl();
		final String preProcessed = alterTablePreProcessor(ddl.ddlText());
		if (preProcessed != null) {
			final OraCdcTransaction transaction = getTransaction(record);
			final OraCdcRedoMinerStatement orm = new OraCdcRedoMinerStatement(
					isCdb ? (((long)ddl.conId()) << 32) |  (ddl.obj() & 0xFFFFFFFFL) : ddl.obj(),
					DDL, ddl.ddlText().getBytes(), lwnUnixMillis, record.scn(), record.rba(),
					(long) record.subScn(), RowId.ZERO, false);
			transaction.addStatement(orm);
		} else {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Skipping OP:24.1\n{}\n", record);
		}
	}

}
