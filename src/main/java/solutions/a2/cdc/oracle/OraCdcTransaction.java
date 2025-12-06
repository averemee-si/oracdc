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

import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DDL;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UNSUPPORTED;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.FLG_ROWDEPENDENCIES;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KCOCODRW;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KDO_KDOM2;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KDO_ORP_IRP_NULL_POS;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KDO_URP_NULL_POS;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_2_LIN;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_4_LDE;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_11_QMI;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_18_LUP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_30_LNU;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_35_LCU;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_2_IRP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_3_DRP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_5_URP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_6_ORP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_8_CFA;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_16_LMN;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgCompleted;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgFirstPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgHeadPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgLastPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgPrevPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgNextPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.formatOpCode;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.printFbFlags;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeUndoBlock.SUPPL_LOG_UPDATE;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeUndoBlock.SUPPL_LOG_INSERT;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeUndoBlock.SUPPL_LOG_DELETE;
import static solutions.a2.cdc.oracle.utils.OraSqlUtils.alterTablePreProcessor;
import static solutions.a2.oracle.utils.BinaryUtils.putU16;
import static solutions.a2.oracle.utils.BinaryUtils.putU24;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.internals.OraCdcChange;
import solutions.a2.cdc.oracle.internals.OraCdcChangeColb;
import solutions.a2.cdc.oracle.internals.OraCdcChangeDdl;
import solutions.a2.cdc.oracle.internals.OraCdcChangeIndexOp;
import solutions.a2.cdc.oracle.internals.OraCdcChangeRowOp;
import solutions.a2.cdc.oracle.internals.OraCdcChangeUndo;
import solutions.a2.cdc.oracle.internals.OraCdcChangeUndoBlock;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLog;
import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
import solutions.a2.oracle.internals.LobId;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;


/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public abstract class OraCdcTransaction {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTransaction.class);

	protected static final String TRANS_XID = "xid";
	protected static final String TRANS_FIRST_CHANGE = "firstChange";
	protected static final String TRANS_NEXT_CHANGE = "nextChange";
	protected static final String QUEUE_SIZE = "queueSize";
	protected static final String QUEUE_OFFSET = "tailerOffset";
	protected static final String TRANS_COMMIT_SCN = "commitScn";

	boolean firstRecord = true;
	private final long firstChange;
	private long nextChange = 0;
	private final String xid;
	private long commitScn;
	private boolean startsWithBeginTrans = true;
	private boolean needsSorting = false;
	long transSize;

	boolean partialRollback = false;
	List<PartialRollbackEntry> rollbackEntriesList;
	Set<Integer> rollbackPairs;
	private boolean suspicious = false;

	private String username;
	private String osUsername;
	private String hostname;
	private long auditSessionId;
	private String sessionInfo;
	private String clientId;

	private final Map<Integer, Deque<RowChangeHolder>> halfDone = new HashMap<>();
	private final Map<Integer, List<RowChangeHolder>> finishedQueue = new HashMap<>();
	private boolean isCdb = false;


	OraCdcTransaction(final String xid, final long firstChange, final boolean isCdb) {
		this.xid = xid;
		this.firstChange = firstChange;
		this.transSize = 0;
		this.isCdb = isCdb;
	}

	void checkForRollback(final OraCdcStatementBase oraSql, final long index) {
		if (firstRecord) {
			firstRecord = false;
			nextChange = oraSql.getScn();
			if (oraSql.isRollback()) {
				suspicious = true;
				LOGGER.error(
						"""
						
						=====================
						"The partial rollback redo record in transaction {} is the first statement in that transaction.
						Detailed information about redo record:
						{}
						=====================
						
						""", oraSql.toStringBuilder().toString(), xid);
			}
		} else {
			if (startsWithBeginTrans &&
					Long.compareUnsigned(firstChange, oraSql.getScn()) > 0) {
				startsWithBeginTrans = false;
				needsSorting = true;
			}
			if (!needsSorting &&
					Long.compareUnsigned(nextChange, oraSql.getScn()) > 0) {
				needsSorting = true;
			}
			nextChange = oraSql.getScn();
			if (oraSql.isRollback()) {
				if (!partialRollback) {
					partialRollback = true;
					rollbackEntriesList = new ArrayList<>();
				}
				final PartialRollbackEntry pre = new PartialRollbackEntry();
				pre.index = index;
				pre.operation = oraSql.getOperation();
				pre.rowId = oraSql.getRowId();
				pre.rba = oraSql.getRba();

				rollbackEntriesList.add(pre);
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("New partial rollback entry at SCN={}, RS_ID(RBA)='{}' for ROWID={} added.",
							oraSql.getScn(), oraSql.getRba(), oraSql.getRowId());
				}
			}
		}
	}

	abstract void processRollbackEntries();

	boolean willItRolledBack(final OraCdcStatementBase oraSql) {
		if (partialRollback) {
			if (oraSql.isRollback()) {
				return true;
			} else {
				final var rba = oraSql.getRba();
				final var rowid = oraSql.getRowId();
				return rollbackPairs.contains(Objects.hash(rba.sqn(), rba.blk(), rba.offset(),rowid.dataBlk(),rowid.rowNum()));
			}
		} else {
			return false;
		}
	}

	public String getXid() {
		return xid;
	}

	public long getCommitScn() {
		return commitScn;
	}

	private void print(boolean errorOutput) {
		final StringBuilder sb = new StringBuilder((int) transSize);
		sb
			.append("\n=====================\n")
			.append("Information about suspicious transaction with XID=")
			.append(getXid())
			.append(", COMMIT_SCN=")
			.append(commitScn)
			.append("\n")
			.append(OraCdcStatementBase.delimitedRowHeader());
		addToPrintOutput(sb);
		sb.append("\n=====================\n");
		if (errorOutput) {
			LOGGER.error(sb.toString());
		} else {
			LOGGER.trace(sb.toString());
		}
	}

	abstract void addToPrintOutput(final StringBuilder sb);

	public void setCommitScn(long commitScn) {
		this.commitScn = commitScn;
		if (partialRollback) {
			// Need to process all entries in reverse order
			rollbackPairs = new HashSet<>();
			processRollbackEntries();
		}
		if (suspicious) {
			print(true);
		} else if (LOGGER.isTraceEnabled()) {
			print(false);
		}
		if (!halfDone.isEmpty()) {
			StringBuilder sb = new StringBuilder(0x400);
			sb
				.append("\n=====================\n")
				.append("Not all redo records in transaction {} have been processed.\n")
				.append("Please send the message below to us by email oracle@a2.solutions\n\n")
				.append("List of unprocessed redo records:");
			int problemNo = 1;
			for (final int halfDoneKey : halfDone.keySet()) {
				sb
					.append("\n")
					.append(problemNo)
					.append(") halfDoneKey=")
					.append(halfDoneKey);
				for (final RowChangeHolder rowChangeHolder : halfDone.get(halfDoneKey)) {
					sb
						.append("\n\trowChangeHolder information:")
						.append(" homogeneous=")
						.append((rowChangeHolder.flags & FLG_HOMOGENEOUS) > 0)
						.append(" needHeadFlag=")
						.append((rowChangeHolder.flags & FLG_NEED_HEAD_FLAG) > 0)
						.append(" onlyLmn=")
						.append(rowChangeHolder.onlyLmn)
						.append(" OP:")
						.append(formatOpCode(rowChangeHolder.operation))
						.append(" oppositeOrder=")
						.append((rowChangeHolder.flags & FLG_OPPOSITE_ORDER) > 0)
						.append(" lmOp=")
						.append(rowChangeHolder.lmOp);
					for (final OraCdcRedoRecord rr : rowChangeHolder.records) {
						sb
							.append("\n\t\t")
							.append(rr.rba());
						if (rr.has5_1()) {
							sb
								.append(" 5.1 FB=")
								.append(printFbFlags(rr.change5_1().fb()));
							if (rr.change5_1().supplementalFb() != 0) {
								sb
									.append(" 5.1 Supplemental FB=")
									.append(printFbFlags(rr.change5_1().supplementalFb()));
							}
							if (rr.has11_x() || rr.has10_x()) {
								sb
									.append(' ')
									.append(formatOpCode(rr.rowChange().operation()))
									.append(" FB=")
									.append(printFbFlags(rr.rowChange().fb()));
							}
						}
						sb
							.append("\n")
							.append(rr.toString())
							.append("\n");
					}
				}
				problemNo++;
			}
			sb.append("\n=====================\n");
			LOGGER.error(sb.toString(), xid);
		}
	}

	public void setCommitScn(long commitScn, OraCdcPseudoColumnsProcessor pseudoColumns, ResultSet resultSet) throws SQLException {
		setCommitScn(commitScn);
		if (pseudoColumns.isAuditNeeded()) {
			if (pseudoColumns.isUsername()) {
				username = resultSet.getString("USERNAME");
			}
			if (pseudoColumns.isOsUsername()) {
				osUsername = resultSet.getString("OS_USERNAME");
			}
			if (pseudoColumns.isHostname()) {
				hostname = resultSet.getString("MACHINE_NAME");
			}
			if (pseudoColumns.isAuditSessionId()) {
				auditSessionId = resultSet.getLong("AUDIT_SESSIONID");
			}
			if (pseudoColumns.isSessionInfo()) {
				sessionInfo = resultSet.getString("SESSION_INFO");
			}
			clientId = resultSet.getString("CLIENT_ID");
		}
	}

	public boolean startsWithBeginTrans() {
		return startsWithBeginTrans;
	}

	public long getFirstChange() {
		return firstChange;
	}

	public long getNextChange() {
		return nextChange;
	}

	void setSuspicious() {
		suspicious = true;
	}

	boolean suspicious() {
		return suspicious;
	}

	public String getUsername() {
		return username;
	}

	public String getOsUsername() {
		return osUsername;
	}

	public String getHostname() {
		return hostname;
	}

	public long getAuditSessionId() {
		return auditSessionId;
	}

	public String getSessionInfo() {
		return sessionInfo;
	}

	public String getClientId() {
		return clientId;
	}

	void printPartialRollbackEntryDebug(final PartialRollbackEntry pre) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Working with partial rollback statement for ROWID={} at RBA='{}'", pre.rowId, pre.rba);
		}
	}

	void printUnpairedRollbackEntryError(final PartialRollbackEntry pre) {
		suspicious = true;
		LOGGER.error(
				"""
				
				=====================
				No pair for partial rollback statement with ROWID={} RBA='{}' in transaction XID='{}'!
				=====================
				
				""", pre.rowId, pre.rba, getXid());
	}

	abstract void addStatement(final OraCdcStatementBase oraSql);
	public abstract boolean getStatement(OraCdcStatementBase oraSql);
	abstract long size();
	abstract int length();
	abstract int offset();
	abstract void close();
	abstract Set<LobId> lobIds(final boolean all);

	static class PartialRollbackEntry {
		long index;
		short operation;
		RowId rowId;
		RedoByteAddress rba;
	}

	private static final byte FLG_NEED_HEAD_FLAG   = 0x01;
	private static final byte FLG_REORDER          = 0x02;
	private static final byte FLG_OPPOSITE_ORDER   = 0x04;
	private static final byte FLG_HOMOGENEOUS      = 0x08;
	private static final byte FLG_PARTIAL_ROLLBACK = 0x10;
	private static class RowChangeHolder {
		private final List<OraCdcRedoRecord> records;
		private final short operation;
		private boolean complete;
		private short lmOp;
		private RedoByteAddress rba = RedoByteAddress.MAX_VALUE;
		private boolean onlyLmn;
		private byte flags = FLG_NEED_HEAD_FLAG | FLG_HOMOGENEOUS;

		RowChangeHolder(final boolean partialRollback, final short operation) {
			this.operation = operation;
			this.records = new ArrayList<>();
			this.complete = false;
			this.lmOp = UNSUPPORTED;
			if (partialRollback) {
				onlyLmn = false;
				flags |= FLG_PARTIAL_ROLLBACK;
			} else
				onlyLmn = true;
		}
		void add(final OraCdcRedoRecord record) {
			records.add(record);
			if (record.rba().compareTo(rba) < 0)
				rba = record.rba();
			if ((flags & FLG_PARTIAL_ROLLBACK) == 0) {
				if (record.has11_x())
					onlyLmn = onlyLmn && (record.change11_x().operation() == _11_16_LMN);
				else
					//IOT
					onlyLmn = false;
			}			
			if (!complete && (flags & FLG_PARTIAL_ROLLBACK) == 0 && records.size() == 1) {
				final byte fb5_1 = record.change5_1().fb();
				final byte fb11_x = record.rowChange().fb();
				if (!flgFirstPart(fb5_1) && !flgLastPart(fb5_1) && !flgHeadPart(fb5_1) &&
						!flgFirstPart(fb11_x) && !flgLastPart(fb11_x) && !flgHeadPart(fb11_x))
					flags |= FLG_REORDER;
			}
			if (LOGGER.isDebugEnabled()) {
				if ((flags & FLG_PARTIAL_ROLLBACK) > 0) {
					LOGGER.debug("Adding XID {}, SCN {}, RBA {}, OP:{} fb:{}, OP:{} fb:{}",
							record.xid(),
							record.scn(),
							record.rba(),
							formatOpCode(record.changePrb().operation()),
							printFbFlags(record.changePrb().fb()),
							formatOpCode(record.rowChange().operation()),
							printFbFlags(record.rowChange().fb()));
				} else {
					LOGGER.debug("Adding XID {}, SCN {}, RBA {}, OP:5.1 fb:{}, supp fb:{}, OP:{} fb:{}",
							record.xid(),
							record.scn(),
							record.rba(),
							printFbFlags(record.change5_1().fb()),
							printFbFlags(record.change5_1().supplementalFb()),
							formatOpCode(record.rowChange().operation()),
							printFbFlags(record.rowChange().fb()));
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
				final var record = records.get(index);
				final var fb5_1 = record.change5_1().fb();
				final var supplementalFb = record.change5_1().supplementalFb();
				final var rowOp = record.has11_x();
				final var fb11_x = rowOp 
						? record.change11_x().fb()
						: record.change10_x().fb() == 0
								? supplementalFb
								: record.change10_x().fb();
				if (indexValue == -1)
					switch (lmOp) {
					case UPDATE:
						if (rowOp) {
							if (flgFirstPart(fb5_1) || flgLastPart(fb5_1) || flgHeadPart(fb5_1) ||
									flgFirstPart(fb11_x) || flgLastPart(fb11_x) || flgHeadPart(fb11_x)) {
								indexValue = index;
							}
						} else {
							final byte fbSupp = record.change5_1().supplementalFb();
							if (flgFirstPart(fb5_1) || flgLastPart(fb5_1) || flgHeadPart(fb5_1) ||
									flgFirstPart(fb11_x) || flgLastPart(fb11_x) || flgHeadPart(fb11_x) ||
									flgFirstPart(fbSupp) || flgLastPart(fbSupp) || flgHeadPart(fbSupp)) {
								indexValue = index;
							}
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
					if ((flags & FLG_HOMOGENEOUS) > 0) {
						if (flgFirstPart(fb5_1) && flgFirstPart(fb11_x)) indexFirst = index;
						if (flgLastPart(fb5_1) && flgLastPart(fb11_x)) indexLast = index;	
						if (flgHeadPart(fb5_1) && flgHeadPart(fb11_x)) indexHead = index;
					} else {
						if (rowOp) {
							if (flgFirstPart(fb5_1) ||
								flgFirstPart(fb11_x) ||
								((flgPrevPart(fb5_1) || flgNextPart(fb5_1)) && flgFirstPart(supplementalFb)))
								indexFirst = index;
							if (flgLastPart(fb5_1) ||
								flgLastPart(fb11_x) ||
								((flgPrevPart(fb5_1) || flgNextPart(fb5_1)) && flgLastPart(supplementalFb)))
								indexLast = index;	
							if (flgHeadPart(fb5_1) || flgHeadPart(fb11_x))
								indexHead = index;
						} else {
							if (flgFirstPart(fb11_x)) indexFirst = index;
							if (flgLastPart(fb11_x)) indexLast = index;	
							if (flgHeadPart(fb11_x)) indexHead = index;
						}
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
			if (indexFirst < 0 || indexLast < 0) {
				var sb = new StringBuilder(records.size() * 0x50);
				records.forEach(rr -> {
					sb
						.append("\nRBA ")
						.append(rr.rba().toString())
						.append(", OP:5.1 fb:")
						.append(printFbFlags(rr.change5_1().fb()))
						.append(", supp fb:")
						.append(printFbFlags(rr.change5_1().supplementalFb()))
						.append(", OP:")
						.append(formatOpCode(rr.rowChange().operation()))
						.append(" fb:")
						.append(printFbFlags(rr.rowChange().fb()));
				});
				LOGGER.error(
						"""
						
						=====================
						Unable to determine first and/or last part of operation!
						indexFirst={}, indexLast={}
						Please send the message below to us by email oracle@a2.solutions
						XID={}, lmOp={}
						{}
						=====================
						
						""", indexFirst, indexLast, records.get(0).xid(), lmOp, sb.toString());
				throw new IndexOutOfBoundsException();
			}
			if (indexHead > -1 && indexHead > indexFirst)
				flags |= FLG_OPPOSITE_ORDER;
			else
				flags &= (~FLG_OPPOSITE_ORDER);
			if ((flags & FLG_OPPOSITE_ORDER) > 0)
				sortedRecs.add(records.get(indexLast));
			else
				sortedRecs.add(records.get(indexFirst));
			if (indexValue == 0 && indexValue != indexFirst && indexValue != indexLast)
				sortedRecs.add(records.get(0));
			else {	
				for (int index = 0; index < indexValue; index++)
					sortedRecs.add(records.get(index));
			}
			if ((flags & FLG_OPPOSITE_ORDER) > 0)
				sortedRecs.add(records.get(indexFirst));
			else
				sortedRecs.add(records.get(indexLast));
			records.clear();
			for (int index = 0; index < sortedRecs.size(); index++)
				records.add(sortedRecs.get(index));
			sortedRecs = null;
		}
	}

	public void processRowChange(final OraCdcRedoRecord rr, final boolean partialRollback,
			final long lwnUnixMillis) throws IOException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("processRowChange(partialRollback={}) in XID {} at SCN/RBA {}/{} for OP:{}",
					partialRollback, rr.xid(), Long.toUnsignedString(rr.scn()), rr.rba(),
					formatOpCode(rr.rowChange().operation()));
		RowChangeHolder row = createRowChangeHolder(rr, partialRollback);
		if (row.complete) {
			emitRowChange(row, lwnUnixMillis);
		} else {
			final var key = rr.halfDoneKey();
			Deque<RowChangeHolder> deque =  halfDone.get(key);
			if (deque == null) {
				row.add(rr);
				deque = new ArrayDeque<>();
				deque.addFirst(row);
				halfDone.put(key, deque);
			} else {
				var halfDoneRow =  deque.peekFirst();
				final var lastHalfDone = halfDoneRow.last();
				final boolean push;
				final var lastHalfDoneRowFb = lastHalfDone.rowChange().fb();
				if (partialRollback) {
					push = lastHalfDoneRowFb == rr.rowChange().fb();
				} else {
					if (flgFirstPart(rr.change5_1().supplementalFb()) && flgFirstPart(lastHalfDone.change5_1().supplementalFb()))
						push = true;
					else if (rr.rowChange().fb() == 0 && rr.change5_1().fb() == 0)
						push = false;
					else
						push = lastHalfDoneRowFb == rr.rowChange().fb() && 
								lastHalfDone.change5_1().fb() == rr.change5_1().fb();
				}
				if (push) {
					row.add(rr);
					deque.addFirst(row);
					var waitingList = finishedQueue.get(key);
					if (waitingList == null) {
						waitingList = new ArrayList<>();
						waitingList.add(halfDoneRow);
						finishedQueue.put(key, waitingList);
					}
					waitingList.add(row);
					if (LOGGER.isDebugEnabled())
						LOGGER.debug(
								"An stored incomplete change at RBA {} cannot be merged with an incomplete change in progress at RBA {}",
								lastHalfDone.rba(), rr.rba());
				} else {
					halfDoneRow.add(rr);
					row = null;
					completeRow(halfDoneRow);
					if (halfDoneRow.complete) {
						if (deque.size() == 1) {
							var waitingList = finishedQueue.get(key);
							if (waitingList == null)
								emitRowChange(halfDoneRow, lwnUnixMillis);
							else {
								for (final var waitingRow : waitingList) {
									emitRowChange(waitingRow, lwnUnixMillis);
								}
								waitingList.clear();
								finishedQueue.remove(key);
								waitingList = null;
							}
						} else {
							if (LOGGER.isDebugEnabled()) {
								var waitingList = finishedQueue.get(key);
								LOGGER.debug("awaiting processing for key {}. deque.size()={}, waitingList.size()={}",
										key, deque.size(), waitingList == null ? 0: waitingList.size());
							}
						}
						deque.removeFirst();
						if (deque.isEmpty()) {
							halfDone.remove(key);
						}
					}
				}
			}
		}
	}

	public void processRowChangeLmn(final OraCdcRedoRecord rr, final long lwnUnixMillis) throws IOException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("processRowChangeLmn() in XID {} at SCN/RBA {}/{} for OP:11.16",
					rr.xid(), Long.toUnsignedString(rr.scn()), rr.rba());
			final var key = rr.halfDoneKey();
			final var deque =  halfDone.get(key);
			if (deque != null) {
				final var halfDoneRow =  deque.peekFirst();
				if (halfDoneRow != null && halfDoneRow.lmOp == UPDATE && flgCompleted(rr.change5_1().supplementalFb())) {
					halfDoneRow.complete = true;
					halfDoneRow.add(rr);
					emitRowChange(halfDoneRow, lwnUnixMillis);
					deque.removeFirst();
					if (deque.isEmpty()) {
						halfDone.remove(key);
					}
					return;
				} else
					processRowChange(rr, false, lwnUnixMillis);
			} else
				processRowChange(rr, false, lwnUnixMillis);
	}

	private static final byte[] ZERO_COL_COUNT = {0, 0};
	public void emitMultiRowChange(final OraCdcRedoRecord rr,
			final boolean partialRollback,
			final long lwnUnixMillis) throws IOException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("emitMultiRowChange(partialRollback={}) in XID {} at SCN/RBA {}/{} for OP:{}",
					partialRollback, rr.xid(), Long.toUnsignedString(rr.scn()), rr.rba(),
					formatOpCode(rr.change11_x().operation()));
		final int index;
		final short lmOp;
		final var rowChange = rr.change11_x();
		final var bu = rr.redoLog().bu();
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
				addStatement(orm);
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
				addStatement(orm);
			}
		}
	}

	public void emitDirectBlockChange(final OraCdcRedoRecord rr,
			final OraCdcChangeColb colb,
			final long lwnUnixMillis) throws IOException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("emitDirectBlockChange() in XID {} at SCN/RBA {}/{} for OP:{}",
					xid, Long.toUnsignedString(rr.scn()), rr.rba(),
					formatOpCode(colb.operation()));
		final var record = colb.record();
		final var coords = colb.coords();
		final var redoLog = colb.redoLog();
		final var bu = redoLog.bu();
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
				colb.obj(), colb.dba(), (short) row);
			final OraCdcRedoMinerStatement orm = new OraCdcRedoMinerStatement(
				isCdb ? (((long)colb.conId()) << 32) |  (colb.obj() & 0xFFFFFFFFL): colb.obj(),
				INSERT, baos.toByteArray(), lwnUnixMillis, rr.scn(), rr.rba(),
				(long) rr.subScn(), rowId, false);
			addStatement(orm);
		}
	}


	private RowChangeHolder createRowChangeHolder(final OraCdcRedoRecord record, final boolean partialRollback) {
		final var rowChange = record.rowChange();
		final RowChangeHolder row = new RowChangeHolder(partialRollback, rowChange.operation());
		if (partialRollback) {
			switch (row.operation) {
			case _11_5_URP:
			case _11_6_ORP:
			case _11_16_LMN:
				row.lmOp = UPDATE;
				if (flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
					row.complete = true;
				else if (!flgHeadPart(rowChange.fb()))
					row.flags |= FLG_OPPOSITE_ORDER;
				if (row.operation == _11_16_LMN) {
					row.flags &= (~FLG_NEED_HEAD_FLAG);
					if (flgFirstPart(rowChange.fb()))
						row.flags |= FLG_OPPOSITE_ORDER;
				}
				break;
			case _11_2_IRP:
				row.lmOp = INSERT;
				if (flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
					row.complete = true;
				else if (!flgHeadPart(rowChange.fb()))
					row.flags |= FLG_OPPOSITE_ORDER;
				break;
			case _11_3_DRP:
				row.lmOp = DELETE;
				row.complete = true;
				break;
			case _10_2_LIN:
				row.lmOp = INSERT;
				if (flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
					row.complete = true;
				break;
			case _10_4_LDE:
				row.lmOp = DELETE;
				if (flgFirstPart(record.changePrb().fb()) && flgLastPart(record.changePrb().fb()))
					row.complete = true;
				break;
			case _10_18_LUP:
			case _10_30_LNU:
			case _10_35_LCU:
				row.lmOp = UPDATE;
				if (flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
					row.complete = true;
				break;
			}
		} else {
			final var undoChange = record.change5_1();
			var setlmOp = true;
			if (undoChange.supplementalLogData()) {
				if (undoChange.supplementalDataFor() == SUPPL_LOG_INSERT) {
					row.lmOp = INSERT;
					setlmOp = false;
				} else if (undoChange.supplementalDataFor() == SUPPL_LOG_UPDATE) {
					row.lmOp = UPDATE;
					if (flgFirstPart(undoChange.supplementalFb()) && flgLastPart(undoChange.supplementalFb()))
						row.complete = true;
					else if (!flgHeadPart(undoChange.fb()))
						row.flags |= FLG_OPPOSITE_ORDER;
					setlmOp = false;
				} else if (undoChange.supplementalDataFor() == SUPPL_LOG_DELETE) {
					row.lmOp = DELETE;
					if (flgFirstPart(undoChange.supplementalFb()) && flgLastPart(undoChange.supplementalFb()))
						row.complete = true;
					else if (!flgHeadPart(undoChange.fb()))
						row.flags |= FLG_OPPOSITE_ORDER;
					setlmOp = false;
				}
			}
			switch (row.operation) {
			case _11_5_URP:
			case _11_16_LMN:
				if (setlmOp) {
					row.lmOp = UPDATE;
					if (flgFirstPart(undoChange.fb()) && flgLastPart(undoChange.fb()) &&
							flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb())) {
						row.complete = true;
					} else if (!flgHeadPart(undoChange.fb()))
						row.flags |= FLG_OPPOSITE_ORDER;
					if (row.operation == _11_16_LMN) {
						if (!flgFirstPart(undoChange.fb()) || flgFirstPart(rowChange.fb()))
							row.flags |= FLG_OPPOSITE_ORDER;
					}
				}
				break;
			case _11_2_IRP:
			case _11_6_ORP:
				if (setlmOp) {
					if (row.operation == _11_2_IRP)
						row.lmOp = INSERT;
					else
						row.lmOp = UPDATE;
				}
				if (flgHeadPart(rowChange.fb()) && flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
					row.complete = true;
				else if (!flgHeadPart(rowChange.fb()))
					row.flags |= FLG_OPPOSITE_ORDER;
				break;
			case _11_3_DRP:
				if (setlmOp) {
					row.lmOp = DELETE;
					if (flgHeadPart(undoChange.fb()) && flgFirstPart(undoChange.fb()) && flgLastPart(undoChange.fb()))
						row.complete = true;
					else if (!flgHeadPart(undoChange.fb()))
						row.flags |= FLG_OPPOSITE_ORDER;
				}
				break;
			case _10_2_LIN:
				if (setlmOp) row.lmOp = INSERT;
				if (flgHeadPart(rowChange.fb()) && flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
					row.complete = true;
				else if (rowChange.fb() == 0 &&
						flgFirstPart(undoChange.supplementalFb()) && flgLastPart(undoChange.supplementalFb()))
					row.complete = true;					
				else if (!flgHeadPart(rowChange.fb()))
					row.flags |= FLG_OPPOSITE_ORDER;
				break;
			case _10_4_LDE:
				if (setlmOp) {
					row.lmOp = DELETE;
					if (flgHeadPart(undoChange.fb()) && flgFirstPart(undoChange.fb()) && flgLastPart(undoChange.fb()))
						row.complete = true;
					else if (!flgHeadPart(undoChange.fb()))
						row.flags |= FLG_OPPOSITE_ORDER;
				}
				break;
			case _10_18_LUP:
				row.flags &= (~FLG_NEED_HEAD_FLAG);
				if (setlmOp) {
					row.lmOp = UPDATE;
					if (flgFirstPart(undoChange.fb()) && flgLastPart(undoChange.fb()) &&
							flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
							row.complete = true;
						else if (!flgHeadPart(undoChange.fb()))
							row.flags |= FLG_OPPOSITE_ORDER;
				}
				break;
			case _10_30_LNU:
				if (setlmOp) row.lmOp = UPDATE;
				if (flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
					row.complete = true;
				else if (!flgHeadPart(rowChange.fb()))
					row.flags |= FLG_OPPOSITE_ORDER;
				break;
			}
		}
		if (row.complete)
			row.add(record);
		return row;
	}

	private void completeRow(final RowChangeHolder row) {
		var head = 0;
		var first = 0;
		var last = 0;
		if ((row.flags & FLG_PARTIAL_ROLLBACK) > 0) {
			for (int i = 0; i < row.records.size(); i++) {
				final OraCdcRedoRecord rr = row.records.get(i);
				if (rr.has11_x() && rr.hasPrb()) {
					final byte fb = rr.change11_x().fb();
					if (flgHeadPart(fb)) head++;
					if (flgFirstPart(fb)) first++;
					if (flgLastPart(fb)) last++;
				} else if (rr.has10_x() && rr.hasPrb()) {
					final byte fb = rr.change10_x().fb();
					if (flgHeadPart(fb)) head++;
					if (flgFirstPart(fb)) first++;
					if (flgLastPart(fb)) last++;
				} else {
					LOGGER.warn(
							"""
							
							=====================
							Strange redo record without required op codes 5.6/5.11 and 11.x at RBA {} in '{}'.
							Redo record information
							{}
							=====================
							
							""", rr.rba(), rr.redoLog().fileName(), rr.toString());
				}
			}
			if (head > 0 && first > 0 && last > 0) {
				row.complete = true;
			}
		} else {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Entered complete for row.lmOp={} with size={}", row.lmOp, row.records.size());
			var iotUpdate = false;
			for (int i = 0; i < row.records.size(); i++) {
				final OraCdcRedoRecord rr = row.records.get(i);
				if (rr.has11_x() && rr.has5_1()) {
					if (row.lmOp == INSERT && rr.change11_x().operation() == _11_6_ORP) {
						row.lmOp = UPDATE;
						row.flags &= (~FLG_HOMOGENEOUS);
						completeRow(row);
					}
					if (row.lmOp == UPDATE && rr.change11_x().operation() != _11_5_URP && (row.flags & FLG_HOMOGENEOUS) > 0) {
						row.flags &= (~FLG_HOMOGENEOUS);
						if ((row.flags & FLG_NEED_HEAD_FLAG) > 0 && rr.change11_x().operation() == _11_16_LMN)
							row.flags &= (~FLG_NEED_HEAD_FLAG);
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
						if (flgCompleted(rr.change5_1().supplementalFb())) {
							row.complete = true;
							break;
						}
						final byte fb5_1 = rr.change5_1().fb();
						if (flgHeadPart(fb5_1)) head++;
						if (flgFirstPart(fb5_1)) first++;
						if (flgLastPart(fb5_1)) last++;
						final byte fb11_x = rr.change11_x().fb();
						if (flgHeadPart(fb11_x)) head++;
						if (flgFirstPart(fb11_x)) first++;
						if (flgLastPart(fb11_x)) last++;
					}
				} else if (rr.has10_x() && rr.has5_1()) {
					if (row.lmOp == DELETE) {
						final byte fb = rr.change5_1().fb();
						if (flgHeadPart(fb)) head++;
						if (flgFirstPart(fb)) first++;
						if (flgLastPart(fb)) last++;
					} else if (row.lmOp == INSERT) {
						final byte fb = rr.change10_x().fb();
						if (flgHeadPart(fb)) head++;
						if (flgFirstPart(fb)) first++;
						if (flgLastPart(fb)) last++;
					} else {
						//UPDATE
						iotUpdate = true;
						final byte fb5_1 = rr.change5_1().fb();
						if (flgHeadPart(fb5_1)) head++;
						if (flgFirstPart(fb5_1)) first++;
						if (flgLastPart(fb5_1)) last++;
						final byte fb11_x = rr.change10_x().fb();
						if (flgHeadPart(fb11_x)) head++;
						if (flgFirstPart(fb11_x)) first++;
						if (flgLastPart(fb11_x)) last++;
					}
				} else {
					LOGGER.warn(
							"""
							
							=====================
							Strange redo record without required op codes 5.1 and 11.x at RBA {} in '{}'.
							Redo record information:
							{}
							=====================
							
							""", rr.rba(), rr.redoLog().fileName(), rr.toString());
				}
			}
			if (!row.complete) {
				if ((row.lmOp == INSERT || row.lmOp == DELETE) &&
						head > 0 && first > 0 && last > 0)
					row.complete = true;
				else if (row.lmOp == UPDATE) {
					if ((row.flags & FLG_NEED_HEAD_FLAG) > 0 && head > 1 && first > 1 && last > 1)
						row.complete = true;
					else if ((row.flags & FLG_NEED_HEAD_FLAG) == 0 && first > 1 && last > 1)
						row.complete = true;
					else if (iotUpdate && first > 0 && last > 0)
						row.complete = true;
				} else
					row.complete = false;
			}
		}
		if (LOGGER.isDebugEnabled()) {
			final StringBuilder sb = new StringBuilder(0x800);
			if (row.complete)
				sb.append("Ready to merge redo records into one row for RBA's");
			else
				sb
					.append("Unable to merge redo records for lmOp=")
					.append(row.lmOp)
					.append(", needHeadFlag=")
					.append((row.flags & FLG_NEED_HEAD_FLAG) > 0)
					.append(", head=")
					.append(head)
					.append(", first=")
					.append(first)
					.append(", last=")
					.append(last);
			for (final OraCdcRedoRecord record : row.records) {
				sb
					.append("\n\tXID:")
					.append(record.xid().toString())
					.append(", SCN:")
					.append(record.scn())
					.append(", RBA:")
					.append(record.rba().toString());
				if ((row.flags & FLG_PARTIAL_ROLLBACK) > 0)
					sb
						.append(", OP:")
						.append(formatOpCode(record.changePrb().operation()))
						.append(" fb:")
						.append(printFbFlags(record.changePrb().fb()))
						.append(", OP:")
						.append(formatOpCode(record.rowChange().operation()))
						.append(" fb:")
						.append(printFbFlags(record.rowChange().fb()));
				else
					sb
						.append(", OP:5.1 fb:")
						.append(printFbFlags(record.change5_1().fb()))
						.append(", supp fb:")
						.append(printFbFlags(record.change5_1().supplementalFb()))
						.append(", OP:")
						.append(formatOpCode(record.rowChange().operation()))
						.append(" fb:")
						.append(printFbFlags(record.rowChange().fb()));
				LOGGER.debug(sb.toString());
			}
		}
	}

	private void emitRowChange(final RowChangeHolder row, final long lwnUnixMillis) throws IOException {
		if ((row.flags & FLG_REORDER) > 0) {
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
		if ((row.flags & FLG_PARTIAL_ROLLBACK) > 0 && row.lmOp == DELETE) {
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
				final var rowChange = rr.rowChange();
				if ((row.flags & FLG_HOMOGENEOUS) > 0) {
					// URP, IRP
					setOrValColCount += rowChange.columnCount();
					//TODO Layer 11 operations where the partial rollback also contains
					//TODO supplemental log data but we don't need the contents for partial rollback
					if ((row.flags & FLG_PARTIAL_ROLLBACK) == 0) {
						final OraCdcChangeUndoBlock change = rr.change5_1();
						// URP, IRP, DRP
						supplColCount += change.supplementalCc();
						// 	URP, DRP
						whereColCount += change.columnCount();
					}
				} else if ((row.flags & FLG_PARTIAL_ROLLBACK) == 0) {
					final OraCdcChangeUndoBlock change = rr.change5_1();
					switch (rowChange.operation()) {
						case _11_2_IRP -> {
							setOrValColCount += rowChange.columnCount();
							setOrValColCount += change.supplementalCc();
						}
						case _11_3_DRP -> {
							whereColCount += change.columnCount();
						}
						case _11_6_ORP -> {
							whereColCount += change.columnCount();
							whereColCount += change.supplementalCc();
							whereColCount += rowChange.columnCount();
						}
						case _11_5_URP -> {
							setOrValColCount += rowChange.columnCount();
							whereColCount += change.columnCount();
							whereColCount += change.supplementalCc();
						}
						case _11_8_CFA -> {
							whereColCount += change.supplementalCc();
						}
						case _11_16_LMN -> {
							whereColCount += change.supplementalCc();
						}
						case _10_2_LIN ->
							setOrValColCount += rowChange.columnCount();
						case _10_4_LDE ->
							setOrValColCount += change.columnCount();
						case _10_18_LUP ->
							whereColCount += change.columnCount();
						case _10_30_LNU, _10_35_LCU -> {
							setOrValColCount += rowChange.columnCount();
							whereColCount += change.columnCount();
							whereColCount += change.supplementalCc();
						}
						default -> LOGGER.error(
							"""
							
							=====================
							Unable to count number of columns at RBA {} for OP:{}
							=====================
							
							""", rr.rba(), formatOpCode(rowChange.operation()));
					}
				} else {
					final StringBuilder sb = new StringBuilder(0x400);
					sb
						.append("\n=====================\n")
						.append("Unable to properly process the following RBA's with partial rollback\n");
					for (final OraCdcRedoRecord ocrr : row.records)
						sb
							.append("\n\t")
							.append(ocrr.rba());
					sb
						.append("\nPlease send message above along with the resulting dump of command execution\n\n")
						.append("alter system dump logfile '{}' scn min {} scn max {};\n\n")
						.append("to oracle@a2.solutions")
						.append("\n=====================\n");
					LOGGER.error(sb.toString(),
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
					((row.flags & FLG_PARTIAL_ROLLBACK) > 0 && row.lmOp == UPDATE && first.change11_x().operation() == _11_6_ORP)) {
				if ((row.flags & FLG_PARTIAL_ROLLBACK) > 0) {
					putU16(baos, setOrValColCount);
				} else {
					if (row.lmOp == INSERT)
						putU16(baos, setOrValColCount);
					else
						putU16(baos, supplColCount + whereColCount);
				}
				int colNumOffset = 1;
				final int rsiz = row.size();
				int i = 0;
				if ((row.flags & FLG_OPPOSITE_ORDER) > 0)
					i = rsiz - 1;
				for (;;) {
					final OraCdcRedoRecord rr = row.records.get(i);
					final OraCdcChangeUndoBlock change = rr.change5_1();
					if (rr.has5_1() && row.lmOp != INSERT) {
						if (rr.has11_x()) {
							change.writeSupplementalCols(baos);
							if (OraCdcChangeUndoBlock.KDO_POS + 1 + change.columnCount() <= change.coords().length) {
								change.writeColsWithNulls(
										baos, OraCdcChangeUndoBlock.KDO_POS, 0,
										change.suppOffsetUndo() == 0 ? colNumOffset : change.suppOffsetUndo(),
										KDO_ORP_IRP_NULL_POS);
								colNumOffset += change.columnCount();
							} else {
								LOGGER.warn("Unable to read column data for DELETE at RBA {}, change #{}",
										rr.rba(), change.num());
							}
						} else {
							colNumOffset += change.writeIndexColumns(baos, colNumOffset);
						}
					} else if (row.lmOp != INSERT && !rr.hasPrb()) {
						LOGGER.warn("Redo record {} does not contains expected operation 5.1!", rr.rba());
					}
					if (row.lmOp != DELETE) {
						if (rr.has11_x()) {
							final OraCdcChangeRowOp rowChange = rr.change11_x();
							if (OraCdcChangeRowOp.KDO_POS + rowChange.columnCount() < rowChange.coords().length) {
								if (change != null) {
									rowChange.writeColsWithNulls(
											baos, OraCdcChangeRowOp.KDO_POS, 0,
											change.suppOffsetRedo() == 0 ? colNumOffset : change.suppOffsetRedo(),
											KDO_ORP_IRP_NULL_POS);
								} else if ((row.flags & FLG_PARTIAL_ROLLBACK) > 0) {
									rowChange.writeColsWithNulls(
											baos, OraCdcChangeRowOp.KDO_POS, 0,
											colNumOffset, KDO_ORP_IRP_NULL_POS);
								} else {
									LOGGER.warn("Unable to read column data for INSERT at RBA {}",
											rr.rba());
								}
								colNumOffset += rowChange.columnCount();
							}
						} else if (rr.has10_x()) {
							colNumOffset += rr.change10_x().writeIndexColumns(baos, colNumOffset);
						} else {
							LOGGER.warn("Redo record {} does not contains expected row change operation!", rr.rba());
						}
					}
					if ((row.flags & FLG_OPPOSITE_ORDER) > 0) {
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
				if ((row.flags & FLG_PARTIAL_ROLLBACK) > 0 && first.change11_x().operation() == _11_6_ORP) {
					putU16(baos, 0);
				}
			} else {
				//UPDATE
				int colNumOffsetSet = 1;
				int colNumOffsetWhere = 1;
				ByteArrayOutputStream baosB = new ByteArrayOutputStream(setOrValColCount > 0 ? setOrValColCount * 0x20 : 0x80);
				if (row.onlyLmn)
					putU16(baos, whereColCount + supplColCount);
				else
					putU16(baos, setOrValColCount);
				if ((row.flags & FLG_HOMOGENEOUS) > 0) {
					for (final OraCdcRedoRecord rr : row.records) {
						final OraCdcChangeUndoBlock change = rr.change5_1();
						final var rowChange = rr.rowChange();
						if (rowChange.operation() == _11_5_URP &&
								(rowChange.flags() & KDO_KDOM2) != 0) {
							if (rowChange.coords()[OraCdcChangeRowOp.KDO_POS + 1][1] > 1 &&
									OraCdcChangeRowOp.KDO_POS + 2 < rowChange.coords().length) {
								rowChange.writeKdoKdom2(baos, OraCdcChangeRowOp.KDO_POS);
								if (rr.has5_1())
									change.writeKdoKdom2(baosB, OraCdcChangeUndoBlock.KDO_POS);
							} else {
								LOGGER.warn("Not enough data to process KDO_KDOM2 structure at RBA {}, change #{}",
										rr.rba(), rowChange.num());
							}
						} else if (rowChange.operation() == _11_5_URP &&
								OraCdcChangeRowOp.KDO_POS + 1 + rowChange.columnCount() < rowChange.coords().length) {
							rowChange.writeColsWithNulls(
									baos, OraCdcChangeRowOp.KDO_POS, OraCdcChangeRowOp.KDO_POS + 1,
									(row.flags & FLG_PARTIAL_ROLLBACK) > 0 ? colNumOffsetSet :
										(change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo()),
									KDO_URP_NULL_POS);
							if (rr.has5_1())
								change.writeColsWithNulls(
									baosB, OraCdcChangeUndoBlock.KDO_POS, OraCdcChangeUndoBlock.KDO_POS + 1,
									(row.flags & FLG_PARTIAL_ROLLBACK) > 0 ? colNumOffsetSet :
										(change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetUndo()),
									KDO_URP_NULL_POS);
						} else if (rowChange.operation() == _11_6_ORP &&
								OraCdcChangeRowOp.KDO_POS + rowChange.columnCount() < rowChange.coords().length) {
							rowChange.writeColsWithNulls(
									baos, OraCdcChangeRowOp.KDO_POS, 0,
									(row.flags & FLG_PARTIAL_ROLLBACK) > 0 ? colNumOffsetSet : 
										(change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo()),
									KDO_ORP_IRP_NULL_POS);
							if (rr.has5_1())
								change.writeColsWithNulls(
									baosB, OraCdcChangeUndoBlock.KDO_POS, 0,
									(row.flags & FLG_PARTIAL_ROLLBACK) > 0 ? colNumOffsetSet : 
										(change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetUndo()),
									KDO_ORP_IRP_NULL_POS);
						} else if (row.onlyLmn) {
							change.writeSupplementalCols(baos);
							change.writeSupplementalCols(baosB);
						} else if (rowChange.operation() == _10_30_LNU) {
							rowChange.writeIndexNonKeyColumns(
									baos, OraCdcChangeIndexOp.NON_KEY_10_30_POS,
									change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo(), 0);
							change.writeIndexNonKeyColumns(
									baosB, OraCdcChangeUndoBlock.NON_KEY_10_30_POS, colNumOffsetSet, colNumOffsetWhere);
						} else if (rowChange.operation() == _10_35_LCU) {
							rowChange.writeIndexColumnsOp35(
									baos, OraCdcChangeIndexOp.COL_NUM_10_35_POS, change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo());
							change.writeIndexColumnsOp35(
									baosB, OraCdcChangeUndoBlock.COL_NUM_10_35_POS, change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo());
						} else if (rowChange.operation() != _11_16_LMN) {
							LOGGER.warn("Unable to read column data for UPDATE (SET) at RBA {}, change #{} OP:{}",
									rr.rba(), rowChange.num(), formatOpCode(rowChange.operation()));
						}
						colNumOffsetSet += rowChange.columnCount();
					}
					byte[] baosBBytes = baosB.toByteArray();
					putU24(baos, baosBBytes.length);
					baos.write(baosBBytes);
					baosB.close();
					baosB = null;
					baosBBytes = null;
					if ((row.flags & FLG_PARTIAL_ROLLBACK) > 0) {
						putU16(baos, 0);
					} else {
						putU16(baos, whereColCount + supplColCount);
						for (final OraCdcRedoRecord rr : row.records) {
							final OraCdcChangeUndoBlock change = rr.change5_1();
							change.writeSupplementalCols(baos);
							if (change.columnCount() > 0) {
								if (rr.has11_x()) {
									final short selector = (short) ((change.op() & 0x1F) | (KCOCODRW << 0x08));
									if (selector == _11_5_URP &&
											(change.flags() & KDO_KDOM2) != 0) {
										if (change.coords()[OraCdcChangeUndoBlock.KDO_POS + 1][1] > 1 &&
												OraCdcChangeUndoBlock.KDO_POS + 2 < change.coords().length) {
											change.writeKdoKdom2(baos, OraCdcChangeUndoBlock.KDO_POS);
										} else {
											LOGGER.warn("Not enough data to process KDO_KDOM2 structure at RBA {}, change #{}",
													rr.rba(), change.num());
										}
									} else if (selector == _11_5_URP &&
											OraCdcChangeUndoBlock.KDO_POS + 1 + change.columnCountNn() < change.coords().length) {
										change.writeColsWithNulls(baos, OraCdcChangeUndoBlock.KDO_POS, OraCdcChangeUndoBlock.KDO_POS + 1,
												change.suppOffsetUndo() == 0 ? colNumOffsetWhere : change.suppOffsetUndo(),
												KDO_URP_NULL_POS);
									} else if (selector == _11_6_ORP &&
											OraCdcChangeUndoBlock.KDO_POS + change.columnCountNn() < change.coords().length) {
										change.writeColsWithNulls(baos, OraCdcChangeUndoBlock.KDO_POS, 0,
												change.suppOffsetUndo() == 0 ? colNumOffsetWhere : change.suppOffsetUndo(),
												KDO_ORP_IRP_NULL_POS);
									} else {
										LOGGER.warn("Unable to read column data for UPDATE(WHERE) at RBA {}, change #{}",
												rr.rba(), change.num());
									}
								} else {
									if (change.kdilkType() == OraCdcChangeUndoBlock.KDICLNU) {
										// OP:10.30
										change.writeIndexColumns(baos, OraCdcChangeUndoBlock.KEY_10_30_POS, false,
												change.suppOffsetUndo() == 0 ? colNumOffsetWhere : change.suppOffsetUndo());
									} else if (change.kdilkType() == OraCdcChangeUndoBlock.KDILCNU) {
										// OP:10.35
										change.writeIndexColumns(baos, OraCdcChangeUndoBlock.KEY_10_30_POS, false,
												change.suppOffsetUndo() == 0 ? colNumOffsetWhere : change.suppOffsetUndo());
										change.writeSupplementalCols(baos);
									} else {
										LOGGER.warn(
												"""
												
												=====================
												Unable to process redo record at RBA {} with kdilkType={}.
												Redo record countent:
												{}
												=====================
												
												""", rr.rba(), change.kdilkType(), rr.toString());
									}
								}
								colNumOffsetWhere += change.columnCount();
							}
						}
					}
				} else {
					ByteArrayOutputStream baosW = new ByteArrayOutputStream(totalBytes);
					for (final var rr : row.records) {
						final var change = rr.change5_1();
						final var rowChange = rr.rowChange();
						switch (rowChange.operation()) {
							case _11_5_URP -> {
								change.writeSupplementalCols(baosW);
								if (OraCdcChangeUndoBlock.KDO_POS + 1 + change.columnCountNn() < change.coords().length) {
									change.writeColsWithNulls(baosB, OraCdcChangeUndoBlock.KDO_POS, OraCdcChangeUndoBlock.KDO_POS + 1,
											change.suppOffsetUndo(), KDO_URP_NULL_POS);
									change.writeColsWithNulls(baosW, OraCdcChangeUndoBlock.KDO_POS, OraCdcChangeUndoBlock.KDO_POS + 1,
											change.suppOffsetUndo(), KDO_URP_NULL_POS);
								}
								if (OraCdcChangeRowOp.KDO_POS + 1 + rowChange.columnCount() < rowChange.coords().length) {
									colNumOffsetSet += rowChange.writeColsWithNulls(
											baos, OraCdcChangeRowOp.KDO_POS, OraCdcChangeRowOp.KDO_POS + 1,
											(row.flags & FLG_PARTIAL_ROLLBACK) > 0 ? colNumOffsetSet : 
												(change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo()),
											KDO_URP_NULL_POS);
									colNumOffsetSet += rowChange.ncol(OraCdcChangeRowOp.KDO_POS);
								}
							}
							case _11_2_IRP -> {
								change.writeSupplementalCols(baos);
								rowChange.writeColsWithNulls(
										baos, OraCdcChangeRowOp.KDO_POS, 0,
										change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo(),
										KDO_ORP_IRP_NULL_POS);
								if (rr.has5_1())
									change.writeColsWithNulls(
										baosB, OraCdcChangeUndoBlock.KDO_POS, 0,
										(row.flags & FLG_PARTIAL_ROLLBACK) > 0 ? colNumOffsetSet : 
											(change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetUndo()),
										KDO_ORP_IRP_NULL_POS);
							}
							case _11_6_ORP -> {
								change.writeSupplementalCols(baosW);
								change.writeColsWithNulls(
										baosB, OraCdcChangeUndoBlock.KDO_POS, 0,
										change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetUndo(),
										KDO_ORP_IRP_NULL_POS);
								change.writeColsWithNulls(
										baosW, OraCdcChangeUndoBlock.KDO_POS, 0,
										change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetUndo(),
										KDO_ORP_IRP_NULL_POS);
								colNumOffsetSet += change.columnCount();
								rowChange.writeColsWithNulls(
										baosW, OraCdcChangeRowOp.KDO_POS, 0,
										change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetRedo(),
										KDO_ORP_IRP_NULL_POS);
								colNumOffsetSet += rowChange.columnCount();
							}
							case _11_16_LMN -> {
								change.writeSupplementalCols(baosW);
							}
							case _11_8_CFA -> {
								change.writeSupplementalCols(baosW);
							}
							case _11_3_DRP -> {
								change.writeColsWithNulls(
										baosB, OraCdcChangeUndoBlock.KDO_POS, 0,
										change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetUndo(),
										KDO_ORP_IRP_NULL_POS);
								change.writeColsWithNulls(
										baosW, OraCdcChangeUndoBlock.KDO_POS, 0,
										change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetUndo(),
										KDO_ORP_IRP_NULL_POS);
								colNumOffsetSet += change.columnCount();
							}
							case _10_18_LUP -> {
								colNumOffsetSet += change.writeIndexColumns(baosW,
										change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetUndo());
							}
							case _10_30_LNU -> {
								colNumOffsetSet += ((OraCdcChangeIndexOp) rowChange).writeIndexColumns(baos,
										change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo());
							}
							default ->
								LOGGER.warn("Unknow operation OP:{} at RBA {}", formatOpCode(rowChange.operation()), rr.rba());
						}
					}
					byte[] baosBBytes = baosB.toByteArray();
					putU24(baos, baosBBytes.length);
					baos.write(baosBBytes);
					baosB.close();
					baosB = null;
					baosBBytes = null;
					putU16(baos, whereColCount);
					baos.write(baosW.toByteArray());
					baosW.close();
					baosW = null;
				}
			}
			redoBytes = baos.toByteArray();
		}

		final OraCdcRedoRecord last = row.last();
		final OraCdcChangeUndo change;
		if ((row.flags & FLG_OPPOSITE_ORDER) > 0) {
			if ((row.flags & FLG_PARTIAL_ROLLBACK) > 0) {
				change = last.changePrb();
			} else {
				change = last.change5_1();
			}
		} else {
			if ((row.flags & FLG_PARTIAL_ROLLBACK) > 0) {
				change = first.changePrb();
			} else {
				change = first.change5_1();
			}
		}

		final OraCdcRedoMinerStatement orm;
		if ((row.flags & FLG_OPPOSITE_ORDER) > 0)
			orm = new OraCdcRedoMinerStatement(
					isCdb ? (((long)change.conId()) << 32) |  (change.obj() & 0xFFFFFFFFL): change.obj(),
					row.lmOp, redoBytes,
					lwnUnixMillis, last.scn(), row.rba,
					(long) last.subScn(),
					last.rowid(),
					(row.flags & FLG_PARTIAL_ROLLBACK) > 0);
		else
			orm = new OraCdcRedoMinerStatement(
					isCdb ? (((long)change.conId()) << 32) |  (change.obj() & 0xFFFFFFFFL): change.obj(),
					row.lmOp, redoBytes,
					lwnUnixMillis, first.scn(), row.rba,
					(long) first.subScn(),
					first.rowid(),
					(row.flags & FLG_PARTIAL_ROLLBACK) > 0);
		this.addStatement(orm);
		if (LOGGER.isTraceEnabled())
			LOGGER.trace("Statement created:\n\t{}" + orm.getSqlRedo());
	}

	public boolean completed() {
		return halfDone.isEmpty();
	}

	public void emitDdlChange(OraCdcRedoRecord rr, final long lwnUnixMillis) {
		final OraCdcChangeDdl ddl = rr.changeDdl();
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("emitDdlChange() in XID {} at SCN/RBA {}/{} for OP:{}",
					xid, Long.toUnsignedString(rr.scn()), rr.rba(),
					formatOpCode(ddl.operation()));
		final String preProcessed = alterTablePreProcessor(ddl.ddlText());
		if (preProcessed != null) {
			final OraCdcRedoMinerStatement orm = new OraCdcRedoMinerStatement(
					isCdb ? (((long)ddl.conId()) << 32) |  (ddl.obj() & 0xFFFFFFFFL) : ddl.obj(),
					DDL, ddl.ddlText().getBytes(), lwnUnixMillis, rr.scn(), rr.rba(),
					(long) rr.subScn(), RowId.ZERO, false);
			addStatement(orm);
		} else {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Skipping OP:24.1\n{}\n", rr);
		}
	}

}
