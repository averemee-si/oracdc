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

import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UNSUPPORTED;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KCOCODRW;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KDO_KDOM2;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KDO_ORP_IRP_NULL_POS;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KDO_URP_NULL_POS;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_2_LIN;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_4_LDE;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_18_LUP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_30_LNU;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_35_LCU;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_2_IRP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_3_DRP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_5_URP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_6_ORP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_16_LMN;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgFirstPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgHeadPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgLastPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgNextPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.flgPrevPart;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.formatOpCode;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.printFbFlags;
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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.internals.OraCdcChange;
import solutions.a2.cdc.oracle.internals.OraCdcChangeIndexOp;
import solutions.a2.cdc.oracle.internals.OraCdcChangeRowOp;
import solutions.a2.cdc.oracle.internals.OraCdcChangeUndo;
import solutions.a2.cdc.oracle.internals.OraCdcChangeUndoBlock;
import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
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
	Set<Map.Entry<RedoByteAddress, Long>> rollbackPairs;
	private boolean suspicious = false;

	private String username;
	private String osUsername;
	private String hostname;
	private long auditSessionId;
	private String sessionInfo;
	private String clientId;

	private final Map<Integer, Deque<RowChangeHolder>> halfDone = new HashMap<>();
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
				pre.tableId = oraSql.getTableId();
				pre.operation = oraSql.getOperation();
				pre.rowId = oraSql.getRowId();
				pre.scn = oraSql.getScn();
				pre.rsId = oraSql.getRba();
				pre.ssn = oraSql.getSsn();

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
				final Map.Entry<RedoByteAddress, Long> uniqueAddr = Map.entry(oraSql.getRba(), oraSql.getSsn());
				return rollbackPairs.contains(uniqueAddr);
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
			.append("\n")
			.append("COMMIT_SCN=")
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
						.append(rowChangeHolder.homogeneous)
						.append(" needHeadFlag=")
						.append(rowChangeHolder.needHeadFlag)
						.append(" onlyLmn=")
						.append(rowChangeHolder.onlyLmn)
						.append(" operation=")
						.append(rowChangeHolder.operation)
						.append(" oppositeOrder=")
						.append(rowChangeHolder.oppositeOrder)
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
									.append(formatOpCode(rr.has11_x() ? rr.change11_x().operation() : rr.change10_x().operation()))
									.append(" FB=")
									.append(printFbFlags(rr.has11_x() ? rr.change11_x().fb() : rr.change10_x().fb()));
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

	public boolean needsSorting() {
		return needsSorting;
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
			LOGGER.debug("Working with partial rollback statement for ROWID={} at SCN={}, RBA(RS_ID)='{}', SSN={}",
					pre.rowId, pre.scn, pre.rsId, pre.ssn);
		}
	}

	void printUnpairedRollbackEntryError(final PartialRollbackEntry pre) {
		suspicious = true;
		LOGGER.error(
				"""
				
				=====================
				No pair for partial rollback statement with ROWID={} at SCN={}, RBA(RS_ID)='{}' in transaction XID='{}'!
				=====================
				
				""", pre.rowId, pre.scn, pre.rsId, getXid());
	}

	abstract void addStatement(final OraCdcStatementBase oraSql);
	public abstract boolean getStatement(OraCdcStatementBase oraSql);
	abstract long size();
	abstract int length();
	abstract int offset();
	abstract void close();

	static class PartialRollbackEntry {
		long index;
		long tableId;
		short operation;
		RowId rowId;
		long scn;
		RedoByteAddress rsId;
		long ssn;
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
			if (!partialRollback) {
				if (record.has11_x())
					onlyLmn = onlyLmn && (record.change11_x().operation() == _11_16_LMN);
				else
					//IOT
					onlyLmn = false;
			}			
			if (!complete && !partialRollback && records.size() == 1) {
				final byte fb5_1 = record.change5_1().fb();
				final byte fb11_x = record.has11_x() ? record.change11_x().fb() : record.change10_x().fb();
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
							formatOpCode(record.has11_x() ?
									record.change11_x().operation() :
									record.change10_x().operation()),
							printFbFlags(record.has11_x() ?
									record.change11_x().fb() :
									record.change10_x().fb()));
				} else {
					LOGGER.debug("Adding XID {}, SCN {}, RBA {}, OP:5.1 fb:{}, supp fb:{}, OP:{} fb:{}",
							record.xid(),
							record.scn(),
							record.rba(),
							printFbFlags(record.change5_1().fb()),
							printFbFlags(record.change5_1().supplementalFb()),
							formatOpCode(record.has11_x() ?
									record.change11_x().operation() :
									record.change10_x().operation()),
							printFbFlags(record.has11_x() ?
									record.change11_x().fb() :
									record.change10_x().fb()));
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
				final boolean rowOp = record.has11_x();
				final byte fb11_x = rowOp 
						? record.change11_x().fb()
						: record.change10_x().fb() == 0
								? record.change5_1().supplementalFb()
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
					if (homogeneous) {
						if (flgFirstPart(fb5_1) && flgFirstPart(fb11_x)) indexFirst = index;
						if (flgLastPart(fb5_1) && flgLastPart(fb11_x)) indexLast = index;	
						if (flgHeadPart(fb5_1) && flgHeadPart(fb11_x)) indexHead = index;
					} else {
						if (rowOp) {
							if (flgFirstPart(fb5_1) || flgFirstPart(fb11_x)) indexFirst = index;
							if (flgLastPart(fb5_1) || flgLastPart(fb11_x)) indexLast = index;	
							if (flgHeadPart(fb5_1) || flgHeadPart(fb11_x)) indexHead = index;
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
			if (indexHead > -1 && indexHead > indexFirst)
				oppositeOrder = true;
			else
				oppositeOrder = false;
			if (oppositeOrder)
				sortedRecs.add(records.get(indexLast));
			else
				sortedRecs.add(records.get(indexFirst));
			if (indexValue == 0 && indexValue != indexFirst && indexValue != indexLast)
				sortedRecs.add(records.get(0));
			else {	
				for (int index = 0; index < indexValue; index++)
					sortedRecs.add(records.get(index));
			}
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

	public void processRowChange(final OraCdcRedoRecord record, final boolean partialRollback,
			final long lwnUnixMillis) throws IOException {
		RowChangeHolder row = createRowChangeHolder(record, partialRollback);
		if (row.complete) {
			emitRowChange(row, lwnUnixMillis);
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
				final byte lastHalfDoneRowFb = lastHalfDone.has11_x() ?
						lastHalfDone.change11_x().fb() : lastHalfDone.change10_x().fb();
				final boolean push;
				if (partialRollback) {
					if (record.has11_x())
						push = lastHalfDoneRowFb == record.change11_x().fb();
					else
						push = lastHalfDoneRowFb == record.change10_x().fb();
				} else {
					if (record.has11_x()) {
						if (record.change11_x().fb() == 0 && record.change5_1().fb() == 0)
							push = false;
						else
							push = lastHalfDoneRowFb == record.change11_x().fb() && 
									lastHalfDone.change5_1().fb() == record.change5_1().fb();
					} else {
						if (record.change10_x().fb() == 0 && record.change5_1().fb() == 0)
							push = false;
						else
							push = lastHalfDoneRowFb == record.change10_x().fb() && 
									lastHalfDone.change5_1().fb() == record.change5_1().fb();
					} 
				}
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
						emitRowChange(halfDoneRow, lwnUnixMillis);
						deque.removeFirst();
						if (deque.isEmpty()) {
							halfDone.remove(key);
						}
					}
				}
			}

		}
	}

	void processRowChangeLmnUpdate(final OraCdcRedoRecord record, final long lwnUnixMillis) throws IOException {
		final byte fbLmn = record.change11_x().fb();
		if (flgPrevPart(fbLmn) && flgNextPart(fbLmn)) {
			//Just complete previous record
			final int key = record.halfDoneKey();
			Deque<RowChangeHolder> deque =  halfDone.get(key);
			if (deque == null) {
				LOGGER.error(
						"""
						
						=====================
						Unable to pair OP:11.16 record with row flags {} at RBA {}, SCN {}, XID {} in file {}
						Redo record information:
						{}
						=====================
						
						""", printFbFlags(fbLmn),record.rba(), record.scn(), record.xid(), record.redoLog().fileName(), record);
			} else {
				RowChangeHolder halfDoneRow =  deque.pollFirst();
				halfDoneRow.complete = true;
				emitRowChange(halfDoneRow, lwnUnixMillis);
				if (deque.isEmpty()) {
					halfDone.remove(key);
				}
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Emitting {} with first RBA {}, last RBA {} using OP:11.16 record with row flags {} at RBA {}, SCN {}, XID {} in file {}",
							halfDoneRow.lmOp == UPDATE ? "UPDATE" : halfDoneRow.lmOp == INSERT ? "INSERT" : "DELETE",
							halfDoneRow.first().rba(), halfDoneRow.last().rba(), 
							printFbFlags(fbLmn),record.rba(), record.scn(), record.xid(), record.redoLog().fileName());
				}
			}
		} else if (record.supplementalLogData()) {
			if (flgFirstPart(fbLmn) && !flgNextPart(fbLmn)) {
				processRowChange(record, false, lwnUnixMillis);
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

	private RowChangeHolder createRowChangeHolder(final OraCdcRedoRecord record, final boolean partialRollback) {
		final OraCdcChange rowChange = record.has11_x() 
				? record.change11_x()
				: record.change10_x();
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
					row.oppositeOrder = true;
				if (row.operation == _11_16_LMN) {
					row.needHeadFlag = false;
					if (flgFirstPart(rowChange.fb()))
						row.oppositeOrder = true;
				}
				break;
			case _11_2_IRP:
				row.lmOp = INSERT;
				if (flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
					row.complete = true;
				else if (!flgHeadPart(rowChange.fb()))
					row.oppositeOrder = true;
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
			final OraCdcChangeUndoBlock undoChange = record.change5_1(); 
			switch (row.operation) {
			case _11_5_URP:
			case _11_16_LMN:
				row.lmOp = UPDATE;
				if (flgFirstPart(undoChange.supplementalFb()) && flgLastPart(undoChange.supplementalFb()))
						row.complete = true;
				else if (flgFirstPart(undoChange.fb()) && flgLastPart(undoChange.fb()) &&
						flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
					row.complete = true;
				else if (!flgHeadPart(undoChange.fb()))
					row.oppositeOrder = true;
				if (row.operation == _11_16_LMN) {
					row.needHeadFlag = false;
					if (!flgFirstPart(undoChange.fb()) || flgFirstPart(rowChange.fb()))
						row.oppositeOrder = true;
				}
				break;
			case _11_2_IRP:
			case _11_6_ORP:
				if (row.operation == _11_2_IRP)
					row.lmOp = INSERT;
				else
					row.lmOp = UPDATE;
				if (flgHeadPart(rowChange.fb()) && flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
					row.complete = true;
				else if (!flgHeadPart(rowChange.fb()))
					row.oppositeOrder = true;
				break;
			case _11_3_DRP:
				row.lmOp = DELETE;
				if (flgHeadPart(undoChange.fb()) && flgFirstPart(undoChange.fb()) && flgLastPart(undoChange.fb()))
					row.complete = true;
				else if (!flgHeadPart(undoChange.fb()))
					row.oppositeOrder = true;
				break;
			case _10_2_LIN:
				row.lmOp = INSERT;
				if (flgHeadPart(rowChange.fb()) && flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
					row.complete = true;
				else if (rowChange.fb() == 0 &&
						flgFirstPart(undoChange.supplementalFb()) && flgLastPart(undoChange.supplementalFb()))
					row.complete = true;					
				else if (!flgHeadPart(rowChange.fb()))
					row.oppositeOrder = true;
				break;
			case _10_4_LDE:
				row.lmOp = DELETE;
				if (flgHeadPart(undoChange.fb()) && flgFirstPart(undoChange.fb()) && flgLastPart(undoChange.fb()))
					row.complete = true;
				else if (rowChange.fb() == 0 &&
						flgFirstPart(undoChange.supplementalFb()) && flgLastPart(undoChange.supplementalFb()))
					row.complete = true;					
				else if (!flgHeadPart(undoChange.fb()))
					row.oppositeOrder = true;
				break;
			case _10_18_LUP:
				row.lmOp = UPDATE;
				row.needHeadFlag = false;
				if (flgFirstPart(undoChange.supplementalFb()) && flgLastPart(undoChange.supplementalFb()))
					row.complete = true;
				else if (flgFirstPart(undoChange.fb()) && flgLastPart(undoChange.fb()) &&
					flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
					row.complete = true;
				else if (!flgHeadPart(undoChange.fb()))
					row.oppositeOrder = true;
				break;
			case _10_30_LNU:
				row.lmOp = UPDATE;
				if (flgFirstPart(rowChange.fb()) && flgLastPart(rowChange.fb()))
					row.complete = true;
				else if (!flgHeadPart(rowChange.fb()))
					row.oppositeOrder = true;
				break;
			case _10_35_LCU:
				row.lmOp = UPDATE;
				if (flgFirstPart(undoChange.supplementalFb()) && flgLastPart(undoChange.supplementalFb()))
					row.complete = true;
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
			boolean iotUpdate = false;
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
			if ((row.lmOp == INSERT || row.lmOp == DELETE) &&
					head > 0 && first > 0 && last > 0)
				row.complete = true;
			else if (row.lmOp == UPDATE) {
				if (row.needHeadFlag && head > 1 && first > 1 && last > 1)
					row.complete = true;
				else if (!row.needHeadFlag && first > 1 && last > 1)
					row.complete = true;
				else if (iotUpdate && first > 0 && last > 0)
					row.complete = true;
			} else
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
							.append(formatOpCode(record.has11_x() ?
										record.change11_x().operation() :
										record.change10_x().operation()))
							.append(" fb:")
							.append(printFbFlags(record.has11_x() ?
									record.change11_x().fb() :
									record.change10_x().fb()));
					else
						sb
							.append(", OP:5.1 fb:")
							.append(printFbFlags(record.change5_1().fb()))
							.append(", supp fb:")
							.append(printFbFlags(record.change5_1().supplementalFb()))
							.append(", OP:")
							.append(formatOpCode(record.has11_x() ?
									record.change11_x().operation() :
									record.change10_x().operation()))
							.append(" fb:")
							.append(printFbFlags(record.has11_x() ?
									record.change11_x().fb() :
									record.change10_x().fb()));
				}
				LOGGER.debug(sb.toString());
			}
		}
	}

	private void emitRowChange(final RowChangeHolder row, final long lwnUnixMillis) throws IOException {
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
				final OraCdcChange rowChange = rr.has11_x() ? rr.change11_x() : rr.change10_x();
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
					} else if (rowChange.operation() == _11_5_URP) {
						setOrValColCount += rowChange.columnCount();
						whereColCount += change.columnCount();
						whereColCount += change.supplementalCc();						
					} else if (rowChange.operation() == _10_2_LIN) {
						setOrValColCount += rowChange.columnCount();
					} else if (rowChange.operation() == _10_4_LDE) {
						setOrValColCount += change.columnCount();
					} else if (rowChange.operation() == _10_18_LUP) {
						whereColCount += change.columnCount();
					} else if (rowChange.operation() == _10_30_LNU ||
								rowChange.operation() == _10_35_LCU) {
						setOrValColCount += rowChange.columnCount();
						whereColCount += change.columnCount();
						whereColCount += change.supplementalCc();						
					} else {
						LOGGER.error(
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
					(row.partialRollback && row.lmOp == UPDATE && first.change11_x().operation() == _11_6_ORP)) {
				if (row.partialRollback) {
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
				if (row.oppositeOrder)
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
								} else if (row.partialRollback) {
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
				if (row.homogeneous) {
					for (final OraCdcRedoRecord rr : row.records) {
						final OraCdcChangeUndoBlock change = rr.change5_1();
						final OraCdcChange rowChange = rr.has11_x() ? rr.change11_x() : rr.change10_x();
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
									row.partialRollback ?  colNumOffsetSet : 
										(change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo()),
									KDO_URP_NULL_POS);
							if (rr.has5_1())
								change.writeColsWithNulls(
									baosB, OraCdcChangeUndoBlock.KDO_POS, OraCdcChangeUndoBlock.KDO_POS + 1,
									row.partialRollback ?  colNumOffsetSet : 
										(change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetUndo()),
									KDO_URP_NULL_POS);
						} else if (rowChange.operation() == _11_6_ORP &&
								OraCdcChangeRowOp.KDO_POS + rowChange.columnCount() < rowChange.coords().length) {
							rowChange.writeColsWithNulls(
									baos, OraCdcChangeRowOp.KDO_POS, 0,
									row.partialRollback ?  colNumOffsetSet : 
										(change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo()),
									KDO_ORP_IRP_NULL_POS);
							if (rr.has5_1())
								change.writeColsWithNulls(
									baosB, OraCdcChangeUndoBlock.KDO_POS, 0,
									row.partialRollback ?  colNumOffsetSet : 
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
						} else {
							LOGGER.warn("Unable to read column data for UPDATE (SET) at RBA {}, change #{}",
									rr.rba(), rowChange.num());
						}
						colNumOffsetSet += rowChange.columnCount();
					}
					byte[] baosBBytes = baosB.toByteArray();
					putU24(baos, baosBBytes.length);
					baos.write(baosBBytes);
					baosB.close();
					baosB = null;
					baosBBytes = null;
					if (row.partialRollback) {
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
					for (final OraCdcRedoRecord rr : row.records) {
						final OraCdcChangeUndoBlock change = rr.change5_1();
						final OraCdcChange rowChange = rr.has11_x() ? rr.change11_x() : rr.change10_x();
						if (rowChange.operation() == _11_2_IRP) {
							change.writeSupplementalCols(baos);
							rowChange.writeColsWithNulls(
									baos, OraCdcChangeRowOp.KDO_POS, 0,
									change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo(),
									KDO_ORP_IRP_NULL_POS);
							if (rr.has5_1())
								change.writeColsWithNulls(
									baosB, OraCdcChangeUndoBlock.KDO_POS, 0,
									row.partialRollback ?  colNumOffsetSet : 
										(change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetUndo()),
									KDO_ORP_IRP_NULL_POS);
						} else if (rowChange.operation() == _11_6_ORP) {
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
						} else if (rowChange.operation() == _11_5_URP) {
							change.writeSupplementalCols(baosW);
							if (OraCdcChangeUndoBlock.KDO_POS + 1 + change.columnCountNn() < change.coords().length) {
								change.writeColsWithNulls(baosB, OraCdcChangeUndoBlock.KDO_POS, OraCdcChangeUndoBlock.KDO_POS + 1,
										change.suppOffsetUndo(), KDO_URP_NULL_POS);
							}
							if (OraCdcChangeRowOp.KDO_POS + 1 + rowChange.columnCount() < rowChange.coords().length) {
								colNumOffsetSet += rowChange.writeColsWithNulls(
										baos, OraCdcChangeRowOp.KDO_POS, OraCdcChangeRowOp.KDO_POS + 1,
										row.partialRollback ?  colNumOffsetSet : 
											(change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo()),
										KDO_URP_NULL_POS);
								colNumOffsetSet += rowChange.ncol(OraCdcChangeRowOp.KDO_POS);
							}
						} else if (rowChange.operation() == _10_18_LUP) {
							colNumOffsetSet += change.writeIndexColumns(baosW,
									change.suppOffsetUndo() == 0 ? colNumOffsetSet : change.suppOffsetUndo());
						} else if (rowChange.operation() == _10_30_LNU) {
							colNumOffsetSet += rr.change10_x().writeIndexColumns(baos,
									change.suppOffsetRedo() == 0 ? colNumOffsetSet : change.suppOffsetRedo());
						} else {
							LOGGER.error("Unknow operation OP:{} at RBA {}", formatOpCode(rowChange.operation()), rr.rba());
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
					lwnUnixMillis, last.scn(), row.rba,
					(long) last.subScn(),
					last.rowid(),
					row.partialRollback);
		else
			orm = new OraCdcRedoMinerStatement(
					isCdb ? (((long)change.conId()) << 32) |  (change.obj() & 0xFFFFFFFFL): change.obj(),
					row.lmOp, redoBytes,
					lwnUnixMillis, first.scn(), row.rba,
					(long) first.subScn(),
					first.rowid(),
					row.partialRollback);
		this.addStatement(orm);
		if (LOGGER.isTraceEnabled())
			LOGGER.trace("Statement created:\n\t{}" + orm.getSqlRedo());
	}


}
