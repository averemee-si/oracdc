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
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcStatementBase extends OraCdcRawStatementBase {

	static final int OPERATION_POS = Long.BYTES;
	static final int RBA_PART1_POS = 0x22;
	static final int RBA_PART2_POS = 0x26;
	static final int RBA_PART3_POS = 0x2A;
	static final int ROWID_POS_START = 0x2C;
	static final int ROWID_POS_END = 0x36;
	static final int ROLLBACK_POS = 0x37;

	/** (((long)V$LOGMNR_CONTENTS.CON_ID) {@literal <}{@literal <} 32) | (V$LOGMNR_CONTENTS.DATA_OBJ# {@literal &} 0xFFFFFFFFL) */
	protected long tableId;
	/** V$LOGMNR_CONTENTS.OPERATION_CODE */
	protected short operation;
	/** V$LOGMNR_CONTENTS.SQL_REDO (concatenated!) */
	protected byte[] redoData;
	/** V$LOGMNR_CONTENTS.TIMESTAMP (in millis) */
	protected long ts;
	/** V$LOGMNR_CONTENTS.SCN */
	protected long scn;
	/** V$LOGMNR_CONTENTS.RS_ID */
	protected RedoByteAddress rba;
	/** V$LOGMNR_CONTENTS.SSN */
	protected long ssn;
	/** V$LOGMNR_CONTENTS.ROW_ID */
	protected RowId rowId;
	/** BLOB/CLOB count, default 0 */
	protected byte lobCount;
	/** structure size */
	protected int holderSize;
	/** is this partial rollback? */
	protected boolean rollback;

	// tableId(Long) + operation(Short) + ts(Long) + scn(Long) + ssn(Long) + lobCount(Byte) + PRB(Byte) + RBA + ROWID + <Content length>
	private static final int HOLDER_SIZE = Long.BYTES + Short.BYTES + Long.BYTES + Long.BYTES + Long.BYTES + Byte.BYTES +
			Byte.BYTES + RedoByteAddress.BYTES + RowId.BYTES + Integer.BYTES;
	/**
	 * 
	 * Default constructor
	 * 
	 */
	public OraCdcStatementBase() {}

	/**
	 * 
	 * @param tableId     (((long)V$LOGMNR_CONTENTS.CON_ID) {@literal <}{@literal <} 32) | (V$LOGMNR_CONTENTS.DATA_OBJ# {@literal &} 0xFFFFFFFFL)
	 * @param operation   V$LOGMNR_CONTENTS.OPERATION_CODE
	 * @param redoData    V$LOGMNR_CONTENTS.SQL_REDO (concatenated!)
	 * @param ts          V$LOGMNR_CONTENTS.TIMESTAMP (in millis)
	 * @param scn         V$LOGMNR_CONTENTS.SCN
	 * @param rba         V$LOGMNR_CONTENTS.RS_ID
	 * @param ssn         V$LOGMNR_CONTENTS.SSN
	 * @param rowId       V$LOGMNR_CONTENTS.ROW_ID
	 * @param rollback    V$LOGMNR_CONTENTS.ROLLBACK
	 */
	public OraCdcStatementBase(long tableId, short operation,
			byte[] redoData, long ts, long scn, RedoByteAddress rba, long ssn, RowId rowId, boolean rollback) {
		this.tableId = tableId;
		this.operation = operation;
		this.redoData = redoData;
		this.ts = ts;
		this.scn = scn;
		this.rba = rba;
		this.ssn = ssn;
		this.rowId = rowId;
		// No BLOB/CLOB initially
		this.lobCount = 0;
		// All strings are US7ASCII
		holderSize =  HOLDER_SIZE + redoData.length;
		this.rollback = rollback;
	}

	public OraCdcStatementBase(final byte[] content) {
		restore(content);
	}

	public long getTableId() {
		return tableId;
	}

	public short getOperation() {
		return operation;
	}

	public String opName() {
		return switch (operation) {
			case INSERT -> "INSERT";
			case UPDATE -> "UPDATE";
			case DELETE -> "DELETE";
			default ->     "XML DOC BEGIN";
		};
	}

	public String getSqlRedo() {
		return null;
	}

	public long getTs() {
		return ts;
	}

	public Timestamp getTimestamp() {
		return Timestamp.from(Instant.ofEpochMilli(ts));
	}

	public long getScn() {
		return scn;
	}

	public RedoByteAddress getRba() {
		return rba;
	}

	public long getSsn() {
		return ssn;
	}

	public RowId getRowId() {
		return rowId;
	}

	public byte getLobCount() {
		return lobCount;
	}

	public void setLobCount(byte lobCount) {
		this.lobCount = lobCount;
	}

	public int size() {
		return holderSize;
	}

	public boolean isRollback() {
		return rollback;
	}

	public void clone(final OraCdcStatementBase other) {
		other.tableId = this.tableId;
		other.operation = this.operation;
		other.redoData = this.redoData;
		other.ts = this.ts;
		other.scn = this.scn;
		other.rba = this.rba;
		other.ssn = this.ssn;
		other.rowId = this.rowId;
		other.lobCount = this.lobCount;
		other.rollback = this.rollback;
		other.holderSize = this.holderSize;
	}

	@Override
	public byte[] content() {
		var ba = new byte[(HOLDER_SIZE + redoData.length + 3) & 0xFFFFFFFC];
		putU64(ba, tableId, 0);
		putU16(ba, operation, Long.BYTES);
		putU64(ba, ts, 0xA);
		putU64(ba, scn, 0x12);
		putU64(ba, ssn, 0x1A);
		System.arraycopy(rba.toByteArray(), 0, ba, 0x22, RedoByteAddress.BYTES);
		System.arraycopy(rowId.toByteArray(), 0, ba, 0x2C, RowId.BYTES);
		ba[0x36] = lobCount;
		ba[0x37] = (byte) (rollback ? 1 : 0);
		putU32(ba, redoData.length, 0x38);
		System.arraycopy(redoData, 0, ba, HOLDER_SIZE, redoData.length); 
		return ba;
	}

	@Override
	public void restore(final byte[] content) {
		tableId = BIG_ENDIAN.getU64(content, 0);
		operation = BIG_ENDIAN.getU16(content, OPERATION_POS);
		ts = BIG_ENDIAN.getU64(content, 0xA);
		scn = BIG_ENDIAN.getU64(content, 0x12);
		ssn = BIG_ENDIAN.getU64(content, 0x1A);
		rba = new RedoByteAddress(
				BIG_ENDIAN.getU32(content, RBA_PART1_POS),
				BIG_ENDIAN.getU32(content, RBA_PART2_POS),
				BIG_ENDIAN.getU16(content, RBA_PART3_POS));
		//TODO need allocation-free method!
		rowId = new RowId(
				Arrays.copyOfRange(content, ROWID_POS_START, ROWID_POS_END));
		lobCount = content[0x36];
		rollback = content[ROLLBACK_POS] == 1 ? true : false;
		redoData = Arrays.copyOfRange(content, HOLDER_SIZE, HOLDER_SIZE + BIG_ENDIAN.getU32(content, 0x38));
	}

	public StringBuilder toStringBuilder() {
		final StringBuilder sb = new StringBuilder(APPROXIMATE_SIZE);
		final int objId = (int) tableId;
		sb
			.append("\tDATA_OBJ# = ")
			.append(objId)
			.append("\n\tSCN = ")
			.append(scn)
			.append("\n\tTIMESTAMP = ")
			//TODO
			//TODO Need to pass OraRdbmsInfo.getDbTimeZone() !!!
			//TODO
			.append(Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()))
			.append("\n\tRBA = ")
			.append(rba)
			.append("\n\tSSN = ")
			.append(ssn)
			.append("\n\tROW_ID = ")
			.append(rowId)
			.append("\n\tOPERATION_CODE = ")
			.append(operation)
			.append("\n\tSQL_REDO = ")
			.append(getSqlRedo())
			.append("\n\tROLLBACK = ")
			.append(rollback ? "1" : "0")
			.append("\n");
		return sb;
	}

	public static StringBuilder delimitedRowHeader() {
		return delimitedRowHeader("\t");
	}

	public static StringBuilder delimitedRowHeader(final String delimiter) {
		final StringBuilder sb = new StringBuilder(256);
		sb
			.append("SCN")
			.append(delimiter)
			.append("TIMESTAMP")
			.append(delimiter)
			.append("RBA")
			.append(delimiter)
			.append("SSN")
			.append(delimiter)
			.append("OBJECT_ID")
			.append(delimiter)
			.append("ROWID")
			.append(delimiter)
			.append("OPERATION_CODE")
			.append(delimiter)
			.append("ROLLBACK\n");
		return sb;
	}

	public StringBuilder toDelimitedRow() {
		return toDelimitedRow("\t");
	}

	public StringBuilder toDelimitedRow(final String delimiter) {
		final int objId = (int) tableId;
		final StringBuilder sb = new StringBuilder(APPROXIMATE_SIZE);
		sb
			.append(scn)
			.append(delimiter)
			.append(Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()))
			.append(delimiter)
			.append(rba.toString())
			.append(delimiter)
			.append(ssn)
			.append(delimiter)
			.append(objId)
			.append(delimiter)
			.append(rowId.toString())
			.append(delimiter)
			.append(operation)
			.append(delimiter)
			.append(rollback ? "1" : "0")
			.append("\n");
		return sb;
	}

}
