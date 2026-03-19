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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;
import solutions.a2.oracle.utils.BinaryUtils;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcStatementBase {

	public static final int APPROXIMATE_SIZE = 0x4000;
	public static final BinaryUtils BE = BinaryUtils.get(false);

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

	public long getTableId() {
		return tableId;
	}

	public short getOperation() {
		return operation;
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

	public void content(final byte[] content) {
		tableId = BE.getU64(content, 0);
		operation = BE.getU16(content, Long.BYTES);
		ts = BE.getU64(content, 0xA);
		scn = BE.getU64(content, 0x12);
		ssn = BE.getU64(content, 0x1A);
		rba = new RedoByteAddress(BE.getU32(content, 0x22), BE.getU32(content, 0x26), BE.getU16(content, 0x2A));
		//TODO need allocation-free method!
		rowId = new RowId(Arrays.copyOfRange(content, 0x2C, 0x36));
		lobCount = content[0x36];
		rollback = content[0x37] == 1 ? true : false;
		redoData = Arrays.copyOfRange(content, HOLDER_SIZE, HOLDER_SIZE + BE.getU32(content, 0x38));
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

	private static void putU16(final byte[] ba, final short u16, final int offset) {
		ba[offset] = (byte)(u16 >>> 8);
		ba[offset + 1] = (byte)u16;
	}

	private static void putU32(final byte[] ba, final int u32, final int offset) {
		ba[offset] = (byte)(u32 >>> 24);
		ba[offset + 1] = (byte)(u32 >>> 16);
		ba[offset + 2] = (byte)(u32 >>> 8);
		ba[offset + 3] = (byte)u32;
	}

	private static void putU64(final byte[] ba, final long u64, final int offset) {
		ba[offset] = (byte)(u64 >>> 56);
		ba[offset + 1] = (byte)(u64 >>> 48);
		ba[offset + 2] = (byte)(u64 >>> 40);
		ba[offset + 3] = (byte)(u64 >>> 32);
		ba[offset + 4] = (byte)(u64 >>> 24);
		ba[offset + 5] = (byte)(u64 >>> 16);
		ba[offset + 6] = (byte)(u64 >>> 8);
		ba[offset + 7] = (byte)u64;
	}

}
