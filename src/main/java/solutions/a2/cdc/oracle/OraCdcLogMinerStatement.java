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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WriteMarshallable;

/**
 * Minimlistic presentation of V$LOGMNR_CONTENTS row for OPERATION_CODE = 1|2|3
 * 
 * @author averemee
 */
public class OraCdcLogMinerStatement implements ReadMarshallable, WriteMarshallable {

	/** (((long)V$LOGMNR_CONTENTS.CON_ID) << 32) | (V$LOGMNR_CONTENTS.DATA_OBJ# & 0xFFFFFFFFL) */
	private long tableId;
	/** V$LOGMNR_CONTENTS.OPERATION_CODE */
	private short operation;
	/** V$LOGMNR_CONTENTS.SQL_REDO (concatenated!) */
	private String sqlRedo;
	/** V$LOGMNR_CONTENTS.TIMESTAMP (in millis) */
	private long ts;
	/** V$LOGMNR_CONTENTS.SCN */
	private long scn;
	/** V$LOGMNR_CONTENTS.RS_ID */
	private String rsId;
	/** V$LOGMNR_CONTENTS.SSN */
	private long ssn;
	/** V$LOGMNR_CONTENTS.ROW_ID */
	private String rowId;
	/** BLOB/CLOB count, default 0 */
	private byte lobCount;
	/** structure size */
	private int holderSize;
	/** is this partial rollback */
	private boolean rollback;

	/**
	 * 
	 * Default constructor
	 * 
	 */
	public OraCdcLogMinerStatement() {
		// tableId(Long) + operation(Short) + ts(Long) + scn(Long) + ssn(Long) + lobCount(Byte)
		// Need to add with data actual size of : sqlRedo, rsId, and rowId
		holderSize = Long.BYTES + Short.BYTES + Long.BYTES + Long.BYTES + Long.BYTES + Byte.BYTES;
	}

	/**
	 * 
	 * @param tableId     (((long)V$LOGMNR_CONTENTS.CON_ID) {@literal <}{@literal <} 32) | (V$LOGMNR_CONTENTS.DATA_OBJ# {@literal &} 0xFFFFFFFFL)
	 * @param operation   V$LOGMNR_CONTENTS.OPERATION_CODE
	 * @param sqlRedo     V$LOGMNR_CONTENTS.SQL_REDO (concatenated!)
	 * @param ts          V$LOGMNR_CONTENTS.TIMESTAMP (in millis)
	 * @param scn         V$LOGMNR_CONTENTS.SCN
	 * @param rsId        V$LOGMNR_CONTENTS.RS_ID
	 * @param ssn         V$LOGMNR_CONTENTS.SSN
	 * @param rowId       V$LOGMNR_CONTENTS.ROW_ID
	 * @param rollback    V$LOGMNR_CONTENTS.ROLLBACK
	 */
	public OraCdcLogMinerStatement(long tableId, short operation,
			String sqlRedo, long ts, long scn, String rsId, long ssn, String rowId, boolean rollback) {
		this();
		this.tableId = tableId;
		this.operation = operation;
		this.sqlRedo = sqlRedo;
		this.ts = ts;
		this.scn = scn;
		this.rsId = rsId;
		this.ssn = ssn;
		this.rowId = rowId;
		// No BLOB/CLOB initially
		this.lobCount = 0;
		// All strings are US7ASCII
		holderSize += (
				sqlRedo.length() + rsId.length() + rowId.length());
		this.rollback = rollback;
	}

	public long getTableId() {
		return tableId;
	}

	public short getOperation() {
		return operation;
	}

	public String getSqlRedo() {
		return sqlRedo;
	}

	public long getTs() {
		return ts;
	}

	public long getScn() {
		return scn;
	}

	public String getRsId() {
		return rsId;
	}

	public long getSsn() {
		return ssn;
	}

	public String getRowId() {
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

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(1024);
		final int origTableId = (int) tableId;
		final int conId = (int) (tableId >> 32);
		sb
			.append("OraCdcLogMinerStatement [")
			.append("CON_ID=")
			.append(conId)
			.append(", DATA_OBJ#=")
			.append(origTableId)
			.append(", OPERATION_CODE=")
			.append(operation)
			.append(", SQL_REDO=")
			.append(sqlRedo)
			.append(", TIMESTAMP=")
			.append(ts)
			.append(", SCN=")
			.append(scn)
			.append(", RS_ID=")
			.append(rsId)
			.append(", SSN=")
			.append(ssn)
			.append(", ROW_ID=")
			.append(rowId)
			.append(", ROLLBACK=")
			.append(rollback ? "1" : "0")
			.append(", LOB_COUNT=")
			.append(lobCount)
			.append("]");

		return sb.toString();
	}

	public void clone(final OraCdcLogMinerStatement other) {
		other.tableId = this.tableId;
		other.operation = this.operation;
		other.sqlRedo = this.sqlRedo;
		other.ts = this.ts;
		other.scn = this.scn;
		other.rsId = this.rsId;
		other.ssn = this.ssn;
		other.rowId = this.rowId;
		other.lobCount = this.lobCount;
		other.holderSize = this.holderSize;
	}

	@Override
	public void writeMarshallable(WireOut wire) {
		wire.bytes()
			.writeLong(tableId)
			.writeShort(operation)
			.write8bit(sqlRedo)
			.writeLong(ts)
			.writeLong(scn)
			.write8bit(rsId)
			.writeLong(ssn)
			.write8bit(rowId)
			.writeByte(lobCount);
	}


	@Override
	public void readMarshallable(WireIn wire) throws IORuntimeException {
		Bytes<?> raw = wire.bytes();
		tableId = raw.readLong();
		operation = raw.readShort();
		sqlRedo = raw.read8bit();
		ts = raw.readLong();
		scn = raw.readLong();
		rsId = raw.read8bit();
		ssn = raw.readLong();
		rowId = raw.read8bit();
		lobCount = raw.readByte();
	}


	@Override
	public boolean usesSelfDescribingMessage() {
		// TODO Auto-generated method stub
		return ReadMarshallable.super.usesSelfDescribingMessage();
	}
}
