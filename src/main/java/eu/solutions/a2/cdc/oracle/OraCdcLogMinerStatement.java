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

package eu.solutions.a2.cdc.oracle;

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

	/**
	 * 
	 * Default constructor
	 * 
	 */
	public OraCdcLogMinerStatement() {
	}

	/**
	 * 
	 * @param tableId     (((long)V$LOGMNR_CONTENTS.CON_ID) << 32) | (V$LOGMNR_CONTENTS.DATA_OBJ# & 0xFFFFFFFFL)
	 * @param operation   V$LOGMNR_CONTENTS.OPERATION_CODE
	 * @param sqlRedo     V$LOGMNR_CONTENTS.SQL_REDO (concatenated!)
	 * @param ts          V$LOGMNR_CONTENTS.TIMESTAMP (in millis)
	 * @param scn         V$LOGMNR_CONTENTS.SCN
	 * @param rsId        V$LOGMNR_CONTENTS.RS_ID
	 * @param ssn         V$LOGMNR_CONTENTS.SSN
	 * @param rowId       V$LOGMNR_CONTENTS.ROW_ID
	 */
	public OraCdcLogMinerStatement(
			long tableId, short operation, String sqlRedo, long ts, long scn, String rsId, long ssn, String rowId) {
		super();
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
	}

	public long getTableId() {
		return tableId;
	}

	public void setTableId(long tableId) {
		this.tableId = tableId;
	}

	public short getOperation() {
		return operation;
	}

	public void setOperation(short operation) {
		this.operation = operation;
	}

	public String getSqlRedo() {
		return sqlRedo;
	}

	public void setSqlRedo(String sqlRedo) {
		this.sqlRedo = sqlRedo;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public long getScn() {
		return scn;
	}

	public void setScn(long scn) {
		this.scn = scn;
	}

	public String getRsId() {
		return rsId;
	}

	public void setRsId(String rsId) {
		this.rsId = rsId;
	}

	public long getSsn() {
		return ssn;
	}

	public void setSsn(long ssn) {
		this.ssn = ssn;
	}

	public String getRowId() {
		return rowId;
	}

	public void setRowId(String rowId) {
		this.rowId = rowId;
	}

	public byte getLobCount() {
		return lobCount;
	}

	public void setLobCount(byte lobCount) {
		this.lobCount = lobCount;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(1024);
		final int origTableId = (int) tableId;
		final int conId = (int) (tableId >> 32);
		sb.append("OraCdcLogMinerStatement [");
		sb.append("CON_ID=");
		sb.append(conId);
		sb.append(", DATA_OBJ#=");
		sb.append(origTableId);
		sb.append(", OPERATION_CODE=");
		sb.append(operation);
		sb.append(", SQL_REDO=");
		sb.append(sqlRedo);
		sb.append(", TIMESTAMP=");
		sb.append(ts);
		sb.append(", SCN=");
		sb.append(scn);
		sb.append(", RS_ID=");
		sb.append(rsId);
		sb.append(", SSN=");
		sb.append(ssn);
		sb.append(", ROW_ID=");
		sb.append(rowId);
		sb.append(", LOB_COUNT=");
		sb.append(lobCount);
		sb.append("]");

		return sb.toString();
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
