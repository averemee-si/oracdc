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

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WriteMarshallable;
import solutions.a2.oracle.internals.RedoByteAddress;

/**
 * Minimlistic presentation of V$LOGMNR_CONTENTS row for OPERATION_CODE = 1|2|3
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcLogMinerStatement implements ReadMarshallable, WriteMarshallable {

	static final int APPROXIMATE_SIZE = 0x4000;

	/** (((long)V$LOGMNR_CONTENTS.CON_ID) << 32) | (V$LOGMNR_CONTENTS.DATA_OBJ# & 0xFFFFFFFFL) */
	private long tableId;
	/** V$LOGMNR_CONTENTS.OPERATION_CODE */
	private short operation;
	/** V$LOGMNR_CONTENTS.SQL_REDO (concatenated!) */
	private byte[] redoData;
	/** V$LOGMNR_CONTENTS.TIMESTAMP (in millis) */
	private long ts;
	/** V$LOGMNR_CONTENTS.SCN */
	private long scn;
	/** V$LOGMNR_CONTENTS.RS_ID */
	private RedoByteAddress rba;
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
		// tableId(Long) + operation(Short) + ts(Long) + scn(Long) + ssn(Long) + lobCount(Byte) + RBA
		// Need to add with data actual size of : sqlRedo, rsId, and rowId
		holderSize = Long.BYTES + Short.BYTES + Long.BYTES + Long.BYTES + Long.BYTES + Byte.BYTES +
				RedoByteAddress.BYTES;
	}

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
	public OraCdcLogMinerStatement(long tableId, short operation,
			byte[] redoData, long ts, long scn, RedoByteAddress rba, long ssn, String rowId, boolean rollback) {
		this();
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
		holderSize +=  (Integer.BYTES +  redoData.length + rowId.length());
		this.rollback = rollback;
	}

	public long getTableId() {
		return tableId;
	}

	public short getOperation() {
		return operation;
	}

	public String getSqlRedo() {
		return new String(redoData, StandardCharsets.US_ASCII);
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
		final StringBuilder sb = new StringBuilder(APPROXIMATE_SIZE);
		final int conId = (int) (tableId >> 32);
		sb.append("OraCdcLogMinerStatement [");
			if (conId != 0) {
				sb
					.append("\n\tCON_ID=")
					.append(conId);
			}
			sb
				.append("\n")
				.append(toStringBuilder());
			if (lobCount != 0) {
				sb
					.append("\tLOB_COUNT=")
					.append(lobCount)
					.append("\n");
					
			}
			sb.append("]");
		return sb.toString();
	}

	public void clone(final OraCdcLogMinerStatement other) {
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
	public void writeMarshallable(WireOut wire) {
		wire.bytes()
			.writeLong(tableId)
			.writeShort(operation)
			.writeLong(ts)
			.writeLong(scn)
			.write(rba.toByteArray())
			.writeLong(ssn)
			.write8bit(rowId)
			.writeByte(lobCount)
			.writeBoolean(rollback)
			.writeInt(redoData.length)
			.write(redoData);
	}


	@Override
	public void readMarshallable(WireIn wire) throws IORuntimeException {
		Bytes<?> raw = wire.bytes();
		tableId = raw.readLong();
		operation = raw.readShort();
		ts = raw.readLong();
		scn = raw.readLong();
		final byte[] ba = new byte[RedoByteAddress.BYTES];
		if (raw.read(ba) != RedoByteAddress.BYTES) {
			throw new IORuntimeException("Unable to read RBA as byte array!");
		}
		rba = RedoByteAddress.fromByteArray(ba);
		ssn = raw.readLong();
		rowId = raw.read8bit();
		lobCount = raw.readByte();
		rollback = raw.readBoolean();
		final int size = raw.readInt();
		redoData = new byte[size];
		if (raw.read(redoData) != size) {
			throw new IORuntimeException("Unable to read redo content!");
		}
	}

	@Override
	public boolean usesSelfDescribingMessage() {
		// TODO Auto-generated method stub
		return ReadMarshallable.super.usesSelfDescribingMessage();
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
			.append(rowId)
			.append(delimiter)
			.append(operation)
			.append(delimiter)
			.append(rollback ? "1" : "0")
			.append("\n");
		return sb;
	}

}
