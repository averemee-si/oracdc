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

package solutions.a2.cdc.oracle.internals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.oracle.internals.LobId;
import solutions.a2.oracle.internals.Xid;

/**
 * 
 * 11.17 LogMiner support for LOB operations
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Internals/Redo/Redo11.php">Redo Level 11 - Table Operations (DML)</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcChangeLlb extends OraCdcChange {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcChangeLlb.class);

	public static final byte TYPE_1 = 0x1;
	public static final byte TYPE_3 = 0x3;
	public static final byte TYPE_4 = 0x4;

	public static final byte OP_UNKNOWN = 0x00;
	public static final byte OP_PREPARE = 0x01;
	public static final byte OP_BEGIN_WRITE = 0x02;
	public static final byte OP_LOB_TRIM = 0x03;
	public static final byte OP_LOB_ERASE = 0x04;
	public static final byte OP_END = 0x05;

	private final byte type;
	private byte lobOp = OP_UNKNOWN;
	private int fsiz;
	private int csiz;
	private int lobColumnCount = -1;

	OraCdcChangeLlb(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, operation, record, offset, headerLength);
		if (coords.length < 3) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to parse mandatory elements for 11.17 (LLB) #{} at RBA {} in '{}'.\n" +
					"Change contents:\n{}\n" +
					"=====================\n",
					num, rba, redoLog.fileName(), binaryDump());
			throw new IllegalArgumentException();
		}
		type = record[coords[1][0]];
		switch (type) {
		case TYPE_1:
			//TODO 0x28 or 0x50 ?
			elementLengthCheck("11.17 (LLB)", "Type 1", 2, 0x28, "");
			switch (redoLog.bu().getU16(record, coords[2][0])) {
			case (short) 0x01:
			case (short) 0x02:
				lobOp = OP_BEGIN_WRITE;
				break;
			case (short) 0x66:
			case (short) 0x67:
				lobOp = OP_LOB_TRIM;
				break;
			case (short) 0x68:
				lobOp = OP_LOB_ERASE;
				break;
			default:
				LOGGER.warn(
						"Unknown LOB operation code '{} {}' at RBA {} for change 11.17\nChange vector binary dump{}",
						String.format("%02x", record[coords[2][0]]),
						String.format("%02x", record[coords[2][0] + 1]),
						rba, binaryDump());
			}
			xid = new Xid(
					redoLog.bu().getU16(record, coords[2][0] + 0x04),
					redoLog.bu().getU16(record, coords[2][0] + 0x06),
					redoLog.bu().getU32(record, coords[2][0] + 0x08));
			lid = new LobId(record, coords[2][0] + 0xC);
			lColId = redoLog.bu().getU16(record, coords[2][0] + 0x16);
			fsiz = redoLog.bu().getU32(record, coords[2][0] + 0x20);
			if (lobOp == OP_LOB_ERASE)
				csiz = redoLog.bu().getU32(record, coords[2][0] + 0x18);
			obj = redoLog.bu().getU32(record, coords[2][0] + 0x24);
			break;
		case TYPE_3:
			elementLengthCheck("11.17 (LLB)", "Type 3", 2, 0x0C, "");
			lobOp = OP_END;
			xid = new Xid(
					redoLog.bu().getU16(record, coords[2][0]),
					redoLog.bu().getU16(record, coords[2][0] + 0x02),
					redoLog.bu().getU32(record, coords[2][0] + 0x04));
			obj = redoLog.bu().getU32(record, coords[2][0] + 0x08);
			if (coords[2][1] >= 0x0C)
				fsiz = redoLog.bu().getU32(record, coords[2][0] + 0x0C);
			if (coords[2][1] >= 0x24)
				lColId = redoLog.bu().getU16(record, coords[2][0] + 0x22);
			break;
		case TYPE_4:
			// Base table supplemental data
			if (coords.length < 4) {
				LOGGER.error(
						"\n=====================\n" +
						"Unable to parse mandatory elements for 11.17 (LLB) Type 4 #{} at RBA {} in '{}'.\n" +
						"Change contents:\n{}\n" +
						"=====================\n",
						num, rba, redoLog.fileName(), binaryDump());
				throw new IllegalArgumentException();
			}
			//TODO 0x10 or 0x28?
			elementLengthCheck("11.17 (LLB)", "Type 4", 2, 0x10, "");
			lobOp = OP_PREPARE;
			obj = redoLog.bu().getU32(record, coords[2][0]);
			xid = new Xid(
					redoLog.bu().getU16(record, coords[2][0] + 0x08),
					redoLog.bu().getU16(record, coords[2][0] + 0x0A),
					redoLog.bu().getU32(record, coords[2][0] + 0x0C));
			lobColumnCount = coords[3][1] / Short.BYTES;
			break;
		default:
		}
	}

	public short[] lobColumnIds() {
		if (type == TYPE_4) {
			final short[] ids = new short[lobColumnCount];
			for (int i = 0; i < lobColumnCount; i++) {
				ids[i] = redoLog.bu().getU16(record, coords[3][0] + i * Short.BYTES);
			}
			return ids;
		} else {
			return null;
		}
	}

	public byte type() {
		return type;
	}

	public byte lobOp() {
		return lobOp;
	}

	public int fsiz() {
		return fsiz;
	}

	public int csiz() {
		return csiz;
	}

	@Override
	StringBuilder toDumpFormat() {
		final StringBuilder sb = super.toDumpFormat();
		sb
			.append("\n  typ:")
			.append(Byte.toUnsignedInt(type))
			.append(" xid:")
			.append(xid)
			.append(" obj:")
			.append(obj);
		if (type == TYPE_1) {
			if (lobOp == OP_BEGIN_WRITE)
				sb
					.append(" prepare write to lid:")
					.append(lid.toString())
					.append(" fsiz:")
					.append(Integer.toUnsignedLong(fsiz));
			else if (lobOp == OP_LOB_TRIM)
				sb
					.append("\n  DBMS_LOB.TRIM(lob_loc => '")
					.append(lid.toString())
					.append("', newlen => ")
					.append(Integer.toUnsignedLong(fsiz))
					.append(")");
			else if (lobOp == OP_LOB_ERASE)
				sb
					.append("\n  DBMS_LOB.ERASE(lob_loc => '")
					.append(lid.toString())
					.append("', amount => ")
					.append(Integer.toUnsignedLong(fsiz))
					.append(", offset => ")
					.append(Integer.toUnsignedLong(csiz))
					.append(")");
			else
				sb
					.append(" lid:")
					.append(lid.toString())
					.append(" fsiz:")
					.append(Integer.toUnsignedLong(fsiz));
		}
		if (type == TYPE_3)
			sb
				.append(" fsiz:")
				.append(Integer.toUnsignedLong(fsiz));
		if (type == TYPE_4) {
			sb
				.append(" LOB_cc:")
				.append(lobColumnCount)
				.append(" LOB_col_ids: [");
			boolean first = true;
			short[] ids = lobColumnIds();
			for (int i = 0; i < lobColumnCount; i++) {
				if (first) {
					first = false;
				} else {
					sb.append(", ");
				}
				sb.append(Short.toUnsignedInt(ids[i]));
			}
			sb.append("]");
		}
		if (lColId > -1) {
			sb
				.append("\n  column_id: ")
				.append(Short.toUnsignedInt(lColId));
		}
		return sb;
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

}
