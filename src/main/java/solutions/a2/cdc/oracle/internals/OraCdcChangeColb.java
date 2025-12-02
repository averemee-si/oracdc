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

import solutions.a2.oracle.internals.LobId;
import solutions.a2.oracle.internals.UndoByteAddress;
import solutions.a2.oracle.internals.Xid;
import solutions.a2.oracle.utils.FormattingUtils;

/**
 * 
 * 19.1 Direct block logging
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Internals/Redo/Redo11.php">Redo Level 11 - Table Operations (DML)</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcChangeColb extends OraCdcChange {

	public static final int LONG_DUMP_SIZE = 0x24;

	private static final int BLOCK_DUMP_MIN_SIZE = 0x18;

	private boolean longDump;
	private int lobPageNo;
	private byte itc;
	private short headerSize;

	OraCdcChangeColb(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, operation, record, offset, headerLength);
		elementNumberCheck(1);
		if (encrypted && !redoLog.tsEncKeyAvailable())
			return;
		if (coords.length > 1 && (
				record[coords[1][0]] == 0x6 ||
				(encrypted && record[coords[1][0]] == (byte) 0x89))) {
			longDump = false;
			elementLengthCheck("19.1 (KCBLCOLB)", "", 0, BLOCK_DUMP_MIN_SIZE, "");
			obj = redoLog.bu().getU32(record, coords[0][0] + 0x4);
			itc = record[coords[0][0] + 0x10];
			bdba = redoLog.bu().getU32(record, coords[0][0] + 0x14);
			lobDataOffset = (BLOCK_DUMP_MIN_SIZE + Byte.toUnsignedInt(itc) * 0x1A + 7) & 0xFFFFFFF8;
			elementLengthCheck("19.1 (KCBLCOLB)", "", 0, lobDataOffset + 8, "");
			headerSize = redoLog.bu().getU16(record, coords[0][0] + lobDataOffset + 0x6);
			elementLengthCheck("19.1 (KCBLCOLB)", "", 0, lobDataOffset + headerSize, "");
			qmRowCount = redoLog.bu().getU16(record, coords[0][0] + lobDataOffset + 0x2);
		} else {
			longDump = true;
			elementLengthCheck("19.1 (KCBLCOLB)", "", 0, LONG_DUMP_SIZE, "");
			lid = new LobId(record, coords[0][0] + 0x4);
			dataObj = redoLog.bu().getU32(record, coords[0][0]);
			lobDataOffset = LONG_DUMP_SIZE; 
			lobPageNo = redoLog.bu().getU32(record, coords[0][0] + 0x18);
		}
	}

	public int colbSize() {
		return coords[0][1] - LONG_DUMP_SIZE;
	}

	public boolean longDump() {
		return longDump;
	}

	@Override
	StringBuilder toDumpFormat() {
		final StringBuilder sb = super.toDumpFormat();
		sb.append("\nDirect Loader block redo entry");
		if (encrypted && !redoLog.tsEncKeyAvailable())
			return sb;
		if (longDump) {
			sb
				.append("\nLong field block dump:\nObject Id  ")
				.append(Integer.toUnsignedLong(dataObj))
				.append("\nLobId: ")
				.append(lid.toStringSignificant())
				.append(" PageNo ")
				.append(String.format("%8d", lobPageNo))
				.append("\nVersion: ")
				.append(String.format("0x%04x", Short.toUnsignedInt(redoLog.bu().getU16(record, coords[0][0] + 0x14))))
				.append('.')
				.append(String.format("%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[0][0] + 0x10))))
				.append("  pdba: ")
				.append(String.format("%8d", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[0][0] + 0x1C))))
				.append('\n');
			for (int i = LONG_DUMP_SIZE; i < coords[0][1]; i++) {
				sb
					.append(String.format("%02x", Byte.toUnsignedInt(record[coords[0][0] + i])))
					.append(' ');
				if (((i - LONG_DUMP_SIZE) % 0x18) == 0x17 && i != coords[0][1] - 1)
					sb.append("\n    ");
			}
			sb.append('\n');
		} else {
			sb
				.append("\nBlock header dump:  ")	//TODO
				.append("\n Object id on Block? ")
				.append(obj > 0 ? 'Y' : 'N')
				.append("\n seg/obj: 0x")
				.append(String.format("%x", Integer.toUnsignedLong(obj)))
				.append("  csc:  0x");
			FormattingUtils.leftPad(sb, redoLog.bu().getScn(record, coords[0][0] + 0x8), 0x10);
			sb
				.append("  itc: ")
				.append(Byte.toUnsignedInt(itc))
				//TODO flg: E
				.append("  typ: ")
				.append(record[coords[0][0]])
				.append(" - ")
				.append(record[coords[0][0]] == 1 ? "DATA" : record[coords[0][0]] == 2 ? "INDEX" : "UNKNOWN")
				.append("\n     brn: 0  bdba: 0x")	//TODO brn
				//TODO ver: 0x01 opc: 0
				//TODO inc: 0  exflg: 0
				.append(String.format("%08x", Integer.toUnsignedLong(bdba)))
				.append("\n\n Itl           Xid                  Uba         Flag  Lck        Scn/Fsc");
			for (int i = 0; i < Byte.toUnsignedInt(itc); i++) {
				final int startPos = coords[0][0] + BLOCK_DUMP_MIN_SIZE + i * 0x1A;
				short lock = redoLog.bu().getU16(record, startPos + 0x10);
				final StringBuilder flags = new StringBuilder();
				final boolean scn = (lock & 0x8000) != 0;
				flags
					.append((lock & 0x8000) == 0 ? '-' : 'C')
					.append((lock & 0x4000) == 0 ? '-' : 'B')
					.append((lock & 0x2000) == 0 ? '-' : 'U')
					.append((lock & 0x1000) == 0 ? '-' : 'T');
				lock &= 0x0FFF;
				sb
					.append("\n0x")
					.append(String.format("%02x", i + 1))
					.append("   ")
					.append(new Xid(
							redoLog.bu().getU16(record, startPos),
							redoLog.bu().getU16(record, startPos + 2),
							redoLog.bu().getU32(record, startPos + 4)))
					.append("  ")
					.append(new UndoByteAddress(redoLog.bu().getU56(record, startPos + 8)))
					.append("  ")
					.append(flags)
					.append("  ")
					.append(String.format("%3d", scn ? 0 : lock))
					.append("  ")
					.append(scn ? "scn" : "fsc")
					.append(" 0x");
				if (scn) {
					sb.append(FormattingUtils.leftPad(redoLog.bu().getScn(record, startPos + 0x12), 0x10));
				} else {
					sb
						.append(String.format("%04x", Short.toUnsignedInt(redoLog.bu().getU16(record, startPos + 0x12))))
						.append('.')
						.append(String.format("%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, startPos + 0x14))));
				}
			}
			if (record[coords[0][0]] == 1) {
				final int startPos = coords[0][0] + lobDataOffset;
				//TODO bdba: 0x
				//TODO data_block_dump,data header at 0x
				sb
					.append("\n===============")
					.append("\ntsiz: 0x")
					.append(String.format("%04x", coords[0][1] - lobDataOffset))
					.append("\nhsiz: 0x")
					.append(String.format("%02x", Short.toUnsignedInt(headerSize)))
					//TODO pbl:
					//TODO flag
					.append("\nntab=")
					.append(Byte.toUnsignedInt(record[startPos + 0x1]))
					.append("\nnrow=")
					.append(Short.toUnsignedInt(qmRowCount))
					.append("\nfrre=")
					.append(redoLog.bu().getU16(record, startPos + 0x4))
					.append("\nfsbo=0x")
					.append(String.format("%02x", Short.toUnsignedInt(headerSize)))
					.append("\nfseo=0x")
					.append(String.format("%04x", Short.toUnsignedInt(redoLog.bu().getU16(record, startPos + 0x8))))
					.append("\navsp=0x")
					.append(String.format("%04x", Short.toUnsignedInt(redoLog.bu().getU16(record, startPos + 0xA))))
					.append("\ntosp=0x")
					.append(String.format("%04x", Short.toUnsignedInt(redoLog.bu().getU16(record, startPos + 0xC))));
				//TODO 0xe:pti[0]	nrow=XX	offs=0
				for (int row = 0; row < Short.toUnsignedInt(qmRowCount); row++) {
					int off = 0x12 + row * Short.BYTES;
					sb
						.append("\n0x")
						.append(String.format("%02x", off))
						.append(":pri[")
						.append(row)
						.append("]\toffs=0x")
						.append(String.format("%04x", Short.toUnsignedInt(redoLog.bu().getU16(record, startPos + off))));
				}
				sb.append("\nblock_row_dump:");
				for (int row = 0; row < Short.toUnsignedInt(qmRowCount); row++) {
					int off = Short.toUnsignedInt(redoLog.bu().getU16(record, startPos + 0x12 + row * Short.BYTES));
					fb = record[startPos + off];
					columnCount = Byte.toUnsignedInt(record[startPos + off + 2]);
					sb
						.append("\ntab 0, row ")
						.append(row)
						.append(", @0x")
						.append(String.format("%04x", off))
						.append("\ntl: ")
						.append(coords[0][1] - off - lobDataOffset)
						.append(" fb: ")
						.append(printFbFlags(fb))
						.append(" lb: 0x")
						.append(String.format("%1x", Byte.toUnsignedInt(record[startPos + off + 1])))
						.append("  cc: ")
						.append(columnCount);
					int rowDiff = lobDataOffset + off + 3;
					for (int col = 0; col < columnCount; col++) {
						rowDiff = printColumnBytes(sb, col, 0, rowDiff);
					}
				}
			}
			sb.append("\nend_of_block_dump");
		}
		return sb;
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

}
