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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.oracle.internals.RowId;
import solutions.a2.oracle.internals.Xid;

import static solutions.a2.oracle.utils.BinaryUtils.putOraColSize;
import static solutions.a2.oracle.utils.BinaryUtils.putU16;

/**
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Presentations/RedoInternals.ppt">Redo Internals</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcChangeUndoBlock extends OraCdcChangeUndo {

	public static final int KDO_POS = 0x3;
	public static final int NON_KEY_10_30_POS = 5;
	public static final int KEY_10_30_POS = 4;
	public static final int COL_NUM_10_35_POS = 5;

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcChangeUndoBlock.class);
	private static final int SUPPL_LOG_MIN_LENGTH = 0x14;
	private static final int KTUDB_MIN_LENGTH = 0x14;
	private static final int KDILK_MIN_LENGTH = 0x14;

	private static final byte KDLIK = 1;
	private static final byte KDLIK_KEY = 2;
	private static final byte KDLIK_NONKEY = 4;
	private static final byte KDICLPU = 0x3;
	private static final byte KDICLRE = 0x5;
	private static final byte KDICLUP = 0x12;

	public static final byte KDICLNU = 0x1E;
	public static final byte KDILCNU = 0x23;

	private boolean supplementalLogData = false;
	private byte supplementalFb = 0;
	private int supplementalCc = 0;
	private int supplementalCcNn = 0;
	private int suppDataStartIndex = -1;
	private int suppOffsetUndo = 0;
	private int suppOffsetRedo = 0;
	private boolean ktub = false;
	private boolean ktbRedo = false;
	private boolean kdoOpCode = false;
	private boolean kdliCommon = false;
	private byte kdilk = 0;
	private byte kdilkType;

	OraCdcChangeUndoBlock(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, _5_1_RDB, record, offset, headerLength);
		if (encrypted && !redoLog.tsEncKeyAvailable())
			return;
		// Element 1 - ktudb
		elementNumberCheck(1);
		elementLengthCheck("ktudb", "(OP:5.1)", 0, KTUDB_MIN_LENGTH, "");
		xid = new Xid(
				redoLog.bu().getU16(record, coords[0][0] + 0x08),
				redoLog.bu().getU16(record, coords[0][0] + 0x0A),
				redoLog.bu().getU32(record, coords[0][0] + 0x0C));

		// Element 2 - ktub
		if (coords.length > 1) {
			ktub = true;
			ktub(1, false);
		} else {
			LOGGER.warn("ktubl is missed (OP:5.1) for change #{} at RBA {}", num, rba);
		}
		// element 3 - defined by opc
		if (coords.length > 2) {
			if ((flg & (FLG_MULTI_BLOCK_UNDO_HEADER | FLG_MULTI_BLOCK_UNDO_MIDDLE | FLG_MULTI_BLOCK_UNDO_FOOTER)) != 0) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Multi block undo {} (OP:5.1) for change #{} at RBA {} in '{}'.",
							(flg & (FLG_MULTI_BLOCK_UNDO_HEADER)) != 0 ?
									"header" : ((flg & (FLG_MULTI_BLOCK_UNDO_MIDDLE)) != 0 ?
									"middle" : "footer"), num, rba, redoLog.fileName());
				}
			} else {
				switch (opc) {
				case _10_22_ULK:
					// Element 3: KTB Redo
					ktbRedo(2);
					ktbRedo = true;
					// Element 4: kdilk
					elementLengthCheck("kdilk", "(OP:5.1)", 0, KDILK_MIN_LENGTH, "");
					kdilk |= KDLIK;
					kdilkType = record[coords[3][0]];
					if (kdilkType == KDILCNU && coords.length > 5) {
						columnCount = coords[5][1]/Short.BYTES;
					}
					if ((kdilkType == KDICLPU ||
						kdilkType == KDICLRE ||
						kdilkType == KDICLUP ||
						kdilkType == KDICLNU ||
						kdilkType == KDILCNU) &&
							(record[coords[3][0] + 2] & 0x80) > 0) {
						supplementalLogData = true;
						if (kdilkType == KDILCNU) {
							suppDataStartIndex = 6 + columnCount;
							if (coords.length > suppDataStartIndex + 1) {
								supplementalCc = coords[suppDataStartIndex + 1][1]/Short.BYTES;
							}
						} else {
							suppDataStartIndex = coords.length - 1;
						}
						elementLengthCheck("mandatory supplemental logging data", "(OP:5.1)", suppDataStartIndex, SUPPL_LOG_MIN_LENGTH, "");
						supplementalFb = record[coords[suppDataStartIndex][0] + 0x1];
						suppOffsetUndo = Short.toUnsignedInt(redoLog.bu().getU16(record, coords[suppDataStartIndex][0] + 0x6));
						suppOffsetRedo = Short.toUnsignedInt(redoLog.bu().getU16(record, coords[suppDataStartIndex][0] + 0x8));
					}
					if (coords.length > 3 && kdilkType != KDILCNU) {
						kdilk |= KDLIK_KEY;
						if (coords.length > 4 && suppDataStartIndex > 5) {
							kdilk |= KDLIK_NONKEY;
							if (kdilkType != KDILCNU) {
								columnCount = Byte.toUnsignedInt(record[coords[5][0] + 2]);
							}
							fb = record[coords[5][0]];
						}
					}
					break;
				case _11_1_IUR:
					// Element 3: KTB Redo
					// Element 4:KDO
					// Element 5+: Column data
					ktbRedo(2);
					ktbRedo = true;
					kdo(KDO_POS);
					kdoOpCode = true;
					final var selector = (op & 0x1F) | (KCOCODRW << 0x08);
					if (selector == _11_5_URP) {
						if (columnCountNn == columnCount &&
							coords.length > (5 + columnCount)) {
							supplementalLogData = true;
							suppDataStartIndex = 0x5 + columnCount; 
						} else if (columnCountNn < columnCount &&
								coords.length > (5 + columnCountNn)) {
							supplementalLogData = true;
							suppDataStartIndex = 0x5 + columnCountNn; 
						}
					} else if ((selector == _11_2_IRP ||
							selector == _11_3_DRP ||
							selector == _11_4_LKR ||
							selector == _11_6_ORP ||
							selector == _11_8_CFA ||
							selector == _11_16_LMN) && coords.length > (4 + columnCount)) {
						supplementalLogData = true;
						suppDataStartIndex = 0x4 + columnCount;
					}

					if (supplementalLogData) {
						elementLengthCheck("mandatory supplemental logging data", "(OP:5.1)", suppDataStartIndex, SUPPL_LOG_MIN_LENGTH, "");
						supplementalFb = record[coords[suppDataStartIndex][0] + 0x1];
						suppOffsetUndo = Short.toUnsignedInt(redoLog.bu().getU16(record, coords[suppDataStartIndex][0] + 0x6));
						suppOffsetRedo = Short.toUnsignedInt(redoLog.bu().getU16(record, coords[suppDataStartIndex][0] + 0x8));
						supplementalCcNn = Short.toUnsignedInt(redoLog.bu().getU16(record, coords[suppDataStartIndex][0] + 0x2));
						if (coords.length > (suppDataStartIndex + 1)) {
							supplementalCc = coords[suppDataStartIndex + 1][1] / Short.BYTES;
						} else {
							supplementalCc = supplementalCcNn;
						}
					}
					break;
				case _26_1_UINDO:
					// Element 3: KTB Redo
					ktbRedo(2);
					ktbRedo = true;
					if (coords.length > 3) {
						kdliCommon = true;
						kdliCommon(3);
						for (int index = 0x4; index < coords.length; index++) {
							kdli(index);
						}
					}
					break;
				case _14_8_OPUTRN:
					if (LOGGER.isDebugEnabled()) {
						//TODO - truncate
						LOGGER.debug("TODO skipping opc {} (OP:5.1) for change #{} at RBA {} in '{}'.",
								formatOpCode(opc), num, rba, redoLog.fileName());
					}
					break;
				default:
					if (LOGGER.isTraceEnabled()) {
						LOGGER.trace("Skipping opc {} (OP:5.1) for change #{} at RBA {} in '{}'",
								formatOpCode(opc), num, rba, redoLog.fileName());
					}
				}
			}
		} else {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("3rd element is missed (OP:5.1) for change #{} at RBA {} in '{}'", num, rba, redoLog.fileName());
			}
		}
	}

	@Override
	StringBuilder toDumpFormat() {
		final StringBuilder sb = super.toDumpFormat();
		if (encrypted && !redoLog.tsEncKeyAvailable())
			return sb;
		sb
			.append("\nktudb redo: siz: ")
			.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[0][0])))
			.append(" spc: ")
			.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[0][0] + 0x02)))
			.append(" flg: ")
			.append(String.format("0x%04x", Short.toUnsignedInt(redoLog.bu().getU16(record, coords[0][0] + 0x04))))
			.append(" seq: ")
			.append(String.format("0x%04x", Short.toUnsignedInt(redoLog.bu().getU16(record, coords[0][0] + 0x10))))
			.append(" rec: ")
			.append(String.format("0x%02x", Byte.toUnsignedInt(record[coords[0][0] + 0x12])))
			.append("\n            xid:  ")
			.append(xid);
		if (ktub) {
			ktub(sb, 1, true);
		}
		if (ktbRedo) {
			if (opc == _11_1_IUR) {
				sb.append("\nKDO undo record:");
			} else if (opc == _10_22_ULK) {
				sb.append("\nindex undo for leaf key operations");
			}
			ktbRedo(sb, 2);
		}
		if (kdoOpCode) {
			kdo(sb, 3, false);
		}
		if ((kdilk & KDLIK) != 0) {
			sb
				.append("\nDump kdilk : itl=")
				.append(Byte.toUnsignedInt(record[coords[3][0] + 1]))
				.append(", kdxlkflg=")
				.append(String.format("0x%x", Byte.toUnsignedInt(record[coords[3][0] + 2])))
				.append(" sdc=")
				.append(Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[3][0] + 0xC)))
				.append(" indexid=")
				.append(String.format("0x%x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[3][0] + 4))))
				.append(" block=")
				.append(String.format("0x%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[3][0] + 8))));
			switch (kdilkType) {
			case KDICLPU:
				sb.append("\n(kdxlpu): purge leaf row");
				break;
			case KDICLRE:
				sb.append("\n(kdxlre): restore leaf row (clear leaf delete flags)");
				break;
			case KDICLUP:
				sb.append("\n(kdxlup): update keydata in row");
				break;
			case KDICLNU:
				sb.append("\n(kdxlnu): whole nonkey update");
				break;
			case KDILCNU: 
				sb.append("\n(kdxlcnu): column-vector nonkey update");
				break;
			}
			if (kdilkType == KDICLPU)
				sb.append("\n(kdxlpu): purge leaf row");
			else if (kdilkType == KDICLRE)
				sb.append("\n(kdxlre): restore leaf row (clear leaf delete flags)");
			else if (kdilkType == KDICLUP)
				sb.append("\n(kdxlup): update keydata in row");
			if (coords.length > 3) {
				if (kdilkType == KDILCNU) {
					if (coords[3][1] > 0x19) {
						sb
							.append("\nncol: ")
							.append(Byte.toUnsignedInt(record[coords[3][0] + 0x15]))
							.append(" nnew: ")
							.append(Byte.toUnsignedInt(record[coords[3][0] + 0x16]))
							.append(" size: ")
							.append(redoLog.bu().getU16(record, coords[3][0] + 0x18))
							.append(" flag: ")
							.append(String.format("0x%02x", Byte.toUnsignedInt(record[coords[3][0] + 0x14])));
					}
					printIndexKey(sb, true, 4);
					sb.append("\nnonkey columns updated:");
					for (int i = 0; i < columnCount; i++) {
						final int index = i + 6;
						printColumnBytes(sb, record[coords[5][0] + Short.BYTES * i], coords[index][1], index, 0);
					}
				} else {
					printIndexKey(sb, true, 4);
					if (coords.length > 4 && suppDataStartIndex > 5) {
						printIndexKey(sb, false, 5);
					}
				}
			}
		}
		if (supplementalLogData) {
			/*
			 kdogspare1 -> record[coords[suppDataStartIndex][0] + 0xC]
			 kdogspare2 -> redoLog.bu().getU16(record, coords[suppDataStartIndex][0] + 0x10)
			 Objv# -> coords[suppDataStartIndex][0] + 0x4)
			 */
			sb
				.append("\nLOGMINER DATA:")
				.append("\n Number of columns supplementally logged: ")
				.append(supplementalCc)
				.append("\nopcode: ")
				.append(printLmOpCode(record[coords[suppDataStartIndex][0]]))
				.append("\n segcol# in Undo starting from ")
				.append(suppOffsetUndo)
				.append("\n segcol# in Redo starting from ")
				.append(suppOffsetRedo)
				.append("\n pos: ")
				.append(suppDataStartIndex)
				.append(" fb: ")
				.append(printFbFlags(supplementalFb));
			if (supplementalCc > 0) {
				if (kdilkType == KDILCNU) {
					sb.append("\n Supplemental logging:");
					for (int i = 0; i < supplementalCc; i++) {
						final int colNum = redoLog.bu().getU16(record, coords[suppDataStartIndex + 1][0] + i * Short.BYTES);
						final int colSize = redoLog.bu().getU16(record, coords[suppDataStartIndex + 2][0] + i * Short.BYTES);
						sb
							.append("\n  col  ")
							.append(String.format("%2d", i))
							.append(":  segcol#: ")
							.append(String.format("%2d", colNum))
							.append('(')
							.append(String.format("%2d", colSize))
							.append("): ");
						if (colSize == 0) {
							sb.append("*NULL*");
						} else {
							for (int j = 0; j < colSize; j++) {
								sb
									.append(' ')
									.append(String.format("%02x", Byte.toUnsignedInt(record[coords[suppDataStartIndex + 3 + i][0] + j])))
									.append((j % 0x19 == 0x18) && (j != colSize - 1) ? "\n" : "" );
							}
						}
					}
				} else {
					final int colNumArrayPos = suppDataStartIndex + 1;
					final int dataEndPos = supplementalCc + suppDataStartIndex + 0x3;
					int colOrder = 0;
					for (int i = suppDataStartIndex + 0x3; i < dataEndPos; i++) {
						if (i < coords.length) {
							final int colNum = redoLog.bu().getU16(record, coords[colNumArrayPos][0] + colOrder * Short.BYTES);
							colOrder++;
							printColumnBytes(sb, colNum, coords[i][1], i, 0);
						} else {
							break;
						}
					}
				}
			}
		}
		if (kdliCommon) {
			kdliCommon(sb, 3);
			for (int index = 0x4; index < coords.length; index++) {
				kdli(sb, index);
			}
		}
		return sb;
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

	public byte supplementalFb() {
		return supplementalFb;
	}

	public int supplementalCc() {
		return supplementalCc;
	}

	public int suppOffsetUndo() {
		return suppOffsetUndo;
	}

	public int suppOffsetRedo() {
		return suppOffsetRedo;
	}

	private void printIndexKey(final StringBuilder sb, final boolean keyOrNot, final int index) {
		sb
			.append('\n')
			.append(keyOrNot ? "key" : "nonkey")
			.append(" :(")
			.append(Integer.toUnsignedLong(coords[index][1]))
			.append("):")
			.append(coords[index][1] > 0x14 ? "\n" : " ");
		for (int i = 0; i < coords[index][1]; i++) {
			if (i % 25 == 24 && i != coords[index][1] - 1)
				sb.append('\n');
			sb.append(String.format(" %02x", Byte.toUnsignedInt(record[coords[index][0] + i])));
		}
	}

	private static String printLmOpCode(final byte opCode) {
		switch (opCode) {
			case SUPPL_LOG_UPDATE: return "UPDATE";
			case SUPPL_LOG_INSERT: return "INSERT";
			case SUPPL_LOG_DELETE: return "DELETE";
			default:               return "??????";
		}
	}

	public byte supplementalDataFor() {
		return record[coords[suppDataStartIndex][0]];
	}

	@Override
	public int columnCount() {
		if ((kdilk & KDLIK) != 0)
			if ((kdilk & KDLIK_NONKEY) != 0)
				return columnCount + columnCountNn();
			else
				return columnCountNn();
		else
			return columnCount;
	}

	@Override
	public int columnCountNn() {
		if ((kdilk & KDLIK) != 0)
			return indexKeyColCount(4);
		else
			return columnCountNn;
	}

	public int writeIndexColumns(final ByteArrayOutputStream baos, final int colNumIndex) throws IOException {
		return writeIndexColumns(baos, 4, (kdilk & KDLIK_NONKEY) != 0, colNumIndex);
	}

	public void writeSupplementalCols(final ByteArrayOutputStream baos) throws IOException {
		if (supplementalCc == 0)
			return;
		final int colNumIndex = suppDataStartIndex + 1;
		final int dataEndPos = supplementalCc + suppDataStartIndex + 0x3;
		int colIndex = 0;
		for (int i = suppDataStartIndex + 0x3; i < dataEndPos; i++) {
			if (i < coords.length) {
				final int colNum = redoLog.bu().getU16(record, coords[colNumIndex][0] + colIndex * Short.BYTES);
				putU16(baos, colNum);
				if (kdilkType == KDILCNU) {
					final int colSize = redoLog.bu().getU16(record, coords[suppDataStartIndex + 2][0] + colIndex * Short.BYTES);
					if (colSize == 0) {
						baos.write(0xFF);
					} else {
						putOraColSize(baos, colSize);
						baos.write(record, coords[i][0], colSize);
					}
				} else {
					if (colIndex < supplementalCcNn) {
						final int colSize = coords[i][1];
						if (colSize == 0) {
							baos.write(0xFF);
						} else {
							putOraColSize(baos, colSize);
							baos.write(record, coords[i][0], colSize);
						}
					} else {
						baos.write(0xFF);
					}
				}
				colIndex++;
			} else {
				LOGGER.warn("Incorrect index {} when processing redo record at rba {}", i, rba);
				break;
			}
		}
	}

	public byte kdilkType() {
		return kdilkType;
	}

	public boolean supplementalLogData() {
		return supplementalLogData;
	}

	public RowId supplementalRowId() {
		if (supplementalLogData && SUPPL_LOG_ROW_MIN_LENGTH <= coords[suppDataStartIndex][1]) {
			return new RowId(
					dataObj,
					redoLog.bu().getU32(record, coords[suppDataStartIndex][0] + 0x14),
					redoLog.bu().getU16(record, coords[suppDataStartIndex][0] + 0x18));
		} else
			return new RowId(dataObj, bdba, slot);
	}

}
