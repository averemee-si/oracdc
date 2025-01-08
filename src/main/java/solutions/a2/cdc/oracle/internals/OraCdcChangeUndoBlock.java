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

import solutions.a2.oracle.internals.Xid;

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

	public static int KDO_POS = 0x3;
	static final int SUPPL_LOG_MIN_LENGTH = 0x14;
	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcChangeUndoBlock.class);
	private static final int KTUDB_MIN_LENGTH = 0x14;
	private static final int SUPPL_LOG_ROW_MIN_LENGTH = 0x1A;

	boolean supplementalLogData = false;
	byte supplementalFb = 0;
	short supplementalSlot = -1;
	int supplementalBdba;
	int supplementalCc = 0;
	int supplementalCcNn = 0;
	int suppDataStartIndex;
	int suppOffsetUndo = 0;
	int suppOffsetRedo = 0;
	private boolean ktub = false;
	private boolean ktbRedo = false;
	private boolean kdoOpCode = false;

	OraCdcChangeUndoBlock(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, _5_1_RDB, record, offset, headerLength);
		// Element 1 - ktudb
		if (coords.length < 1 || coords[0][1] < KTUDB_MIN_LENGTH) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to parse mandatory ktudb element (OP:5.1) for change #{} at RBA {} in '{}'.\n" +
					"Change contents:\n{}\n" +
					"=====================\n",
					num, rba, redoLog.fileName(), binaryDump());
			throw new IllegalArgumentException();
		}
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
				case _26_1_UINDO:
					// Element 3: KTB Redo
					ktbRedo(2);
					ktbRedo = true;
					//TODO  - LOB related
					break;
				case _14_8_OPUTRN:
					if (LOGGER.isDebugEnabled()) {
						//TODO - truncate
						LOGGER.debug("TODO skipping opc {} (OP:5.1) for change #{} at RBA {} in '{}'.",
								formatOpCode(opc), num, rba, redoLog.fileName());
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
					final int selector = (op & 0x1F) | (KCOCODRW << 0x08);
					if ((selector == _11_3_DRP ||
							selector == _11_2_IRP ||
							selector == _11_6_ORP ||
							selector == _11_16_LMN ||
							selector == _11_4_LKR) &&
							coords.length > (5 + columnCount)) {
						supplementalLogData = true;
						suppDataStartIndex = 0x4 + columnCount; 
					} else if ((selector == _11_5_URP) &&
							columnCountNn == columnCount &&
							coords.length > (5 + columnCount)) {
						supplementalLogData = true;
						suppDataStartIndex = 0x5 + columnCount; 
					} else if ((selector == _11_5_URP) &&
							columnCountNn < columnCount &&
							coords.length > (5 + columnCountNn)) {
						supplementalLogData = true;
						suppDataStartIndex = 0x5 + columnCountNn; 
					}

					if (supplementalLogData) {
						if (coords[suppDataStartIndex][1] < SUPPL_LOG_MIN_LENGTH) {
							LOGGER.error(
									"\n=====================\n" +
									"Unable to parse mandatory supplemental logging data (OP:5.1) for change #{} at RBA {} in '{}'.\n" +
									"Change contents:\n{}\n" +
									"=====================\n",
									num, rba, redoLog.fileName(), binaryDump());
							throw new IllegalArgumentException();
						}
						supplementalFb = record[coords[suppDataStartIndex][0] + 0x1];
						suppOffsetUndo = Short.toUnsignedInt(redoLog.bu().getU16(record, coords[suppDataStartIndex][0] + 0x6));
						suppOffsetRedo = Short.toUnsignedInt(redoLog.bu().getU16(record, coords[suppDataStartIndex][0] + 0x8));
						supplementalCcNn = Short.toUnsignedInt(redoLog.bu().getU16(record, coords[suppDataStartIndex][0] + 0x2));
						if (coords.length > (suppDataStartIndex + 1)) {
							supplementalCc = coords[suppDataStartIndex + 1][1] / Short.BYTES;
						} else {
							supplementalCc = supplementalCcNn;
						}
						if (SUPPL_LOG_ROW_MIN_LENGTH <= coords[suppDataStartIndex][1]) {
							supplementalBdba = redoLog.bu().getU32(record, coords[suppDataStartIndex][0] + 0x14);
							supplementalSlot = redoLog.bu().getU16(record, coords[suppDataStartIndex][0] + 0x18);
						}
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
			kdo(sb, 3);
		}
		if (supplementalLogData) {
			sb
				.append("\nSupplemental log data:")
				.append("cc: ")
				.append(supplementalCc)
				.append(" pos: ")
				.append(suppDataStartIndex)
				.append(" undo offset: ")
				.append(suppOffsetUndo)
				.append(" redo offset: ")
				.append(suppOffsetRedo)
				.append(" fb: ")
				.append(printFbFlags(supplementalFb));
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
		return sb;
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

	public byte supplementalFb() {
		return supplementalFb;
	}

	public int supplementalCcNn() {
		return supplementalCcNn;
	}

	public int suppDataStartIndex() {
		return suppDataStartIndex;
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

}
