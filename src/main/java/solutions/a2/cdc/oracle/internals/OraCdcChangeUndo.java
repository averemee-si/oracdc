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

import solutions.a2.oracle.internals.RowId;
import solutions.a2.oracle.internals.UndoByteAddress;
import solutions.a2.oracle.internals.Xid;
import solutions.a2.oracle.utils.FormattingUtils;

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

public class OraCdcChangeUndo extends OraCdcChange {

	static final short FLG_MULTI_BLOCK_UNDO_HEADER = 0x0001;
	static final short FLG_MULTI_BLOCK_UNDO_FOOTER = 0x0002;
	static final short FLG_MULTI_BLOCK_UNDO_MIDDLE = 0x0100;
	static final short FLG_BEGIN_TRANSACTION = 0x0008;
	static final short FLG_USER_UNDO_DONE = 0x0010;
	static final short FLG_LAST_BUFFER_SPLIT = 0x0004;
	static final short FLG_TEMP_OBJECT = 0x0020;
	static final short FLG_TBS_UNDO = 0x0080;
	static final short FLG_USER_ONLY = 0x0040;

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcChangeUndo.class);
	private static final int KTUVXOFF_MIN_LENGTH = 0x08;
	private static final int KTUDH_MIN_LENGTH = 0x20;
	private static final int KTUB_MIN_LENGTH = 0x18;

	OraCdcChangeUndo(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, operation, record, offset, headerLength);
		if (encrypted && !redoLog.tsEncKeyAvailable())
			return;
		switch (operation) {
		case _5_2_RDH:
			if (coords.length < 1 || coords[0][1] < KTUDH_MIN_LENGTH) {
				LOGGER.error(
						"\n=====================\n" +
						"Unable to parse mandatory ktudh element (OP:5.2) for change #{} at RBA {} in '{}'.\n" +
						"Change contents:\n{}\n" +
						"=====================\n",
						num, rba, redoLog.fileName(), binaryDump());
				throw new IllegalArgumentException();
			}
			xid(redoLog.bu().getU16(record, coords[0][0] + 0x00), 
					redoLog.bu().getU32(record, coords[0][0] + 0x04));
			flg = record[coords[0][0] + 0x10];
			break;
		case _5_6_IRB:
		case _5_11_BRB:
			// Only ktub element is needed
			if (coords.length < 1) {
				LOGGER.error(
						"\n=====================\n" +
						"Unable to parse mandatory ktub element (OP:5.6/5.11) for change #{} at RBA {}\n" +
						"Change contents:\n{}\n" +
						"=====================\n",
						num, rba, binaryDump());
			}
			ktub(0, true);
			break;
		}
	}

	@Override
	StringBuilder toDumpFormat() {
		final StringBuilder sb = super.toDumpFormat();
		if (encrypted && !redoLog.tsEncKeyAvailable())
			return sb;
		switch (operation) {
		case _5_2_RDH:
			sb
				.append("\nktudh redo: slt: ")
				.append(String.format("0x%04x", Short.toUnsignedInt(redoLog.bu().getU16(record, coords[0][0]))))
				.append(" sqn: ")
				.append(String.format("0x%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[0][0] + 0x04))))
				.append(" flg: ")
				.append(String.format("0x%04x", Short.toUnsignedInt(flg)))
				.append(" siz: ")
				.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[0][0] + 0x12)))
				.append(" fbi: ")
				.append(Byte.toUnsignedInt(record[coords[0][0] + 0x14]))
				.append("\n            uba: ")
				.append((new UndoByteAddress(redoLog.bu().getU56(record, coords[0][0] + 0x08))).toString())
				.append("    pxid:  ")
				.append(new Xid(redoLog.bu().getU16(record, coords[0][0] + 0x18),
						redoLog.bu().getU16(record, coords[0][0] + 0x1A),
						redoLog.bu().getU32(record, coords[0][0] + 0x1C)).toString());
			if (redoLog.cdb()) {
				if (coords.length > 1 && coords[1][1] == 0x4) {
					sb
						.append("        pdbuid:");
				}
			}
			break;
		case _5_6_IRB:
			ktub(sb, 0, true);
			if (coords.length > 1 && coords[1][1] >= KTUVXOFF_MIN_LENGTH) {
				sb
					.append("\nktuxvoff: ")
					.append(String.format("0x%04x", Short.toUnsignedInt(redoLog.bu().getU16(record, coords[1][0]))))
					.append("  ktuxvflg: ")
					.append(String.format("0x%04x", Short.toUnsignedInt(redoLog.bu().getU16(record, coords[1][0] + 0x04))));
			}
			break;
		case _5_11_BRB:
			//TODO - need constants for RDBMS versions!
			if (redoLog.versionMajor() < 0x13) {
				ktub(sb, 0, false);
			} else {
				ktub(sb, 0, true);
			}
			break;
		}
		return sb;
	}

	public RowId rowId() {
		return new RowId(dataObj, bdba, slot);
	}

	public RowId rowId(OraCdcChange rowChange) {
		if (rowChange.operation == _11_5_URP &&
				rowChange.coords.length > (rowChange.columnCount + 2) &&
				rowChange.record[rowChange.coords[rowChange.columnCount + 3][0]] == OraCdcChangeUndoBlock.SUPPL_LOG_UPDATE &&
				OraCdcChangeUndoBlock.SUPPL_LOG_ROW_MIN_LENGTH <= rowChange.coords[rowChange.columnCount + 3][1]) {
			var suppDataStartIndex = rowChange.columnCount + 3;
			return new RowId(dataObj, 
					redoLog.bu().getU32(rowChange.record, rowChange.coords[suppDataStartIndex][0] + 0x14),
					redoLog.bu().getU16(rowChange.record, rowChange.coords[suppDataStartIndex][0] + 0x18));
		} else
			return new RowId(dataObj, rowChange.bdba, rowChange.slot);
	}

	void ktub(final int index, boolean partialRollback) {
		if (coords[index][1] < KTUB_MIN_LENGTH) {
			LOGGER.error("Unable to parse required ktub element (OP:{}) for change #{} at RBA {}",
					formatOpCode(operation), num, rba);
			throw new IllegalArgumentException();
		}
		obj = redoLog.bu().getU32(record, coords[index][0]);
		dataObj = redoLog.bu().getU32(record, coords[index][0] + 0x04);
		opc = (short) (((record[coords[index][0] + 0x10]) << 8) | record[coords[index][0] + 0x11]);
		slt = record[coords[index][0] + 0x12];
		flg = redoLog.bu().getU16(record, coords[index][0] + 0x14);
		if (partialRollback) {
			//TODO
//			xid(slt, 0x0000FFFF & redoLog.bu().getU16(record, coords[index][0] + 0x16));
			xid(slt, 0);
		}
		//TODO
	}

	void ktub(final StringBuilder sb, final int index, final boolean ktublFlag) {
		final boolean ktubl = ((flg & FLG_BEGIN_TRANSACTION) != 0) && ktublFlag;
		sb
			.append(ktubl ? "\nktubl" : "\nktubu")
			.append(" redo: slt: ")
			.append(Short.toUnsignedInt(slt));
		if (redoLog.versionMajor() >= 19) {
			sb
				.append(" wrp: ")
				.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x16)));
		}
		sb
			.append(" flg: ")
			.append(String.format("0x%04x", Short.toUnsignedInt(flg)));
		if (redoLog.versionMajor() >= 19) {
			sb
				.append(" prev dba: ")
				.append(String.format("0x%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[index][0] + 0x0C))));
		}
		sb
			.append(" rci: ")
			.append(Byte.toUnsignedInt(record[coords[index][0] + 0x13]))
			.append(" opc: ")
			.append(formatOpCode(opc))
			.append(" [objn: ")
			.append(obj)
			.append(" objd: ")
			.append(dataObj)
			.append(" tsn: ")
			.append(Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[index][0] + 0x08)))
			.append("]");
		// Format and abbreviation depends on RDBMS version!
		final String undoType;
		if ((flg & FLG_MULTI_BLOCK_UNDO_HEADER) != 0) {
			undoType = "MBU - HEAD  ";
		} else if ((flg & FLG_MULTI_BLOCK_UNDO_FOOTER) != 0) {
			undoType = "MBU - TAIL  ";
		} else if ((flg & FLG_MULTI_BLOCK_UNDO_MIDDLE) != 0) {
			undoType = "MBU - MID   ";
		} else {
			undoType = "Regular undo";
		}
		final String userUndoDone;
		if ((flg & FLG_USER_UNDO_DONE) != 0) {
			userUndoDone = "Yes";
		} else {
			userUndoDone = " No";
		}
		final String lastBufferSplit;
		if ((flg & FLG_LAST_BUFFER_SPLIT) != 0) {
			lastBufferSplit = "Yes";
		} else {
			lastBufferSplit = " No";
		}
		final String tempObject;
		if ((flg & FLG_TEMP_OBJECT) != 0) {
			tempObject = "Yes";
		} else {
			tempObject = " No";
		}
		final String tbsUndo;
		if ((flg & FLG_TBS_UNDO) != 0) {
			tbsUndo = "Yes";
		} else {
			tbsUndo = " No";
		}
		final String userOnly;
		if ((flg & FLG_USER_ONLY) != 0) {
			userOnly = "Yes";
		} else {
			userOnly = " No";
		}

		if (ktubl) {
			if (coords[index][1] == 0x1C) {
				ktubUndoFlags(sb, undoType, userUndoDone, lastBufferSplit, tempObject, tbsUndo, userOnly);
				sb.append("\nBegin trans");
				printBuExtIdxFlg2(sb, index);
			} else if (coords[index][1] >= 0x4C) {
				ktubUndoFlags(sb, undoType, userUndoDone, lastBufferSplit, tempObject, tbsUndo, userOnly);
				sb
					.append("\nBegin trans")
					.append("\n prev ctl uba: ")
					.append((new UndoByteAddress(redoLog.bu().getU56(record, coords[index][0] + 0x1C))).toString())
					.append(" prev ctl max cmt scn:  0x")
					.append(FormattingUtils.leftPad(redoLog.bu().getScn(record, coords[index][0] + 0x24), 0x10))
					.append("\n prev tx cmt scn:  0x")
					.append(FormattingUtils.leftPad(redoLog.bu().getScn(record, coords[index][0] + 0x2C), 0x10))
					.append("\n txn start scn:  0x")
					.append(FormattingUtils.leftPad(redoLog.bu().getScn(record, coords[index][0] + 0x38), 0x10))
					.append("  logon user: ")
					.append(Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[index][0] + 0x48)))
					.append("\n prev brb:  ")
					.append(String.format("0x%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[index][0] + 0x40))))
					.append("  prev bcl:  ")
					.append(String.format("0x%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[index][0] + 0x44))));
				printBuExtIdxFlg2(sb, index);
			} else {
				LOGGER.warn("Unable to parse ktubl element with length {} for change #{} at RBA {}",
						coords[index][1], num, rba);
			}
		} else {
			ktubUndoFlags(sb, undoType, userUndoDone, lastBufferSplit, tempObject, tbsUndo, userOnly);
		}
	}

	private void ktubUndoFlags(final StringBuilder sb,
			final String undoType, final String userUndoDone, final String lastBufferSplit,
			final String tempObject, final String tbsUndo, final String userOnly) {
		sb
			.append("\n[Undo type  ] ")
			.append(undoType)
			.append("  [User undo done   ] ")
			.append(userUndoDone)
			.append(" [Last buffer split] ")
			.append(lastBufferSplit)
			.append("\n")
			.append("[Temp object]          ")
			.append(tempObject)
			.append("  [Tablespace Undo  ] ")
			.append(tbsUndo)
			.append(" [User only        ] ")
			.append(userOnly);
	}

	private void printBuExtIdxFlg2(final StringBuilder sb, final int index) {
		sb
			.append("\nBuExt idx: ")
			.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x1A)))
			.append(" flg2: ")
			.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x18)));
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

}
