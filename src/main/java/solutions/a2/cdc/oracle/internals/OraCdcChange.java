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

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.UndoByteAddress;
import solutions.a2.oracle.internals.Xid;
import solutions.a2.oracle.utils.FormattingUtils;

/**
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Presentations/RedoInternals.ppt">Redo Internals</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * internals and redo layers and operations description from
 *     <a href="https://askmaclean.com/archives/redo-opcode-reference.html">REDO Opcode</a> 
 *     <a href="https://jonathanlewis.wordpress.com/2017/07/25/redo-op-codes/">Redo OP Codes</a>
 *     <a href="https://onlinedbalearning.blogspot.com/2019/06/redo-opcode.html">REDO Opcode</a>
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcChange {

	/** Layer 5: Transaction Undo -  KCOCOTUN     [ktucts.h] */
	public static final byte KCOCOTUN = 0x05;
	/** KTURDB: Undo block */
	public static final short _5_1_RDB = 0x0501;
	/** KTURDH: Undo rollback segment header i.e. BEGIN TRANSACTION */
	public static final short _5_2_RDH = 0x0502;
	/** KTURCM: Change transaction slot state i.e. COMMIT/ROLLBACK */
	public static final short _5_4_RCM = 0x0504;
	/** KTUIRB: Mark undo record "user undo done" i.e. PARTIAL ROLLBACK */
	public static final short _5_6_IRB = 0x0506;
	/** KTUBRB: Rollback DBA in transaction table entry: yet another PARTIAL ROLLBACK */
	public static final short _5_11_BRB = 0x050B;
	/** KTUTSL: Transaction start audit log record */
	public static final short _5_19_TSL = 0x0513;
	/** KTUTSC: Transaction continue audit log record */
	public static final short _5_20_TSC = 0x0514;

	/** Layer 10: Index operation  -  KCOCODIX     [kdi.h] */
	public static final byte KCOCODIX = 0x0A;
	/** KDICLIN: Insert leaf row */
	public static final short _10_2_LIN = 0x0A02;
	/** KDICLDE: Mark leaf row deleted */
	public static final short _10_4_LDE = 0x0A04;
	/** KDICLNE: Initialize new leaf block */
	public static final short _10_8_LNE = 0x0A08;
	/** KDICLUP: Update keydata in row */
	public static final short _10_18_LUP = 0x0A12;
	/** KDICULK: Undo operation on leaf key above the cache (undo) */
	public static final short _10_22_ULK = 0x0A16;

	/** Layer 11: Row Operation    -  KCOCODRW     [kdocts.h] */
	public static final byte KCOCODRW = 0x0B;
	/** KDOIUR: Interpret Undo Record */
	public static final short _11_1_IUR = 0x0B01;
	/** KDOIRP: Insert Row Piece */
	public static final short _11_2_IRP = 0x0B02;
	/** KDODRP: Delete Row Piece */
	public static final short _11_3_DRP = 0x0B03;
	/** KDOLKR: LOCK Row Piece */
	public static final short _11_4_LKR = 0x0B04;
	/** KDOURP: Update Row Piece */
	public static final short _11_5_URP = 0x0B05;
	/** KDOORP: Overwrite Row Piece */
	public static final short _11_6_ORP = 0x0B06;
	/** KDOCFA: Change Forwarding address */
	public static final short _11_8_CFA = 0x0B08;
	/** KDOSKL: Set Key Links - Change the forward and backward key links on a cluster key */
	public static final short _11_10_SKL = 0x0B0A;
	/** KDOQMI: Quick Multi-Insert */
	public static final short _11_11_QMI = 0x0B0B;
	/** KDOQMD: Quick Multi-Delete */
	public static final short _11_12_QMD = 0x0B0C;
	/** KDOLMN: Logminer support */
	public static final short _11_16_LMN = 0x0B10;
	/** KDOLLB: Logminer support */
	public static final short _11_17_LLB = 0x0B11;
	/** KDOCMP: Logminer support */
	public static final short _11_22_CMP = 0x0B16;

	//TODO - truncate?
	/** Layer 14: Transaction Extent - KCOCOTEX [kte.h] */
	public static final byte KTEOPUTRN = 0x0E;
	/** KTEOPUTRN: undo for truncate ops, flush the object */
	public static final short _14_8_OPUTRN = 0x0E08;

	//TODO
	//TODO
	//TODO
	//TODO - LOB too
	/** Layer 19: Direct Loader Log Blocks - KCOCODLB [kcbl.h] */
	public static final byte KCOCODLB = 0x13;
	/** KCBLCOLB: Direct block logging */
	public static final short _19_1_COLB = 0x1301;

	/** Layer 24: Logminer related (DDL or OBJV# redo) - KCOCOKRV [krv0.h] */
	public static final byte KCOCOKRV = 0x18;
	/** KRVDDL: common portion of the ddl */
	public static final short _24_1_DDL = 0x1801;

	//TODO
	//TODO
	//TODO
	/** Layer 25: AQ Related - KCOCOQUE [kdqs.h] */
	public static final byte KCOCOQUE = 0x19;
	/** KDQSUN: undo */
	public static final short _25_1_SUN = 0x1901;
	/** KDQSIN: init */
	public static final short _25_2_SIN = 0x1902;
	/** KDQSEN: enqueue */
	public static final short _25_3_SEN = 0x1903;
	/** KDQSUP: update */
	public static final short _25_4_SUP = 0x1904;

	//TODO
	//TODO
	//TODO
	/** Layer 26 : LOB Related - KCOCOLOB [kdli3.h] */
	public static final byte KCOCOLOB = 0x1A;
	/** KDLIRUNDO: Generic lob undo */
	public static final short _26_1_UINDO = 0x1A01;
	/** KDLIRREDO: Generic lob redo */
	public static final short _26_2_REDO = 0x1A02;
	/** KDLIRFRMT: lob block format redo */
	public static final short _26_3_FRMT = 0x1A03;
	/** KDLIRINVL: lob invalidation redo */
	public static final short _26_4_INVL = 0x1A04;
	/** KDLIRLOAD: lob cache-load redo */
	public static final short _26_5_LOAD = 0x1A05;
	/** KDLIRBIMG: direct lob direct-load redo */
	public static final short _26_6_BIMG = 0x1A06;

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcChange.class);
	private static final int KTB_REDO_MIN_LENGTH = 0x00000008;
	private  static final String[] KDO_XTYPES = {
			"XA",	//Redo
			"XR",	//Rollback
			"CR"	//Undefined/Unknown
	};

	public static final byte FLG_ROWDEPENDENCIES = 0x40;

	int length;
	final short operation;
	final OraCdcRedoLog redoLog;
	final int[][] coords;
	final RedoByteAddress rba;
	final short conId;
	int obj;
	int dataObj;
	short opc;
	short slt;
	short flg;
	Xid xid;
	byte op;
	int bdba;
	int columnCount;
	int columnCountNn = Integer.MAX_VALUE;
	short slot;
	final short num;
	final byte[] record;
	byte qmRowCount;
	byte fb;
	byte flags;
	private final short cls;
	private final short afn;
	private final int dba;
	private final long scn;
	private final byte seq;
	private final byte typ;
	private final byte encrypted;
	private final int changeDataObj;

	OraCdcChange(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		this.num = num;
		this.redoLog = redoRecord.redoLog();
		this.operation = operation;
		this.rba = redoRecord.rba();
		this.record = record;
		cls = redoLog.bu().getU16(record, offset + 0x02);
		afn = redoLog.bu().getU16(record, offset + 0x04);
		dba = redoLog.bu().getU32(record, offset + 0x08);
		scn = redoLog.bu().getScn(record, offset + 0x0C);
		seq = record[offset + 0x14];
		typ = record[offset + 0x15];
		if ((typ & 0x80) != 0) {
			encrypted = 1;
		} else {
			encrypted = 0;
		}
		changeDataObj = (
				Short.toUnsignedInt(redoLog.bu().getU16(record, offset + 0x06)) << 16) |
				Short.toUnsignedInt(redoLog.bu().getU16(record, offset + 0x16));
		if (redoLog.cdb()) {
			flg = redoLog.bu().getU16(record, offset + 0x1C);
			conId = redoLog.bu().getU16(record, offset + 0x18);
		} else {
			flg = conId = -1;
		}
		final int dataStart = offset + headerLength;
		final int vectorSize = (Short.toUnsignedInt(redoLog.bu().getU16(record, dataStart)) - Short.BYTES) / Short.BYTES;
		final int vectorLengthsSize = (Short.BYTES * (vectorSize + 1) + Short.BYTES) & 0xFFFC;
		int curentStart = offset + headerLength + vectorLengthsSize;
		length = 0;

		coords = new int[vectorSize][2];
		for (int i = 0; i < vectorSize; i++) {
			final int elementLength = Short.toUnsignedInt(redoLog.bu().getU16(record, dataStart + Short.BYTES * (i + 1)));
			final int ceiledLength = (elementLength + Short.BYTES + 1) & 0xFFFC;
			length +=  ceiledLength;
			coords[i][0] = curentStart;
			coords[i][1] = elementLength;
			curentStart += ceiledLength;
		}
		length += (headerLength + vectorLengthsSize);
	}

	void xid(final short slt, final int sqn) {
		final short usn = (short) (cls >= 0x0F ? (cls - 0x0F) / 2 : -1);
		xid = new Xid(usn, slt, sqn);
	}

	void elementLengthCheck(final String part, final String abbreviation, final int index, final int minLength, final String addClause) {
		if (coords[index][1] < minLength) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to parse '{}' {} element for change #{} at RBA {} in '{}'.\n" +
					"Actual size {} is smaller than required {}{}!" +
					"Change contents:\n{}\n" +
					"\n=====================\n",
					part, abbreviation, num, rba, redoLog.fileName(), coords[index][1], minLength, addClause, binaryDump());
			throw new IllegalArgumentException();
		}
	}

	/**
	 * 
	 * <a href="http://www.juliandyke.com/Internals/Redo/KTBRedo.php">KTB Redo</a>
	 * 
	 * @param index
	 */
	void ktbRedo(final int index) {
		if (coords[index][1] < KTB_REDO_MIN_LENGTH) {
			return;
		}
		if (Byte.toUnsignedInt(record[coords[index][0]]) == 0x01 ||
				Byte.toUnsignedInt(record[coords[index][0]]) == 0x11) {
			final int start = (Byte.toUnsignedInt(record[coords[index][0] + 1]) & 0x08) == 0 ?
					4 : 8;
			xid = new Xid(
					redoLog.bu().getU16(record, coords[index][0] + start),
					redoLog.bu().getU16(record, coords[index][0] + start + 0x02),
					redoLog.bu().getU32(record, coords[index][0] + start + 0x04));
		}
	}

	/**
	 * 
	 * Dump representation of a <a href="http://www.juliandyke.com/Internals/Redo/KTBRedo.php">KTB Redo</a>
	 * 
	 * @param sb
	 * @param index
	 */
	void ktbRedo(final StringBuilder sb, final int index) {
		if (coords[index][1] < KTB_REDO_MIN_LENGTH) {
			return;
		}
		final byte opKtbRedo = record[coords[index][0]];
		final byte flgKtbRedo = record[coords[index][0] + 1];
		final int start = (Byte.toUnsignedInt(flgKtbRedo) & 0x08) == 0 ? 4 : 8;
		sb
			.append("\nKTB Redo\nop: ")
			.append(String.format("0x%02x", Byte.toUnsignedInt(opKtbRedo)))
			.append("  ver: ")
			.append(String.format("0x%02x", Byte.toUnsignedInt(flgKtbRedo) & 0x03))
			.append("\ncompat bit: ")
			.append(Byte.toUnsignedInt(flgKtbRedo) & 0x04)
			.append((Byte.toUnsignedInt(flgKtbRedo) & 0x04) != 0 ?
					" (post-11)" : " (pre-11)")
			.append(" padding: ")
			.append((Byte.toUnsignedInt(flgKtbRedo) & 0x10) != 0 ? 0 : 1);
		switch (opKtbRedo) {
		case 0x01:
		case 0x11:
			checkKtbRedoSize("record 'F'", index, start + 0x10);
			sb
				.append("\nop: F  xid:  ")
				.append((new Xid(
						redoLog.bu().getU16(record, coords[index][0] + start),
						redoLog.bu().getU16(record, coords[index][0] + start + 0x02),
						redoLog.bu().getU32(record, coords[index][0] + start + 0x04))).toString())
				.append("    uba: ")
				.append((new UndoByteAddress(redoLog.bu().getU56(record, coords[index][0] + start + 0x08))).toString());
			break;
		case 0x02:
			checkKtbRedoSize("record 'C'", index, start + 0x08);
			sb
				.append("\nop: C  uba: ")
				.append((new UndoByteAddress(redoLog.bu().getU56(record, coords[index][0] + start))).toString());
			break;
		case 0x03:
			sb.append("\nop: Z");
			break;
		case 0x04:
			checkKtbRedoSize("record 'L'", index, start + 0x18);
			final StringBuilder sbFlagsL = new StringBuilder("----");
			final short flgLkc = redoLog.bu().getU16(record, coords[index][0] + start + 0x10);
			if (((byte)(flgLkc >>> 8) & 0x80) != 0) {
				sbFlagsL.setCharAt(0, 'C');
			}
			if (((byte)(flgLkc >>> 8) & 0x40) != 0) {
				sbFlagsL.setCharAt(1, 'B');
			}
			if (((byte)(flgLkc >>> 8) & 0x20) != 0) {
				sbFlagsL.setCharAt(2, 'U');
			}
			if (((byte)(flgLkc >>> 8) & 0x10) != 0) {
				sbFlagsL.setCharAt(3, 'T');
			}
			
			sb
				.append("\nop: L  itl: xid:  ")
				.append((new Xid(
						redoLog.bu().getU16(record, coords[index][0] + start),
						redoLog.bu().getU16(record, coords[index][0] + start + 0x02),
						redoLog.bu().getU32(record, coords[index][0] + start + 0x04))).toString())
				.append(" uba: ")
				.append((new UndoByteAddress(redoLog.bu().getU56(record, coords[index][0] + start + 0x08))).toString())
				.append("\n                      flg: ")
				.append(sbFlagsL)
				.append("    lkc:  ")
				.append(Byte.toUnsignedInt(((byte)flgLkc)))
				.append("     scn:  0x")
				.append(FormattingUtils.leftPad(redoLog.bu().getScn4Record(record, coords[index][0] + start + 0x12), 16));
			break;
		case 0x05:
			sb.append("\nop: R");
			break;
		case 0x06:
			sb.append("\nop: N");
			break;
		default:
			LOGGER.error(
					"\n=====================\n" +
					"Unknown 'KTB Redo' op code '{}' for change #{} at RBA {} in '{}'.\n" +
					"Change contents:\n{}\n" +
					"\n=====================\n",
					opKtbRedo, num, rba, redoLog.fileName(), binaryDump());
		}
		// Block cleanout
		if ((opKtbRedo & 0x10) != 0) {
			checkKtbRedoSize("block cleanout record", index, start + 0x30);
			sb
				.append("\nBlock cleanout record, scn:  0x")
				.append(FormattingUtils.leftPad(redoLog.bu().getScn(record, coords[index][0] + start + 0x28), 16))
				.append(" ver: ")
				.append(String.format("0x%02x", (record[coords[index][0] + start + 0x26] & 0x03)))
				.append(" opt: ")
				.append(String.format("0x%02x", Byte.toUnsignedInt(record[coords[index][0] + start + 0x24])))
				.append(" bigscn: ")
				.append((record[coords[index][0] + start + 0x26] & 0x08) != 0 ? "Y" : "N")
				.append(" compact: ")
				.append((record[coords[index][0] + start + 0x26] & 0x04) != 0 ? "Y" : "N")
				.append(" spare: 00000000")
				.append(", entries follow...");

			final int noOfLines = Byte.toUnsignedInt(record[coords[index][0] + start + 0x25]);
			checkKtbRedoSize("block cleanout record", index, start + 0x30 + noOfLines * 0x8);
			for (int i = 0; i < noOfLines; i++) {
				sb
					.append("\n  itli: ")
					.append(Byte.toUnsignedInt(record[coords[index][0] + start + 0x30 + (i * 0x08)]))
					.append("  flg: (opt=")
					.append(record[coords[index][0] + start + 0x31 + (i * 0x08)] & 0x03)
					.append(" whr=")
					.append(record[coords[index][0] + start + 0x31 + (i * 0x08)] >> 0x02)
					.append(")  scn:  0x")
					.append(FormattingUtils.leftPad(redoLog.bu().getScn4Record(record, coords[index][0] + start + 0x32 + (i * 0x8)), 16));
			}
		}
	}

	private void checkKtbRedoSize(final String abbreviation, final int index, final int minLength) {
		elementLengthCheck("KTB Redo", abbreviation, index, minLength, "");
	}

	public static final int KDO_URP_NULL_POS = 0x1A;
	public static final int KDO_ORP_IRP_NULL_POS = 0x2D;
	private static final int KDO_NCOL_URP_POS = 0x16;
	private static final int KDO_OPCODE_MIN_LENGTH = 0x10;
	private static final int KDO_OPCODE_ORP_IRP_MIN_LENGTH = 0x30;
	private static final int KDO_OPCODE_DRP_MIN_LENGTH = 0x14;
	private static final int KDO_OPCODE_URP_MIN_LENGTH = 0x1C;
	private static final int KDO_OPCODE_LMN_MIN_LENGTH = 0x10;
	private static final int KDO_OPCODE_CFA_MIN_LENGTH = 0x20;
	private static final int KDO_OPCODE_SKL_MIN_LENGTH = 0x14;
	private static final int KDO_OPCODE_LKR_MIN_LENGTH = 0x14;
	private static final int KDO_OPCODE_QM_MIN_LENGTH = 0x18;
	private  static final String[] KDO_OP_CODES = {
			"000",	// Dummy
			"IUR",	// Interpret Undo Redo
			"IRP",	// Insert Row Piece
			"DRP",	// Delete Row Piece
			"LKR",	// Lock Row
			"URP",	// Update Row Piece
			"ORP",	// Overwrite Row Piece
			"MFC",	// Manipulate First Column
			"CFA",	// Change Forwarding Address
			"CKI",	// Cluster Key Index
			"SKL",	// Set Key Link
			"QMI",	// Quick Multi-row Insert
			"QMD",	// Quick Multi-row Delete
			"013",	// Dummy
			"DSC",
			"015",	// Dummy
			"LMN",
			"LLB",	// LogMiner LOB support
			"018",	// Dummy
			"019",	// Dummy
			"SHK",
			"021",	// Dummy
			"CMP",
			"DCU",
			"MRK" };

	void kdo(final int index) {
		kdoOpElemLengthCheck(index, KDO_OPCODE_MIN_LENGTH, "");
		bdba = redoLog.bu().getU32(record, coords[index][0]);
		op = record[coords[index][0] + 0x0A];
		flags = record[coords[index][0] + 0x0B];
		final int selector = (op & 0x1F) | (KCOCODRW << 0x08);
		switch (selector) {
		case _11_2_IRP:
		case _11_6_ORP:
			kdoOpElemLengthCheck(index, KDO_OPCODE_ORP_IRP_MIN_LENGTH);
			fb = record[coords[index][0] + 0x10];
			columnCount = Byte.toUnsignedInt(record[coords[index][0] + 0x12]);
			slot = redoLog.bu().getU16(record, coords[index][0] + 0x2A);
			kdoNullElemLengthCheck(index, KDO_ORP_IRP_NULL_POS + (columnCount + 7) / 8);
			break;
		case _11_3_DRP:
			kdoOpElemLengthCheck(index, KDO_OPCODE_DRP_MIN_LENGTH);
			slot = redoLog.bu().getU16(record, coords[index][0] + 0x10);
			break;
		case _11_4_LKR:
			kdoOpElemLengthCheck(index, KDO_OPCODE_LKR_MIN_LENGTH);
			slot = redoLog.bu().getU16(record, coords[index][0] + 0x10);
			break;
		case _11_5_URP:
			kdoOpElemLengthCheck(index, KDO_OPCODE_URP_MIN_LENGTH);
			fb = record[coords[index][0] + 0x10];
			slot = redoLog.bu().getU16(record, coords[index][0] + 0x14);
			columnCount = Byte.toUnsignedInt(record[coords[index][0] + 0x17]);
			int ccFromArray = Integer.MIN_VALUE;
			if (index + 1 < coords.length && 
					(ccFromArray = coords[index + 1][1] / Short.BYTES) > columnCount &&
					index + 1 + ccFromArray < coords.length) {
				columnCountNn = columnCount;
				columnCount = ccFromArray;
				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace("Trailing NULL's cc adjustment at RBA {} in '{}'", rba, redoLog.fileName());
				}
			} else {
				kdoNullElemLengthCheck(index, KDO_URP_NULL_POS + (columnCount + 7) / 8);
				columnCountNn = 0;
				byte mask = 1;
				int diff = KDO_URP_NULL_POS;
				for (int i = 0; i < columnCount; i++) {
					if ((record[coords[index][0] + diff] & mask) != 0 &&
							coords[index + i + 0x2][1] > 0) {
						break;
					} else {
						columnCountNn++;
					}
					mask <<= 1;
					if (mask == 0) {
						mask = 1;
						diff++;
					}
				}
			}
			break;
		case _11_16_LMN:
			kdoOpElemLengthCheck(index, KDO_OPCODE_LMN_MIN_LENGTH);
			fb = record[coords[index][0] + 0x7];
			columnCount = 0;
			columnCountNn = 0;
			break;
		case _11_8_CFA:
			kdoOpElemLengthCheck(index, KDO_OPCODE_CFA_MIN_LENGTH);
			slot = redoLog.bu().getU16(record, coords[index][0] + 0x18);
			break;
		case _11_10_SKL:
			kdoOpElemLengthCheck(index, KDO_OPCODE_SKL_MIN_LENGTH);
			slot = record[coords[index][0] + 0x1B];
			break;
		case _11_11_QMI:
		case _11_12_QMD:
			kdoOpElemLengthCheck(index, KDO_OPCODE_QM_MIN_LENGTH);
			qmRowCount = record[coords[index][0] + 0x12];
			break;
		}
	}

	private void kdoOpElemLengthCheck(final int index, final int minLength) {
		kdoOpElemLengthCheck(index, minLength, getKdoOpCodeAbbreviation(op & 0x1F), "");
	}

	private void kdoOpElemLengthCheck(final int index, final int minLength, final String abbreviation) {
		kdoOpElemLengthCheck(index, minLength, abbreviation, "");
	}

	private void kdoOpElemLengthCheck(final int index, final int minLength, final String abbreviation, final String addClause) {
		elementLengthCheck("KDO Op code", abbreviation, index, minLength, addClause);
	}

	private void kdoNullElemLengthCheck(final int index, final int minLength) {
		kdoOpElemLengthCheck(index, minLength, getKdoOpCodeAbbreviation(op & 0x1F), " for NULL values");
	}

	void kdo(final StringBuilder sb, final int index) {
		sb
			.append("\nKDO Op code: ")
			.append(getKdoOpCodeAbbreviation(op & 0x1F))
			.append(" row dependencies ")
			.append((op & FLG_ROWDEPENDENCIES) == 0 ? "Disabled" : "Enabled")
			.append("\n  xtype: ")
			.append(KDO_XTYPES[(flags & 0x03) - 1])
			.append(" flags: ")
			.append(String.format("0x%08x", flags & 0xFC))
			.append("  bdba: ")
			.append(String.format("0x%08x", Integer.toUnsignedLong(bdba)))
			.append("  hdba: ")
			.append(String.format("0x%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[index][0] + 0x04))))
			.append("\nitli: ")
			.append(Byte.toUnsignedInt(record[coords[index][0] + 0x0C]))
			.append("  ispac: ")
			.append(Byte.toUnsignedInt(record[coords[index][0] + 0x0D]))
			.append("  maxfr: ")
			.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x08)));
		final int selector = (op & 0x1F) | (KCOCODRW << 0x08);
		switch (selector) {
		case _11_2_IRP:
		case _11_6_ORP:
			sb
				.append("\ntabn: ")
				.append(Byte.toUnsignedInt(record[coords[index][0] + 0x2C]))
				.append(" slot: ")
				.append(Short.toUnsignedInt(slot))
				.append("(")
				.append(String.format("0x%x", Short.toUnsignedInt(slot)))
				.append(")")
				.append(" size/delt: ")
				.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x28)))
				.append("\nfb: ")
				.append(printFbFlags(fb))
				.append(" lb: ")
				.append(String.format("0x%x", Byte.toUnsignedInt(record[coords[index][0] + 0x11])))
				.append("  cc: ")
				.append(Byte.toUnsignedInt(record[coords[index][0] + 0x12]));
			if ((fb & 0x40) != 0) {
				sb
					.append(" cki: ")
					.append(Byte.toUnsignedInt(record[coords[index][0] + 0x13]));
			}
			if (selector == _11_2_IRP && flgFirstPart(fb) && !flgHeadPart(fb)) {
				sb
					.append("\nhrid: ")
					.append(String.format("0x%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[index][0] + 0x14))))
					.append('.')
					.append(String.format("%x", Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x18))))
					;
			}
			if (!flgLastPart(fb)) {
				sb
					.append("\nnrid:  ")
					.append(String.format("0x%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[index][0] + 0x1C))))
					.append('.')
					.append(String.format("%x", Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x20))));
			}
			if ((fb & 0x80) != 0) {
				sb
					.append("\ncurc: 0 comc: 0 pk: ")
					.append(String.format("0x%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[index][0] + 0x14))))
					.append('.')
					.append(String.format("%x", Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x1C))))
					.append(" nk: ")
					.append(String.format("%x", Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x20))))
					;
			}
			sb
				.append("\nnull: ")
				.append(columnCount > 0x0B ?
						"\n01234567890123456789012345678901234567890123456789012345678901234567890123456789\n" :
						"");
			int diff = 0;
			byte mask = 1;
			for (int i = 0; i < columnCount; i++) {
				if ((record[coords[index][0] + KDO_ORP_IRP_NULL_POS + diff] & mask) != 0) {
					sb.append('N');
				} else {
					sb.append('-');
				}
				if (i % 0x50 == 0x4F) {
					sb.append("\n");
				}
				mask <<= 1;
				if (mask == 0) {
					mask = 1;
					diff++;
				}
			}
			if (index + 1 < coords.length) {
				short sizeDelt = redoLog.bu().getU16(record, coords[index][0] + 0x28);
				if (coords[index + 1][1] == sizeDelt && (columnCount >= 0) && columnCount != 1) {
					//TODO
					//TODO compression
					//TODO
				} else {
					for (int col = 0; col < columnCount; col++) {
						final int colIndex = index + 0x1 + col;
						printColumnBytes(sb, col, coords[colIndex][1], colIndex, 0);
					}
				}
			}
			break;
		case _11_3_DRP:
			sb
				.append("\ntabn: ")
				.append(Byte.toUnsignedInt(record[coords[index][0] + 0x12]))
				.append(" slot: ")
				.append(Short.toUnsignedInt(slot))
				.append("(")
				.append(String.format("0x%x", Short.toUnsignedInt(slot)))
				.append(")");
			break;
		case _11_4_LKR:
			sb
				.append("\ntabn: ")
				.append(Byte.toUnsignedInt(record[coords[index][0] + 0x12]))
				.append(" slot: ")
				.append(Short.toUnsignedInt(slot))
				.append(" to: ")
				.append(Byte.toUnsignedInt(record[coords[index][0] + 0x13]));
			break;
		case _11_5_URP:
			sb
				.append("\ntabn: ")
				.append(Byte.toUnsignedInt(record[coords[index][0] + 0x13]))
				.append(" slot: ")
				.append(Short.toUnsignedInt(slot))
				.append("(")
				.append(String.format("0x%x", Short.toUnsignedInt(slot)))
				.append(")")
				.append(" flag: ")
				.append(String.format("0x%02x", Byte.toUnsignedInt(fb)))
				.append(" lock: ")
				.append(Byte.toUnsignedInt(record[coords[index][0] + 0x11]))
				.append(" ckix: ")
				.append(Byte.toUnsignedInt(record[coords[index][0] + 0x12]))
				.append("\nncol: ")
				.append(Byte.toUnsignedInt(record[coords[index][0] + KDO_NCOL_URP_POS]))
				.append(" nnew: ")
				.append(columnCount)
				.append(" size: ")
				.append(redoLog.bu().getU16(record, coords[index][0] + 0x18));	// Signed short!
			if (index + 1 < coords.length) {
				if ((flags & 0x80) != 0) {
					//
					//TODO Element [index+1] contains row data
					//
					sb.append("\nTODO\nTODO - read row data from single element!\nTODO!");
				} else if ((index + 1 + columnCountNn) < coords.length) {
					final int colNumIndex = index + 1;
					for (int i = 0; i < columnCount; i++) {
						if (i < columnCountNn) {
							final int colDataIndex = index + i + 2;
							printColumnBytes(sb, redoLog.bu().getU16(record, coords[colNumIndex][0] + i * Short.BYTES), coords[colDataIndex][1], colDataIndex, 0);
						} else {
							sb
								.append("\ncol ")
								.append(String.format("%2d", redoLog.bu().getU16(record, coords[colNumIndex][0] + i * Short.BYTES)))
								.append(": *NULL*");
						}
					}
				}
			}
			break;
		case _11_8_CFA:
			sb
				.append("\ntabn: ")
				.append(Byte.toUnsignedInt(record[coords[index][0] + 0x1B]))
				.append(" slot: ")
				.append(Short.toUnsignedInt(slot))
				.append("(")
				.append(String.format("0x%x", Short.toUnsignedInt(slot)))
				.append(")")
				.append(" flag: ")
				.append(String.format("0x%02x", Byte.toUnsignedInt(record[coords[index][0] + 0x1A])))
				.append("\nlock: ")
				.append(Byte.toUnsignedInt(record[coords[index][0] + 0x1C]))
				.append(" nrid: ")
				.append(String.format("0x%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[index][0] + 0x10))))
				.append('.')
				.append(String.format("%x", Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x14))));
			break;
		case _11_10_SKL:
			if (coords[index][1] >= 0x20) {
				sb
					.append("\nflag: ")
					.append((record[coords[index][0] + 0x1C] & 0x01) != 0 ? 'F' : '-')
					.append((record[coords[index][0] + 0x1C] & 0x02) != 0 ? 'B' : '-')
					.append(" lock: ")
					.append(Byte.toUnsignedInt(record[coords[index][0] + 0x1D]))
					.append(" slot: ")
					.append(Short.toUnsignedInt(slot))
					.append("(")
					.append(String.format("0x%x", Short.toUnsignedInt(slot)))
					.append(")");
				if ((record[coords[index][0] + 0x1C] & 0x01) != 0) {
					sb
						.append("\nfwd: 0x")
						.append(String.format("%02x", Byte.toUnsignedInt(record[coords[index][0] + 0x10])))
						.append(String.format("%02x", Byte.toUnsignedInt(record[coords[index][0] + 0x11])))
						.append(String.format("%02x", Byte.toUnsignedInt(record[coords[index][0] + 0x12])))
						.append(String.format("%02x", Byte.toUnsignedInt(record[coords[index][0] + 0x13])))
						.append(".")
						.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x14)));
				}
				if ((record[coords[index][0] + 0x1C] & 0x02) != 0) {
					sb
						.append("\nbkw: 0x")
						.append(String.format("%02x", Byte.toUnsignedInt(record[coords[index][0] + 0x16])))
						.append(String.format("%02x", Byte.toUnsignedInt(record[coords[index][0] + 0x17])))
						.append(String.format("%02x", Byte.toUnsignedInt(record[coords[index][0] + 0x18])))
						.append(String.format("%02x", Byte.toUnsignedInt(record[coords[index][0] + 0x19])))
						.append(".")
						.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x1A)));
				}
			}
			break;
		case _11_11_QMI:
		case _11_12_QMD:
			sb
				.append("\ntabn: ")
				.append(Byte.toUnsignedInt(record[coords[index][0] + 0x10]))
				.append(" lock: ")
				.append(Byte.toUnsignedInt(record[coords[index][0] + 0x11]))
				.append(" nrow: ")
				.append(Byte.toUnsignedInt(qmRowCount));
			if (selector == _11_12_QMD) {
				if (coords[index][1] < KDO_OPCODE_QM_MIN_LENGTH + (Byte.toUnsignedInt(qmRowCount) - 1) * Short.BYTES) {
					LOGGER.error("Unable to parse 'KDO Op code' QMD element for change #{} at RBA {} in '{}'.\nActual size {} is smaller than required {}!",
							num, rba, redoLog.fileName(), coords[index][1], KDO_OPCODE_QM_MIN_LENGTH + (Byte.toUnsignedInt(qmRowCount) - 1) * Short.BYTES);
				} else {
					for (int row = 0; row < Byte.toUnsignedInt(qmRowCount); ++row) {
						sb
							.append("\nslot[")
							.append(row)
							.append("]: ")
							.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x14 + row * Short.BYTES)));
					}
				}
			} else {
				//_11_11_QMI
				if (index + 2 < coords.length) {
					int rowDiff = 0;
					for (int row = 0; row < Byte.toUnsignedInt(qmRowCount); ++row) {
						sb
							.append("\nslot[")
							.append(row)
							.append("]: ")
							.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + 0x14 + row * Short.BYTES)))
							.append("\ntl: ")
							.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index + 1][0] + row * Short.BYTES)))
							.append(" fb: ")
							.append(printFbFlags(record[coords[index + 2][0] + rowDiff++]))
							.append(" lb: ")
							.append(String.format("%x", Byte.toUnsignedInt(record[coords[index + 2][0] + rowDiff++])))
							.append("  cc: ");
						final int columnCount = Byte.toUnsignedInt(record[coords[index + 2][0] + rowDiff++]);
						sb.append(columnCount);
						if ((op & FLG_ROWDEPENDENCIES) != 0) {
							// Skip dependent row SCN
							rowDiff += redoLog.bigScn() ? Long.BYTES : (Integer.BYTES + Short.BYTES);
						}
						for (int col = 0; col < columnCount; col++) {
							rowDiff = printColumnBytes(sb, col, index + 2, rowDiff);
						}
					}
				}
			}
			break;
		}
	}

	public int ncol(final int index) {
		if (((op & 0x1F) | (KCOCODRW << 0x08)) == _11_5_URP) {
			return Byte.toUnsignedInt(record[coords[index][0] + KDO_NCOL_URP_POS]);
		} else {
			return 0;
		}
	}

	private String getKdoOpCodeAbbreviation(final int kdoOpCode) {
		if (kdoOpCode > KDO_OP_CODES.length) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to find abbreviation for KDO Op Code {}. Please contact us at oracle@a2-solutions.eu" +
					"\n=====================\n", kdoOpCode);
			return String.format("%03d", kdoOpCode);
		} else {
			return KDO_OP_CODES[kdoOpCode];
		}
	}

	public static StringBuilder printFbFlags(final byte rowFb) {
		final StringBuilder sb = new StringBuilder();
		sb
			.append((rowFb & 0x80) != 0 ? 'K' : '-')		// Cluster key
			.append((rowFb & 0x40) != 0 ? 'C' : '-')		// Clustered row or COMPLETED?
			.append(flgHeadPart(rowFb)  ? 'H' : '-')		// Head piece of row 
			.append((rowFb & 0x10) != 0 ? 'D' : '-')		// Deleted row
			.append(flgFirstPart(rowFb) ? 'F' : '-')		// First piece in row
			.append(flgLastPart(rowFb)  ? 'L' : '-')		// Last piece in row
			.append(flgPrevPart(rowFb)  ? 'P' : '-')		// Previous row piece exists
			.append(flgNextPart(rowFb)  ? 'N' : '-');		// Next row piece exists
		return sb;
	}

	public static boolean flgHeadPart(final byte flag) {
		return (flag & 0x20) != 0;
	}
	public static boolean flgFirstPart(final byte flag) {
		return (flag & 0x08) != 0;
	}
	public static boolean flgLastPart(final byte flag) {
		return (flag & 0x04) != 0;
	}
	public static boolean flgPrevPart(final byte flag) {
		return (flag & 0x02) != 0;
	}
	public static boolean flgNextPart(final byte flag) {
		return (flag & 0x01) != 0;
	}


	int printColumnBytes(final StringBuilder sb, final int col, final int index, final int position) {
		int rowDiff = position;
		int colSize = Byte.toUnsignedInt(record[coords[index][0] + rowDiff]);
		rowDiff += Byte.BYTES;
		if (colSize ==  0xFE) {
			colSize = Short.toUnsignedInt(redoLog.bu().getU16(record, coords[index][0] + rowDiff));
			rowDiff += Short.BYTES;
		} else  if (colSize == 0xFF) {
			colSize = 0;
		}
		return printColumnBytes(sb, col, colSize, index, rowDiff);
	}

	int printColumnBytes(final StringBuilder sb, final int col, final int colSize, final int index, final int position) {
		int rowDiff = position;
		sb
			.append("\ncol ")
			.append(String.format("%2d", col))
			.append(": ");
		if (colSize == 0) {
			sb.append("*NULL*");
			return rowDiff;
		} else {
			sb
				.append("[")
				.append(String.format("%2d", colSize))
				.append("] ")
				.append(colSize > 0x14 ? "\n" : "");
			for (int i = 0; i < colSize; i++) {
				sb
					.append(" ")
					.append(String.format("%02x", Byte.toUnsignedInt(record[coords[index][0] + rowDiff + i])))
					.append((i % 0x19 == 0x18) && (i != colSize -1) ? "\n" : "" );
			}
			return (rowDiff + colSize);
		}
	}

	StringBuilder toDumpFormat() {
		final StringBuilder sb = new StringBuilder(1024);
		sb
			.append("CHANGE #")
			.append(num);
		if (typ == 0x06) {
			sb.append(" MEDIA RECOVERY MARKER");
		}
		if (redoLog.cdb()) {
			sb
				.append(" CON_ID:")
				.append(conId);
		}
		if (typ != 0x06) {
			sb
			.append(" TYP:")
			.append(typ)
			.append(" CLS:")
			.append(cls)
			.append(" AFN:")
			.append(afn)
			.append(" DBA:")
			.append(String.format("0x%08x", Integer.toUnsignedLong(dba)))
			.append(" OBJ:")
			.append(Integer.toUnsignedLong(changeDataObj));
		}
		sb.append(" SCN:0x");
		FormattingUtils.leftPad(sb, scn, 16);
		sb
			.append(" SEQ:")
			.append(seq)
			.append(" OP:")
			.append(formatOpCode(operation))
			.append(" ENC:")
			.append(encrypted);
		if (typ != 6) {
			sb.append(" RBL:0");
		}
		sb
			.append(" FLG:")
			.append(String.format("0x%04x", Short.toUnsignedInt(flg)))
			.append("");
		return sb;
	}

	public static String formatOpCode(final short op) {
		final StringBuilder sb = new StringBuilder();
		sb
			.append(Byte.toUnsignedInt((byte)(op >>> 8)))
			.append(".")
			.append((byte)op);
		return sb.toString();
	}

	public String binaryDump() {
		final StringBuilder sb = new StringBuilder(record.length * Integer.BYTES);
		for (int i = 0; i < coords.length; i++) {
			sb
				.append("\nElement # ")
				.append(i)
				.append(", length=")
				.append(coords[i][1])
				.append("\n");
			for (int j = 0; j < coords[i][1]; j++) {
				sb
					.append(' ')
					.append(String.format("%02x", record[coords[i][0] + j]));
			}
		}
		return sb.toString();
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

	public short num() {
		return num;
	}

	public int obj() {
		return obj;
	}

	public byte fb() {
		return fb;
	}

	public short operation() {
		return operation;
	}

	public int[][] coords() {
		return coords;
	}

	public byte qmRowCount() {
		return qmRowCount;
	}

	public byte[] record() {
		return record;
	}

	public OraCdcRedoLog redoLog() {
		return redoLog;
	}

	public byte op() {
		return op;
	}

	public int dataObj() {
		return dataObj;
	}

	public int bdba() {
		return bdba;
	}

	public short conId() {
		return conId;
	}

	public int columnCount() {
		return columnCount;
	}

	public int columnCountNn() {
		return columnCountNn;
	}

	public byte flags() {
		return flags;
	}

}
