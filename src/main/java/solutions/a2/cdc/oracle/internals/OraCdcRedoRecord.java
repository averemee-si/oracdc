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

import static solutions.a2.cdc.oracle.internals.OraCdcChange.KCOCODRW;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._5_1_RDB;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._5_2_RDH;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._5_4_RCM;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._5_6_IRB;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._5_11_BRB;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._5_19_TSL;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._5_20_TSC;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_2_IRP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_2_LIN;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_4_LDE;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_18_LUP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_30_LNU;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_35_LCU;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_3_DRP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_4_LKR;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_5_URP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_6_ORP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_8_CFA;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_10_SKL;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_11_QMI;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_12_QMD;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_16_LMN;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_17_LLB;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_22_CMP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._19_1_COLB;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._24_1_DDL;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._24_4_MISC;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._24_6_DLR10;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._24_8_XML;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._26_2_REDO;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._26_6_BIMG;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.formatOpCode;
import static solutions.a2.utils.ExceptionUtils.getExceptionStackTrace;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;
import solutions.a2.oracle.internals.Xid;
import solutions.a2.oracle.utils.BinaryUtils;
import solutions.a2.oracle.utils.FormattingUtils;

/**
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Presentations/RedoInternals.ppt">Redo Internals</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * internals.
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcRedoRecord {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoRecord.class);

	/* VLD field constants */
	/** The contents are not valid */
	final static int KCRVOID = 0;
	/** Includes change vectors */
	final static int KCRVALID = 1;
	/** Includes commit SCN */
	final static int KCRCOMIT = 2;
	/** Includes dependent SCN */
	final static int KCRDEPND = 4;
	/** New SCN mark record. SCN allocated exactly at this point in the redo log by this instance */
	final static int KCRNMARK = 8;
	/** Old SCN mark record. SCN allocated at or before this point in the redo. May be allocated by another instance */
	final static int KCROMARK = 16;
	/** New SCN was allocated to ensure redo for some block would be ordered by inc/seq# when redo sorted by SCN */
	final static int KCRORDER = 32;

	private final static int HEADER_LEN_PLAIN = 0x18;
	private final static int HEADER_LEN_LWN = 0x44;

	private final OraCdcRedoLog redoLog;
	private final int len;
	private final byte vld;
	private final long scn;
	private final short subScn;
	private final RedoByteAddress rba;
	private final byte[] record;
	private final List<OraCdcChange> changeVectors;
	private int ts;
	private int conUid;
	private boolean hasLwn;
	private int lwnLen;
	private int indKTURDB = -1;
	private int indKTURCM = -1;
	private int indKTUTSL = -1;
	private int indKCOCODRW = -1;
	private int indDDL = -1;
	private int indKTUIRB = -1;
	private int indKRVMISC = -1;
	private int indKRVDLR10 = -1;
	private int indKRVXML = -1;
	private int indLLB = -1;
	private int indKCOCOLOB = -1;
	private int indKCOCODLB = -1;
	private int indKCOCODIX = -1;

	OraCdcRedoRecord(final OraCdcRedoLog redoLog, final long scn) {
		this.redoLog = redoLog;
		this.scn = scn;
		record = redoLog.recordBytes();
		rba = redoLog.recordRba();
		len = redoLog.bu().getU32(record, 0x00);
		vld = record[0x04];
		subScn = redoLog.bu().getU16(record, 0x0C);
		changeVectors = new ArrayList<>(0x8);
		buildChangeVector();
	}

	private void buildChangeVector() {
		final int changeHeaderLen;
		if (redoLog.cdb()) {
			changeHeaderLen = 0x20;
			conUid = redoLog.bu().getU32(record, 0x10);
		} else {
			changeHeaderLen = 0x18;
			conUid = -1;
		}
		int offset;
		if ((vld & KCRDEPND) != 0) {
			hasLwn = true;
			lwnLen = redoLog.bu().getU32(record, 0x20);
			ts = redoLog.bu().getU32(record, 0x40);
			offset = HEADER_LEN_LWN;
		} else {
			hasLwn = false;
			lwnLen = 0;
			ts = 0;
			offset = HEADER_LEN_PLAIN;
		}
		short changeNo = 1;
		while (offset < len) {
			final byte layer = record[offset + 0x00];
			final short operation = (short)(
					(Byte.toUnsignedInt(layer) << 8 )| Byte.toUnsignedInt(record[offset + 0x01]));
			final OraCdcChange change;
			switch (operation) {
				case _5_1_RDB -> {
					change = new OraCdcChangeUndoBlock(changeNo, this, operation, record, offset, changeHeaderLen);
					indKTURDB = changeNo - 1;
				}
				case _5_2_RDH -> {
					change = new OraCdcChangeUndo(changeNo, this, operation, record, offset, changeHeaderLen);
				}
				case _5_4_RCM -> {
					change = new OraCdcChangeRcm(changeNo, this, operation, record, offset, changeHeaderLen);
					indKTURCM = changeNo - 1;
				}
				case _5_6_IRB, _5_11_BRB -> {
					change = new OraCdcChangeUndo(changeNo, this, operation, record, offset, changeHeaderLen);
					indKTUIRB = changeNo - 1;
				}
				case _5_19_TSL, _5_20_TSC -> {
					change = new OraCdcChangeAudit(changeNo, this, operation, record, offset, changeHeaderLen);
					indKTUTSL = changeNo - 1;
				}
				case _10_2_LIN, _10_4_LDE, _10_18_LUP, _10_30_LNU, _10_35_LCU -> {
					change = new OraCdcChangeIndexOp(changeNo, this, operation, record, offset, changeHeaderLen);
					indKCOCODIX = changeNo - 1;
				}
				case _11_2_IRP, _11_3_DRP, _11_4_LKR, _11_5_URP, _11_6_ORP, _11_8_CFA,
					_11_11_QMI, _11_10_SKL, _11_12_QMD, _11_16_LMN, _11_22_CMP -> {
						change = new OraCdcChangeRowOp(changeNo, this, operation, record, offset, changeHeaderLen);
						indKCOCODRW = changeNo - 1;
				}
				case _11_17_LLB -> {
					change = new OraCdcChangeLlb(changeNo, this, operation, record, offset, changeHeaderLen);
					indLLB = changeNo - 1;
				}
				case _24_1_DDL -> {
					change = new OraCdcChangeDdl(changeNo, this, operation, record, offset, changeHeaderLen);
					if (((OraCdcChangeDdl)change).valid())
						indDDL = changeNo - 1;
				}
				case _24_4_MISC -> {
					change = new OraCdcChangeKrvMisc(changeNo, this, operation, record, offset, changeHeaderLen);
					indKRVMISC = changeNo - 1;
				}
				case _24_6_DLR10 -> {
					change = new OraCdcChangeKrvDlr10(changeNo, this, operation, record, offset, changeHeaderLen);
					indKRVDLR10 = changeNo - 1;
				}
				case _24_8_XML -> {
					change = new OraCdcChangeKrvXml(changeNo, this, operation, record, offset, changeHeaderLen);
					indKRVXML = changeNo - 1;
				}
				case _26_2_REDO, _26_6_BIMG -> {
					change = new OraCdcChangeLobs(changeNo, this, operation, record, offset, changeHeaderLen);
					indKCOCOLOB = changeNo - 1;
				}
				case _19_1_COLB -> {
					change = new OraCdcChangeColb(changeNo, this, operation, record, offset, changeHeaderLen);
					indKCOCODLB = changeNo - 1;
				}
				default -> {
					try {
						change = new OraCdcChange(changeNo, this, operation, record, offset, changeHeaderLen);
					} catch (Exception e) {
						LOGGER.error(
								"""
								
								=====================
								'{}' while parsing change# {} OP:{} at RBA={}, SCN={}, SUBSCN={} in '{}'
								Record content:
								{}
								Erorstack:
								{}
								=====================
								
								""", e.getMessage(), changeNo, formatOpCode(operation), rba,
									Long.toUnsignedString(scn), Short.toUnsignedInt(subScn),
									redoLog.fileName(), rawToHex(record), getExceptionStackTrace(e));
						throw e;
					}
				}
			}
			changeVectors.add(change);
			if (indKTURDB > -1 && layer == KCOCODRW) {
				change.dataObj = changeVectors.get(indKTURDB).dataObj;
				change.obj = changeVectors.get(indKTURDB).obj;
			}
			changeNo++;
			offset += change.length;
		}
	}

	public final List<OraCdcChange> changeVectors() {
		return changeVectors;
	}

	public RedoByteAddress rba() {
		return rba;
	}

	public boolean has5_1() {
		return indKTURDB > -1;
	}

	public OraCdcChangeUndoBlock change5_1() {
		if (has5_1())
			return (OraCdcChangeUndoBlock) changeVectors.get(indKTURDB);
		else
			return null;
	}

	public boolean has5_4() {
		return indKTURCM > -1;
	}

	public OraCdcChangeRcm change5_4() {
		if (has5_4())
			return (OraCdcChangeRcm) changeVectors.get(indKTURCM);
		else
			return null;
	}

	public boolean has11_x() {
		return indKCOCODRW > -1;
	}

	public OraCdcChangeRowOp change11_x() {
		if (has11_x())
			return (OraCdcChangeRowOp) changeVectors.get(indKCOCODRW);
		else
			return null;
	}

	public boolean hasDdl() {
		return indDDL > -1;
	}

	public OraCdcChangeDdl changeDdl() {
		if (hasDdl())
			return (OraCdcChangeDdl) changeVectors.get(indDDL);
		else
			return null;
	}

	public boolean hasAudit() {
		return indKTUTSL > -1;
	}

	public OraCdcChangeAudit changeAudit() {
		if (hasAudit())
			return (OraCdcChangeAudit) changeVectors.get(indKTUTSL);
		else
			return null;
	}

	public boolean hasPrb() {
		return indKTUIRB > -1;
	}

	public OraCdcChangeUndo changePrb() {
		if (hasPrb())
			return (OraCdcChangeUndo) changeVectors.get(indKTUIRB);
		else
			return null;
	}

	public boolean hasKrvMisc() {
		return indKRVMISC > -1;
	}

	public OraCdcChangeKrvMisc changeKrvMisc() {
		if (hasKrvMisc())
			return (OraCdcChangeKrvMisc) changeVectors.get(indKRVMISC);
		else
			return null;
	}

	public boolean hasLlb() {
		return indLLB > -1;
	}

	public OraCdcChangeLlb changeLlb() {
		if (hasLlb())
			return (OraCdcChangeLlb) changeVectors.get(indLLB);
		else
			return null;
	}

	public boolean has26_x() {
		return indKCOCOLOB > -1;
	}

	public OraCdcChangeLobs change26_x() {
		if (has26_x())
			return (OraCdcChangeLobs) changeVectors.get(indKCOCOLOB);
		else
			return null;
	}

	public boolean hasColb() {
		return indKCOCODLB > -1;
	}

	public OraCdcChangeColb changeColb() {
		if (hasColb())
			return (OraCdcChangeColb) changeVectors.get(indKCOCODLB);
		else
			return null;
	}

	public boolean hasKrvDlr10() {
		return indKRVDLR10 > -1;
	}

	public OraCdcChangeKrvDlr10 changeKrvDlr10() {
		if (hasKrvDlr10())
			return (OraCdcChangeKrvDlr10) changeVectors.get(indKRVDLR10);
		else
			return null;
	}

	public boolean hasKrvXml() {
		return indKRVXML > -1;
	}

	public OraCdcChangeKrvXml changeKrvXml() {
		if (hasKrvXml())
			return (OraCdcChangeKrvXml) changeVectors.get(indKRVXML);
		else
			return null;
	}

	public boolean has10_x() {
		return indKCOCODIX > -1;
	}

	public OraCdcChangeIndexOp change10_x() {
		if (has10_x())
			return (OraCdcChangeIndexOp) changeVectors.get(indKCOCODIX);
		else
			return null;
	}

	public Xid xid() {
		if (indKTURDB > -1) {
			return ((OraCdcChangeUndoBlock) changeVectors.get(indKTURDB)).xid;
		} else if (indKTURCM > -1) {
			return ((OraCdcChangeRcm) changeVectors.get(indKTURCM)).xid;
		} else if (indKTUIRB > -1) {
			return ((OraCdcChangeUndo) changeVectors.get(indKTUIRB)).xid;
		} else if (indLLB > -1) {
			return ((OraCdcChangeLlb) changeVectors.get(indLLB)).xid;
		} else if (indKCOCOLOB > -1) {
			return ((OraCdcChangeLobs) changeVectors.get(indKCOCOLOB)).xid;
		} else if (indKRVMISC > -1) {
			return ((OraCdcChangeKrvMisc) changeVectors.get(indKRVMISC)).xid;
		} else if (indKRVDLR10 > -1) {
			return ((OraCdcChangeKrvDlr10) changeVectors.get(indKRVDLR10)).xid;
		} else if (indKRVXML > -1) {
			return ((OraCdcChangeKrvXml) changeVectors.get(indKRVXML)).xid;
		} else if (indDDL > -1) {
			return ((OraCdcChangeDdl) changeVectors.get(indDDL)).xid;
		} else {
			return null;
		}
	}

	public int halfDoneKey() {
		if (indKTURDB > -1) {
			final var change = (OraCdcChangeUndoBlock) changeVectors.get(indKTURDB);
			if (change.supplementalLogData())
				return Objects.hash(false,
						change.supplementalDataFor(),
						change.dataObj);
			else {
				if (indKCOCODRW > -1) {
					return Objects.hash(false,
							changeVectors.get(indKCOCODRW).operation == _11_3_DRP ? _11_3_DRP : _11_6_ORP,
							change.dataObj);
				} else if (indKCOCODIX > -1) {
					final var idxChange = changeVectors.get(indKCOCODIX);
					return Objects.hash(false,
							idxChange.operation == _10_4_LDE ? _11_3_DRP : _11_6_ORP,
							idxChange.dataObj);
				} else
					return 0;
			}
		} else if (indKTUIRB > -1) {
			if (indKCOCODRW > -1) {
				return Objects.hash(true, changeVectors.get(indKTUIRB).dataObj);
			} else
				return 0;
		} else
			return 0;
	}

	public RowId rowid() {
		if (has5_1() && has11_x()) {
			final OraCdcChangeUndoBlock change = (OraCdcChangeUndoBlock) changeVectors.get(indKTURDB);
			if (change.supplementalSlot > -1)
				return new RowId(
					change.dataObj,
					change.supplementalBdba,
					change.supplementalSlot);
			else
				return new RowId(
					change.dataObj,
					change.bdba,
					change.slot);
		} else if (hasPrb() && has11_x()) {
			final OraCdcChange rowChange = changeVectors.get(indKCOCODRW);
			return new RowId(
					changeVectors.get(indKTUIRB).dataObj,
					rowChange.bdba,
					rowChange.slot);
		} else {
			return RowId.ZERO;
		}
	}

	public long scn() {
		return scn;
	}

	public short subScn() {
		return subScn;
	}

	public int len() {
		return len;
	}

	public OraCdcRedoLog redoLog() {
		return redoLog;
	}

	public int conUid() {
		return conUid;
	}

	public int ts() {
		return ts;
	}

	public boolean hasLwn() {
		return hasLwn;
	}

	public int lwnLen() {
		return lwnLen;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(4096);
		sb
			.append("\n")
			.append("REDO RECORD - Thread:")
			.append(String.format("%1$x", redoLog.thread()))
			.append(" RBA: ")
			.append(rba.toString())
			.append(" LEN: ")
			.append(String.format("0x%04x", len))
			.append(" VLD: ")
			.append(String.format("0x%02x", Byte.toUnsignedInt(vld)));
		if (redoLog.cdb()) {
			sb
				.append(" CON_UID: ")
				.append(Integer.toUnsignedString(conUid));
		}
		sb
			.append("\nSCN: 0x")
			.append(FormattingUtils.leftPad(scn, 0x10))
			.append(" SUBSCN: ")
			.append(Short.toUnsignedInt(subScn));
		if (ts != 0) {
			sb
				.append(" ")
				.append(BinaryUtils.parseTimestamp(ts));
		}
		if (hasLwn) {
			sb
				.append("\n(LWN RBA: ")
				.append(rba.toString())
				.append(" LEN: ")
				.append(String.format("0x%08x", Integer.toUnsignedLong(lwnLen)))
				.append(" NST: ")
				.append(String.format("0x%04x", Short.toUnsignedInt(
									redoLog.bu().getU16(record, 0x1A))))
				.append(" SCN: 0x")
				.append(FormattingUtils.leftPad(
									redoLog.bu().getScn(record, 0x28), 0x10))
				.append(")");
		}
		for (OraCdcChange change : changeVectors) {
			try {
				sb
					.append("\n")
					.append(change.toDumpFormat());
			} catch (Exception aiob) {
				LOGGER.error(
						"""
						
						=====================
						Unable to print change vectors from {} at SCN={}, RBA {} due to
						{}
						Redo record contents:
						{}
						Please send this output to oracle@a2.solutions
						=====================
						""", redoLog.fileName(), Long.toUnsignedString(scn), rba, aiob.getMessage(), rawToHex(record));
			}
		}
		return sb.toString();
	}

	public byte[] content() {
		return record;
	}

	public OraCdcRedoRecord(final OraCdcRedoLog redoLog, final long scn, final String rbaAsString, byte[] data) {
		this.redoLog = redoLog;
		this.scn = scn;
		record = data;
		rba = RedoByteAddress.fromLogmnrContentsRs_Id(rbaAsString);
		len = redoLog.bu().getU32(record, 0x00);
		vld = record[0x04];
		subScn = redoLog.bu().getU16(record, 0x0C);
		changeVectors = new ArrayList<>(0x8);
		buildChangeVector();
	}

	public OraCdcRedoRecord(final OraCdcRedoLog redoLog, final long scn, final RedoByteAddress rba, byte[] data) {
		this.redoLog = redoLog;
		this.scn = scn;
		record = data;
		this.rba = rba;
		len = redoLog.bu().getU32(record, 0x00);
		vld = record[0x04];
		subScn = redoLog.bu().getU16(record, 0x0C);
		changeVectors = new ArrayList<>(0x8);
		buildChangeVector();
	}

}
