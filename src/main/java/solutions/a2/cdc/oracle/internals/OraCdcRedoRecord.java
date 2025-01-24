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
import static solutions.a2.cdc.oracle.internals.OraCdcChange._24_1_DDL;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._24_4_MISC;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._26_2_REDO;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._26_6_BIMG;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

	/* VLD field constants */
	/** The contents are not valid */
	static int KCRVOID = 0;
	/** Includes change vectors */
	static int KCRVALID = 1;
	/** Includes commit SCN */
	static int KCRCOMIT = 2;
	/** Includes dependent SCN */
	static int KCRDEPND = 4;
	/** New SCN mark record. SCN allocated exactly at this point in the redo log by this instance */
	static int KCRNMARK = 8;
	/** Old SCN mark record. SCN allocated at or before this point in the redo. May be allocated by another instance */
	static int KCROMARK = 16;
	/** New SCN was allocated to ensure redo for some block would be ordered by inc/seq# when redo sorted by SCN */
	static int KCRORDER = 32;

	private final OraCdcRedoLog redoLog;
	private final int len;
	private final byte vld;
	private final long scn;
	private final short subScn;
	private final int ts;
	private final RedoByteAddress rba;
	private final int conUid;
	private final List<OraCdcChange> changeVectors;
	private int indKTURDB = -1;
	private int indKTURCM = -1;
	private int indKTUTSL = -1;
	private int indKCOCODRW = -1;
	private int indDDL = -1;
	private int indKTUIRB = -1;

	boolean supplementalLogData = false;
	byte supplementalFb = 0;

	OraCdcRedoRecord(final OraCdcRedoLog redoLog, final long scn) {
		this.redoLog = redoLog;
		final byte[] record = redoLog.recordBytes();
		this.scn = scn;
		rba = redoLog.recordRba();
		len = redoLog.bu().getU32(record, 0x00);
		vld = record[0x04];
		ts = redoLog.recordTimestamp();
		subScn = redoLog.bu().getU16(record, 0x0C);
		changeVectors = new ArrayList<>(0x8);

		final int changeHeaderLen;
		if (redoLog.cdb()) {
			changeHeaderLen = 0x20;
			conUid = redoLog.bu().getU32(record, 0x10);
		} else {
			changeHeaderLen = 0x18;
			conUid = -1;
		}
	
		int offset = redoLog.recordHeaderSize();
		short changeNo = 1;
		while (offset < len) {
			final byte layer = record[offset + 0x00];
			final short operation = (short)(
					(Byte.toUnsignedInt(layer) << 8 )| Byte.toUnsignedInt(record[offset + 0x01]));
			final OraCdcChange change;
			switch (operation) {
			case _5_1_RDB:
				change = new OraCdcChangeUndoBlock(changeNo, this, operation, record, offset, changeHeaderLen);
				supplementalLogData = ((OraCdcChangeUndoBlock)change).supplementalLogData;
				supplementalFb = ((OraCdcChangeUndoBlock)change).supplementalFb;
				indKTURDB = changeNo - 1;
				break;
			case _5_2_RDH:
				change = new OraCdcChangeUndo(changeNo, this, operation, record, offset, changeHeaderLen);
				break;
			case _5_4_RCM:
				change = new OraCdcChangeRcm(changeNo, this, operation, record, offset, changeHeaderLen);
				indKTURCM = changeNo - 1;
				break;
			case _5_6_IRB:
			case _5_11_BRB:
				change = new OraCdcChangeUndo(changeNo, this, operation, record, offset, changeHeaderLen);
				indKTUIRB = changeNo - 1;
				break;
			case _5_19_TSL:
			case _5_20_TSC:
				change = new OraCdcChangeAudit(changeNo, this, operation, record, offset, changeHeaderLen);
				indKTUTSL = changeNo - 1;
				break;
			case _11_2_IRP:
			case _11_3_DRP:
			case _11_4_LKR:
			case _11_5_URP:
			case _11_6_ORP:
			case _11_8_CFA:
			case _11_11_QMI:
			case _11_10_SKL:
			case _11_12_QMD:
			case _11_16_LMN:
			case _11_22_CMP:
				change = new OraCdcChangeRowOp(changeNo, this, operation, record, offset, changeHeaderLen);
				indKCOCODRW = changeNo - 1;
				break;
			case _11_17_LLB:
				change = new OraCdcChangeLlb(changeNo, this, operation, record, offset, changeHeaderLen);
				break;
			case _24_1_DDL:
				change = new OraCdcChangeDdl(changeNo, this, operation, record, offset, changeHeaderLen);
				if (((OraCdcChangeDdl)change).valid()) {
					indDDL = changeNo - 1;
				}
				break;
			case _24_4_MISC:
				change = new OraCdcChangeKrvMisc(changeNo, this, operation, record, offset, changeHeaderLen);
				break;
			case _26_2_REDO:
			case _26_6_BIMG:
				change = new OraCdcChangeLobs(changeNo, this, operation, record, offset, changeHeaderLen);
				break;
			default:
				change = new OraCdcChange(changeNo, this, operation, record, offset, changeHeaderLen);
			}
			changeVectors.add(change);
			if (has5_1() && layer == KCOCODRW) {
				change.dataObj = change5_1().dataObj;
				change.obj = change5_1().obj;
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

	public Xid xid() {
		if (has5_1()) {
			return change5_1().xid;
		} else if (has5_4()) {
			return change5_4().xid;
		} else if (hasPrb()) {
			return changePrb().xid;
		} else if (hasDdl()) {
			return changeDdl().xid;
		} else {
			return null;
		}
	}

	public int halfDoneKey() {
		if (has5_1() && has11_x()) {
			if (change11_x().operation == _11_3_DRP)
				return Objects.hash(false, _11_3_DRP, change5_1().xid, change11_x().dataObj);
			else
				return Objects.hash(false, _11_6_ORP, change5_1().xid, change11_x().dataObj);
		} else if (hasPrb() && has11_x()) {
			return Objects.hash(true, changePrb().xid, changePrb().dataObj);
		} else {
			return 0;
		}
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
			final OraCdcChange rowChange = changeVectors.get(indKCOCODRW);;
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

	public boolean supplementalLogData() {
		return supplementalLogData;
	}

	public int conUid() {
		return conUid;
	}

	//TODO
	public long unixMillis() {
		return BinaryUtils.parseTimestamp(ts).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
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
			.append("\nSCN: ")
			.append(FormattingUtils.leftPad(scn, 0x10))
			.append(" SUBSCN: ")
			.append(Short.toUnsignedInt(subScn));
		//TODO - replace to ts!!!
		if (redoLog.recordTimestamp() != 0) {
			sb
				.append(" ")
				.append(BinaryUtils.parseTimestamp(ts));
		}
		for (OraCdcChange change : changeVectors) {
			sb
				.append("\n")
				.append(change.toDumpFormat());
		}
		return sb.toString();
	}

}
