/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.

 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle.internals;

import solutions.a2.oracle.internals.Xid;
import solutions.a2.oracle.utils.FormattingUtils;

/**
 * 
 * KRVMISC - LMNR xact finalize marker
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Presentations/RedoInternals.ppt">Redo Internals</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcChangeKrvMisc extends OraCdcChange {

	private final long startScn;
	private final byte outcome;

	OraCdcChangeKrvMisc(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, _24_4_MISC, record, offset, headerLength);
		elementNumberCheck(1);
		elementLengthCheck("OP:24.4", "", 0, 0x10, "");
		xid = new Xid(
				redoLog.bu().getU16(record, coords[0][0] + 0x04),
				redoLog.bu().getU16(record, coords[0][0] + 0x06),
				redoLog.bu().getU32(record, coords[0][0] + 0x08));
		if (coords.length > 0x3 && coords[3][1] > 0x7) {
			startScn = redoLog.bu().getScn(record, coords[3][0]);
			outcome = record[coords[1][0]];
		} else if (coords.length > 0x1 && coords[1][1] > 0xF) {
			startScn = redoLog.bu().getScn(record, coords[1][0]);
			outcome = -1;
		}
		else {
			startScn = 0;
			outcome = -1;
		}
	}

	public long startScn() {
		return startScn;
	}

	boolean beginTrans() {
		return outcome == -1 && xid.sqn() != 0;
	}

	@Override
	StringBuilder toDumpFormat() {
		final StringBuilder sb = super.toDumpFormat();
		sb
			.append("\nLMNR xact finalize marker:  xid: ")
			.append(xid);
		if (outcome > -1)
			sb.append(" outcome: ")
				.append(outcome)
				.append(" - ")
				.append(outcome == 1 ? "COMMIT" : "ABORT/ROLLBACK");
		if (startScn > 0) {
			sb.append("  start scn: 0x");
			FormattingUtils.leftPad(sb, startScn, 16);
		}
		return sb;
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

}
