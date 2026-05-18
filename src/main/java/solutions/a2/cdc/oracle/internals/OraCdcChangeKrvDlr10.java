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
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle.internals;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import solutions.a2.oracle.internals.Xid;

/**
 * 
 * KRVDLR10 - Direct load redo 10g
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Presentations/RedoInternals.ppt">Redo Internals</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcChangeKrvDlr10 extends OraCdcChange {

	private static final Logger LOGGER = LogManager.getLogger(OraCdcChangeKrvDlr10.class);

	OraCdcChangeKrvDlr10(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, _24_6_DLR10, record, offset, headerLength);
		if (encrypted && !redoLog.tsEncKeyAvailable())
			return;
		elementNumberCheck(1);
		if (coords[0][1] < 0x10) {
			LOGGER.error(
					"\n=====================\n" +
					"Not enough data to create OP:24.6 at RBA {}.\nChange binary dump:\n{}" +
					"\n=====================\n",
					rba, binaryDump());
			throw new IllegalArgumentException("Unable to create OP:24.6");
		}
		xid = new Xid(
				redoLog.bu().getU16(record, coords[0][0] + 0x8),
				redoLog.bu().getU16(record, coords[0][0] + 0xA),
				redoLog.bu().getU32(record, coords[0][0] + 0xC));
	}

	@Override
	StringBuilder toDumpFormat() {
		final StringBuilder sb = super.toDumpFormat();
		if (encrypted && !redoLog.tsEncKeyAvailable())
			return sb;
		sb
			.append("\nDirect load redo 10g:  xid: ")
			.append(xid);
		return sb;
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

}
