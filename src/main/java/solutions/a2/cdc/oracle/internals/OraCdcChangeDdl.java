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

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.oracle.internals.Xid;

/**
 * 
 * DDL record
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Presentations/RedoInternals.ppt">Redo Internals</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcChangeDdl extends OraCdcChange {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcChangeDdl.class);
	private static final int DDLDEF_MIN_LENGTH = 0x12;
	private static final int DDL_SQL_POS = 0x07;
	private static final int DDL_OBJ_POS = 0x0B;

	final short kind;
	final boolean valid;

	OraCdcChangeDdl(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, operation, record, offset, headerLength);
		if (coords.length < 1 || coords[0][1] < DDLDEF_MIN_LENGTH) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to parse mandatory element (OP:24.1) for change #{} at RBA {} in '{}'.\n" +
					"Change contents:\n{}\n" +
					"=====================\n",
					num, rba, redoLog.fileName(), binaryDump());
			throw new IllegalArgumentException();
		}
		xid = new Xid(
				redoLog.bu().getU16(record, coords[0][0] + 0x04),
				redoLog.bu().getU16(record, coords[0][0] + 0x06),
				redoLog.bu().getU32(record, coords[0][0] + 0x08));
		kind = redoLog.bu().getU16(record, coords[0][0] + 0x10);
		if (DDL_OBJ_POS < coords.length &&
				kind != 0x04 && kind != 0x05 && kind != 0x06 &&
				kind != 0x08 && kind != 0x09 && kind != 0x0A) {
			obj = redoLog.bu().getU32(record, coords[DDL_OBJ_POS][0]);
			valid = true;
		} else {
			valid = false;
		}

	}

	public boolean valid() {
		return valid;
	}

	public String ddlText() {
		return new String(Arrays.copyOfRange(record, coords[DDL_SQL_POS][0], coords[DDL_SQL_POS][0] + coords[DDL_SQL_POS][1]));
	}

	@Override
	StringBuilder toDumpFormat() {
		final StringBuilder sb = super.toDumpFormat();
		if (valid()) {
			sb
				.append("\nDDL Type: ")
				.append(kind)
				.append("  obj: ")
				.append(Integer.toUnsignedLong(obj))
				.append('\n')
				.append(ddlText());
		}
		return sb;
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

}
