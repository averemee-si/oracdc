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

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.oracle.internals.UndoByteAddress;

/**
 * 
 * RCM - Rollback/Commit Management
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Presentations/RedoInternals.ppt">Redo Internals</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcChangeRcm extends OraCdcChange {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcChangeRcm.class);
	private static final int KTUCM_MIN_LENGTH = 0x14;
	private static final int KTUCF_MIN_LENGTH = 0x10;
	private static final int ROLLBACK = 0x04;

	private final int unixTime;

	OraCdcChangeRcm(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, _5_4_RCM, record, offset, headerLength);
		// Only ktucm element is needed for rollback/commit management
		if (coords.length < 1 || coords[0][1] < KTUCM_MIN_LENGTH) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to parse mandatory ktucm element (OP:5.4) for change #{} at RBA {} in '{}'.\n" +
					"Change contents:\n{}\n" +
					"=====================\n",
					num, rba, redoLog.fileName(), binaryDump());
			throw new IllegalArgumentException();
		}
		xid(redoLog.bu().getU16(record, coords[0][0] + 0x00), 
				redoLog.bu().getU32(record, coords[0][0] + 0x04));
		flg = record[coords[0][0] + 0x10];
		if (coords[coords.length - 1][1] == 0x4) {
			unixTime = redoLog.bu().getU32(record, coords[coords.length - 1][0]);
		} else {
			unixTime = 0;
		}
		
	}

	public boolean rollback() {
		if ((flg & ROLLBACK) != 0) {
			return true;
		} else {
			return false;
		}
	}

	public int unixTime() {
		return unixTime;
	}

	public Instant unixInstant() {
		return Instant.ofEpochSecond(Integer.toUnsignedLong(unixTime));
	}

	@Override
	StringBuilder toDumpFormat() {
		final StringBuilder sb = super.toDumpFormat();
		sb
			.append("\nktucm redo: slt: ")
			.append(String.format("0x%04x", Short.toUnsignedInt(redoLog.bu().getU16(record, coords[0][0] + 0x00))))
			.append(" sqn: ")
			.append(String.format("0x%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[0][0] + 0x04))))
			.append(" srt: ")
			.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[0][0] + 0x08)))
			.append(" sta: ")
			.append(Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[0][0] + 0x0C)))
			.append(" flg: ")
			.append(String.format("0x%x", Short.toUnsignedInt(flg)));
		if (coords.length > 1 && coords[1][1] >= KTUCF_MIN_LENGTH) {
			sb
				.append(" ktucf redo: uba: ")
				.append((new UndoByteAddress(redoLog.bu().getU56(record, coords[1][0]))).toString())
				.append(" ext: ")
				.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[1][0] + 0x08)))
				.append(" spc: ")
				.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[1][0] + 0x0A)))
				.append(" fbi: ")
				.append(Byte.toUnsignedInt(record[coords[1][0] + 0x0C]));
		}
		if (rollback()) {
			sb.append("\nrolled back transaction");
		}
		return sb;
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

}
