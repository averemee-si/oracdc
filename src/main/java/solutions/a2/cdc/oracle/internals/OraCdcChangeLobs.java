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

/**
 * 
 * Layer 26 opcodes 2 and 6
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Presentations/RedoInternals.ppt">Redo Internals</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcChangeLobs extends OraCdcChange {

	public static final int LOB_BIMG_INDEX = 3;

	OraCdcChangeLobs(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, operation, record, offset, headerLength);
		elementNumberCheck(2);
		if (operation == _26_2_REDO) {
			ktbRedo(0);
			kdliCommon(1);
			if (coords.length > 2)
				for (int i = 2; i < coords.length; i++)
					kdli(i);
		} else {
			kdliCommon(0);
			kdli(1);
			if (coords.length > 2) {
				kdli(2);
			}
			if (coords.length > LOB_BIMG_INDEX) {
				if ((lobFlags & FLG_LOB_BIMG) > 0) {
					if (lobDataOffset < 0)
						lobDataOffset = coords[LOB_BIMG_INDEX][0];
				} else {
					kdli(3);
				}
			}
			if (coords.length > 4) {
				for (int i = 4; i < coords.length; i++) {
					kdli(i);
				}
			}
		}
	}

	public int kdliFillLen() {
		if (operation == _26_2_REDO && lobDataOffset > 7)
			return Short.toUnsignedInt(redoLog.bu().getU16(record, lobDataOffset -2));
		else
			return -1;
	}

	@Override
	StringBuilder toDumpFormat() {
		final StringBuilder sb = super.toDumpFormat();
		if (operation == _26_2_REDO) {
			ktbRedo(sb, 0);
			kdliCommon(sb, 1);
			for (int i = 2; i < coords.length; i++) {
				kdli(sb, i);
			}
		} else {
			kdliCommon(sb, 0);
			kdli(sb, 1);
			if (coords.length > 2) {
				kdli(sb, 2);
			}
			if (coords.length > LOB_BIMG_INDEX) {
				if ((lobFlags & FLG_LOB_BIMG) > 0) {
					sb.append("\nKDLI data load\n");
					printLobContent(sb, LOB_BIMG_INDEX, 0);
				} else {
					kdli(sb, LOB_BIMG_INDEX);
				}
			}
			if (coords.length > 4) {
				for (int i = 4; i < coords.length; i++) {
					kdli(sb, i);
				}
			}
		}
		return sb;
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

}
