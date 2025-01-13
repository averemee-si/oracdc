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

/**
 * 
 * Layer 26 opcodes 2 & 6
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


	OraCdcChangeLobs(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, operation, record, offset, headerLength);
		elementNumberCheck(2);
		if (operation == _26_2_REDO) {
			ktbRedo(0);
			kdliCommon(1);
			if (coords.length > 3)
				for (int i = 2; i < coords.length; i++)
					kdli(i);
		} else {
			kdliCommon(0);
			kdli(1);
			if (coords.length > 2) {
				kdli(2);
			}
			if (coords.length > 3) {
				if (lobBimg) {
					if (lobDataOffset < 0)
						lobDataOffset = coords[3][0];
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
			if (coords.length > 3) {
				if (lobBimg) {
					sb.append("\nKDLI data load\n");
					printLobContent(sb, 3, 0);
				} else {
					kdli(sb, 3);
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
