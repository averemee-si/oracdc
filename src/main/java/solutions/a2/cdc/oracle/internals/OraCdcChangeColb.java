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

import solutions.a2.oracle.internals.LobId;

/**
 * 
 * 19.1 Direct block logging
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Internals/Redo/Redo11.php">Redo Level 11 - Table Operations (DML)</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcChangeColb extends OraCdcChange {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcChangeColb.class);

	public static final int SIZE = 0x24;

	private final int lobPageNo;

	OraCdcChangeColb(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, operation, record, offset, headerLength);
		if (coords.length < 1) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to parse mandatory elements for 19.1 (KCBLCOLB) #{} at RBA {} in '{}'.\n" +
					"Change contents:\n{}\n" +
					"=====================\n",
					num, rba, redoLog.fileName(), binaryDump());
			throw new IllegalArgumentException();
		}
		elementLengthCheck("19.1 (KCBLCOLB)", "", 0, SIZE, "");
		lid = new LobId(record, coords[0][0] + 0x4, LobId.SIZE);
		dataObj = redoLog.bu().getU32(record, coords[0][0]);
		lobDataOffset = SIZE; 
		lobPageNo = redoLog.bu().getU32(record, coords[0][0] + 0x18);
	}

	@Override
	StringBuilder toDumpFormat() {
		final StringBuilder sb = super.toDumpFormat();
		sb
			.append("\nDirect Loader block redo entry\nLong field block dump:\nObject Id  ")
			.append(Integer.toUnsignedLong(dataObj))
			.append("\nLobId: ")
			.append(lid.toStringSignificant())
			.append(" PageNo ")
			.append(String.format("%8d", lobPageNo))
			.append("\nVersion: ")
			.append(String.format("0x%04x", Short.toUnsignedInt(redoLog.bu().getU16(record, coords[0][0] + 0x14))))
			.append('.')
			.append(String.format("%08x", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[0][0] + 0x10))))
			.append("  pdba: ")
			.append(String.format("%8d", Integer.toUnsignedLong(redoLog.bu().getU32(record, coords[0][0] + 0x1C))))
			.append('\n');
		for (int i = SIZE; i < coords[0][1]; i++) {
			sb
				.append(String.format("%02x", Byte.toUnsignedInt(record[coords[0][0] + i])))
				.append(' ');
			if (((i - SIZE) % 0x18) == 0x17 && i != coords[0][1] - 1)
				sb.append("\n    ");
		}
		sb.append('\n');
		return sb;
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

}
