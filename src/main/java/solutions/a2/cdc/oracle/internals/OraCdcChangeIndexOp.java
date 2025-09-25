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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * 
 * Index operations 10.2, 10.4, 10.18, 10.30
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Internals/Redo/Redo11.php">Redo Level 11 - Table Operations (DML)</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcChangeIndexOp extends OraCdcChange {

	public static final int NON_KEY_10_30_POS = 2;
	public static final int COL_NUM_10_35_POS = 2;

	private boolean nonKeyData = false; 

	OraCdcChangeIndexOp(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, operation, record, offset, headerLength);
		if (encrypted && !redoLog.tsEncKeyAvailable())
			return;
		elementNumberCheck(1);

		// Element 1: KTB Redo
		ktbRedo(0);
		// Element 2: Index info
		elementLengthCheck("index info", formatOpCode(operation), 1, 6, "");
		if (coords.length > 1) {
			if (operation == _10_35_LCU) {
				nonKeyData = true;
				columnCount = coords[2][1] / Short.BYTES;
			} else {
				if (coords.length > 3) {
					nonKeyData = true;
					columnCount = Byte.toUnsignedInt(record[coords[3][0] + 2]);
					fb = record[coords[3][0]];
				} else if (operation == _10_30_LNU) {
					nonKeyData = true;
					columnCount = Byte.toUnsignedInt(record[coords[2][0] + 2]);
					fb = record[coords[2][0]];
				}
			}
		}
	}

	public int writeIndexColumns(final ByteArrayOutputStream baos, final int colNumIndex) throws IOException {
		return writeIndexColumns(baos, 2, nonKeyData, colNumIndex);
	}

	@Override
	public int columnCount() {
		if (nonKeyData) {
			return columnCount + columnCountNn();
		} else {
			return columnCountNn();
		}
	}

	@Override
	public int columnCountNn() {
		if (operation == _10_35_LCU)
			return 0;
		else {
			if (coords.length > 2 && operation != _10_30_LNU)
				return indexKeyColCount(2);
			else
				return 0;
		}
	}

	@Override
	StringBuilder toDumpFormat() {
		final StringBuilder sb = super.toDumpFormat();
		if (encrypted && !redoLog.tsEncKeyAvailable())
			return sb;
		sb
			.append(operation == _10_2_LIN
				? "\nindex redo (kdxlin):  insert leaf row, count="
				: operation == _10_4_LDE
					? "\nindex redo (kdxlde):  delete leaf row, count="
					: operation == _10_18_LUP
						? "\nindex redo (kdxlup): update keydata, count="
						: operation == _10_30_LNU
							? "\nindex redo (kdxlnu): update nonkey, count="
							: "\nindex redo (kdxlcnu): update nonkey, count=")
			.append(coords.length);
		ktbRedo(sb, 0);
		if (coords.length > 1) {
			sb
				.append("\nREDO: ")
				.append(String.format("0x%x", Byte.toUnsignedInt(record[coords[1][0] + 1])))
				.append(" SINGLE / ")
				.append(nonKeyData ? "NONKEY / -- " : "-- / --")
				.append("\nitl: ")
				.append(Byte.toUnsignedInt(record[coords[1][0]]))
				.append(", sno: ")
				.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[1][0] + 2)))
				.append(", row size ")
				.append(Short.toUnsignedInt(redoLog.bu().getU16(record, coords[1][0] + 4)));
			if (operation == _10_30_LNU) {
				printNonKeyData(sb, 2);
			} else if (operation == _10_35_LCU && coords[1][1] > 0xB) {
				sb
					.append("\nncol: ")
					.append(Byte.toUnsignedInt(record[coords[1][0] + 0x7]))
					.append(" nnew: ")
					.append(Byte.toUnsignedInt(record[coords[1][0] + 0x8]))
					.append(" size: ")
					.append(redoLog.bu().getU16(record, coords[1][0] + 0xA))
					.append(" flag: ")
					.append(String.format("0x%02x", Byte.toUnsignedInt(record[coords[1][0] + 0x6])))
					.append("\nnonkey columns updated:");
				for (int i = 0; i < columnCount; i++) {
					final int index = i + 3;
					printColumnBytes(sb, record[coords[2][0] + Short.BYTES * i], coords[index][1], index, 0);
				}
			} else {
				if (coords.length > 2) {
					sb
						.append(operation == _10_2_LIN ? "\ninsert key: (" : "\nkeydata: (")
						.append(coords[2][1])
						.append("):")
						.append(coords[2][1] > 0x14 ? "\n" : " ");
					for (int i = 0; i < coords[2][1]; i++) {
						if (i % 25 == 24 && i != coords[2][1] - 1)
							sb.append('\n');
						sb.append(String.format(" %02x", Byte.toUnsignedInt(record[coords[2][0] + i])));
					}
					if (nonKeyData) {
						printNonKeyData(sb, 3);
					}
				}
			}
		}
		return sb;
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

	private void printNonKeyData(final StringBuilder sb, final int index) {
		sb
			.append("\nfb: ")
			.append(printFbFlags(fb))
			.append(" lb: ")
			.append(String.format("0x%x", Byte.toUnsignedInt(record[coords[index][0] + 1])))
			.append("  cc: ")
			.append(columnCount)
			.append("\nnonkey (length: ")
			.append(coords[index][1])
			.append("):")
			.append("\n(")
			.append(coords[index][1] - 3)
			.append("):")
			.append(coords[index][1] - 3 > 0x14 ? "\n" : " ");
		for (int i = 3; i < coords[index][1]; i++) {
			if ((i - 3) % 25 == 24 && i != coords[index][1] - 1)
				sb.append('\n');
			sb.append(String.format(" %02x", Byte.toUnsignedInt(record[coords[index][0] + i])));
		}
	}

}
