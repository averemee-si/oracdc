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
 * KRVXML - xmlredo - doc or dif - opcode
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Presentations/RedoInternals.ppt">Redo Internals</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcChangeKrvXml extends OraCdcChange {

	private static final Logger LOGGER = LogManager.getLogger(OraCdcChangeKrvXml.class);
	private byte type;
	private short internalColId;

	public static final byte TYPE_XML_DOC = 0x1;

	public static final short XML_DOC_BEGIN = 0x01;
	public static final short XML_DOC_END = 0x02;

	OraCdcChangeKrvXml(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, _24_8_XML, record, offset, headerLength);
		if (encrypted && !redoLog.tsEncKeyAvailable())
			return;
		elementNumberCheck(1);
		if (record[coords[0][0]] == TYPE_XML_DOC) {
			type = TYPE_XML_DOC;
			elementNumberCheck(0x8);
			if (coords[2][1] < 0x8) {
				LOGGER.error(
						"\n=====================\n" +
						"Not enough data to create OP:24.8 at RBA {}.\nChange binary dump:\n{}" +
						"\n=====================\n",
						rba, binaryDump());
				throw new IllegalArgumentException("Unable to create OP:24.8");
			}
			xid = new Xid(
					redoLog.bu().getU16(record, coords[2][0]),
					redoLog.bu().getU16(record, coords[2][0] + 0x02),
					redoLog.bu().getU32(record, coords[2][0] + 0x04));
			if (coords[3][1] < 0x4) {
				LOGGER.error(
						"\n=====================\n" +
						"Not enough data to create OP:24.8 at RBA {}.\nChange binary dump:\n{}" +
						"\n=====================\n",
						rba, binaryDump());
				throw new IllegalArgumentException("Unable to create OP:24.8");
			}
			obj = redoLog.bu().getU32(record, coords[3][0]);
			if (coords[5][1] < 0x2) {
				LOGGER.error(
						"\n=====================\n" +
						"Not enough data to create OP:24.8 at RBA {}.\nChange binary dump:\n{}" +
						"\n=====================\n",
						rba, binaryDump());
				throw new IllegalArgumentException("Unable to create OP:24.8");
			}
			internalColId = redoLog.bu().getU16(record, coords[5][0]);
		} else {
			type = 0;
		}
	}

	public short internalColId() {
		return internalColId;
	}

	public short status() {
		if (type == 1)
			return redoLog.bu().getU16(record, coords[6][0]);
		else
			return (short) 0xFFFF;
	}

	public byte type() {
		return type;
	}

	@Override
	StringBuilder toDumpFormat() {
		final StringBuilder sb = super.toDumpFormat();
		if (encrypted && !redoLog.tsEncKeyAvailable())
			return sb;
		if (type == 0x1) {
			sb
				.append("\nXML redo opcode type: ")
				.append(Byte.toUnsignedInt(type))
				.append(" xid:")
				.append(xid)
				.append(" obj:")
				.append(Integer.toUnsignedLong(obj))
				.append("\ninternal column id: ")
				.append(Short.toUnsignedInt(internalColId))
				.append("\nXML data load\n");
			printLobContent(sb, 0x7, 0);
		}
		return sb;
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

}
