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

import solutions.a2.oracle.internals.RedoByteAddress;

import static solutions.a2.cdc.oracle.internals.OraCdcChange.formatOpCode;

/**
 * 
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcParseException extends Exception {

	private static final long serialVersionUID = 8984978265320163772L;
	private final RedoByteAddress rba;
	private final byte[] bytes;
	private final short num;
	private final short operation;

	OraCdcParseException(final String message, final RedoByteAddress rba, final byte[] bytes, final short num, final short operation) {
		super(message);
		this.rba = rba;
		this.bytes = bytes;
		this.num = num;
		this.operation = operation;
	}

	RedoByteAddress rba() {
		return rba;
	}

	short num() {
		return num;
	}

	String opCode () {
		if (operation > 0)
			return formatOpCode(operation);
		else
			return Short.toString(operation);
	}

	public String recordBytes() {
		if (bytes != null) {
			final int recordSize = bytes.length;
			final StringBuilder sb = new StringBuilder(recordSize * 4);
			for (int i = 0; i < recordSize; i++) {
				sb
					.append(" ")
					.append(String.format("%02x", Byte.toUnsignedInt(bytes[i])))
					.append((i % 0x19 == 0x18) && (i != recordSize -1) ? "\n" : "" );
			}
			return sb.toString();
		} else {
			return null;
		}
	}

}
