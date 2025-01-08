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
