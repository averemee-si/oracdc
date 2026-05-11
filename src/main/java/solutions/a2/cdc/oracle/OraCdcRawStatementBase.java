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

package solutions.a2.cdc.oracle;

import solutions.a2.oracle.utils.BinaryUtils;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public abstract class OraCdcRawStatementBase {

	public static final BinaryUtils BIG_ENDIAN = BinaryUtils.get(false);
	static final int APPROXIMATE_SIZE = 0x4000;

	abstract public byte[] content();
	abstract public void restore(final byte[] content);

	static void putU16(final byte[] ba, final short u16, final int offset) {
		ba[offset] = (byte)(u16 >>> 8);
		ba[offset + 1] = (byte)u16;
	}

	static void putU32(final byte[] ba, final int u32, final int offset) {
		ba[offset] = (byte)(u32 >>> 24);
		ba[offset + 1] = (byte)(u32 >>> 16);
		ba[offset + 2] = (byte)(u32 >>> 8);
		ba[offset + 3] = (byte)u32;
	}

	static void putU64(final byte[] ba, final long u64, final int offset) {
		ba[offset] = (byte)(u64 >>> 56);
		ba[offset + 1] = (byte)(u64 >>> 48);
		ba[offset + 2] = (byte)(u64 >>> 40);
		ba[offset + 3] = (byte)(u64 >>> 32);
		ba[offset + 4] = (byte)(u64 >>> 24);
		ba[offset + 5] = (byte)(u64 >>> 16);
		ba[offset + 6] = (byte)(u64 >>> 8);
		ba[offset + 7] = (byte)u64;
	}

}
