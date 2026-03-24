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
