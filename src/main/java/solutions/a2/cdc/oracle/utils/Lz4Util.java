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

package solutions.a2.cdc.oracle.utils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class Lz4Util {

	private static final LZ4Factory factory = LZ4Factory.fastestInstance();

	public static byte[] compress(final String stringData) {
		if (stringData == null || stringData.length() == 0) {
			throw new IllegalArgumentException("Cannot compress null or empty string");
		}
		final LZ4Compressor compressor = factory.fastCompressor();
		final byte[] byteData = stringData.getBytes(StandardCharsets.UTF_8);
		final int decompressedLength = byteData.length;
		final int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
		final byte[] compressed = new byte[maxCompressedLength];
		try {
			final int compressedLength =
					compressor.compress(byteData, 0, decompressedLength, compressed, 0, maxCompressedLength);
			return Arrays.copyOfRange(compressed, 0, compressedLength);
		} catch (LZ4Exception lz4e) {
			throw new IllegalArgumentException("Failed to compress string", lz4e);
		}
	}

	public static String decompress(final byte[] compressedData) {
		if (compressedData == null || compressedData.length == 0) {
			throw new IllegalArgumentException("Cannot decompress null or empty bytes");
		}
		LZ4SafeDecompressor decompressor = factory.safeDecompressor();
		//TODO 64??? or howto oversize buffer better...
		final byte[] decompressedBytes = new byte[compressedData.length * 64];
		try {
			final int decompressedLength =
					decompressor.decompress(compressedData, 0, compressedData.length, decompressedBytes, 0);
			return new String(Arrays.copyOfRange(decompressedBytes, 0, decompressedLength));
			
		} catch (LZ4Exception lz4e) {
			throw new IllegalArgumentException("Failed to uncompress byte array", lz4e);
		}
	}

}
