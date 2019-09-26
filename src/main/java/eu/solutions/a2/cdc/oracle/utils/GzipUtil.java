/**
 * Copyright (c) 2018-present, http://a2-solutions.eu
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

package eu.solutions.a2.cdc.oracle.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipUtil {

	public static byte[] compress(final String stringData) {
		if (stringData == null || stringData.length() == 0) {
			throw new IllegalArgumentException("Cannot compress null or empty string");
		}
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
				gzos.write(stringData.getBytes(StandardCharsets.UTF_8));
			}
			return baos.toByteArray();
		} catch (IOException ioe) {
			throw new IllegalArgumentException("Failed to compress string", ioe);
		}
	}

	public static String decompress(final byte[] compressedData) {
		if (compressedData == null || compressedData.length == 0) {
			throw new IllegalArgumentException("Cannot decompress null or empty bytes");
		}
		if (!isCompressed(compressedData)) {
			return new String(compressedData);
		} else {
			try (ByteArrayInputStream bais = new ByteArrayInputStream(compressedData)) {
				try (GZIPInputStream gzis = new GZIPInputStream(bais)) {
					try (InputStreamReader isr = new InputStreamReader(gzis, StandardCharsets.UTF_8)) {
						try (BufferedReader br = new BufferedReader(isr)) {
							StringBuilder result = new StringBuilder();
							String line = null;
							while((line = br.readLine()) != null) {
								result.append(line);
							}
							return result.toString();
						}
					}
				}
			} catch (IOException ioe) {
				throw new IllegalArgumentException("Failed to uncompress byte array", ioe);
			}
		}
	}

	public static boolean isCompressed(final byte[] compressedData) {
		return (compressedData[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) &&
				(compressedData[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
	}
}
