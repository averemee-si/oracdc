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

package solutions.a2.cdc.oracle.utils;

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
