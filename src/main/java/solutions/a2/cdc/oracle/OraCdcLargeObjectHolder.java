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

package solutions.a2.cdc.oracle;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * Chronicle queue for LOB columns
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcLargeObjectHolder extends OraCdcRawStatementBase {

	/** LOB object Id */
	private long lobId;
	/** LOB content */
	private byte[] content;
	/** SYS.XMLTYPE column name without dictionary i.e. "COL 2" */
	private String columnId;
	/** Size of this structure */
	private int holderSize = 0;

	/**
	 * 
	 * Default constructor
	 * 
	 */
	public OraCdcLargeObjectHolder() {
		holderSize = Long.BYTES;
	}

	/**
	 * 
	 * @param lobId
	 * @param content
	 */
	public OraCdcLargeObjectHolder(final long lobId, final byte[] content) {
		this();
		this.lobId = lobId;
		this.content = content;
		holderSize += content.length;
	}

	/**
	 * 
	 * @param columnId
	 * @param content
	 */
	public OraCdcLargeObjectHolder(final String columnId, final byte[] content) {
		this();
		this.lobId = 0;
		this.columnId = columnId;
		this.content = content;
		// ColumnId is always US7ASCII!
		holderSize += (content.length + columnId.length());
	}

	public long getLobId() {
		return lobId;
	}

	public byte[] getContent() {
		return content;
	}

	public String getColumnId() {
		return columnId;
	}

	public int size() {
		return holderSize;
	}

	@Override
	public byte[] content() {
		var length = Long.BYTES;
		if (lobId == 0)
			length += 1 + columnId.length();
		length += Integer.BYTES + content.length;
		var bytes = new byte[length];
		putU64(bytes, lobId, 0);
		var pos = Long.BYTES;
		if (lobId == 0) {
			bytes[pos++] = (byte) columnId.length();
			System.arraycopy(columnId.getBytes(US_ASCII), 0, bytes, pos, columnId.length());
			pos += columnId.length();
		}
		System.arraycopy(content, 0, bytes, pos, content.length);
		return bytes;
	}

	public void restore(final byte[] bytes) {
		lobId = BIG_ENDIAN.getU64(bytes, 0);
		var pos = Long.BYTES;
		if (lobId == 0) {
			columnId = new String(bytes, pos, bytes[pos], US_ASCII);
			pos += bytes[pos];
		}
		var length = BIG_ENDIAN.getU32(bytes, pos);
		if (length > 0) {
			pos += Integer.BYTES;
			content = Arrays.copyOfRange(bytes, pos, pos + length);
		} else
			content = new byte[0];
	}

}
