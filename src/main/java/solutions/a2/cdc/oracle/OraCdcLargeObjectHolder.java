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
