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

package eu.solutions.a2.cdc.oracle;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WriteMarshallable;

/**
 * Chronicle queue for LOB columns
 * 
 * @author averemee
 */
public class OraCdcLargeObjectHolder implements ReadMarshallable, WriteMarshallable {

	/** LOB object Id */
	private long lobId;
	/** LOB content */
	private byte[] content;
	/** SYS.XMLTYPE column name without dictionary i.e. "COL 2" */
	private String columnId;

	/**
	 * 
	 * Default constructor
	 * 
	 */
	public OraCdcLargeObjectHolder() {}

	/**
	 * 
	 * @param lobId
	 * @param content
	 */
	public OraCdcLargeObjectHolder(final long lobId, final byte[] content) {
		super();
		this.lobId = lobId;
		this.content = content;
	}

	/**
	 * 
	 * @param columnId
	 * @param content
	 */
	public OraCdcLargeObjectHolder(final String columnId, final byte[] content) {
		super();
		this.lobId = 0;
		this.columnId = columnId;
		this.content = content;
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

	@Override
	public void writeMarshallable(WireOut wire) {
		wire.bytes().writeLong(lobId);
		if (lobId == 0) {
			// only for SYS.XMLTYPE
			wire.bytes().write8bit(columnId);
		}
		if (content == null || content.length < 1) {
			wire.bytes().writeInt(-1);
		} else {
			wire.bytes().writeInt(content.length);
			wire.bytes().write(content);
		}
	}


	@Override
	public void readMarshallable(WireIn wire) throws IORuntimeException {
		Bytes<?> raw = wire.bytes();
		lobId = raw.readLong();
		if (lobId == 0) {
			// SYS.XMLTYPE
			columnId = raw.read8bit();
		}
		final int length = raw.readInt();
		if (length > 0) {
			content = new byte[length];
			raw.read(content);
		} else {
			content = new byte[0];
		}
	}


	@Override
	public boolean usesSelfDescribingMessage() {
		// TODO Auto-generated method stub
		return ReadMarshallable.super.usesSelfDescribingMessage();
	}
}
