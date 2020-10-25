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
	private int lobId;
	/** LOB content */
	private byte[] content;

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
	public OraCdcLargeObjectHolder(final int lobId, final byte[] content) {
		super();
		this.lobId = lobId;
		this.content = content;
	}

	public int getLobId() {
		return lobId;
	}

	public byte[] getContent() {
		return content;
	}

	@Override
	public void writeMarshallable(WireOut wire) {
		wire.bytes().writeInt(lobId);
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
		lobId = raw.readInt();
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
