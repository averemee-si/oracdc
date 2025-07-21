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

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WriteMarshallable;
import solutions.a2.cdc.oracle.data.OraBlob;
import solutions.a2.cdc.oracle.data.OraClob;
import solutions.a2.cdc.oracle.data.OraNClob;
import solutions.a2.cdc.oracle.data.OraXml;

import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.SQLXML;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

/**
 * Chronicle queue for LOB columns
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcLargeObjectHolder implements ReadMarshallable, WriteMarshallable {

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

	public Struct getContent(final int jdbcType) throws SQLException {
		if (jdbcType == BLOB) {
			final Struct blob = new Struct(OraBlob.schema());
			blob.put("V",  content);
			return blob;
		} else if (jdbcType == CLOB || jdbcType == NCLOB) {
			final Struct lob = new Struct(jdbcType == CLOB ? OraClob.schema() : OraNClob.schema());
			try {
				lob.put("V",  new String(content, "UTF-16"));
			} catch (UnsupportedEncodingException e) {
				throw new SQLException("Invalid encoding for UTF-16 encoded LOB for HEXTORAW " + rawToHex(content) +  ".", e);
			}
			return lob;
		} else if (jdbcType == SQLXML) {
			final Struct xml = new Struct(OraXml.schema());
			try {
				xml.put("V",  new String(content, "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				throw new SQLException("Invalid encoding for UTF-8 encoded XML for HEXTORAW " + rawToHex(content) +  ".", e);
			}
			return xml;
		}
		return null;
	}

	public String getColumnId() {
		return columnId;
	}

	public int size() {
		return holderSize;
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
