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

package solutions.a2.cdc.oracle.utils.file;

import java.io.IOException;
import java.io.RandomAccessFile;

import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
import solutions.a2.oracle.internals.Xid;

/**
 * 
 * Writes transaction data to a binary file
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcIncidentWriter implements AutoCloseable {

	private static final int HEADER_SIZE = 0x200;
	private final RandomAccessFile out;
	private final String outFileName;
	

	OraCdcIncidentWriter(final Xid xid, final String outFileName) throws IOException {
		this.outFileName = outFileName;
		out = new RandomAccessFile(outFileName, "rw");
		out.writeShort(xid.usn());
		out.writeShort(xid.slt());
		out.writeInt(xid.sqn());
		out.seek(HEADER_SIZE - 2 * Short.BYTES - Integer.BYTES);
	}

	public void write(final OraCdcRedoRecord rr) throws IOException {
		out.writeInt(rr.rba().sqn());
		out.writeInt(rr.rba().blk());
		out.writeShort(rr.rba().offset());
		out.writeLong(rr.scn());
		final var content = rr.content();
		out.writeInt(content.length);
		out.write(content);
	}

	@Override
	public void close() throws IOException {
		out.close();
	}

	public String outFileName() {
		return outFileName;
	}

}
