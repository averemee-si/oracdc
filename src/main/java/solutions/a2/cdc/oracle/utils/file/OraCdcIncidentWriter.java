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

import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.TYPE_1;

import java.io.IOException;

import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.Xid;

/**
 * 
 * Writes transaction data to a binary file
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcIncidentWriter extends OraCdcIncidentBase {

	private long commitScn = 0L;
	private RedoByteAddress commitRba = RedoByteAddress.MIN_VALUE;

	OraCdcIncidentWriter(final Xid xid, final String outFileName) throws IOException {
		super(outFileName);
		raf.writeShort(xid.usn());
		raf.writeShort(xid.slt());
		raf.writeInt(xid.sqn());
		raf.seek(HEADER_SIZE);
	}

	void write(final OraCdcRedoRecord rr) throws IOException {
		if (rr.has5_4() && !rr.change5_4().rollback()) {
			commitScn = rr.scn();
			commitRba = rr.rba();
		}
		if (rr.hasLlb() && rr.changeLlb().type() == TYPE_1) {
			transFromLobId.add(rr.changeLlb().lid());
		}
		if (rr.hasColb()) {
			var colb = rr.changeColb();
			if (colb.longDump()) {
				if (colb.lid() == null ||
						(colb.lid() != null && !transFromLobId.contains(colb.lid())))
					return;
			}
		}
		raf.writeInt(rr.rba().sqn());
		raf.writeInt(rr.rba().blk());
		raf.writeShort(rr.rba().offset());
		raf.writeLong(rr.scn());
		final var content = rr.content();
		raf.writeInt(content.length);
		raf.write(content);
	}

	void writeHeader(long transStartScn, RedoByteAddress transStartRba, long transEndScn,
			RedoByteAddress transEndRba) throws IOException {
		raf.seek(2 * Short.BYTES + Integer.BYTES);
		raf.writeLong(transStartScn);
		raf.writeInt(transStartRba.sqn());
		raf.writeInt(transStartRba.blk());
		raf.writeShort(transStartRba.offset());
		raf.writeLong(transEndScn);
		raf.writeInt(transEndRba.sqn());
		raf.writeInt(transEndRba.blk());
		raf.writeShort(transEndRba.offset());
		raf.writeLong(commitScn);
		raf.writeInt(commitRba.sqn());
		raf.writeInt(commitRba.blk());
		raf.writeShort(commitRba.offset());
	}

}
