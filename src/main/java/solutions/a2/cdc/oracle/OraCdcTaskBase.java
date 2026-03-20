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

import java.util.Comparator;
import java.util.concurrent.CountDownLatch;

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.Xid;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public interface OraCdcTaskBase {

	OraCdcSourceConnectorConfig config();
	OraConnectionObjects oraConnections();
	OraRdbmsInfo rdbmsInfo();
	CountDownLatch runLatch();
	int getTableVersion(final long combinedDataObjectId);
	void putTableVersion(final long combinedDataObjectId, final int version);
	void stop();
	void stop(boolean stopWorker);
	void putReadRestartScn(final Coords transData);

	public static class Coords {
		private long scn;
		private RedoByteAddress rba;
		private long subScn;
		public Coords() {}
		public Coords(long scn, RedoByteAddress rba, long subScn) {
			this.scn = scn;
			this.rba = rba;
			this.subScn = subScn;
		}
		public void init(long scn, RedoByteAddress rba, long subScn) {
			this.scn = scn;
			this.rba = rba;
			this.subScn = subScn;
		}
		public long scn() {
			return scn;
		}
		public RedoByteAddress rba() {
			return rba;
		}
		public long subScn() {
			return subScn;
		}
		public void resetRbaSubScn() {
			rba = null;
			subScn = -1L;
		}
	};

	record XidCoords(Xid xid, Coords coords) {
		static final Comparator<XidCoords> COMPARATOR = new Comparator<>() {
			@Override
			public int compare(XidCoords first, XidCoords second) {
				var scnComparision = Long.compareUnsigned(first.coords.scn(), second.coords.scn());
				return scnComparision == 0
					? Long.compare(first.coords.subScn(), second.coords.subScn())
					: scnComparision;
			}
		};
		@Override
		public int hashCode() {
			return xid.hashCode();
		}
		@Override
		public boolean equals(Object other) {
			if (this == other) return true;
			if (other == null) return false;
			if (!(other instanceof XidCoords)) return false;
			var xrt = (XidCoords) other;
			return xid.equals(xrt.xid);
		}
	}


}