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

import java.util.concurrent.CountDownLatch;

import solutions.a2.oracle.internals.RedoByteAddress;

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

}