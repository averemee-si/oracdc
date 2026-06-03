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

import java.util.Comparator;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

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

		private static final Logger LOGGER = LogManager.getLogger(XidCoords.class);

		static final Comparator<XidCoords> COMPARATOR = new Comparator<>() {
			@Override
			public int compare(XidCoords first, XidCoords second) {
				if (first == null || second == null) {
					String what = null;
					String xid = null;
					String scn = null;
					String rba = null;
					String subscn = null;
					if (first == null && second == null) {
						what = "BOTH";
						xid = "N/A";
						scn = "N/A";
						rba = "N/A";
						subscn = "N/A";
					} else if (first == null) {
						what = "FIRST";
						xid = second.xid().toString();
						scn = Long.toUnsignedString(second.coords().scn());
						rba = second.coords().rba().toString();
						subscn = Long.toUnsignedString(second.coords().subScn());
					} else {
						what = "SECOND";
						xid = first.xid().toString();
						scn = Long.toUnsignedString(first.coords().scn());
						rba = first.coords().rba().toString();
						subscn = Long.toUnsignedString(first.coords().subScn());
					}
					LOGGER.warn(
							"""
							
							=====================
							{} operand(s) is(are) NULL!
							Non-NULL operand (if present):
							XID={}, SCN={}, RBA={}, SUBSCN={}
							=====================
							
							""",
								what, xid, scn, rba, subscn);
					return 0;
				}
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