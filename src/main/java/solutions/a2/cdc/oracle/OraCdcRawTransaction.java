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

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
import solutions.a2.oracle.internals.Xid;

/**
 * 
 * Dictionary check utilities
 * 
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class OraCdcRawTransaction {

	private final Xid xid;
	private final ZoneId dbZoneId;
	private List<OraCdcRedoRecord> records;
	private final OraCdcLobExtras lobExtras;
	private long commitScn;
	private long size = 0;

	public OraCdcRawTransaction(Xid xid, ZoneId dbZoneId, int initialCapacity, OraCdcLobExtras lobExtras) {
		this.xid = xid;
		this.dbZoneId = dbZoneId;
		records = new ArrayList<>(initialCapacity);
		this.lobExtras = lobExtras;
	}

	Xid xid() {
		return xid;
	}

	ZoneId dbZoneId() {
		return dbZoneId;
	}

	public void add(final OraCdcRedoRecord rr, int ts) {
		rr.ts(ts);
		records.add(rr);
		size += rr.len();
	}

	List<OraCdcRedoRecord> records() {
		Collections.sort(records);
		return records;
	}

	public void commitScn(final long commitScn) {
		this.commitScn = commitScn;
	}

	long commitScn() {
		return commitScn;
	}

	OraCdcLobExtras lobExtras() {
		return lobExtras;
	}

	long firstChange() {
		return  (records.size() > 0) ? records.get(0).scn() : 0; 
	}

	long nextChange() {
		return  (records.size() > 0) ? records.get(records.size() - 1).scn() : 0; 
	}

	long size() {
		return size;
	}
	
	int length() {
		return records.size();
	}

	boolean hasRows() {
		return records.size() > 0;
	}

	public void close() {
		records.clear();
		records = null;
	}

}
