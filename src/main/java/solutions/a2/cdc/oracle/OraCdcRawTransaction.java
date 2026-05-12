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

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import solutions.a2.cdc.oracle.OraCdcTaskBase.Coords;
import solutions.a2.cdc.oracle.OraCdcTaskBase.XidCoords;
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
	private XidCoords xc;

	public OraCdcRawTransaction(Xid xid, ZoneId dbZoneId, int initialCapacity, OraCdcLobExtras lobExtras) {
		this(xid, dbZoneId, initialCapacity, lobExtras, null);
	}

	public OraCdcRawTransaction(Xid xid, ZoneId dbZoneId, int initialCapacity, OraCdcLobExtras lobExtras, Coords coords) {
		this.xid = xid;
		this.dbZoneId = dbZoneId;
		records = new ArrayList<>(initialCapacity);
		this.lobExtras = lobExtras;
		xc = new XidCoords(xid, coords);
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
		xc = null;
	}

	XidCoords xidCoords() {
		return xc;
	}

}
