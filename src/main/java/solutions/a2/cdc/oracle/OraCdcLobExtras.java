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

 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle;

import java.util.HashMap;
import java.util.Map;

import org.agrona.collections.Int2ObjectHashMap;

import solutions.a2.cdc.oracle.internals.OraCdcChangeLlb;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcLobExtras {

	private final Int2ObjectHashMap<Map<Short, Short>[]> intColIdsMap = new Int2ObjectHashMap<>();

	public short intColumnId(final int obj, final short col, final boolean direct) {
		final Map<Short, Short>[] columns = intColIdsMap.get(obj);
		if (columns == null)
			return -1;
		else {
			Short intColumnIdBoxed = columns[direct ? 0 : 1].get(col);
			if (intColumnIdBoxed == null)
				return -1;
			else
				return intColumnIdBoxed;
		}
	}

	@SuppressWarnings("unchecked")
	public void buildColMap(final OraCdcChangeLlb llb) {
		final short[][] columnMap = llb.columnMap();
		Map<Short, Short>[] columns = intColIdsMap.get(llb.obj());
		if (columns == null) {
			columns = (Map<Short, Short>[]) new Map[2];
			columns[0] = new HashMap<>();
			columns[1] = new HashMap<>();
			intColIdsMap.put(llb.obj(), columns);
		}
		for (int i = 0; i < columnMap.length; i++)
			if (!columns[0].containsKey(columnMap[i][0])) {
				columns[0].put(columnMap[i][0], columnMap[i][1]);
				columns[1].put(columnMap[i][1], columnMap[i][0]);
			}
	}

}
