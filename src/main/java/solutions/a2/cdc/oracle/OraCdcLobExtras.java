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

import java.util.HashMap;
import java.util.Map;

import solutions.a2.cdc.oracle.internals.OraCdcChangeLlb;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcLobExtras {

	private final Map<Integer, Map<Short, Short>[]> intColIdsMap = new HashMap<>();

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
