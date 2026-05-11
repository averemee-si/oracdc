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

package solutions.a2.cdc.oracle.runtime.data;

import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.ResultSet.CONCUR_READ_ONLY;

import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleResultSet;
import oracle.sql.ROWID;

/**
 * 
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class RowIdStoreArrayList implements RowIdStore {

	private final List<ROWID> keyArray;
	
	RowIdStoreArrayList() {
		this.keyArray = new ArrayList<>();
	}

	@Override
	public void readKeys(final OracleConnection connection, final String sqlSelectKeys) throws SQLException {
		var statement = connection.prepareStatement(sqlSelectKeys,TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
		var resultSet = (OracleResultSet) statement.executeQuery();
		while (resultSet.next())
			keyArray.add(resultSet.getROWID(1));
		resultSet.close();
		resultSet = null;
		statement.close();
		statement = null;
	}

	@Override
	public int size() {
		return keyArray.size();
	}

	@Override
	public Array getRowIdArray(OracleConnection connection, int rowNumStart, int rowNumEnd) throws SQLException {
		var arraySize = rowNumEnd - rowNumStart;
		var rowIds = new String[arraySize];
		int i = 0;
		for (int rowNum = rowNumStart; rowNum < rowNumEnd; rowNum++)
			rowIds[i++] = keyArray.get(rowNum).stringValue();
		return connection.createOracleArray("SYS.ODCIVARCHAR2LIST", rowIds);
	}

	@Override
	public void close() {
	}

}
