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

package eu.solutions.a2.cdc.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OraRdbmsInfo {

	private final String versionString;
	private final int versionMajor;
	private final boolean cdb;
	private final boolean cdbRoot;

	private final static int CDB_INTRODUCED = 12;

	public OraRdbmsInfo(final Connection connection) throws SQLException {
		PreparedStatement ps = connection.prepareStatement(OraDictSqlTexts.RDBMS_VERSION,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = ps.executeQuery();
		if (rs.next())
			versionString = rs.getString("VERSION");
		else
			throw new SQLException("Unable to detect RDBMS version!");
		rs.close();
		rs = null;
		ps.close();
		ps = null;

		versionMajor = Integer.parseInt(
				versionString.substring(0, versionString.indexOf(".")));

		if (versionMajor < CDB_INTRODUCED) {
			cdb = false;
			cdbRoot = false;
		} else {
			ps = connection.prepareStatement(OraDictSqlTexts.CHECK_CDB_PDB,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = ps.executeQuery();
			if (rs.next()) {
				if ("YES".equalsIgnoreCase(rs.getString("CDB"))) {
					cdb = true;
					if ("CDB$ROOT".equalsIgnoreCase(rs.getString("CON_NAME")))
						cdbRoot = true;
					else
						cdbRoot = false;
				} else {
					cdb = false;
					cdbRoot = false;
				}
			} else
				throw new SQLException("Unable to detect CDB/PDB status!");
			rs.close();
			rs = null;
			ps.close();
			ps = null;
		}
	}


	public String getVersionString() {
		return versionString;
	}

	public int getVersionMajor() {
		return versionMajor;
	}

	public boolean isCdb() {
		return cdb;
	}

	public boolean isCdbRoot() {
		return cdbRoot;
	}

}
