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

package solutions.a2.cdc.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * PgRdbmsInfo: Various PostgreSQL Database routines
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class PgRdbmsInfo {

	private static final Logger LOGGER = LoggerFactory.getLogger(PgRdbmsInfo.class);

	/**
	 * Returns set of column names for primary key or it equivalent (unique with all non-null)
	 * 
	 * @param connection         - Connection to data dictionary
	 * @param tableOwner         - Table schema
	 * @param tableName          - Table name
	 * @return                   - Set with names of primary key columns. null if nothing found
	 * @throws SQLException
	 */
	public static Set<String> getPkColumnsFromDict(
			final Connection connection,
			final String tableOwner,
			final String tableName) throws SQLException {
		Set<String> result = null;
		PreparedStatement ps = connection.prepareStatement(
						PgDictSqlTexts.WELL_DEFINED_PK_COLUMNS,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ps.setString(1, tableOwner);
		ps.setString(2, tableName);

		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			if (result == null)
				result = new HashSet<>();
			result.add(rs.getString("COLUMN_NAME"));
		}
		rs.close();
		rs = null;
		ps.close();
		ps = null;
		if (result == null) {
			// Try to find unique index with non-null columns only
			ps = connection.prepareStatement(
							PgDictSqlTexts.LEGACY_DEFINED_PK_COLUMNS,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ps.setString(1, tableOwner);
			ps.setString(2, tableName);
			rs = ps.executeQuery();
			String indexOwner = null;
			String indexName = null;
			while (rs.next()) {
				if (result == null) {
					result = new HashSet<>();
					indexOwner = rs.getString("TABLE_SCHEMA");
					indexName = rs.getString("INDEX_NAME");
				}
				result.add(rs.getString("COLUMN_NAME"));
			}
			rs.close();
			rs = null;
			ps.close();
			ps = null;
			if (result != null) {
				printPkWarning(result, true, tableOwner, tableName,indexOwner, indexName);
			} else  {
				ps = connection.prepareStatement(
								PgDictSqlTexts.WELL_DEFINED_UNIQUE_COLUMNS,
						ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				ps.setString(1, tableOwner);
				ps.setString(2, tableName);
				rs = ps.executeQuery();
				while (rs.next()) {
					if (result == null) {
						result = new HashSet<>();
						indexOwner = rs.getString("TABLE_SCHEMA");
						indexName = rs.getString("INDEX_NAME");
					} else if (!StringUtils.equals(indexName, rs.getString("INDEX_NAME"))) {
						break;
					}
					result.add(rs.getString("COLUMN_NAME"));
				}
				rs.close();
				rs = null;
				ps.close();
				ps = null;
				if (result != null) {
					printPkWarning(result, false, tableOwner, tableName,indexOwner, indexName);
				}
			}
		}
		return result;
	}

	private static void printPkWarning(final Set<String> result, final boolean notNull,
			final String tableOwner, final String tableName,
			final String indexOwner, final String indexName) {
		final StringBuilder sb = new StringBuilder(128);
		boolean firstCol = true;
		for (String columnName : result) {
			if (firstCol) {
				firstCol = false;
			} else {
				sb.append(",");
			}
			sb.append(columnName);
		}
		LOGGER.info(
				"\n" +
				"=====================\n" +
				"Table {}.{} does not have a primary key constraint.\n" +
				"Unique index {}.{} with {}column(s) '{}' will be used instead of the missing primary key.\n" +
				"=====================\n",
				tableOwner, tableName, indexOwner, indexName, 
				(notNull ? "NOT NULL " : ""), sb.toString());
	}


}
