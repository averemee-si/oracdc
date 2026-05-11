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

package solutions.a2.cdc.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.Strings;
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
		final Set<String> result = new HashSet<>();
		PreparedStatement ps = connection.prepareStatement(
						PgDictSqlTexts.WELL_DEFINED_PK_COLUMNS,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ps.setString(1, tableOwner);
		ps.setString(2, tableName);

		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			result.add(rs.getString("COLUMN_NAME"));
		}
		rs.close();
		rs = null;
		ps.close();
		ps = null;
		if (result.size() == 0) {
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
				if (result.size() == 0) {
					indexOwner = rs.getString("TABLE_SCHEMA");
					indexName = rs.getString("INDEX_NAME");
				} else {
					if ((!Strings.CS.equals(indexName, rs.getString("INDEX_NAME"))) || 
							(!Strings.CS.equals(indexOwner, rs.getString("TABLE_SCHEMA")))) {
						break;
					}
				}
				result.add(rs.getString("COLUMN_NAME"));
			}
			rs.close();
			rs = null;
			ps.close();
			ps = null;
			if (result.size() > 0) {
				printPkWarning(result, true, tableOwner, tableName,indexOwner, indexName);
			} else  {
				ps = connection.prepareStatement(
								PgDictSqlTexts.WELL_DEFINED_UNIQUE_COLUMNS,
						ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				ps.setString(1, tableOwner);
				ps.setString(2, tableName);
				rs = ps.executeQuery();
				while (rs.next()) {
					if (result.size() == 0) {
						indexOwner = rs.getString("TABLE_SCHEMA");
						indexName = rs.getString("INDEX_NAME");
					} else if ((!Strings.CS.equals(indexName, rs.getString("INDEX_NAME"))) || 
							(!Strings.CS.equals(indexOwner, rs.getString("TABLE_SCHEMA")))) {
						break;
					}
					result.add(rs.getString("COLUMN_NAME"));
				}
				rs.close();
				rs = null;
				ps.close();
				ps = null;
				if (result.size() > 0) {
					printPkWarning(result, false, tableOwner, tableName,indexOwner, indexName);
				} else {
					LOGGER.warn(
							"\n" +
							"=====================\n" +
							"Table {}.{} does not have a primary or unique key constraint.\n" +
							"Only INSERT operation will be performed for this table!\n" +
							"=====================\n",
							tableOwner, tableName);
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
