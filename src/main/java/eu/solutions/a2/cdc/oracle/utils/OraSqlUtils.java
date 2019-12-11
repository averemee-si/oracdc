/**
 * Copyright (c) 2018-present, http://a2-solutions.eu
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

package eu.solutions.a2.cdc.oracle.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import eu.solutions.a2.cdc.oracle.OraDictSqlTexts;

public class OraSqlUtils {

	private static final String SQL_AND = " and ";

	public static String parseTableSchemaList(final boolean exclude, final boolean logMiner, final List<String> listSchemaObj) {
		String schemaNameField = "L.LOG_OWNER";
		String objNameField = "L.MASTER";
		if (logMiner) {
			schemaNameField = "M.SEG_OWNER";
			objNameField = "M.SEG_NAME";
		}

		final StringBuilder sb = new StringBuilder(512);
		sb.append(SQL_AND);
		sb.append("(");

		for (int i = 0; i < listSchemaObj.size(); i++) {
			final String schemaObj = listSchemaObj.get(i).trim();
			if (schemaObj.contains(".")) {
				final String[] pairSchemaObj = schemaObj.split("\\.");
				if ("%".equals(pairSchemaObj[1]) || "*".equals(pairSchemaObj[1])) {
					// Only schema name present
					sb.append("(");
					sb.append(schemaNameField);
					sb.append(exclude ? "!='" : "='");
					sb.append(pairSchemaObj[0]);
					sb.append("')");
				} else {
					// Process pair... ... ...
					sb.append("(");
					sb.append(schemaNameField);
					sb.append(exclude ? "!='" : "='");
					sb.append(pairSchemaObj[0]);
					sb.append("'");
					sb.append(SQL_AND);
					sb.append(objNameField);
					sb.append(exclude ? "!='" : "='");
					sb.append(pairSchemaObj[1]);
					sb.append("')");
				}
			} else {
				// Just plain table name without owner
				sb.append("(");
				sb.append(objNameField);
				sb.append(exclude ? "!='" : "='");
				sb.append(schemaObj);
				sb.append("')");
			}
			if (i < listSchemaObj.size() - 1) {
				if (exclude)
					sb.append(SQL_AND);
				else
					sb.append(" or ");
			}
		}

		sb.append(")");
		return sb.toString();
	}

	public static Set<String> getPkColumnsFromDict(
			final Connection connection,
			final String owner,
			final String tableName) throws SQLException {
		Set<String> result = null;
		PreparedStatement ps = connection.prepareStatement(OraDictSqlTexts.WELL_DEFINED_PK_COLUMNS);
		ps.setString(1, owner);
		ps.setString(2, owner);
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
			ps = connection.prepareStatement(OraDictSqlTexts.LEGACY_DEFINED_PK_COLUMNS);
			ps.setString(1, owner);
			ps.setString(2, owner);
			rs = ps.executeQuery();
			while (rs.next()) {
				if (result == null)
					result = new HashSet<>();
				result.add(rs.getString("COLUMN_NAME"));
			}
			rs.close();
			rs = null;
			ps.close();
			ps = null;
		}
		return result;
	}

}
