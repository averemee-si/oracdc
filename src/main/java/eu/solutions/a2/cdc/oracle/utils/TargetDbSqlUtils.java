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

package eu.solutions.a2.cdc.oracle.utils;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.solutions.a2.cdc.oracle.HikariPoolConnectionFactory;
import eu.solutions.a2.cdc.oracle.OraColumn;

public class TargetDbSqlUtils {

	@SuppressWarnings("serial")
	private static final Map<Integer, String> MYSQL_MAPPING =
			Collections.unmodifiableMap(new HashMap<Integer, String>() {{
				put(Types.BOOLEAN, "tinyint");
				put(Types.TINYINT, "tinyint");
				put(Types.SMALLINT, "smallint");
				put(Types.INTEGER, "int");
				put(Types.BIGINT, "bigint");
				put(Types.FLOAT, "float");
				put(Types.DOUBLE, "double");
				put(Types.DECIMAL, "decimal");
				put(Types.DATE, "datetime");
				put(Types.TIMESTAMP, "timestamp");
				put(Types.VARCHAR, "varchar(255)");
				put(Types.BINARY, "varbinary(1000)");
			}});
	@SuppressWarnings("serial")
	private static final Map<Integer, String> POSTGRESQL_MAPPING =
			Collections.unmodifiableMap(new HashMap<Integer, String>() {{
				put(Types.BOOLEAN, "boolean");
				put(Types.TINYINT, "smallint");
				put(Types.SMALLINT, "smallint");
				put(Types.INTEGER, "integer");
				put(Types.BIGINT, "bigint");
				put(Types.FLOAT, "real");
				put(Types.DOUBLE, "double precision");
				put(Types.DECIMAL, "numeric");
				put(Types.DATE, "timestamp");
				put(Types.TIMESTAMP, "timestamp");
				put(Types.VARCHAR, "text");
				put(Types.BINARY, "bytea");
			}});
	@SuppressWarnings("serial")
	private static final Map<Integer, String> ORACLE_MAPPING =
			Collections.unmodifiableMap(new HashMap<Integer, String>() {{
				put(Types.BOOLEAN, "CHAR(1)");
				put(Types.TINYINT, "NUMBER(3)");
				put(Types.SMALLINT, "NUMBER(5)");
				put(Types.INTEGER, "NUMBER(10)");
				put(Types.BIGINT, "NUMBER(19)");
				put(Types.FLOAT, "BINARY_FLOAT");
				put(Types.DOUBLE, "BINARY_DOUBLE");
				put(Types.DECIMAL, "NUMBER");
				put(Types.DATE, "DATE");
				put(Types.TIMESTAMP, "TIMESTAMP");
				put(Types.VARCHAR, "VARCHAR2(4000)");
				put(Types.BINARY, "RAW(2000)");
			}});
	private static Map<Integer, String> dataTypes = null;

	public static String createTableSql(
			final String tableName,
			final HashMap<String, OraColumn> pkColumns,
			final List<OraColumn> allColumns) {
		final StringBuilder sbCreateTable = new StringBuilder(256);
		final StringBuilder sbPrimaryKey = new StringBuilder(64);
		final int dbType = HikariPoolConnectionFactory.getDbType();

		if (dataTypes == null) {
			if (dbType == HikariPoolConnectionFactory.DB_TYPE_POSTGRESQL) {
				dataTypes = POSTGRESQL_MAPPING;
			} else if (dbType == HikariPoolConnectionFactory.DB_TYPE_ORACLE) {
				dataTypes = ORACLE_MAPPING;
			} else {
				//TODO - more types required
				dataTypes = MYSQL_MAPPING;
			}
		}

		sbCreateTable.append("create table ");
		sbCreateTable.append(tableName);
		sbCreateTable.append("(\n");

		sbPrimaryKey.append(",\nconstraint ");
		sbPrimaryKey.append(tableName);
		sbPrimaryKey.append("_PK primary key(");
		
		Iterator<Entry<String, OraColumn>> iterator = pkColumns.entrySet().iterator();
		while (iterator.hasNext()) {
			OraColumn column = iterator.next().getValue();
			sbCreateTable.append(" ");
			sbCreateTable.append(getTargetDbColumn(dbType, column));
			sbCreateTable.append(" not null");

			sbPrimaryKey.append(column.getColumnName());

			if (iterator.hasNext()) {
				sbCreateTable.append(",\n");
				sbPrimaryKey.append(",");
			}
		}
		sbPrimaryKey.append(")");

		final int nonPkColumnCount = allColumns.size();
		for (int i = 0; i < nonPkColumnCount; i++) {
			OraColumn column = allColumns.get(i);
			sbCreateTable.append(",\n  ");
			sbCreateTable.append(getTargetDbColumn(dbType, column));
			if (!column.isNullable()) {
				sbCreateTable.append(" not null");
			}
		}

		sbCreateTable.append(sbPrimaryKey);
		sbCreateTable.append("\n)");
		return sbCreateTable.toString();
	}

	private static String getTargetDbColumn(final int dbType, final OraColumn column) {
		final StringBuilder sb = new StringBuilder(64);
		sb.append(column.getColumnName());
		sb.append(" ");
		if (column.getJdbcType() != Types.DECIMAL)
			sb.append(dataTypes.get(column.getJdbcType()));
		else {
			if (dbType == HikariPoolConnectionFactory.DB_TYPE_POSTGRESQL || 
					dbType == HikariPoolConnectionFactory.DB_TYPE_ORACLE) {
				sb.append(dataTypes.get(column.getJdbcType()));
			} else if (dbType == HikariPoolConnectionFactory.DB_TYPE_MYSQL) {
				sb.append(dataTypes.get(column.getJdbcType()));
				sb.append("(38,");
				sb.append(column.getDataScale());
				sb.append(")");
			}
		}
		return sb.toString();
	}

}
