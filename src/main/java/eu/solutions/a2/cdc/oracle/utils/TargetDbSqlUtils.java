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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.solutions.a2.cdc.oracle.HikariPoolConnectionFactory;
import eu.solutions.a2.cdc.oracle.OraColumn;

/**
 * 
 * @author averemee
 *
 */
public class TargetDbSqlUtils {

	public static final int INSERT = 0;
	public static final int UPDATE = 1;
	public static final int DELETE = 2;

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

	public static String createTableSql(
			final String tableName,
			final int dbType,
			final Map<String, OraColumn> pkColumns,
			final List<OraColumn> allColumns) {
		final StringBuilder sbCreateTable = new StringBuilder(256);
		final StringBuilder sbPrimaryKey = new StringBuilder(64);

		final Map<Integer, String> dataTypesMap;
		if (dbType == HikariPoolConnectionFactory.DB_TYPE_POSTGRESQL) {
			dataTypesMap = POSTGRESQL_MAPPING;
		} else if (dbType == HikariPoolConnectionFactory.DB_TYPE_ORACLE) {
			dataTypesMap = ORACLE_MAPPING;
		} else {
			//TODO - more types required
			dataTypesMap = MYSQL_MAPPING;
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
			sbCreateTable.append(getTargetDbColumn(dbType, dataTypesMap, column));
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
			sbCreateTable.append(getTargetDbColumn(dbType, dataTypesMap, column));
			if (!column.isNullable()) {
				sbCreateTable.append(" not null");
			}
		}

		sbCreateTable.append(sbPrimaryKey);
		sbCreateTable.append("\n)");
		return sbCreateTable.toString();
	}

	private static String getTargetDbColumn(final int dbType, final Map<Integer, String> dataTypesMap, final OraColumn column) {
		final StringBuilder sb = new StringBuilder(64);
		sb.append(column.getColumnName());
		sb.append(" ");
		if (column.getJdbcType() != Types.DECIMAL)
			sb.append(dataTypesMap.get(column.getJdbcType()));
		else {
			if (dbType == HikariPoolConnectionFactory.DB_TYPE_POSTGRESQL || 
					dbType == HikariPoolConnectionFactory.DB_TYPE_ORACLE) {
				sb.append(dataTypesMap.get(column.getJdbcType()));
			} else if (dbType == HikariPoolConnectionFactory.DB_TYPE_MYSQL) {
				sb.append(dataTypesMap.get(column.getJdbcType()));
				sb.append("(38,");
				sb.append(column.getDataScale());
				sb.append(")");
			}
		}
		return sb.toString();
	}

	public static List<String> generateSinkSql(final String tableName,
			final int dbType,
			final Map<String, OraColumn> pkColumns,
			final List<OraColumn> allColumns) {

		final int pkColCount = pkColumns.size();
		final StringBuilder sbDelUpdWhere = new StringBuilder(128);
		sbDelUpdWhere.append(" where ");

		final StringBuilder sbInsSql = new StringBuilder(512);
		final StringBuilder sbOraMergeOnList  = new StringBuilder(64);
		final StringBuilder sbOraInsertList  = new StringBuilder(256);
		final StringBuilder sbOraValuesList  = new StringBuilder(256);
		if (dbType != HikariPoolConnectionFactory.DB_TYPE_ORACLE) {
			sbInsSql.append("insert into ");
			sbInsSql.append(tableName);
			sbInsSql.append("(");
		}
		final StringBuilder sbUpsert = new StringBuilder(128);
		if (dbType == HikariPoolConnectionFactory.DB_TYPE_POSTGRESQL) {
			sbUpsert.append(" on conflict(");
		} else if (dbType == HikariPoolConnectionFactory.DB_TYPE_MYSQL) {
			sbUpsert.append(" on duplicate key update ");
		} else if (dbType == HikariPoolConnectionFactory.DB_TYPE_ORACLE) {
			sbInsSql.append("merge into ");
			sbInsSql.append(tableName);
			sbInsSql.append(" D using(select ");
		}
		
		Iterator<Entry<String, OraColumn>> iterator = pkColumns.entrySet().iterator();
		int pkColumnNo = 0;
		while (iterator.hasNext()) {
			final String columnName = iterator.next().getValue().getColumnName();
			if (pkColumnNo > 0) {
				sbDelUpdWhere.append(" and ");
			}
			sbDelUpdWhere.append(columnName);
			sbDelUpdWhere.append("=?");

			if (dbType == HikariPoolConnectionFactory.DB_TYPE_ORACLE) {
				sbInsSql.append("? ");
				sbOraMergeOnList.append("D.");
				sbOraMergeOnList.append(columnName);
				sbOraMergeOnList.append("=");
				sbOraMergeOnList.append("ORACDC.");
				sbOraMergeOnList.append(columnName);
				sbOraInsertList.append(columnName);
				sbOraValuesList.append("ORACDC.");
				sbOraValuesList.append(columnName);
			}
			sbInsSql.append(columnName);
			if (pkColumnNo < pkColCount - 1) {
				sbInsSql.append(",");
				if (dbType == HikariPoolConnectionFactory.DB_TYPE_ORACLE) {
					sbOraMergeOnList.append(",");
					sbOraInsertList.append(",");
					sbOraValuesList.append(",");
				}
			}
			if (dbType == HikariPoolConnectionFactory.DB_TYPE_POSTGRESQL) {
				sbUpsert.append(columnName);
				if (pkColumnNo < pkColCount - 1) {
					sbUpsert.append(",");
				}
			}
			pkColumnNo++;
		}
		if (dbType == HikariPoolConnectionFactory.DB_TYPE_POSTGRESQL) {
			sbUpsert.append(") do update set ");
		} else if (dbType == HikariPoolConnectionFactory.DB_TYPE_ORACLE) {
				sbOraInsertList.append(",");
				sbOraValuesList.append(",");
		}

		final StringBuilder sbUpdSql = new StringBuilder(256);
		sbUpdSql.append("update ");
		sbUpdSql.append(tableName);
		sbUpdSql.append(" set ");
		final int nonPkColumnCount = allColumns.size();
		for (int i = 0; i < nonPkColumnCount; i++) {
			sbInsSql.append(",");
			if (dbType == HikariPoolConnectionFactory.DB_TYPE_ORACLE) {
				sbInsSql.append("? ");
			}
			sbInsSql.append(allColumns.get(i).getColumnName());

			sbUpdSql.append(allColumns.get(i).getColumnName());
			if (i < nonPkColumnCount - 1) {
				sbUpdSql.append("=?,");
			} else {
				sbUpdSql.append("=?");
			}
			if (dbType == HikariPoolConnectionFactory.DB_TYPE_POSTGRESQL) {
				sbUpsert.append(allColumns.get(i).getColumnName());
				sbUpsert.append("=EXCLUDED.");
				sbUpsert.append(allColumns.get(i).getColumnName());
				if (i < nonPkColumnCount - 1) {
					sbUpsert.append(",");
				}
			} else if (dbType == HikariPoolConnectionFactory.DB_TYPE_MYSQL) {
				sbUpsert.append(allColumns.get(i).getColumnName());
				sbUpsert.append("=VALUES(");
				sbUpsert.append(allColumns.get(i).getColumnName());
				sbUpsert.append(")");
				if (i < nonPkColumnCount - 1) {
					sbUpsert.append(",");
				}
			} else if (dbType == HikariPoolConnectionFactory.DB_TYPE_ORACLE) {
				sbUpsert.append("D.");
				sbUpsert.append(allColumns.get(i).getColumnName());
				sbUpsert.append("=");
				sbUpsert.append("ORACDC.");
				sbUpsert.append(allColumns.get(i).getColumnName());
				sbOraInsertList.append(allColumns.get(i).getColumnName());
				sbOraValuesList.append("ORACDC.");
				sbOraValuesList.append(allColumns.get(i).getColumnName());
				if (i < nonPkColumnCount - 1) {
					sbUpsert.append(",");
					sbOraInsertList.append(",");
					sbOraValuesList.append(",");
				}
			}
		}

		if (dbType == HikariPoolConnectionFactory.DB_TYPE_ORACLE) {
			sbInsSql.append(" from DUAL) ORACDC\non (");
			sbInsSql.append(sbOraMergeOnList);
			sbInsSql.append(")");
			sbInsSql.append("\nwhen matched then update\nset ");
			sbInsSql.append(sbUpsert);
			sbInsSql.append("\nwhen not matched then\ninsert(");
			sbInsSql.append(sbOraInsertList);
			sbInsSql.append(")");
			sbInsSql.append("\nvalues(");
			sbInsSql.append(sbOraValuesList);
			sbInsSql.append(")");
		} else {
			sbInsSql.append(") values(");
			final int totalColumns = nonPkColumnCount + pkColCount;
			for (int i = 0; i < totalColumns; i++) {
				if (i < totalColumns - 1) {
					sbInsSql.append("?,");
				} else {
					sbInsSql.append("?)");
				}
			}
			sbInsSql.append(sbUpsert);
		}

		final StringBuilder sbDelSql = new StringBuilder(128);
		sbDelSql.append("delete from ");
		sbDelSql.append(tableName);
		sbDelSql.append(sbDelUpdWhere);
		sbUpdSql.append(sbDelUpdWhere);

		final List<String> generatedSql = new ArrayList<>();
		generatedSql.add(INSERT, sbInsSql.toString());
		generatedSql.add(UPDATE, sbUpdSql.toString());
		generatedSql.add(DELETE, sbDelSql.toString());
		return generatedSql;
	}

}
