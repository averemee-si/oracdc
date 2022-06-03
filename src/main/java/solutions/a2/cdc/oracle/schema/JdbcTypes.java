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

package solutions.a2.cdc.oracle.schema;

import java.sql.Types;

/**
 * 
 * @author averemee
 *
 */
public class JdbcTypes {

	public static String getTypeName(final int jdbcType) {
		switch (jdbcType) {
		case Types.DATE:
			return "DATE";
		case Types.TIMESTAMP:
			return "TIMESTAMP";
		case Types.TIMESTAMP_WITH_TIMEZONE:
			return "TIMESTAMP_WITH_TIMEZONE";
		case Types.BOOLEAN:
			return "BOOLEAN";
		case Types.TINYINT:
			return "TINYINT";
		case Types.SMALLINT:
			return "SMALLINT";
		case Types.INTEGER:
			return "INTEGER";
		case Types.BIGINT:
			return "BIGINT";
		case Types.FLOAT:
			return "FLOAT";
		case Types.DOUBLE:
			return "DOUBLE";
		case Types.DECIMAL:
			return "DECIMAL";
		case Types.NUMERIC:
			return "NUMERIC";
		case Types.BINARY:
			return "BINARY";
		case Types.CHAR:
			return "CHAR";
		case Types.VARCHAR:
			return "VARCHAR";
		case Types.NCHAR:
			return "NCHAR";
		case Types.NVARCHAR:
			return "NVARCHAR";
		case Types.ROWID:
			return "ROWID";
		case Types.CLOB:
			return "CLOB";
		case Types.NCLOB:
			return "NCLOB";
		case Types.BLOB:
			return "BLOB";
		case Types.SQLXML:
			return "XMLTYPE";
		}
		return "UNSUPPORTED!!!";
	}

	public static int getTypeId(final String jdbcTypeName) {
		switch (jdbcTypeName) {
		case "DATE":
			return Types.DATE;
		case "TIMESTAMP":
			return Types.TIMESTAMP;
		case "TIMESTAMP_WITH_TIMEZONE":
			return Types.TIMESTAMP_WITH_TIMEZONE;
		case "BOOLEAN":
			return Types.BOOLEAN;
		case "TINYINT":
			return Types.TINYINT;
		case "SMALLINT":
			return Types.SMALLINT;
		case "INTEGER":
			return Types.INTEGER;
		case "BIGINT":
			return Types.BIGINT;
		case "FLOAT":
			return Types.FLOAT;
		case "DOUBLE":
			return Types.DOUBLE;
		case "DECIMAL":
			return Types.DECIMAL;
		case "NUMERIC":
			return Types.NUMERIC;
		case "BINARY":
			return Types.BINARY;
		case "CHAR":
			return Types.CHAR;
		case "VARCHAR":
			return Types.VARCHAR;
		case "NCHAR":
			return Types.NCHAR;
		case "NVARCHAR":
			return Types.NVARCHAR;
		case "ROWID":
			return Types.ROWID;
		case "CLOB":
			return Types.CLOB;
		case "NCLOB":
			return Types.NCLOB;
		case "BLOB":
			return Types.BLOB;
		case "XMLTYPE":
			return Types.SQLXML;
		}
		return Integer.MIN_VALUE;
	}

	public static boolean isNumeric(final int jdbcType) {
		switch (jdbcType) {
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		case Types.BIGINT:
		case Types.FLOAT:
		case Types.DOUBLE:
		case Types.DECIMAL:
		case Types.NUMERIC:
			return true;
		default:
			return false;
		}
	}

	public static final String[] NUMERICS = {
		"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"
	};

}
