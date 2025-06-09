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

import static java.sql.Types.CHAR;
import static java.sql.Types.VARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.TINYINT;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.BIGINT;
import static java.sql.Types.FLOAT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.DATE;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.BINARY;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.ROWID;
import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.SQLXML;

import static oracle.jdbc.OracleTypes.INTERVALDS;
import static oracle.jdbc.OracleTypes.INTERVALYM;
import static oracle.jdbc.OracleTypes.JSON;
import static oracle.jdbc.OracleTypes.VECTOR;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class JdbcTypes {

	public static String getTypeName(final int jdbcType) {
		switch (jdbcType) {
		case DATE:
			return "DATE";
		case TIMESTAMP:
			return "TIMESTAMP";
		case TIMESTAMP_WITH_TIMEZONE:
			return "TIMESTAMP_WITH_TIMEZONE";
		case INTERVALDS:
			return "INTERVAL DAY TO SECOND";
		case INTERVALYM:
			return "INTERVAL YEAR TO MONTH";
		case BOOLEAN:
			return "BOOLEAN";
		case TINYINT:
			return "TINYINT";
		case SMALLINT:
			return "SMALLINT";
		case INTEGER:
			return "INTEGER";
		case BIGINT:
			return "BIGINT";
		case FLOAT:
			return "FLOAT";
		case DOUBLE:
			return "DOUBLE";
		case DECIMAL:
			return "DECIMAL";
		case NUMERIC:
			return "NUMERIC";
		case BINARY:
			return "BINARY";
		case CHAR:
			return "CHAR";
		case VARCHAR:
			return "VARCHAR";
		case NCHAR:
			return "NCHAR";
		case NVARCHAR:
			return "NVARCHAR";
		case ROWID:
			return "ROWID";
		case CLOB:
			return "CLOB";
		case NCLOB:
			return "NCLOB";
		case BLOB:
			return "BLOB";
		case SQLXML:
			return "XMLTYPE";
		case JSON:
			return "JSON";
		case VECTOR:
			return "VECTOR";
		}
		return "UNSUPPORTED!!!";
	}

	public static int getTypeId(final String jdbcTypeName) {
		switch (jdbcTypeName) {
		case "DATE":
			return DATE;
		case "TIMESTAMP":
			return TIMESTAMP;
		case "TIMESTAMP_WITH_TIMEZONE":
			return TIMESTAMP_WITH_TIMEZONE;
		case "INTERVAL DAY TO SECOND":
			return INTERVALDS;
		case "INTERVAL YEAR TO MONTH":
			return INTERVALYM;
		case "BOOLEAN":
			return BOOLEAN;
		case "TINYINT":
			return TINYINT;
		case "SMALLINT":
			return SMALLINT;
		case "INTEGER":
			return INTEGER;
		case "BIGINT":
			return BIGINT;
		case "FLOAT":
			return FLOAT;
		case "DOUBLE":
			return DOUBLE;
		case "DECIMAL":
			return DECIMAL;
		case "NUMERIC":
			return NUMERIC;
		case "BINARY":
			return BINARY;
		case "CHAR":
			return CHAR;
		case "VARCHAR":
			return VARCHAR;
		case "NCHAR":
			return NCHAR;
		case "NVARCHAR":
			return NVARCHAR;
		case "ROWID":
			return ROWID;
		case "CLOB":
			return CLOB;
		case "NCLOB":
			return NCLOB;
		case "BLOB":
			return BLOB;
		case "XMLTYPE":
			return SQLXML;
		case "JSON":
			return JSON;
		case "VECTOR":
			return VECTOR;
		}
		return Integer.MIN_VALUE;
	}

	public static boolean isNumeric(final int jdbcType) {
		switch (jdbcType) {
		case TINYINT:
		case SMALLINT:
		case INTEGER:
		case BIGINT:
		case FLOAT:
		case DOUBLE:
		case DECIMAL:
		case NUMERIC:
			return true;
		default:
			return false;
		}
	}

	public static final String[] NUMERICS = {
		"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"
	};

}
