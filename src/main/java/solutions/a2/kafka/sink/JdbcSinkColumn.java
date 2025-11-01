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

package solutions.a2.kafka.sink;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.SQLXML;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static oracle.jdbc.OracleTypes.INTERVALDS;
import static oracle.jdbc.OracleTypes.INTERVALYM;
import static oracle.jdbc.OracleTypes.JSON;
import static oracle.jdbc.OracleTypes.VECTOR;
import static solutions.a2.utils.ExceptionUtils.getExceptionStackTrace;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.data.OraBlob;
import solutions.a2.cdc.oracle.data.OraClob;
import solutions.a2.cdc.oracle.data.OraInterval;
import solutions.a2.cdc.oracle.data.OraIntervalDS;
import solutions.a2.cdc.oracle.data.OraIntervalYM;
import solutions.a2.cdc.oracle.data.OraJson;
import solutions.a2.cdc.oracle.data.OraNClob;
import solutions.a2.cdc.oracle.data.OraNumber;
import solutions.a2.cdc.oracle.data.OraTimestamp;
import solutions.a2.cdc.oracle.data.OraVector;
import solutions.a2.cdc.oracle.data.OraXml;
import solutions.a2.kafka.Column;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class JdbcSinkColumn extends Column {

	private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkColumn.class);

	/**
	 * Used in Sink connector
	 * 
	 * @param field
	 * @param partOfPk
	 * @param partOfKeyStruct
	 */
	public JdbcSinkColumn(final Field field, final boolean partOfPk, final boolean partOfKeyStruct) throws SQLException {
		this.columnName = field.name();
		if (partOfPk)
			flags |= (FLG_PART_OF_PK | FLG_MANDATORY);
		if (field.schema().isOptional())
			flags |= FLG_NULLABLE;
		else
			flags |= FLG_MANDATORY;
		if (partOfKeyStruct)
			flags |= FLG_PART_OF_KEY_STRUCT;
		final String typeFromSchema = field.schema().type().getName().toUpperCase();
		switch (typeFromSchema) {
			case "INT8" -> jdbcType = TINYINT;
			case "INT16" -> jdbcType = SMALLINT;
			case "INT32" -> {
				if (field.schema().name() != null && Date.LOGICAL_NAME.equals(field.schema().name()))
					jdbcType = DATE;
				else
					jdbcType = INTEGER;
			}
			case "INT64" -> {
				if (field.schema().name() != null && Timestamp.LOGICAL_NAME.equals(field.schema().name()))
					jdbcType = TIMESTAMP;
				else
					jdbcType = BIGINT;
			}
			case "FLOAT32" -> jdbcType = FLOAT;
			case "FLOAT64" -> jdbcType = DOUBLE;
			case "BYTES" -> {
				if (field.schema().name() != null) {
					switch (field.schema().name()) {
						case Decimal.LOGICAL_NAME -> {
							jdbcType = DECIMAL;
							try {
								dataScale = Integer.valueOf(field.schema().parameters().get(Decimal.SCALE_FIELD));
							} catch (Exception e) {
								LOGGER.error(
										"""
										
										=====================
										'{}' while parsing SCALE_FIELD. Exception stack trace:
										{}
										=====================
										
										""", e.getMessage(), getExceptionStackTrace(e));
							}
						}
						case OraNumber.LOGICAL_NAME -> jdbcType = NUMERIC;
						case OraIntervalYM.LOGICAL_NAME -> jdbcType = INTERVALYM;
						case OraIntervalDS.LOGICAL_NAME -> jdbcType = INTERVALDS;
						default -> {
							LOGGER.error(
									"""
									
									=====================
									Unknown logical name {} for BYTES Schema.
									Setting column {} JDBC type to BINARY.
									=====================
									
									""", field.schema().name(), field.name());
							jdbcType = BINARY;
						}
					}
				} else {
					jdbcType = BINARY;
				}
			}
			case "STRING" -> {
				if (field.schema().name() != null) {
					switch (field.schema().name()) {
						case  OraTimestamp.LOGICAL_NAME ->  jdbcType = TIMESTAMP_WITH_TIMEZONE;
						case OraInterval.LOGICAL_NAME -> jdbcType = JAVA_SQL_TYPE_INTERVALDS_STRING;
						default -> {
							LOGGER.error(
									"""
									
									=====================
									Unknown logical name {} for STRING Schema.
									Setting column {} JDBC type to VARCHAR.
									=====================
									
									""", field.schema().name(), field.name());
							jdbcType = VARCHAR;
						}
					}
				} else {
					jdbcType = VARCHAR;
				}
			}
			case "STRUCT" -> {
				if (field.schema().name() != null)
					switch (field.schema().name()) {
						case OraBlob.LOGICAL_NAME -> jdbcType = BLOB;
						case OraClob.LOGICAL_NAME -> jdbcType = CLOB;
						case OraNClob.LOGICAL_NAME -> jdbcType = NCLOB;
						case OraXml.LOGICAL_NAME -> jdbcType = SQLXML;
						case OraJson.LOGICAL_NAME -> jdbcType = JSON;
						case OraVector.LOGICAL_NAME -> jdbcType = VECTOR;
					}
			}
			case "BOOLEAN" ->  jdbcType = BOOLEAN;
			default ->  throw new SQLException("Not supported type '" + typeFromSchema + "'!");
		}
	}


	/**
	 * 
	 * @param dbType
	 * @param statement
	 * @param columnNo
	 * @param keyStruct
	 * @param valueStruct
	 * @throws SQLException
	 */
	public void bindWithPrepStmt(
			final int dbType,
			final PreparedStatement statement,
			final int columnNo,
			final Struct keyStruct,
			final Struct valueStruct) throws SQLException  {
		bindWithPrepStmt(dbType, statement, columnNo,
				((flags & FLG_PART_OF_KEY_STRUCT) > 0 ? keyStruct.get(columnName) : valueStruct.get(columnName)));
	}


	@Override
	public int hashCode() {
		return Objects.hash(
				columnName, jdbcType, flags, dataScale);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		JdbcSinkColumn other = (JdbcSinkColumn) obj;
		return
				Objects.equals(columnName, other.columnName) &&
				flags == other.flags &&
				jdbcType == other.jdbcType &&
				dataScale == other.dataScale;
	}

	public String getValueAsString(final Struct keyStruct, final Struct valueStruct) {
		return (flags & FLG_PART_OF_KEY_STRUCT) > 0
					? keyStruct.get(columnName).toString()
					: valueStruct.get(columnName).toString();
	}

}
