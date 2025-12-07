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
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static oracle.jdbc.OracleTypes.INTERVALDS;
import static oracle.jdbc.OracleTypes.INTERVALYM;
import static oracle.jdbc.OracleTypes.JSON;
import static oracle.jdbc.OracleTypes.VECTOR;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_INT8;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_INT16;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_INT32;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_INT64;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_FLOAT32;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_FLOAT64;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_BOOLEAN;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_STRING;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_BYTES;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_DECIMAL;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_NUMBER;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_TIMESTAMP;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_TIMESTAMPTZ;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_TIMESTAMPLTZ;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_INTERVALYM;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_INTERVALDS;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_INT8;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_INT16;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_INT32;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_INT64;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_FLOAT32;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_FLOAT64;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_BOOLEAN;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_STRING;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_BYTES;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_DECIMAL;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_NUMBER;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_TIMESTAMP;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_TIMESTAMPTZ;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_TIMESTAMPLTZ;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_INTERVALYM;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_INTERVALDS;
import static solutions.a2.kafka.sink.JdbcSinkConnectionPool.DB_TYPE_POSTGRESQL;
import static solutions.a2.utils.ExceptionUtils.getExceptionStackTrace;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.sql.NUMBER;
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
import solutions.a2.oracle.jdbc.types.OracleTimestamp;
import solutions.a2.oracle.jdbc.types.TimestampWithTimeZone;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class JdbcSinkColumn extends Column {

	private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkColumn.class);
	private JdbcSinkBinder binder;

	/**
	 * Used in Sink connector
	 * 
	 * @param field
	 * @param partOfPk
	 */
	public JdbcSinkColumn(final Field field, final boolean partOfPk) throws SQLException {
		this.columnName = field.name();
		if (partOfPk)
			flags |= (FLG_PART_OF_PK | FLG_MANDATORY);
		if (field.schema().isOptional())
			flags |= FLG_NULLABLE;
		else
			flags |= FLG_MANDATORY;
		final String typeFromSchema = field.schema().type().getName().toUpperCase();
		switch (typeFromSchema) {
		case "BOOLEAN" ->  {
			jdbcType = BOOLEAN;
			if (partOfPk)
				binder = new JdbcSinkBinder() {
					@Override
					public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
						statement.setBoolean(columnNo, (Boolean) keyStruct.get(columnName));
					}
				};
			else
				binder = new JdbcSinkBinder() {
					@Override
					public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
						final var value = (Boolean) valueStruct.get(columnName);
						if (value == null) statement.setNull(columnNo, jdbcType); 
						else statement.setBoolean(columnNo, value);
					}
				};
		}
			case "INT8" -> {
				jdbcType = TINYINT;
				if (partOfPk)
					binder = new JdbcSinkBinder() {
						@Override
						public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
							statement.setByte(columnNo, (Byte) keyStruct.get(columnName));
						}
					};
				else
					binder = new JdbcSinkBinder() {
						@Override
						public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
							final var value = (Byte) valueStruct.get(columnName);
							if (value == null) statement.setNull(columnNo, jdbcType); 
							else statement.setByte(columnNo, value);
						}
					};
			}
			case "INT16" -> {
				jdbcType = SMALLINT;
				if (partOfPk)
					binder = new JdbcSinkBinder() {
						@Override
						public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
							statement.setShort(columnNo, (Short) keyStruct.get(columnName));
						}
					};
				else
					binder = new JdbcSinkBinder() {
						@Override
						public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
							final var value = (Short) valueStruct.get(columnName);
							if (value == null) statement.setNull(columnNo, jdbcType); 
							else statement.setShort(columnNo, value);
						}
					};
			}
			case "INT32" -> {
				if (field.schema().name() != null && Date.LOGICAL_NAME.equals(field.schema().name())) {
					jdbcType = DATE;
					if (partOfPk)
						binder = new JdbcSinkBinder() {
							@Override
							public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
								statement.setDate(columnNo, new java.sql.Date(((java.util.Date) keyStruct.get(columnName)).getTime()));
							}
						};
					else
						binder = new JdbcSinkBinder() {
							@Override
							public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
								final var value = valueStruct.get(columnName);
								if (value == null) statement.setNull(columnNo, jdbcType); 
								else statement.setDate(columnNo, new java.sql.Date(((java.util.Date) value).getTime()));
							}
						};
				} else {
					jdbcType = INTEGER;
					if (partOfPk)
						binder = new JdbcSinkBinder() {
							@Override
							public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
								statement.setInt(columnNo, (Integer) keyStruct.get(columnName));
							}
						};
					else
						binder = new JdbcSinkBinder() {
							@Override
							public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
								final var value = (Integer) valueStruct.get(columnName);
								if (value == null) statement.setNull(columnNo, jdbcType); 
								else statement.setInt(columnNo, value);
							}
						};
				}
			}
			case "INT64" -> {
				if (field.schema().name() != null && Timestamp.LOGICAL_NAME.equals(field.schema().name())) {
					jdbcType = TIMESTAMP;
					if (partOfPk)
						binder = new JdbcSinkBinder() {
							@Override
							public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
								statement.setTimestamp(columnNo, new java.sql.Timestamp(((java.util.Date) keyStruct.get(columnName)).getTime()));
							}
						};
					else
						binder = new JdbcSinkBinder() {
							@Override
							public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
								final var value = valueStruct.get(columnName);
								if (value == null) statement.setNull(columnNo, jdbcType); 
								else statement.setTimestamp(columnNo, new java.sql.Timestamp(((java.util.Date) value).getTime()));
							}
						};
				} else {
					jdbcType = BIGINT;
					if (partOfPk)
						binder = new JdbcSinkBinder() {
							@Override
							public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
								try {
									statement.setLong(columnNo, (Long) keyStruct.get(columnName));
								} catch (ClassCastException cce) {
									statement.setLong(columnNo, (Integer) keyStruct.get(columnName));
								}
							}
						};
					else
						binder = new JdbcSinkBinder() {
							@Override
							public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
								if (valueStruct.get(columnName) == null) statement.setNull(columnNo, jdbcType); 
								else {
									try {
										statement.setLong(columnNo, (Long) valueStruct.get(columnName));
									} catch (ClassCastException cce) {
										statement.setLong(columnNo, (Integer) valueStruct.get(columnName));
									}
								}
							}
						};
				}
			}
			case "FLOAT32" -> {
				jdbcType = FLOAT;
				if (partOfPk)
					binder = new JdbcSinkBinder() {
						@Override
						public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
							statement.setFloat(columnNo, (Float) keyStruct.get(columnName));
						}
					};
				else
					binder = new JdbcSinkBinder() {
						@Override
						public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
							final var value = (Float) valueStruct.get(columnName);
							if (value == null) statement.setNull(columnNo, jdbcType); 
							else {
								if (!handleFloatNegativeInfinity(statement, columnNo, value)) {
									statement.setFloat(columnNo, value);
								}
							}
						}
					};
			}
			case "FLOAT64" -> {
				jdbcType = DOUBLE;
				if (partOfPk)
					binder = new JdbcSinkBinder() {
						@Override
						public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
							statement.setDouble(columnNo, (Double) keyStruct.get(columnName));
						}
					};
				else
					binder = new JdbcSinkBinder() {
						@Override
						public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
							final var value = (Double) valueStruct.get(columnName);
							if (value == null) statement.setNull(columnNo, jdbcType); 
							else {
								if (!handleFloatNegativeInfinity(statement, columnNo, value)) {
									statement.setDouble(columnNo, value);
								}
							}
						}
					};
			}
			case "BYTES" -> {
				if (field.schema().name() != null) {
					switch (field.schema().name()) {
						case Decimal.LOGICAL_NAME -> {
							jdbcType = DECIMAL;
							dataScale = scale(field);
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										statement.setBigDecimal(columnNo, (BigDecimal) keyStruct.get(columnName));
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var value = (BigDecimal) valueStruct.get(columnName);
										if (value == null) statement.setNull(columnNo, jdbcType); 
										else {
											if (!handleFloatNegativeInfinity(statement, columnNo, value)) {
												statement.setBigDecimal(columnNo, value);
											}
										}
									}
								};
						}
						case OraNumber.LOGICAL_NAME -> {
							jdbcType = NUMERIC;
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var value = keyStruct.get(columnName);
										final byte[] ba;
										if (value instanceof ByteBuffer) ba = ((ByteBuffer) value).array();
										else ba = (byte[]) value;
										statement.setBigDecimal(columnNo, new NUMBER(ba).bigDecimalValue());
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var value = valueStruct.get(columnName);
										if (value == null) statement.setNull(columnNo, jdbcType); 
										else {
											final byte[] ba;
											if (value instanceof ByteBuffer) ba = ((ByteBuffer) value).array();
											else ba = (byte[]) value;
											try {
												statement.setBigDecimal(columnNo, new NUMBER(ba).bigDecimalValue());
											} catch (SQLException sqle) {
												if (ba.length == 1 && ba[0] == 0x0) {
													LOGGER.error(
															"""
															
															=====================
															Invalid dump value for Oracle NUMBER: 'Typ=2 Len=1: 0', value set to null!!!
															=====================
															
															""");
													statement.setNull(columnNo, jdbcType);
												} else
													throw sqle;
											}
										}
									}
								};
						}
						case OraIntervalYM.LOGICAL_NAME -> {
							jdbcType = INTERVALYM;
							//TODO mapping in TargetDbSqlUtils!
						}
						case OraIntervalDS.LOGICAL_NAME -> {
							jdbcType = INTERVALDS;
							//TODO mapping in TargetDbSqlUtils!
						}
						default -> {
							LOGGER.error(
									"""
									
									=====================
									Unknown logical name {} for BYTES Schema.
									Setting column {} JDBC type to BINARY.
									=====================
									
									""", field.schema().name(), field.name());
							jdbcType = BINARY;
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										statement.setBytes(columnNo, ((ByteBuffer) keyStruct.get(columnName)).array());
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var value = (ByteBuffer) valueStruct.get(columnName);
										if (value == null) statement.setNull(columnNo, jdbcType); 
										else statement.setBytes(columnNo, value.array());
									}
								};
						}
					}
				} else {
					jdbcType = BINARY;
					if (partOfPk)
						binder = new JdbcSinkBinder() {
							@Override
							public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
								statement.setBytes(columnNo, ((ByteBuffer) keyStruct.get(columnName)).array());
							}
						};
					else
						binder = new JdbcSinkBinder() {
							@Override
							public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
								final var value = (ByteBuffer) valueStruct.get(columnName);
								if (value == null) statement.setNull(columnNo, jdbcType); 
								else statement.setBytes(columnNo, value.array());
							}
						};
				}
			}
			case "STRING" -> {
				if (field.schema().name() != null) {
					switch (field.schema().name()) {
						case  OraTimestamp.LOGICAL_NAME ->  {
							jdbcType = TIMESTAMP_WITH_TIMEZONE;
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										try {
											statement.setObject(columnNo,
													ZonedDateTime.parse((String) keyStruct.get(columnName), ISO_OFFSET_DATE_TIME)
														.toOffsetDateTime());
										} catch (DateTimeParseException dtpe) {
											throw new SQLException(dtpe);
										}
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var value = (String) valueStruct.get(columnName);
										if (value == null) statement.setNull(columnNo, jdbcType); 
										else {
											try {
												statement.setObject(columnNo,
														ZonedDateTime.parse(value, ISO_OFFSET_DATE_TIME)
															.toOffsetDateTime());
											} catch (DateTimeParseException dtpe) {
												throw new SQLException(dtpe);
											}
										}
									}
								};
						}
						case OraInterval.LOGICAL_NAME -> {
							//TODO mapping in TargetDbSqlUtils!
							jdbcType = JAVA_SQL_TYPE_INTERVALDS_STRING;
						}
						default -> {
							LOGGER.error(
									"""
									
									=====================
									Unknown logical name {} for STRING Schema.
									Setting column {} JDBC type to VARCHAR.
									=====================
									
									""", field.schema().name(), field.name());
							jdbcType = VARCHAR;
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										if (dbType == DB_TYPE_POSTGRESQL)
											statement.setString(columnNo, StringUtils.remove((String) keyStruct.get(columnName), CHAR_0));
										else
											statement.setString(columnNo, (String) keyStruct.get(columnName));
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var value = (String) valueStruct.get(columnName);
										if (value == null) statement.setNull(columnNo, jdbcType); 
										else {
											if (dbType == DB_TYPE_POSTGRESQL)
												statement.setString(columnNo, StringUtils.remove(value, CHAR_0));
											else
												statement.setString(columnNo, value);
										}
									}
								};
						}
					}
				} else {
					jdbcType = VARCHAR;
					if (partOfPk)
						binder = new JdbcSinkBinder() {
							@Override
							public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
								if (dbType == DB_TYPE_POSTGRESQL)
									statement.setString(columnNo, StringUtils.remove((String) keyStruct.get(columnName), CHAR_0));
								else
									statement.setString(columnNo, (String) keyStruct.get(columnName));
							}
						};
					else
						binder = new JdbcSinkBinder() {
							@Override
							public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
								final var value = (String) valueStruct.get(columnName);
								if (value == null) statement.setNull(columnNo, jdbcType); 
								else {
									if (dbType == DB_TYPE_POSTGRESQL)
										statement.setString(columnNo, StringUtils.remove(value, CHAR_0));
									else
										statement.setString(columnNo, value);
								}
							}
						};
				}
			}
			case "STRUCT" -> {
				if (field.schema().name() != null)
					switch (field.schema().name()) {
						case OraBlob.LOGICAL_NAME -> {
							jdbcType = BLOB;
							//TODO
							//TODO
							//TODO
						}
						case OraClob.LOGICAL_NAME -> {
							jdbcType = CLOB;
							//TODO
							//TODO
							//TODO
						}
						case OraNClob.LOGICAL_NAME -> {
							jdbcType = NCLOB;
							//TODO
							//TODO
							//TODO
						}
						case OraXml.LOGICAL_NAME -> {
							jdbcType = SQLXML;
							//TODO
							//TODO
							//TODO
						}
						case OraJson.LOGICAL_NAME -> {
							jdbcType = JSON;
							//TODO
							//TODO
							//TODO
						}
						case OraVector.LOGICAL_NAME -> {
							jdbcType = VECTOR;
							//TODO
							//TODO
							//TODO
						}
						case WRAPPED_INT8, WRAPPED_OPT_INT8 -> {
							flags |= FLG_WRAPPED;
							jdbcType = TINYINT;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										statement.setByte(columnNo, (Byte) struct.get("V"));
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = (Byte) struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else statement.setByte(columnNo, value);
										}
									}
								};
						}
						case WRAPPED_INT16, WRAPPED_OPT_INT16 -> {
							flags |= FLG_WRAPPED;
							jdbcType = SMALLINT;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										statement.setShort(columnNo, (Short) struct.get("V"));
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = (Short) struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else statement.setShort(columnNo, value);
										}
									}
								};
						}
						case WRAPPED_INT32, WRAPPED_OPT_INT32 -> {
							flags |= FLG_WRAPPED;
							jdbcType = INTEGER;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										statement.setInt(columnNo, (Integer) struct.get("V"));
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = (Integer) struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else statement.setInt(columnNo, value);
										}
									}
								};
						}
						case WRAPPED_INT64, WRAPPED_OPT_INT64 -> {
							flags |= FLG_WRAPPED;
							jdbcType = BIGINT;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										try {
											statement.setLong(columnNo, (Long) struct.get("V"));
										} catch (ClassCastException cce) {
											statement.setLong(columnNo, (Integer) struct.get("V"));
										}
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = (Long) struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else statement.setLong(columnNo, value);
										}
									}
								};
						}
						case WRAPPED_FLOAT32, WRAPPED_OPT_FLOAT32 -> {
							flags |= FLG_WRAPPED;
							jdbcType = FLOAT;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										statement.setFloat(columnNo, (Float) struct.get("V"));
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = (Float) struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else statement.setFloat(columnNo, value);
										}
									}
								};
						}
						case WRAPPED_FLOAT64, WRAPPED_OPT_FLOAT64 -> {
							flags |= FLG_WRAPPED;
							jdbcType = DOUBLE;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										statement.setDouble(columnNo, (Double) struct.get("V"));
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = (Double) struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else statement.setDouble(columnNo, value);
										}
									}
								};
						}
						case WRAPPED_BOOLEAN, WRAPPED_OPT_BOOLEAN -> {
							flags |= FLG_WRAPPED;
							jdbcType = BOOLEAN;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										statement.setBoolean(columnNo, (Boolean) struct.get("V"));
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = (Boolean) struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else statement.setBoolean(columnNo, value);
										}
									}
								};
						}
						case WRAPPED_STRING, WRAPPED_OPT_STRING -> {
							flags |= FLG_WRAPPED;
							jdbcType = VARCHAR;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										statement.setString(columnNo, (String) struct.get("V"));
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = (String) struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else statement.setString(columnNo, value);
										}
									}
								};
						}
						case WRAPPED_BYTES, WRAPPED_OPT_BYTES -> {
							flags |= FLG_WRAPPED;
							jdbcType = BINARY;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										statement.setBytes(columnNo, ((ByteBuffer) struct.get("V")).array());
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = (String) struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else statement.setString(columnNo, value);
										}
									}
								};
						}
						case WRAPPED_DECIMAL, WRAPPED_OPT_DECIMAL -> {
							flags |= FLG_WRAPPED;
							jdbcType = DECIMAL;
							dataScale = scale(field);
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
							jdbcType = DECIMAL;
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										final var value = struct.get("V");
										final byte[] ba = value instanceof ByteBuffer
												? getBytes(value) : (byte[]) value;
										statement.setBigDecimal(columnNo, new NUMBER(ba).bigDecimalValue().setScale(dataScale));
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else {
												final byte[] ba = value instanceof ByteBuffer
														? getBytes(value) : (byte[]) value;
												try {
													statement.setBigDecimal(columnNo, new NUMBER(ba).bigDecimalValue().setScale(dataScale));
												} catch (SQLException sqle) {
													handleNUMBERparseException(sqle, ba, statement, columnNo);
												}
											}
										}
									}
								};
						}
						case WRAPPED_NUMBER, WRAPPED_OPT_NUMBER -> {
							flags |= FLG_WRAPPED;
							jdbcType = NUMERIC;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										final var value = struct.get("V");
										final byte[] ba = value instanceof ByteBuffer
												? getBytes(value) : (byte[]) value;
										statement.setBigDecimal(columnNo, new NUMBER(ba).bigDecimalValue());
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else {
												final byte[] ba = value instanceof ByteBuffer
														? getBytes(value) : (byte[]) value;
												try {
													statement.setBigDecimal(columnNo, new NUMBER(ba).bigDecimalValue());
												} catch (SQLException sqle) {
													handleNUMBERparseException(sqle, ba, statement, columnNo);
												}
											}											
										}
									}
								};
						}
						case WRAPPED_TIMESTAMP, WRAPPED_OPT_TIMESTAMP -> {
							flags |= FLG_WRAPPED;
							jdbcType = TIMESTAMP;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										final var value = struct.get("V");
										final byte[] ba = value instanceof ByteBuffer
												? getBytes(value) : (byte[]) value;
										statement.setTimestamp(columnNo, OracleTimestamp.toTimestamp(ba));
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else {
												final byte[] ba = value instanceof ByteBuffer
														? getBytes(value) : (byte[]) value;
												statement.setTimestamp(columnNo, OracleTimestamp.toTimestamp(ba));
											}										
										}
									}
								};
						}
						case WRAPPED_TIMESTAMPTZ, WRAPPED_OPT_TIMESTAMPTZ -> {
							flags |= FLG_WRAPPED;
							jdbcType = TIMESTAMP_WITH_TIMEZONE;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										final var value = struct.get("V");
										final byte[] ba = value instanceof ByteBuffer
												? getBytes(value) : (byte[]) value;
										statement.setObject(columnNo, TimestampWithTimeZone.toOffsetDateTime(ba));
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else {
												final byte[] ba = value instanceof ByteBuffer
														? getBytes(value) : (byte[]) value;
												statement.setObject(columnNo, TimestampWithTimeZone.toOffsetDateTime(ba));
											}										
										}
									}
								};
						}
						case WRAPPED_TIMESTAMPLTZ, WRAPPED_OPT_TIMESTAMPLTZ -> {
							flags |= FLG_WRAPPED;
							jdbcType = TIMESTAMP_WITH_TIMEZONE;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;
							final ZoneId zoneId;
							if (field.schema().parameters() != null && field.schema().parameters().get("tz") != null)
								zoneId = ZoneId.of(field.schema().parameters().get("tz"));
							else
								zoneId = ZoneId.systemDefault();
							if (partOfPk)
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(int dbType, PreparedStatement statement, int columnNo, Struct keyStruct, Struct valueStruct) throws SQLException {
										final var struct = (Struct) keyStruct.get(columnName);
										final var value = struct.get("V");
										final byte[] ba = value instanceof ByteBuffer
												? getBytes(value) : (byte[]) value;
										statement.setObject(columnNo,
											OracleTimestamp.toZonedDateTime(ba, zoneId).toOffsetDateTime());
									}
								};
							else
								binder = new JdbcSinkBinder() {
									@Override
									public void bind(final int dbType, final PreparedStatement statement, final int columnNo, final Struct keyStruct, final Struct valueStruct) throws SQLException {
										final var struct = (Struct) valueStruct.get(columnName);
										if (struct == null) statement.setNull(columnNo, jdbcType); 
										else {
											final var value = struct.get("V");
											if (value == null) statement.setNull(columnNo, jdbcType);
											else {
												final byte[] ba = value instanceof ByteBuffer
														? getBytes(value) : (byte[]) value;
												statement.setObject(columnNo,
														OracleTimestamp.toZonedDateTime(ba, zoneId).toOffsetDateTime());
											}										
										}
									}
								};
						}
						case WRAPPED_INTERVALYM, WRAPPED_OPT_INTERVALYM -> {
							//TODO mapping in TargetDbSqlUtils!
							flags |= FLG_WRAPPED;
							jdbcType = INTERVALYM;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
						}
						case WRAPPED_INTERVALDS, WRAPPED_OPT_INTERVALDS -> {
							//TODO mapping in TargetDbSqlUtils!
							flags |= FLG_WRAPPED;
							jdbcType = INTERVALDS;
							if (Strings.CS.endsWith(field.schema().name(), ".opt")) flags |= FLG_NULLABLE;  
						}
					}
			}
			default ->  throw new SQLException("Not supported type '" + typeFromSchema + "'!");
		}
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

	String getValueAsString(final Struct keyStruct, final Struct valueStruct) {
		return (flags & FLG_PART_OF_PK) > 0
					? keyStruct.get(columnName).toString()
					: valueStruct.get(columnName).toString();
	}

	JdbcSinkBinder binder() {
		return binder;
	}

	private int scale(final Field field) {
		try {
			return Integer.parseInt(field.schema().parameters().get(Decimal.SCALE_FIELD));
		} catch (Exception e) {
			LOGGER.error(
					"""
					
					=====================
					'{}' while parsing SCALE_FIELD. Exception stack trace:
					{}
					=====================
					
					""", e.getMessage(), getExceptionStackTrace(e));
		}
		return 0;
	}

	private void handleNUMBERparseException(SQLException sqle, byte[] ba, PreparedStatement statement, int columnNo)
			throws SQLException {
		if (ba.length == 1 && ba[0] == 0x0) {
			LOGGER.error(
					"""
					
					=====================
					Invalid dump value for Oracle NUMBER: 'Typ=2 Len=1: 0', value set to null!!!
					=====================
					
					""");
			statement.setNull(columnNo, jdbcType);
		} else
			throw sqle;
	}

	private static byte[] getBytes(final Object value) {
		final ByteBuffer bb = ((ByteBuffer) value).slice();
		final var ba = new byte[bb.remaining()];
		bb.get(ba);
		return ba;
	}

}
