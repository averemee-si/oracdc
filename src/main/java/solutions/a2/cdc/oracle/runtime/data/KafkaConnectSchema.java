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

package solutions.a2.cdc.oracle.runtime.data;

import static java.sql.Types.BOOLEAN;
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
import static java.sql.Types.VARCHAR;
import static java.sql.Types.CHAR;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.BINARY;
import static java.sql.Types.ROWID;
import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.SQLXML;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static oracle.jdbc.OracleTypes.BINARY_FLOAT;
import static oracle.jdbc.OracleTypes.BINARY_DOUBLE;
import static oracle.jdbc.OracleTypes.INTERVALDS;
import static oracle.jdbc.OracleTypes.INTERVALYM;
import static oracle.jdbc.OracleTypes.JSON;
import static oracle.jdbc.OracleTypes.TIMESTAMPLTZ;
import static oracle.jdbc.OracleTypes.VECTOR;
import static solutions.a2.cdc.oracle.OraCdcTableBase.FLG_SUPPLEMENTAL_LOG_ALL;
import static solutions.a2.cdc.oracle.OraColumn.JAVA_SQL_TYPE_INTERVALDS_STRING;
import static solutions.a2.cdc.oracle.OraColumn.JAVA_SQL_TYPE_INTERVALYM_STRING;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_BOOLEAN_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_BYTES_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_DECIMAL_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_FLOAT32_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_FLOAT64_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_INT16_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_INT32_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_INT64_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_INT8_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_INTERVALDS_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_INTERVALYM_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_NUMBER_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_BOOLEAN_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_BYTES_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_DECIMAL_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_FLOAT32_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_FLOAT64_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_INT16_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_INT32_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_INT64_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_INT8_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_INTERVALDS_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_INTERVALYM_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_NUMBER_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_STRING_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_TIMESTAMPLTZ_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_TIMESTAMPTZ_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_TIMESTAMP_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_STRING_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_TIMESTAMPLTZ_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_TIMESTAMPTZ_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_TIMESTAMP_SCHEMA;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.ZoneId;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.sql.NUMBER;
import oracle.sql.json.OracleJsonFactory;
import solutions.a2.cdc.oracle.OraCdcDecoder;
import solutions.a2.cdc.oracle.OraColumn;
import solutions.a2.cdc.oracle.OraRdbmsInfo;
import solutions.a2.cdc.oracle.data.OraInterval;
import solutions.a2.cdc.oracle.data.OraIntervalDS;
import solutions.a2.cdc.oracle.data.OraIntervalYM;
import solutions.a2.cdc.oracle.data.OraNumber;
import solutions.a2.cdc.oracle.data.OraTimestamp;
import solutions.a2.cdc.oracle.internals.OraCdcTdeColumnDecrypter;
import solutions.a2.cdc.oracle.OraCdcTableBase;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class KafkaConnectSchema {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectSchema.class);

	private final OraRdbmsInfo rdbmsInfo;
	private final OraCdcTdeColumnDecrypter decrypter;
	private final boolean suppLogAll;

	KafkaConnectSchema(final OraRdbmsInfo rdbmsInfo, final OraCdcTdeColumnDecrypter decrypter, final OraCdcTableBase table) {
		this.rdbmsInfo = rdbmsInfo;
		this.decrypter = decrypter;
		suppLogAll = (table.flags() & FLG_SUPPLEMENTAL_LOG_ALL) > 0;
	}

	KafkaConnectSchema(final OraRdbmsInfo rdbmsInfo) {
		this.rdbmsInfo = rdbmsInfo;
		decrypter = null;
		suppLogAll = true;
	}

	public Schema get(final OraColumn column) {
		final Schema schema;
		final OraCdcDecoder decoder;
		var jdbcType = column.getJdbcType();
		switch (jdbcType) {
			case CHAR, VARCHAR, NCHAR, NVARCHAR -> {
				schema = stringSchema(column);
				var charset = rdbmsInfo == null
							? "US7ASCII"
							: jdbcType == CHAR || jdbcType == VARCHAR
									? rdbmsInfo.charset()
									: rdbmsInfo.nCharset();
				if (suppLogAll) {
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(charset, decrypter, column.salted())
							: OraCdcDecoderFactory.get(charset);
				} else {
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(schema, charset, decrypter, column.salted())
							: OraCdcDecoderFactory.get(schema, charset);
				}
			}
			case LONGVARCHAR, LONGNVARCHAR -> {
				schema = stringSchema(column);
				if (suppLogAll) {
					decoder = decoderFromJdbcType(column);
				} else {
					decoder = decoderFromJdbcType(column, schema);
				}
			}
			case NUMERIC -> {
				if (suppLogAll) {
					var builder = OraNumber.builder();
					if (column.defaultValuePresent()) {
						try {
							var typedDefaultValue = (new NUMBER(column.defaultValue(), 10)).getBytes();
							column.typedDefaultValue(typedDefaultValue);
							builder.defaultValue(typedDefaultValue);
						} catch (SQLException sqle) {
							LOGGER.error(
									"""
									
									=====================
									Unable to convert default value (DATA_DEFAULT) '{}' of column '{}' to oracle.sql.NUMBER!
									=====================
									
									""", column.defaultValue(), column.getColumnName());
							throw new NumberFormatException(sqle.getMessage());
						} catch (NumberFormatException nfe) {
							logDefaultValueError(column.getColumnName(), "oracle.sql.NUMBER", column.defaultValue());
						}
					}
					schema = optionalOrRequired(column.mandatory(), builder);
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(decrypter, column.salted())
							: OraCdcDecoderFactory.get();
				} else {
					schema = column.mandatory()
							? WRAPPED_NUMBER_SCHEMA
							: WRAPPED_OPT_NUMBER_SCHEMA;
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(schema, decrypter, column.salted())
							: OraCdcDecoderFactory.get(schema);
				}
			}
			case FLOAT -> {
				if (suppLogAll) {
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(BINARY_FLOAT, decrypter, column.salted())
							: OraCdcDecoderFactory.get(BINARY_FLOAT);
					var builder = SchemaBuilder.float32();
					if (column.defaultValuePresent()) {
						try {
							var typedDefaultValue = Float.parseFloat(column.defaultValue());
							column.typedDefaultValue(typedDefaultValue);
							builder.defaultValue(typedDefaultValue);
						} catch (NumberFormatException nfe) {
							logDefaultValueError(column.getColumnName(), "float", column.defaultValue());
						}
					}
					schema = optionalOrRequired(column.mandatory(), builder);
				} else {
					schema = column.mandatory()
							? WRAPPED_FLOAT32_SCHEMA
							: WRAPPED_OPT_FLOAT32_SCHEMA;
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(schema, BINARY_FLOAT, decrypter, column.salted())
							: OraCdcDecoderFactory.get(schema, BINARY_FLOAT);
				}
			}
			case DOUBLE -> {
				if (suppLogAll) {
					var builder = SchemaBuilder.float64();
					if (column.defaultValuePresent()) {
						try {
							var typedDefaultValue = Double.parseDouble(column.defaultValue());
							column.typedDefaultValue(typedDefaultValue);
							builder.defaultValue(typedDefaultValue);
						} catch (NumberFormatException nfe) {
							logDefaultValueError(column.getColumnName(), "double", column.defaultValue());
						}
					}
					if (column.IEEE754()) {
						decoder = column.encrypted()
								? OraCdcDecoderFactory.get(BINARY_DOUBLE, decrypter, column.salted())
								: OraCdcDecoderFactory.get(BINARY_DOUBLE);
					} else {
						decoder = decoderFromJdbcType(column);
					}
					schema = optionalOrRequired(column.mandatory(), builder);
				} else {
					schema = column.mandatory()
							? WRAPPED_FLOAT64_SCHEMA
							: WRAPPED_OPT_FLOAT64_SCHEMA;
					if (column.IEEE754()) {
						decoder = column.encrypted()
								? OraCdcDecoderFactory.get(schema, BINARY_DOUBLE, decrypter, column.salted())
								: OraCdcDecoderFactory.get(schema, BINARY_DOUBLE);
					} else {
						decoder = column.encrypted()
								? OraCdcDecoderFactory.get(schema, DOUBLE, decrypter, column.salted())
								: OraCdcDecoderFactory.get(schema, DOUBLE);
					}
				}
			}
			case DECIMAL -> {
				if (suppLogAll) {
					decoder = column.encrypted()
							? OraCdcDecoderFactory.getNUMBER(column.getDataScale(), decrypter, column.salted())
							: OraCdcDecoderFactory.getNUMBER(column.getDataScale());
					var builder = Decimal.builder(column.getDataScale());
					if (column.defaultValuePresent()) {
						try {
							var typedDefaultValue = (new BigDecimal(column.defaultValue())).setScale(column.getDataScale());
							column.typedDefaultValue(typedDefaultValue);
							builder.defaultValue(typedDefaultValue);
						} catch (NumberFormatException nfe) {
							logDefaultValueError(column.getColumnName(), "java.math.BigDecimal", column.defaultValue());
						}
					}
					schema = optionalOrRequired(column.mandatory(), builder);
				} else {
					schema = column.mandatory()
							? WRAPPED_DECIMAL_SCHEMA(column.getDataScale())
							: WRAPPED_OPT_DECIMAL_SCHEMA(column.getDataScale());
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(schema, decrypter, column.salted())
							: OraCdcDecoderFactory.get(schema);
				}
			}
			case TINYINT -> {
				if (suppLogAll) {
					decoder = decoderFromJdbcType(column);
					var builder = SchemaBuilder.int8();
					if (column.defaultValuePresent()) {
						try {
							var typedDefaultValue = Byte.parseByte(column.defaultValue());
							column.typedDefaultValue(typedDefaultValue);
							builder.defaultValue(typedDefaultValue);
						} catch (NumberFormatException nfe) {
							logDefaultValueError(column.getColumnName(), "byte", column.defaultValue());
						}
					}
					schema = optionalOrRequired(column.mandatory(), builder);
				} else {
					schema = column.mandatory()
							? WRAPPED_INT8_SCHEMA
							: WRAPPED_OPT_INT8_SCHEMA;
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(schema, decrypter, column.salted())
							: OraCdcDecoderFactory.get(schema, jdbcType);
				}
			}
			case SMALLINT -> {
				if (suppLogAll) {
					decoder = decoderFromJdbcType(column);
					var builder = SchemaBuilder.int16();
					if (column.defaultValuePresent()) {
						try {
							var typedDefaultValue = Short.parseShort(column.defaultValue());
							column.typedDefaultValue(typedDefaultValue);
							builder.defaultValue(typedDefaultValue);
						} catch (NumberFormatException nfe) {
							logDefaultValueError(column.getColumnName(), "short", column.defaultValue());
						}
					}
					schema = optionalOrRequired(column.mandatory(), builder);
				} else {
					schema = column.mandatory()
							? WRAPPED_INT16_SCHEMA
							: WRAPPED_OPT_INT16_SCHEMA;
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(schema, decrypter, column.salted())
							: OraCdcDecoderFactory.get(schema, jdbcType);
				}
			}
			case INTEGER -> {
				if (suppLogAll) {
					decoder = decoderFromJdbcType(column);
					var builder = SchemaBuilder.int32();
					if (column.defaultValuePresent()) {
						try {
							var typedDefaultValue = Integer.parseInt(column.defaultValue());
							column.typedDefaultValue(typedDefaultValue);
							builder.defaultValue(typedDefaultValue);
						} catch (NumberFormatException nfe) {
							logDefaultValueError(column.getColumnName(), "int", column.defaultValue());
						}
					}
					schema = optionalOrRequired(column.mandatory(), builder);
				} else {
					schema = column.mandatory()
							? WRAPPED_INT32_SCHEMA
							: WRAPPED_OPT_INT32_SCHEMA;
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(schema, decrypter, column.salted())
							: OraCdcDecoderFactory.get(schema, jdbcType);
				}
			}
			case BIGINT -> {
				if (suppLogAll) {
					decoder = decoderFromJdbcType(column);
					var builder = SchemaBuilder.int64();
					if (column.defaultValuePresent()) {
						try {
							var typedDefaultValue = Long.parseLong(column.defaultValue());
							column.typedDefaultValue(typedDefaultValue);
							builder.defaultValue(typedDefaultValue);
						} catch (NumberFormatException nfe) {
							logDefaultValueError(column.getColumnName(), "long", column.defaultValue());
						}
					}
					schema = optionalOrRequired(column.mandatory(), builder);
				} else {
					schema = column.mandatory()
							? WRAPPED_INT64_SCHEMA
							: WRAPPED_OPT_INT64_SCHEMA;
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(schema, decrypter, column.salted())
							: OraCdcDecoderFactory.get(schema, jdbcType);
				}
			}
			case BOOLEAN -> {
				if (suppLogAll) {
					decoder = decoderFromJdbcType(column);
					var builder = SchemaBuilder.bool();
					if (column.defaultValuePresent()) {
						try {
							var typedDefaultValue = Boolean.parseBoolean(column.defaultValue());
							column.typedDefaultValue(typedDefaultValue);
							builder.defaultValue(typedDefaultValue);
						} catch (NumberFormatException nfe) {
							logDefaultValueError(column.getColumnName(), "column.getColumnName(), bool", column.defaultValue());
						}
					}
					schema = optionalOrRequired(column.mandatory(), builder);
				} else {
					schema = column.mandatory()
							? WRAPPED_BOOLEAN_SCHEMA
							: WRAPPED_OPT_BOOLEAN_SCHEMA;
					decoder = OraCdcDecoderFactory.get(schema, jdbcType);
				}
			}
			case TIMESTAMP_WITH_TIMEZONE -> {
				if (suppLogAll) {
					var zoneId = rdbmsInfo == null ? ZoneId.systemDefault() : rdbmsInfo.getDbTimeZone();
					schema = column.mandatory()
							? OraTimestamp.builder().required().build()
							: OraTimestamp.builder().optional().build();
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(zoneId, column.localTimeZone(), decrypter, column.salted())
							: OraCdcDecoderFactory.get(zoneId, column.localTimeZone());
				} else {
					schema = (column.mandatory())
							? WRAPPED_TIMESTAMPTZ_SCHEMA
							: WRAPPED_OPT_TIMESTAMPTZ_SCHEMA;
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(schema, decrypter, column.salted())
							: OraCdcDecoderFactory.get(schema);
				}
			}
			case TIMESTAMPLTZ -> {
				var tz = rdbmsInfo == null ? ZoneId.systemDefault() : rdbmsInfo.getDbTimeZone();
				schema = column.mandatory()
						? WRAPPED_TIMESTAMPLTZ_SCHEMA(tz.getId())
						: WRAPPED_OPT_TIMESTAMPLTZ_SCHEMA(tz.getId());
				decoder = column.encrypted()
						? OraCdcDecoderFactory.get(schema, decrypter, column.salted())
						: OraCdcDecoderFactory.get(schema);
			}
			case DATE, TIMESTAMP -> {
				if (suppLogAll) {
					schema = column.mandatory()
							? Timestamp.builder().required().build()
							: Timestamp.builder().optional().build();
					decoder = decoderFromJdbcType(column);
				} else {
					schema = column.mandatory()
							? WRAPPED_TIMESTAMP_SCHEMA
							: WRAPPED_OPT_TIMESTAMP_SCHEMA;
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(schema, decrypter, column.salted())
							: OraCdcDecoderFactory.get(schema);
				}
			}
			case ROWID -> {
				schema = Schema.STRING_SCHEMA;
				decoder = null;
			}
			case INTERVALYM -> {
				if (suppLogAll) {
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(decrypter, column.salted())
							: OraCdcDecoderFactory.get();
					schema = column.mandatory()
							? OraIntervalYM.builder().required().build()
							: OraIntervalYM.builder().optional().build();
				} else {
					schema = column.mandatory()
							? WRAPPED_INTERVALYM_SCHEMA
							: WRAPPED_OPT_INTERVALYM_SCHEMA;
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(schema, decrypter, column.salted())
							: OraCdcDecoderFactory.get(schema);
				}
			}
			case INTERVALDS -> {
				if (suppLogAll) {
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(decrypter, column.salted())
							: OraCdcDecoderFactory.get();
					schema = column.mandatory()
							? OraIntervalDS.builder().required().build()
							: OraIntervalDS.builder().optional().build();
				} else {
					schema = column.mandatory()
							? WRAPPED_INTERVALDS_SCHEMA
							: WRAPPED_OPT_INTERVALDS_SCHEMA;
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(schema, decrypter, column.salted())
							: OraCdcDecoderFactory.get(schema);
				}
			}
			case JAVA_SQL_TYPE_INTERVALYM_STRING, JAVA_SQL_TYPE_INTERVALDS_STRING -> {
				decoder = column.encrypted()
						? OraCdcDecoderFactory.get(jdbcType, decrypter, column.salted())
						: OraCdcDecoderFactory.get(jdbcType);
				schema = column.mandatory()
						? OraInterval.builder().required().build()
						: OraInterval.builder().optional().build();
			}
			case BINARY -> {
				if (suppLogAll) {
					schema = column.mandatory()
							? Schema.BYTES_SCHEMA
							: Schema.OPTIONAL_BYTES_SCHEMA;
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(decrypter, column.salted())
							: OraCdcDecoderFactory.get();
				} else {
					schema = column.mandatory()
							? WRAPPED_BYTES_SCHEMA
							: WRAPPED_OPT_BYTES_SCHEMA;
					decoder = column.encrypted()
							? OraCdcDecoderFactory.get(schema, decrypter, column.salted())
							: OraCdcDecoderFactory.get(schema);
				}
			}
			case LONGVARBINARY -> {
				if (suppLogAll) {
					schema = column.mandatory()
							? Schema.BYTES_SCHEMA
							: Schema.OPTIONAL_BYTES_SCHEMA;
					decoder = decoderFromJdbcType(column);
				} else {
					schema = column.mandatory()
							? WRAPPED_BYTES_SCHEMA
							: WRAPPED_OPT_BYTES_SCHEMA;
					decoder = decoderFromJdbcType(column, schema);
				}
			}
			case BLOB, CLOB, NCLOB, SQLXML, VECTOR -> {
				schema = null;		//TODO re-implement LOB transform here
				decoder = decoderFromJdbcType(column);
			}
			case JSON -> {
				schema = null;		//TODO re-implement LOB transform here
				decoder = column.encrypted()
						? OraCdcDecoderFactory.get(rdbmsInfo == null ? new OracleJsonFactory() : rdbmsInfo.jsonFactory(),
											decrypter, column.salted())
						: OraCdcDecoderFactory.get(rdbmsInfo == null ? new OracleJsonFactory() : rdbmsInfo.jsonFactory());
			}
			default -> {
				schema = null;
				decoder = null;
			}
		}
		column.decoder(decoder);
		return schema;
	}

	private OraCdcDecoder decoderFromJdbcType(final OraColumn column) {
		return column.encrypted()
				? OraCdcDecoderFactory.get(column.getJdbcType(), decrypter, column.salted())
				: OraCdcDecoderFactory.get(column.getJdbcType());
	}

	private OraCdcDecoder decoderFromJdbcType(final OraColumn column, final Schema schema) {
		return column.encrypted()
				? OraCdcDecoderFactory.get(schema, column.getJdbcType(), decrypter, column.salted())
				: OraCdcDecoderFactory.get(schema, column.getJdbcType());
	}

	private Schema optionalOrRequired(boolean mandatory, SchemaBuilder builder) {
		return mandatory
				? builder.required().build()
				: builder.optional().build();
	}

	private void logDefaultValueError(String columnName, String dataTypeName, String defaultValue) {
		LOGGER.error(
				"""
				
				=====================
				Unable to convert default value (DATA_DEFAULT) "{}" of column '{}' to '{}'!
				This default value "{}" will be ignored! 
				=====================
				
				""", defaultValue, columnName, dataTypeName, defaultValue);
	}

	private Schema stringSchema(final OraColumn column) {
		if (suppLogAll) {
			var builder = SchemaBuilder.string();
			if (column.defaultValuePresent()) {
				String typedDefaultValue = null;
				if (Strings.CS.startsWith(column.defaultValue(), "'") &&
						Strings.CS.endsWith(column.defaultValue(), "'")) {
					typedDefaultValue = StringUtils.substringBetween(column.defaultValue(), "'", "'");
					column.typedDefaultValue(typedDefaultValue);
					LOGGER.trace("Setting default value of column '{}' to '{}'",
							column.getColumnName(), typedDefaultValue);
				} else {
					typedDefaultValue = column.defaultValue();
					LOGGER.warn(
							"""
							
							=====================
							Default value for CHAR/NCHAR/VARCHAR2/NVARCHAR2 must be inside single quotes!
							Setting default value (DATA_DEFAULT) of column '{}' to "{}"
							=====================
							
							""", column.getColumnName(), typedDefaultValue);
				}
				builder = builder.defaultValue(typedDefaultValue);
			}
			return optionalOrRequired(column.mandatory(), builder);
		} else
			return column.mandatory()
					? WRAPPED_STRING_SCHEMA
					: WRAPPED_OPT_STRING_SCHEMA;
	}

}
