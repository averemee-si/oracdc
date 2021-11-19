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

package eu.solutions.a2.cdc.oracle;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import eu.solutions.a2.cdc.oracle.data.OraBlob;
import eu.solutions.a2.cdc.oracle.data.OraClob;
import eu.solutions.a2.cdc.oracle.data.OraNClob;
import eu.solutions.a2.cdc.oracle.data.OraNumber;
import eu.solutions.a2.cdc.oracle.data.OraTimestamp;
import eu.solutions.a2.cdc.oracle.data.OraXmlBinary;
import eu.solutions.a2.cdc.oracle.schema.JdbcTypes;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import oracle.sql.NUMBER;

/**
 * 
 * @author averemee
 *
 */
@JsonInclude(Include.NON_EMPTY)
public class OraColumn {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraColumn.class);

	public static final String ROWID_KEY = "ORA_ROW_ID";
	public static final String MVLOG_SEQUENCE = "SEQUENCE$$";
	public static final String ORA_ROWSCN = "ORA_ROWSCN";

	private String columnName;
	private String nameFromId;
	private boolean partOfPk;
	private int jdbcType;
	private boolean nullable;
	private Integer dataScale;
	private Boolean binaryFloatDouble;
	private Boolean localTimeZone;
	private Boolean secureFile;
	private Boolean defaultValuePresent;
	private String defaultValue;
	private Object typedDefaultValue;
	private String storageColumnName;

	/**
	 * 
	 * Used in Source Connector
	 * 
	 * @param mviewSource         for MView log or archived redo log
	 * @param useOracdcSchemas    true for extended schemas
	 * @param processLobs         when true and useOracdcSchemas eq true BLOB/CLOB/NCLOB columns are processed
	 * @param resultSet
	 * @param keySchema
	 * @param valueSchema
	 * @param schemaType
	 * @param pkColsSet
	 * @throws SQLException
	 * @throws UnsupportedColumnDataTypeException 
	 */
	public OraColumn(
			final boolean mviewSource,
			final boolean useOracdcSchemas,
			final boolean processLobs,
			final ResultSet resultSet,
			final SchemaBuilder keySchema,
			final SchemaBuilder valueSchema,
			final int schemaType,
			final Set<String> pkColsSet) throws SQLException, UnsupportedColumnDataTypeException {
		this.columnName = resultSet.getString("COLUMN_NAME");
		this.nullable = "Y".equals(resultSet.getString("NULLABLE")) ? true : false;
		this.nameFromId = "\"COL " + resultSet.getInt("COLUMN_ID") + "\"";

		defaultValue = resultSet.getString("DATA_DEFAULT");
		if (resultSet.wasNull()) {
			this.defaultValuePresent = false;
		} else {
			this.defaultValuePresent = true;
		}

		if (mviewSource) {
			final String partOfPkString = resultSet.getString("PK");
			if (!resultSet.wasNull() && "Y".equals(partOfPkString)) {
				this.partOfPk = true;
			} else {
				this.partOfPk = false;
			}
		} else {
			if (pkColsSet != null && pkColsSet.contains(this.columnName)) {
				this.partOfPk = true;
			} else {
				this.partOfPk = false;
			}
		}

		final String oraType = resultSet.getString("DATA_TYPE");
		if ("DATE".equals(oraType) || StringUtils.startsWith(oraType, "TIMESTAMP")) {
			if (useOracdcSchemas) {
				dataScale = resultSet.getInt("DATA_SCALE");
				if (resultSet.wasNull()) {
					dataScale = null;
				}
				if (StringUtils.endsWith(oraType, "WITH LOCAL TIME ZONE")) {
					// 231:
					// TIMESTAMP [(fractional_seconds)] WITH LOCAL TIME ZONE
					localTimeZone = true;
					jdbcType = Types.TIMESTAMP_WITH_TIMEZONE;
					oraTimestampField(keySchema, valueSchema);
				} else if (StringUtils.endsWith(oraType, "WITH TIME ZONE")) {
					// 181: TIMESTAMP [(fractional_seconds)] WITH TIME ZONE
					localTimeZone = false;
					jdbcType = Types.TIMESTAMP_WITH_TIMEZONE;
					oraTimestampField(keySchema, valueSchema);
				} else {
					// 12: DATE, 180: TIMESTAMP [(fractional_seconds_precision)]
					jdbcType = Types.TIMESTAMP;
					timestampField(keySchema, valueSchema);
				}
			} else {
				jdbcType = Types.TIMESTAMP;
				timestampField(keySchema, valueSchema);
			}
		} else {
			switch (oraType) {
				case "FLOAT":
					// A subtype of the NUMBER datatype having precision p.
					// A FLOAT value is represented internally as NUMBER.
					// The precision p can range from 1 to 126 binary digits.
					// A FLOAT value requires from 1 to 22 bytes.
					if (useOracdcSchemas) {
						jdbcType = Types.NUMERIC;
						oraNumberField(keySchema, valueSchema);
					} else {
						binaryFloatDouble = false;
						jdbcType = Types.DOUBLE;
						doubleField(keySchema, valueSchema);
					}
					break;
				case "NUMBER":
					int dataPrecision = resultSet.getInt("DATA_PRECISION");
					final boolean precisionIsNull = resultSet.wasNull();
					dataScale = resultSet.getInt("DATA_SCALE");
					final boolean scaleIsNull = resultSet.wasNull();
					if (scaleIsNull) {
						dataScale = null;
					} else if (precisionIsNull) {
						//DATA_SCALE set but DATA_PRECISION is unknown....
						//Set it to MAX
						dataPrecision = 38;
					}
					if (precisionIsNull && scaleIsNull) {
						// NUMBER w/out precision and scale
						// OEBS and other legacy systems specific
						// Can be Integer or decimal or float....
						if (useOracdcSchemas) {
							jdbcType = Types.NUMERIC;
							oraNumberField(keySchema, valueSchema);
						} else {
							binaryFloatDouble = false;
							jdbcType = Types.DOUBLE;
							doubleField(keySchema, valueSchema);
						}
					} else if (dataScale == null || dataScale == 0) {
						// Integer 
						if (dataPrecision < 3) {
							jdbcType = Types.TINYINT;
							byteField(keySchema, valueSchema);
						} else if (dataPrecision < 5) {
							jdbcType = Types.SMALLINT;
							shortField(keySchema, valueSchema);
						} else if (dataPrecision < 10) {
							jdbcType = Types.INTEGER;
							intField(keySchema, valueSchema);
						} else if (dataPrecision < 19) {
							jdbcType = Types.BIGINT;
							longField(keySchema, valueSchema);
						} else {
							// Too big for BIGINT...
							jdbcType = Types.DECIMAL;
							decimalField(0, keySchema, valueSchema);
						}
					} else {
						// Decimal values
						jdbcType = Types.DECIMAL;
						decimalField(dataScale, keySchema, valueSchema);
					}
					break;
				case "BINARY_FLOAT":
					jdbcType = Types.FLOAT;
					binaryFloatDouble = true;
					floatField(keySchema, valueSchema);
					break;
				case "BINARY_DOUBLE":
					jdbcType = Types.DOUBLE;
					binaryFloatDouble = true;
					doubleField(keySchema, valueSchema);
					break;
				case "CHAR":
					jdbcType = Types.CHAR;
					stringField(keySchema, valueSchema);
					break;
				case "NCHAR":
					jdbcType = Types.NCHAR;
					stringField(keySchema, valueSchema);
					break;
				case "VARCHAR2":
					jdbcType = Types.VARCHAR;
					stringField(keySchema, valueSchema);
					break;
				case "NVARCHAR2":
					jdbcType = Types.NVARCHAR;
					stringField(keySchema, valueSchema);
					break;
				case "CLOB":
					jdbcType = Types.CLOB;
					if (mviewSource) {
						stringField(keySchema, valueSchema);
					} else if (processLobs) {
						// Archived redo as source and LOB processing
						setLobAttributes(resultSet);
					}
					break;
				case "NCLOB":
					jdbcType = Types.NCLOB;
					if (mviewSource) {
						stringField(keySchema, valueSchema);
					} else if (processLobs) {
						// Archived redo as source and LOB processing
						setLobAttributes(resultSet);
					}
					break;
				case "RAW":
					jdbcType = Types.BINARY;
					bytesField(keySchema, valueSchema);
					break;
				case "BLOB":
					jdbcType = Types.BLOB;
					if (mviewSource) {
						bytesField(keySchema, valueSchema);
					} else if (processLobs) {
						// Archived redo as source and LOB processing
						setLobAttributes(resultSet);
					}
					break;
				case "XMLTYPE":
					jdbcType = Types.SQLXML;
					if (mviewSource) {
						stringField(keySchema, valueSchema);
					} else if (processLobs) {
						// Archived redo as source and LOB processing
						setLobAttributes(resultSet);
						storageColumnName = resultSet.getString("STORAGE_NAME");
					}
					break;
				default:
					LOGGER.warn("Datatype {} for column {} is not supported!",
							oraType, this.columnName);
					throw new UnsupportedColumnDataTypeException(this.columnName);
			}
		}
		schemaEpilogue(keySchema, valueSchema, schemaType);
	}

	/**
	 * Used in Sink connector
	 * 
	 * @param avroSchema
	 * @param partOfPk
	 */
	public OraColumn(final Field field, final boolean partOfPk) throws SQLException {
		this.columnName = field.name();
		this.partOfPk = partOfPk;
		this.nullable = field.schema().isOptional();
		this.nameFromId = null;
		final String typeFromSchema = field.schema().type().getName().toUpperCase();
		switch (typeFromSchema) {
		case "INT8":
			jdbcType = Types.TINYINT;
			break;
		case "INT16":
			jdbcType = Types.SMALLINT;
			break;
		case "INT32":
			if (field.schema().name() != null && Date.LOGICAL_NAME.equals(field.schema().name())) {
				jdbcType = Types.DATE;
			} else {
				jdbcType = Types.INTEGER;
			}
			break;
		case "INT64":
			if (field.schema().name() != null && Timestamp.LOGICAL_NAME.equals(field.schema().name())) {
				jdbcType = Types.TIMESTAMP;
			} else {
				jdbcType = Types.BIGINT;
			}
			break;
		case "FLOAT32":
			jdbcType = Types.FLOAT;
			break;
		case "FLOAT64":
			jdbcType = Types.DOUBLE;
			break;
		case "BYTES":
			if (field.schema().name() != null) {
				switch (field.schema().name()) {
				case Decimal.LOGICAL_NAME:
					jdbcType = Types.DECIMAL;
					try {
						dataScale = Integer.valueOf(field.schema().parameters().get(Decimal.SCALE_FIELD));
					} catch (Exception e) {
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
					}
					break;
				case OraNumber.LOGICAL_NAME:
					jdbcType = Types.NUMERIC;
					break;
				case OraBlob.LOGICAL_NAME:
					jdbcType = Types.BLOB;
					break;
				case OraClob.LOGICAL_NAME:
					jdbcType = Types.CLOB;
					break;
				case OraNClob.LOGICAL_NAME:
					jdbcType = Types.NCLOB;
					break;
				case OraXmlBinary.LOGICAL_NAME:
					jdbcType = Types.SQLXML;
					break;
				default:
					LOGGER.error("Unknown logical name {} for BYTES Schema.", field.schema().name());
					LOGGER.error("Setting column {} JDBC type to binary.", field.name());
					jdbcType = Types.BINARY;
				}
			} else {
				jdbcType = Types.BINARY;
			}
			if (Decimal.LOGICAL_NAME.equals(field.schema().name())) {
			} else if (OraNumber.LOGICAL_NAME.equals(field.schema().name())) {
			}
			break;
		case "STRING":
			if (field.schema().name() != null && OraTimestamp.LOGICAL_NAME.equals(field.schema().name())) {
				jdbcType = Types.TIMESTAMP_WITH_TIMEZONE;
			} else {
				jdbcType = Types.VARCHAR;
			}
			break;
		default:
			throw new SQLException("Not supported type '" + typeFromSchema + "'!");
		}
	}

	/**
	 * 
	 * Deserialize OraColumn
	 * 
	 * @param columnData
	 * @param keySchema
	 * @param valueSchema
	 * @param schemaType
	 * @throws SQLException 
	 */
	public OraColumn(Map<String, Object> columnData,
			final SchemaBuilder keySchema, final SchemaBuilder valueSchema,
			final int schemaType) throws SQLException {
		columnName = (String) columnData.get("columnName");
		nameFromId = (String) columnData.get("nameFromId");
		partOfPk = (boolean) columnData.get("partOfPk");
		jdbcType = (int) columnData.get("jdbcType");
		nullable = (boolean) columnData.get("nullable");
		dataScale = (Integer) columnData.get("dataScale");
		binaryFloatDouble = (Boolean) columnData.get("binaryFloatDouble");
		localTimeZone = (Boolean) columnData.get("localTimeZone");
		
		switch (jdbcType) {
		case Types.DATE:
		case Types.TIMESTAMP:
			timestampField(keySchema, valueSchema);
			break;
		case Types.TIMESTAMP_WITH_TIMEZONE:
			if (localTimeZone == null) {
				localTimeZone = false;
			}
			// This is only for oracdc extended types!!!
			oraTimestampField(keySchema, valueSchema);
		case Types.TINYINT:
			byteField(keySchema, valueSchema);
			break;
		case Types.SMALLINT:
			shortField(keySchema, valueSchema);
			break;
		case Types.INTEGER:
			intField(keySchema, valueSchema);
			break;
		case Types.BIGINT:
			longField(keySchema, valueSchema);
			break;
		case Types.DECIMAL:
			if (dataScale == null) {
				dataScale = 0;
			}
			decimalField(dataScale, keySchema, valueSchema);
			break;
		case Types.NUMERIC:
			// This is only for oracdc extended types!!!
			oraNumberField(keySchema, valueSchema);
			break;
		case Types.FLOAT:
			if (binaryFloatDouble == null) {
				binaryFloatDouble = false;
			}
			floatField(keySchema, valueSchema);
			break;
		case Types.DOUBLE:
			if (binaryFloatDouble == null) {
				binaryFloatDouble = false;
			}
			doubleField(keySchema, valueSchema);
			break;
		case Types.BINARY:
			bytesField(keySchema, valueSchema);
			break;
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.NCHAR:
		case Types.NVARCHAR:
		case Types.ROWID:
			stringField(keySchema, valueSchema);
			break;
		case Types.CLOB:
			valueSchema.field(this.columnName, OraClob.schema());
			break;
		case Types.NCLOB:
			valueSchema.field(this.columnName, OraNClob.schema());
			break;
		case Types.BLOB:
			valueSchema.field(this.columnName, OraBlob.schema());
			break;
		case Types.SQLXML:
			valueSchema.field(this.columnName, OraXmlBinary.schema());
			break;
		default:
			throw new SQLException("Unsupported JDBC type " +
					jdbcType + " for column " +
					columnName + ".");
		}
		schemaEpilogue(keySchema, valueSchema, schemaType);
	}

	private void schemaEpilogue(
			final SchemaBuilder keySchema, final SchemaBuilder valueSchema,
			final int schemaType) {
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM && this.partOfPk) {
			valueSchema.field(this.columnName,
					keySchema.build().field(this.columnName).schema());
		}
	}
	

	/**
	 * Used internally for ROWID support
	 * 
	 * @param columnName
	 * @param partOfPk
	 * @param jdbcType
	 * @param nullable
	 */
	private OraColumn(
			final String columnName,
			final boolean partOfPk,
			final int jdbcType,
			boolean nullable) {
		this.columnName = columnName;
		this.partOfPk = partOfPk;
		this.jdbcType = jdbcType;
		this.nullable = nullable;
		this.nameFromId = null;
	}

	/*
	 * New Style call... ... ...
	 */
	public static OraColumn getRowIdKey() {
		OraColumn rowIdColumn = new OraColumn(ROWID_KEY, true, Types.ROWID, false);
		return rowIdColumn;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getNameFromId() {
		return nameFromId;
	}

	public void setNameFromId(String nameFromId) {
		this.nameFromId = nameFromId;
	}

	public boolean isPartOfPk() {
		return partOfPk;
	}

	public void setPartOfPk(boolean partOfPk) {
		this.partOfPk = partOfPk;
	}

	public int getJdbcType() {
		return jdbcType;
	}

	public void setJdbcType(int jdbcType) {
		this.jdbcType = jdbcType;
	}

	public boolean isNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	public Integer getDataScale() {
		return dataScale;
	}

	public void setDataScale(Integer dataScale) {
		this.dataScale = dataScale;
	}

	public Boolean isBinaryFloatDouble() {
		return binaryFloatDouble;
	}

	public void setBinaryFloatDouble(Boolean binaryFloatDouble) {
		this.binaryFloatDouble = binaryFloatDouble;
	}

	public Boolean isLocalTimeZone() {
		return localTimeZone;
	}

	public void setLocalTimeZone(Boolean localTimeZone) {
		this.localTimeZone = localTimeZone;
	}

	public Boolean getSecureFile() {
		return secureFile;
	}

	public void setSecureFile(Boolean secureFile) {
		this.secureFile = secureFile;
	}

	public Boolean getDefaultValuePresent() {
		return defaultValuePresent;
	}

	public void setDefaultValuePresent(Boolean defaultValuePresent) {
		this.defaultValuePresent = defaultValuePresent;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public Object getTypedDefaultValue() {
		if (!defaultValuePresent) {
			return null;
		} else if (typedDefaultValue == null) {
			//TODO
			//TODO Currently only Oracle VARCHAR2/NVARCHAR2 and NUMBER supported
			//TODO
			try {
				switch (jdbcType) {
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.NCHAR:
				case Types.NVARCHAR:
					if (StringUtils.startsWith(defaultValue, "'") &&
						StringUtils.endsWith(defaultValue, "'")) {
						typedDefaultValue = StringUtils.substringBetween(defaultValue, "'", "'");
					} else {
						LOGGER.warn("Default value for CHAR/NCHAR/VARCHAR2/NVARCHAR2 must be inside single quotes!");
						typedDefaultValue = defaultValue;
					}
					break;
				case Types.TINYINT:
					typedDefaultValue = Byte.parseByte(defaultValue);
					break;
				case Types.SMALLINT:
					typedDefaultValue = Short.parseShort(defaultValue);
					break;
				case Types.INTEGER:
					typedDefaultValue = Integer.parseInt(defaultValue);
					break;
				case Types.BIGINT:
					typedDefaultValue = Long.parseLong(defaultValue);
					break;
				case Types.FLOAT:
					typedDefaultValue = Float.parseFloat(defaultValue);
					break;
				case Types.DOUBLE:
					typedDefaultValue = Double.parseDouble(defaultValue);
					break;
				case Types.DECIMAL:
					typedDefaultValue = (new BigDecimal(defaultValue)).setScale(dataScale);
					break;
				case Types.NUMERIC:
					try {
						typedDefaultValue = (new NUMBER(defaultValue, 10)).getBytes();
					} catch (SQLException sqle) {
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
						throw new NumberFormatException(sqle.getMessage());
					}
					break;
				default:
					LOGGER.error("Default value {} for column {} with type {} currently is not supported!",
						defaultValue, columnName, JdbcTypes.getTypeName(jdbcType));
					typedDefaultValue = null;
				}
			} catch (NumberFormatException nfe) {
				LOGGER.error("Invalid number value {} for column {} with type {}!\nSetting it to null!!!",
						defaultValue, columnName, JdbcTypes.getTypeName(jdbcType));
				typedDefaultValue = null;
			}
			return typedDefaultValue;
		} else {
			return typedDefaultValue;
		}
	}

	public String getStorageColumnName() {
		return storageColumnName;
	}

	/**
	 * 
	 * @param dbType
	 * @param statement
	 * @param columnNo
	 * @param columnValue
	 * @throws SQLException
	 */
	public void bindWithPrepStmt(
			final int dbType,
			final PreparedStatement statement,
			final int columnNo,
			final Object columnValue) throws SQLException  {
		if (columnValue == null) {
			statement.setNull(columnNo, jdbcType);
		} else {
			switch (jdbcType) {
			case Types.DATE:
				statement.setDate(columnNo, new java.sql.Date(((java.util.Date) columnValue).getTime()));
				break;
			case Types.TIMESTAMP:
				statement.setTimestamp(columnNo, new java.sql.Timestamp(((java.util.Date) columnValue).getTime()));
				break;
			case Types.TIMESTAMP_WITH_TIMEZONE:
				statement.setObject(columnNo, OraTimestamp.toLogical((String) columnValue));
				break;
			case Types.BOOLEAN:
				statement.setBoolean(columnNo, (boolean) columnValue);
				break;
			case Types.TINYINT:
				statement.setByte(columnNo, (Byte) columnValue);
				break;
			case Types.SMALLINT:
				statement.setShort(columnNo, (Short) columnValue);
				break;
			case Types.INTEGER:
				statement.setInt(columnNo, (Integer) columnValue);
				break;
			case Types.BIGINT:
				try {
					statement.setLong(columnNo, (Long) columnValue);
				} catch (ClassCastException cce) {
					statement.setLong(columnNo, (Integer) columnValue);
				}
				break;
			case Types.FLOAT:
				statement.setFloat(columnNo, (float) columnValue);
				break;
			case Types.DOUBLE:
				statement.setDouble(columnNo, (double) columnValue);
				break;
			case Types.DECIMAL:
				statement.setBigDecimal(columnNo, (BigDecimal) columnValue);
				break;
			case Types.NUMERIC:
				BigDecimal bd = OraNumber.toLogical(((ByteBuffer) columnValue).array());
				if (bd == null) {
					statement.setNull(columnNo, Types.NUMERIC);
				} else {
					statement.setBigDecimal(columnNo, bd);
				}
				break;
			case Types.BINARY:
				statement.setBytes(columnNo, ((ByteBuffer) columnValue).array());
				break;
			case Types.VARCHAR:
				// 0x00 PostgreSQL problem
				if (dbType == OraCdcJdbcSinkConnectionPool.DB_TYPE_POSTGRESQL) {
					statement.setString(columnNo, StringUtils.replace((String) columnValue, "\0", StringUtils.EMPTY));
				} else { 
					statement.setString(columnNo, (String) columnValue);
				}
				break;
			default:
				LOGGER.error("Unsupported data type {} for column {}.",
						JdbcTypes.getTypeName(jdbcType), columnName);
				throw new SQLException("Unsupported data type: " + JdbcTypes.getTypeName(jdbcType));
			}
		}
	}

	public String unsupportedTypeValue() {
		final StringBuilder sb = new StringBuilder(128);
		sb.append("Column: ");
		sb.append(columnName);
		sb.append(", JDBC Type Code ");
		sb.append(jdbcType);
		sb.append(" support not yet implemented!");
		return sb.toString();
	}

	private void stringField(final SchemaBuilder keySchema, final SchemaBuilder valueSchema) {
		if (this.partOfPk) {
			keySchema.field(this.columnName, Schema.STRING_SCHEMA);
		} else {
			if (this.nullable) {
				valueSchema.field(this.columnName, Schema.OPTIONAL_STRING_SCHEMA);
			} else {
				valueSchema.field(this.columnName, Schema.STRING_SCHEMA);
			}
		}
	}

	private void bytesField(final SchemaBuilder keySchema, final SchemaBuilder valueSchema) {
		if (this.partOfPk) {
			keySchema.field(this.columnName, Schema.BYTES_SCHEMA);
		} else {
			if (this.nullable) {
				valueSchema.field(this.columnName, Schema.OPTIONAL_BYTES_SCHEMA);
			} else {
				valueSchema.field(this.columnName, Schema.BYTES_SCHEMA);
			}
		}
	}

	private void byteField(final SchemaBuilder keySchema, final SchemaBuilder valueSchema) {
		if (this.partOfPk) {
			keySchema.field(this.columnName, Schema.INT8_SCHEMA);
		} else {
			if (this.nullable) {
				valueSchema.field(this.columnName, Schema.OPTIONAL_INT8_SCHEMA);
			} else {
				valueSchema.field(this.columnName, Schema.INT8_SCHEMA);
			}
		}
	}

	private void shortField(final SchemaBuilder keySchema, final SchemaBuilder valueSchema) {
		if (this.partOfPk) {
			keySchema.field(this.columnName, Schema.INT16_SCHEMA);
		} else {
			if (this.nullable) {
				valueSchema.field(this.columnName, Schema.OPTIONAL_INT16_SCHEMA);
			} else {
				valueSchema.field(this.columnName, Schema.INT16_SCHEMA);
			}
		}
	}

	private void intField(final SchemaBuilder keySchema, final SchemaBuilder valueSchema) {
		if (this.partOfPk) {
			keySchema.field(this.columnName, Schema.INT32_SCHEMA);
		} else {
			if (this.nullable) {
				valueSchema.field(this.columnName, Schema.OPTIONAL_INT32_SCHEMA);
			} else {
				valueSchema.field(this.columnName, Schema.INT32_SCHEMA);
			}
		}
	}

	private void longField(final SchemaBuilder keySchema, final SchemaBuilder valueSchema) {
		if (this.partOfPk) {
			keySchema.field(this.columnName, Schema.INT64_SCHEMA);
		} else {
			if (this.nullable) {
				valueSchema.field(this.columnName, Schema.OPTIONAL_INT64_SCHEMA);
			} else {
				valueSchema.field(this.columnName, Schema.INT64_SCHEMA);
			}
		}
	}

	private void decimalField(final int scale, final SchemaBuilder keySchema, final SchemaBuilder valueSchema) {
		if (this.partOfPk) {
			keySchema.field(this.columnName, Decimal.builder(scale).required().build());
		} else {
			if (this.nullable) {
				valueSchema.field(this.columnName, Decimal.builder(scale).optional().build());
			} else {
				valueSchema.field(this.columnName, Decimal.builder(scale).required().build());
			}
		}
	}

	private void doubleField(final SchemaBuilder keySchema, final SchemaBuilder valueSchema) {
		if (this.partOfPk) {
			keySchema.field(this.columnName, Schema.FLOAT64_SCHEMA);
		} else {
			if (this.nullable) {
				valueSchema.field(this.columnName, Schema.OPTIONAL_FLOAT64_SCHEMA);
			} else {
				valueSchema.field(this.columnName, Schema.FLOAT64_SCHEMA);
			}
		}
	}

	private void floatField(final SchemaBuilder keySchema, final SchemaBuilder valueSchema) {
		if (this.partOfPk) {
			keySchema.field(this.columnName, Schema.FLOAT32_SCHEMA);
		} else {
			if (this.nullable) {
				valueSchema.field(this.columnName, Schema.OPTIONAL_FLOAT32_SCHEMA);
			} else {
				valueSchema.field(this.columnName, Schema.FLOAT32_SCHEMA);
			}
		}
	}

	private void oraNumberField(final SchemaBuilder keySchema, final SchemaBuilder valueSchema) {
		if (this.partOfPk) {
			keySchema.field(this.columnName, OraNumber.builder().required().build());
		} else {
			if (this.nullable) {
				valueSchema.field(this.columnName, OraNumber.builder().optional().build());
			} else {
				valueSchema.field(this.columnName, OraNumber.builder().required().build());
			}
		}
	}

	private void timestampField(final SchemaBuilder keySchema, final SchemaBuilder valueSchema) {
		if (this.partOfPk) {
			keySchema.field(this.columnName, Timestamp.builder().required().build());
		} else {
			if (this.nullable) {
				valueSchema.field(this.columnName, Timestamp.builder().optional().build());
			} else {
				valueSchema.field(this.columnName, Timestamp.builder().required().build());
			}
		}
	}

	private void oraTimestampField(final SchemaBuilder keySchema, final SchemaBuilder valueSchema) {
		if (this.partOfPk) {
			keySchema.field(this.columnName, OraTimestamp.builder().required().build());
		} else {
			if (this.nullable) {
				valueSchema.field(this.columnName, OraTimestamp.builder().optional().build());
			} else {
				valueSchema.field(this.columnName, OraTimestamp.builder().required().build());
			}
		}
	}

	private void setLobAttributes(ResultSet resultSet) throws SQLException {
		if ("YES".equalsIgnoreCase(resultSet.getString("SECUREFILE"))) {
			secureFile = true;
		} else {
			secureFile = false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(
				columnName, nameFromId, partOfPk, jdbcType, nullable,
				dataScale, binaryFloatDouble, localTimeZone, secureFile,
				defaultValuePresent, defaultValue, storageColumnName);
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
		OraColumn other = (OraColumn) obj;
		return
				Objects.equals(columnName, other.columnName) &&
				Objects.equals(nameFromId, other.nameFromId) &&
				partOfPk == other.partOfPk &&
				jdbcType == other.jdbcType &&
				nullable == other.nullable &&
				Objects.equals(dataScale, other.dataScale) && 
				Objects.equals(binaryFloatDouble, other.binaryFloatDouble) &&
				Objects.equals(localTimeZone, other.localTimeZone) &&
				Objects.equals(secureFile, other.secureFile) &&
				Objects.equals(defaultValuePresent, other.defaultValuePresent) &&
				Objects.equals(defaultValue, other.defaultValue) &&
				Objects.equals(storageColumnName, other.storageColumnName);
	}

}
