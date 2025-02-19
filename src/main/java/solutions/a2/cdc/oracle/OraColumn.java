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

package solutions.a2.cdc.oracle;

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
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleResultSet;
import oracle.sql.NUMBER;
import solutions.a2.cdc.oracle.data.OraBlob;
import solutions.a2.cdc.oracle.data.OraClob;
import solutions.a2.cdc.oracle.data.OraInterval;
import solutions.a2.cdc.oracle.data.OraIntervalDS;
import solutions.a2.cdc.oracle.data.OraIntervalYM;
import solutions.a2.cdc.oracle.data.OraNClob;
import solutions.a2.cdc.oracle.data.OraNumber;
import solutions.a2.cdc.oracle.data.OraTimestamp;
import solutions.a2.cdc.oracle.data.OraXmlBinary;
import solutions.a2.cdc.oracle.schema.JdbcTypes;
import solutions.a2.cdc.oracle.utils.KafkaUtils;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.kafka.sink.JdbcSinkConnectionPool;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
@JsonInclude(Include.NON_EMPTY)
public class OraColumn {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraColumn.class);

	public static final String ROWID_KEY = "ORA_ROW_ID";
	public static final String MVLOG_SEQUENCE = "SEQUENCE$$";
	public static final String ORA_ROWSCN = "ORA_ROWSCN";

	public static final int JAVA_SQL_TYPE_INTERVALYM_STRING = -2_000_000_001;
	public static final int JAVA_SQL_TYPE_INTERVALYM_BINARY = -2_000_000_002;
	public static final int JAVA_SQL_TYPE_INTERVALDS_STRING = -2_000_000_003;
	public static final int JAVA_SQL_TYPE_INTERVALDS_BINARY = -2_000_000_004;

	private static final String TYPE_VARCHAR2 = "VARCHAR2";
	private static final String TYPE_NVARCHAR2 = "NVARCHAR2";
	private static final String TYPE_CHAR = "CHAR";
	private static final String TYPE_NCHAR = "NCHAR";
	private static final String TYPE_FLOAT = "FLOAT";
	private static final String TYPE_RAW = "RAW";
	private static final String TYPE_DATE = "DATE";
	private static final String TYPE_TIMESTAMP = "TIMESTAMP";
	private static final String TYPE_NUMBER = "NUMBER";
	private static final String TYPE_BINARY_FLOAT = "BINARY_FLOAT";
	private static final String TYPE_BINARY_DOUBLE = "BINARY_DOUBLE";
	private static final String TYPE_BLOB = "BLOB";
	private static final String TYPE_CLOB = "CLOB";
	private static final String TYPE_NCLOB = "NCLOB";
	private static final String TYPE_XMLTYPE = "XMLTYPE";

	private String columnName;
	private int columnId;
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
	@JsonIgnore
	private Schema schema;
	private String oracleName;
	private boolean partOfKeyStruct;
	private boolean number = false;

	/**
	 * 
	 * Used in Source Connector
	 * 
	 * @param mviewSource         for MView log or archived redo log
	 * @param useOracdcSchemas    true for extended schemas
	 * @param processLobs         when true and useOracdcSchemas eq true BLOB/CLOB/NCLOB columns are processed
	 * @param resultSet
	 * @param pkColsSet
	 * @throws SQLException
	 * @throws UnsupportedColumnDataTypeException 
	 */
	public OraColumn(
			final boolean mviewSource,
			final boolean useOracdcSchemas,
			final boolean processLobs,
			final ResultSet resultSet,
			final Set<String> pkColsSet) throws SQLException, UnsupportedColumnDataTypeException {
		oracleName = resultSet.getString("COLUMN_NAME");
		if (!KafkaUtils.validAvroFieldName(oracleName)) {
			columnName = KafkaUtils.fixAvroFieldName(oracleName, "_");
			LOGGER.warn(
					"\n" +
					"=====================\n" +
					"Column name '{}' is incompatible with Avro/Protobuf naming standard.\n" +
					"This column name is changed to '{}'.\n" + 
					"=====================",
					oracleName, columnName);
		} else {
			columnName = oracleName;
		}
		this.nullable = "Y".equals(resultSet.getString("NULLABLE")) ? true : false;
		this.setColumnId(resultSet.getInt("COLUMN_ID"));

		defaultValue = resultSet.getString("DATA_DEFAULT");
		if (resultSet.wasNull()) {
			defaultValuePresent = false;
		} else {
			if (StringUtils.equalsIgnoreCase(
					StringUtils.trim(defaultValue), "NULL")) {
				defaultValuePresent = false;
				defaultValue = null;
			} else {
				defaultValuePresent = true;
			}
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
		dataScale = resultSet.getInt("DATA_SCALE");
		if (resultSet.wasNull()) {
			dataScale = null;
		}
		Integer dataPrecision = resultSet.getInt("DATA_PRECISION");
		if (resultSet.wasNull()) {
			dataPrecision = null;
		}
		detectTypeAndSchema(oraType, mviewSource, useOracdcSchemas, dataPrecision);

	}

	/**
	 *
	 * Used in Source connector for DDL processing
	 *   It is assumed that the column is not part of the primary key
	 *   It is assumed that the LOB column is SECUREFILE
	 * 
	 * @param useOracdcSchemas    true for extended schemas
	 * @param columnName
	 * @param columnAttributes
	 * @param originalDdl
	 * @param columnId
	 * @throws UnsupportedColumnDataTypeException 
	 */
	public OraColumn(
			final boolean useOracdcSchemas,
			final String columnName,
			final String columnAttributes,
			final String originalDdl,
			final int columnId)
			throws UnsupportedColumnDataTypeException {
		this.columnName = columnName;
		this.setColumnId(columnId);
		// 
		this.partOfPk = false;
		Integer dataPrecision = null;
		this.dataScale = null;
	

		if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_VARCHAR2)) {
			// Ignore SIZE for VARCHAR2/NVARCHAR2/CHAR/NCHAR/RAW
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_VARCHAR2, false, useOracdcSchemas, dataPrecision);
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_NVARCHAR2)) {
			// Ignore SIZE for VARCHAR2/NVARCHAR2/CHAR/NCHAR/RAW
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_NVARCHAR2, false, useOracdcSchemas, dataPrecision);
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_CHAR)) {
			// Ignore SIZE for VARCHAR2/NVARCHAR2/CHAR/NCHAR/RAW
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_CHAR, false, useOracdcSchemas, dataPrecision);
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_NCHAR)) {
			// Ignore SIZE for VARCHAR2/NVARCHAR2/CHAR/NCHAR/RAW
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_NCHAR, false, useOracdcSchemas, dataPrecision);
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_RAW)) {
			// Ignore SIZE for VARCHAR2/NVARCHAR2/CHAR/NCHAR/RAW
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_RAW, false, useOracdcSchemas, dataPrecision);
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_FLOAT)) {
			// Ignore PRECISION for FLOAT
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_FLOAT, false, useOracdcSchemas, dataPrecision);
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_DATE)) {
			// No PRECISION for DATE
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_DATE, false, useOracdcSchemas, dataPrecision);
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_TIMESTAMP)) {
			// Ignore fractional seconds!!!
			if (StringUtils.containsIgnoreCase(columnAttributes, "LOCAL")) {
				// 231: TIMESTAMP [(fractional_seconds_precision)] WITH LOCAL TIME ZONE
				detectIsNullAndDefault(columnAttributes);
				detectTypeAndSchema("TIMESTAMP WITH LOCAL TIME ZONE", false, useOracdcSchemas, dataPrecision);
			} else if (StringUtils.containsIgnoreCase(columnAttributes, "ZONE")) {
				// 181: TIMESTAMP [(fractional_seconds_precision)] WITH TIME ZONE
				detectIsNullAndDefault(columnAttributes);
				detectTypeAndSchema("TIMESTAMP WITH TIME ZONE", false, useOracdcSchemas, dataPrecision);
			} else {
				// 180: TIMESTAMP [(fractional_seconds_precision)]
				detectIsNullAndDefault(columnAttributes);
				detectTypeAndSchema(TYPE_TIMESTAMP, false, useOracdcSchemas, dataPrecision);
			}
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_NUMBER)) {
			detectIsNullAndDefault(columnAttributes);
			final String precisionAndScale = StringUtils.trim(
					StringUtils.substringBetween(columnAttributes, "(", ")")); 
			if (precisionAndScale != null) {
				try {
					if (StringUtils.contains(precisionAndScale, "|")) {
						final String[] tokens = StringUtils.split(precisionAndScale, "|");
						dataPrecision = Integer.parseInt(StringUtils.trim(tokens[0]));
						dataScale = Integer.parseInt(StringUtils.trim(tokens[1]));
					} else {
						dataPrecision = Integer.parseInt(precisionAndScale);
					}
				} catch (NumberFormatException nfe) {
					LOGGER.error("Unable to detect PRECISION and SCALE for column {} while parsing DDL command\n'{}'\nusing pre-processed from DDL '{}'",
							columnName, originalDdl, columnAttributes);
					LOGGER.error("Both PRECISION and SCALE are reset to NULL!!!");
					dataPrecision = null;
					dataScale = null;
				}
			}
			detectTypeAndSchema(TYPE_NUMBER, false, useOracdcSchemas, dataPrecision);
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_BINARY_FLOAT)) {
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_BINARY_FLOAT, false, useOracdcSchemas, dataPrecision);
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_BINARY_DOUBLE)) {
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_BINARY_DOUBLE, false, useOracdcSchemas, dataPrecision);
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_BLOB)) {
			nullable = true;
			defaultValuePresent = false;
			detectTypeAndSchema(TYPE_BLOB, false, useOracdcSchemas, dataPrecision);
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_CLOB)) {
			nullable = true;
			defaultValuePresent = false;
			detectTypeAndSchema(TYPE_CLOB, false, useOracdcSchemas, dataPrecision);
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_NCLOB)) {
			nullable = true;
			defaultValuePresent = false;
			detectTypeAndSchema(TYPE_NCLOB, false, useOracdcSchemas, dataPrecision);
		} else if (StringUtils.startsWithIgnoreCase(columnAttributes, TYPE_XMLTYPE)) {
			nullable = true;
			defaultValuePresent = false;
			detectTypeAndSchema(TYPE_XMLTYPE, false, useOracdcSchemas, dataPrecision);
		} else {
			throw new UnsupportedColumnDataTypeException("Unable to parse DDL statement\n'" +
												originalDdl + "'\nUnsupported datatype");
		}
		
	}


	private void detectIsNullAndDefault(final String columnAttributes) {
		int posNull = -1;
		int posNot = -1;
		int posDefault = -1;
		final String[] tokens = StringUtils.split(columnAttributes);
		for (int i = 0; i < tokens.length; i++) {
			if (StringUtils.equalsIgnoreCase(tokens[i], "null")) {
				posNull = i;
				if (StringUtils.equalsIgnoreCase(tokens[i - 1], "not")) {
					posNot = i - 1;
				}
			}
			if (StringUtils.equalsIgnoreCase(tokens[i], "default")) {
				posDefault = i;
			}
		}
		if (posNull == -1) {
			// By default - NULL
			nullable = true;
		} else if (posNot == -1) {
			// NULL explicitly set
			nullable = true;
		} else {
			nullable = false;
		}

		if (posDefault == -1) {
			defaultValuePresent = false;
		} else if (posDefault + 1 <= tokens.length - 1) {
			defaultValuePresent = true;
			defaultValue = tokens[posDefault + 1]; 
		}

	}

	private void detectTypeAndSchema(
			final String oraType,
			final boolean mviewSource,
			final boolean useOracdcSchemas,
			Integer dataPrecision) throws UnsupportedColumnDataTypeException {
		if (StringUtils.equals(oraType, TYPE_DATE) || StringUtils.startsWith(oraType, TYPE_TIMESTAMP)) {
			if (useOracdcSchemas) {
				if (StringUtils.endsWith(oraType, "WITH LOCAL TIME ZONE")) {
					// 231:
					// TIMESTAMP [(fractional_seconds)] WITH LOCAL TIME ZONE
					localTimeZone = true;
					jdbcType = Types.TIMESTAMP_WITH_TIMEZONE;
					oraTimestampField();
				} else if (StringUtils.endsWith(oraType, "WITH TIME ZONE")) {
					// 181: TIMESTAMP [(fractional_seconds)] WITH TIME ZONE
					localTimeZone = false;
					jdbcType = Types.TIMESTAMP_WITH_TIMEZONE;
					oraTimestampField();
				} else {
					// 12: DATE, 180: TIMESTAMP [(fractional_seconds_precision)]
					jdbcType = Types.TIMESTAMP;
					timestampField();
				}
			} else {
				jdbcType = Types.TIMESTAMP;
				timestampField();
			}
		} else if (StringUtils.startsWith(oraType, "INTERVAL")) {
			if (StringUtils.contains(oraType, "TO MONTH")) {
				if (useOracdcSchemas) {
					jdbcType = JAVA_SQL_TYPE_INTERVALYM_BINARY;
					if (partOfPk || !nullable) {
						schema = OraIntervalYM.builder().required().build();
					} else {
						schema = OraIntervalYM.builder().optional().build();
					}
				} else {
					jdbcType = JAVA_SQL_TYPE_INTERVALYM_STRING;
					oraIntervalField();				}
			} else {
				// 'TO SECOND'
				if (useOracdcSchemas) {
					jdbcType = JAVA_SQL_TYPE_INTERVALDS_BINARY;
					if (partOfPk || !nullable) {
						schema = OraIntervalDS.builder().required().build();
					} else {
						schema = OraIntervalDS.builder().optional().build();
					}
				} else {
					jdbcType = JAVA_SQL_TYPE_INTERVALDS_STRING;
					oraIntervalField();
				}
			}
		} else {
			switch (oraType) {
				case TYPE_FLOAT:
					// A subtype of the NUMBER datatype having precision p.
					// A FLOAT value is represented internally as NUMBER.
					// The precision p can range from 1 to 126 binary digits.
					// A FLOAT value requires from 1 to 22 bytes.
					number = true;
					if (useOracdcSchemas) {
						jdbcType = Types.NUMERIC;
						oraNumberField();
					} else {
						binaryFloatDouble = false;
						jdbcType = Types.DOUBLE;
						doubleField();
					}
					break;
				case TYPE_NUMBER:
					number = true;
					if (dataScale != null && dataPrecision == null) {
						//DATA_SCALE set but DATA_PRECISION is unknown....
						//Set it to MAX
						dataPrecision = 38;
					}
					if (dataPrecision == null && dataScale == null) {
						// NUMBER w/out precision and scale
						// OEBS and other legacy systems specific
						// Can be Integer or decimal or float....
						if (useOracdcSchemas) {
							jdbcType = Types.NUMERIC;
							oraNumberField();
						} else {
							binaryFloatDouble = false;
							jdbcType = Types.DOUBLE;
							doubleField();
						}
					} else if (dataScale == null || dataScale == 0) {
						// Integer 
						if (dataPrecision < 3) {
							jdbcType = Types.TINYINT;
							byteField();
						} else if (dataPrecision < 5) {
							jdbcType = Types.SMALLINT;
							shortField();
						} else if (dataPrecision < 10) {
							jdbcType = Types.INTEGER;
							intField();
						} else if (dataPrecision < 19) {
							jdbcType = Types.BIGINT;
							longField();
						} else {
							// Too big for BIGINT...
							jdbcType = Types.DECIMAL;
							decimalField(0);
						}
					} else {
						// Decimal values
						jdbcType = Types.DECIMAL;
						decimalField(dataScale);
					}
					break;
				case TYPE_BINARY_FLOAT:
					jdbcType = Types.FLOAT;
					binaryFloatDouble = true;
					floatField();
					break;
				case TYPE_BINARY_DOUBLE:
					jdbcType = Types.DOUBLE;
					binaryFloatDouble = true;
					doubleField();
					break;
				case TYPE_CHAR:
					jdbcType = Types.CHAR;
					stringField();
					break;
				case TYPE_NCHAR:
					jdbcType = Types.NCHAR;
					stringField();
					break;
				case TYPE_VARCHAR2:
					jdbcType = Types.VARCHAR;
					stringField();
					break;
				case TYPE_NVARCHAR2:
					jdbcType = Types.NVARCHAR;
					stringField();
					break;
				case TYPE_CLOB:
					jdbcType = Types.CLOB;
					if (mviewSource) {
						stringField();
					}
					break;
				case TYPE_NCLOB:
					jdbcType = Types.NCLOB;
					if (mviewSource) {
						stringField();
					}
					break;
				case TYPE_RAW:
					jdbcType = Types.BINARY;
					bytesField();
					break;
				case TYPE_BLOB:
					jdbcType = Types.BLOB;
					if (mviewSource) {
						bytesField();
					}
					break;
				case TYPE_XMLTYPE:
					jdbcType = Types.SQLXML;
					if (mviewSource) {
						stringField();
					}
					break;
				default:
					LOGGER.warn("Datatype {} for column {} is not supported!",
							oraType, this.columnName);
					throw new UnsupportedColumnDataTypeException(this.columnName);
			}
		}
	}

	/**
	 * Used in Sink connector
	 * 
	 * @param field
	 * @param partOfPk
	 * @param partOfKeyStruct
	 */
	public OraColumn(final Field field, final boolean partOfPk, final boolean partOfKeyStruct) throws SQLException {
		this.columnName = field.name();
		this.partOfPk = partOfPk;
		this.nullable = field.schema().isOptional();
		this.nameFromId = null;
		this.partOfKeyStruct = partOfKeyStruct;
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
				case OraIntervalYM.LOGICAL_NAME:
					jdbcType = JAVA_SQL_TYPE_INTERVALYM_BINARY;
					break;
				case OraIntervalDS.LOGICAL_NAME:
					jdbcType = JAVA_SQL_TYPE_INTERVALDS_BINARY;
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
			if (field.schema().name() != null) {
				switch (field.schema().name()) {
				case  OraTimestamp.LOGICAL_NAME:
					jdbcType = Types.TIMESTAMP_WITH_TIMEZONE;
					break;
				case OraInterval.LOGICAL_NAME:
					jdbcType = JAVA_SQL_TYPE_INTERVALDS_STRING;
					break;
				default:
					LOGGER.error("Unknown logical name {} for STRING Schema.", field.schema().name());
					LOGGER.error("Setting column {} JDBC type to varchar.", field.name());
					jdbcType = Types.VARCHAR;
				}
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
		columnId = (int) columnData.get("columnId");
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
		if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM && this.partOfPk) {
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

	public int getColumnId() {
		return columnId;
	}

	public void setColumnId(int columnId) {
		this.columnId = columnId;
		this.nameFromId = "\"COL " + this.columnId + "\"";
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

	public Boolean isDefaultValuePresent() {
		return defaultValuePresent;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	//TODO
	//TODO This will be splitted to appropriate type handler.....
	//TODO and this will be replaced with simple getter and setter for typedDefaultValue
	//TODO
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

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public Schema getSchema() {
		return schema;
	}

	public String getOracleName() {
		return oracleName;
	}

	public void setOracleName(String oracleName) {
		this.oracleName = oracleName;
	}

	/**
	 * 
	 * @param statement
	 * @param columnNo
	 * @param columnValue
	 * @throws SQLException
	 */
	public void bindWithPrepStmt(
			final PreparedStatement statement,
			final int columnNo,
			final Object columnValue) throws SQLException  {
		bindWithPrepStmt(JdbcSinkConnectionPool.DB_TYPE_ORACLE, statement, columnNo, columnValue);
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
				(partOfKeyStruct ? keyStruct.get(columnName) : valueStruct.get(columnName)));
	}

	/**
	 * 
	 * @param dbType
	 * @param statement
	 * @param columnNo
	 * @param columnValue
	 * @throws SQLException
	 */
	private void bindWithPrepStmt(
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
				if (Float.NEGATIVE_INFINITY == (float) columnValue) {
					if (nullable) {
						statement.setNull(columnNo, Types.FLOAT);
						LOGGER.error(
								"\n=====================\n" +
								"Negative float infinity value for nullable column '{}' at position # {}!\n" +
								"=====================\n",
								columnName, columnNo);
					} else {
						statement.setDouble(columnNo, Float.MIN_VALUE);
						LOGGER.error(
								"\n=====================\n" +
								"Negative float infinity value for column '{}' at position # {}!\n" +
								"Column value is set to Float.MIN_VALUE = '{}'!\n" +
								"=====================\n",
								columnName, columnNo, Float.MIN_VALUE);
					}
				} else {
					statement.setFloat(columnNo, (float) columnValue);
				}
				break;
			case Types.DOUBLE:
				if (Double.NEGATIVE_INFINITY == (double) columnValue) {
					if (nullable) {
						statement.setNull(columnNo, Types.DOUBLE);
						LOGGER.error(
								"\n=====================\n" +
								"Negative double infinity value for nullable column '{}' at position # {}!\n" +
								"=====================\n",
								columnName, columnNo);
					} else {
						statement.setDouble(columnNo, Double.MIN_VALUE);
						LOGGER.error(
								"\n=====================\n" +
								"Negative double infinity value for column '{}' at position # {}!\n" +
								"Column value is set to Double.MIN_VALUE = '{}'!\n" +
								"=====================\n",
								columnName, columnNo, Double.MIN_VALUE);
					}
				} else {
					statement.setDouble(columnNo, (double) columnValue);
				}
				break;
			case Types.DECIMAL:
				statement.setBigDecimal(columnNo, (BigDecimal) columnValue);
				break;
			case Types.NUMERIC:
				byte[] ba;
				boolean setNull = false;
				if (columnValue instanceof ByteBuffer) {
					ba = new byte[((ByteBuffer)columnValue).remaining()];
					((ByteBuffer)columnValue).get(ba);
				} else {
					try {
						ba = (byte[]) columnValue;
					} catch (Exception e) {
						LOGGER.error(
								"\n" +
								"=====================\n" +
								"Exception {} while converting {} to byte[] for column {}, bind # {}! \n" +
								"=====================\n",
								e.getClass().getName(), columnValue.getClass().getName(), columnName, columnNo);
						ba = null;
						setNull = true;
					}
				}
				if (dbType == JdbcSinkConnectionPool.DB_TYPE_ORACLE) {
					((OraclePreparedStatement) statement).setNUMBER(columnNo, new NUMBER(ba));
				} else {
					BigDecimal bd = null;
					bd = OraNumber.toLogical(ba);
					if (bd == null) {
						setNull = true;
					}
					if (setNull) {
						statement.setNull(columnNo, Types.NUMERIC);
					} else {
						statement.setBigDecimal(columnNo, bd);
					}
				}
				break;
			case Types.BINARY:
				statement.setBytes(columnNo, ((ByteBuffer) columnValue).array());
				break;
			case Types.VARCHAR:
				// 0x00 PostgreSQL problem
				if (dbType == JdbcSinkConnectionPool.DB_TYPE_POSTGRESQL) {
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

	private Schema optionalOrRequired(SchemaBuilder builder) {
		if (partOfPk || !nullable) {
			return builder.required().build();
		} else {
			return builder.optional().build();
		}
	}

	private void stringField() {
		SchemaBuilder builder = SchemaBuilder.string();
		if (defaultValuePresent) {
			final String trimmedDefault = StringUtils.trim(defaultValue); 
			if (StringUtils.startsWith(trimmedDefault, "'") &&
							StringUtils.endsWith(trimmedDefault, "'")) {
				typedDefaultValue = StringUtils.substringBetween(trimmedDefault, "'", "'");
				LOGGER.trace("Setting default value of column '{}' to '{}'",
						columnName, typedDefaultValue);
			} else {
				typedDefaultValue = trimmedDefault;
				LOGGER.warn(
						"\n" +
						"=====================\n" +
						"Default value for CHAR/NCHAR/VARCHAR2/NVARCHAR2 must be inside single quotes!" +
						"Setting default value (DATA_DEFAULT) of column '{}' to \"{}\"\n" +
						"=====================\n",
						columnName, typedDefaultValue);
			}
			builder = builder.defaultValue(typedDefaultValue);
		}
		schema = optionalOrRequired(builder);
	}
	private void stringField(final SchemaBuilder keySchema, final SchemaBuilder valueSchema) {
		if (partOfPk) {
			keySchema.field(this.columnName, Schema.STRING_SCHEMA);
		} else {
			if (nullable) {
				valueSchema.field(this.columnName, Schema.OPTIONAL_STRING_SCHEMA);
			} else {
				valueSchema.field(this.columnName, Schema.STRING_SCHEMA);
			}
		}
	}

	private void bytesField() {
		if (partOfPk || !nullable) {
			schema = Schema.BYTES_SCHEMA;
		} else {
			schema = Schema.OPTIONAL_BYTES_SCHEMA;
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

	private void byteField() {
		SchemaBuilder builder = SchemaBuilder.int8();
		if (defaultValuePresent) {
			try {
				typedDefaultValue = Byte.parseByte(StringUtils.trim(defaultValue));
				builder.defaultValue(typedDefaultValue);
			} catch (NumberFormatException nfe) {
				logDefaultValueError("byte");
			}
		}
		schema = optionalOrRequired(builder);
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

	private void shortField() {
		SchemaBuilder builder = SchemaBuilder.int16();
		if (defaultValuePresent) {
			try {
				typedDefaultValue = Short.parseShort(StringUtils.trim(defaultValue));
				builder.defaultValue(typedDefaultValue);
			} catch (NumberFormatException nfe) {
				logDefaultValueError("short");
			}
		}
		schema = optionalOrRequired(builder);
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

	private void intField() {
		SchemaBuilder builder = SchemaBuilder.int32();
		if (defaultValuePresent) {
			try {
				typedDefaultValue = Integer.parseInt(StringUtils.trim(defaultValue));
				builder.defaultValue(typedDefaultValue);
			} catch (NumberFormatException nfe) {
				logDefaultValueError("int");
			}
		}
		schema = optionalOrRequired(builder);
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

	private void longField() {
		SchemaBuilder builder = SchemaBuilder.int64();
		if (defaultValuePresent) {
			try {
				typedDefaultValue = Long.parseLong(StringUtils.trim(defaultValue));
				builder.defaultValue(typedDefaultValue);
			} catch (NumberFormatException nfe) {
				logDefaultValueError("long");
			}
		}
		schema = optionalOrRequired(builder);
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

	private void decimalField(final int scale) {
		SchemaBuilder builder = Decimal.builder(scale);
		if (defaultValuePresent) {
			try {
				typedDefaultValue = (new BigDecimal(StringUtils.trim(defaultValue))).setScale(scale);
				builder.defaultValue(typedDefaultValue);
			} catch (NumberFormatException nfe) {
				logDefaultValueError("java.math.BigDecimal");
			}
		}
		schema = optionalOrRequired(builder);
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

	private void doubleField() {
		SchemaBuilder builder = SchemaBuilder.float64();
		if (defaultValuePresent) {
			try {
				typedDefaultValue = Double.parseDouble(StringUtils.trim(defaultValue));
				builder.defaultValue(typedDefaultValue);
			} catch (NumberFormatException nfe) {
				logDefaultValueError("double");
			}
		}
		schema = optionalOrRequired(builder);
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

	private void floatField() {
		SchemaBuilder builder = SchemaBuilder.float32();
		if (defaultValuePresent) {
			try {
				typedDefaultValue = Float.parseFloat(StringUtils.trim(defaultValue));
				builder.defaultValue(typedDefaultValue);
			} catch (NumberFormatException nfe) {
				logDefaultValueError("float");
			}
		}
		schema = optionalOrRequired(builder);
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

	private void oraNumberField() {
		SchemaBuilder builder = OraNumber.builder();
		if (defaultValuePresent) {
			try {
				typedDefaultValue = (new NUMBER(StringUtils.trim(defaultValue), 10)).getBytes();
				builder.defaultValue(typedDefaultValue);
			} catch (SQLException sqle) {
				LOGGER.error(
						"\n" +
						"=====================\n" +
						"Unable to convert default value (DATA_DEFAULT) '{}' of column '{}' to oracle.sql.NUMBER!\n" +
						"=====================\n",
						defaultValue, columnName);
				throw new NumberFormatException(sqle.getMessage());
			} catch (NumberFormatException nfe) {
				logDefaultValueError("oracle.sql.NUMBER");
			}
		}
		schema = optionalOrRequired(builder);
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

	private void timestampField() {
		if (partOfPk || !nullable) {
			schema = Timestamp.builder().required().build();
		} else {
			schema = Timestamp.builder().optional().build();
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

	private void oraTimestampField() {
		if (partOfPk || !nullable) {
			schema = OraTimestamp.builder().required().build();
		} else {
			schema = OraTimestamp.builder().optional().build();
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

	private void oraIntervalField() {
		if (partOfPk || !nullable) {
			schema = OraInterval.builder().required().build();
		} else {
			schema = OraInterval.builder().optional().build();
		}
	}

	private void logDefaultValueError(final String dataTypeName) {
		LOGGER.error(
				"\n" +
				"=====================\n" +
				"Unable to convert default value (DATA_DEFAULT) \"{}\" of column '{}' to '{}'!\n" +
				"This default value \"{}\" will be ignored!\n" + 
				"=====================\n",
				defaultValue, columnName, dataTypeName, defaultValue);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
				columnName, columnId, partOfPk, jdbcType, nullable,
				dataScale, binaryFloatDouble, localTimeZone, secureFile,
				defaultValuePresent, defaultValue);
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
				Objects.equals(columnId, other.columnId) &&
				partOfPk == other.partOfPk &&
				jdbcType == other.jdbcType &&
				nullable == other.nullable &&
				Objects.equals(dataScale, other.dataScale) && 
				Objects.equals(binaryFloatDouble, other.binaryFloatDouble) &&
				Objects.equals(localTimeZone, other.localTimeZone) &&
				Objects.equals(secureFile, other.secureFile) &&
				Objects.equals(defaultValuePresent, other.defaultValuePresent) &&
				Objects.equals(defaultValue, other.defaultValue);
	}

	public static String canonicalColumnName(final String rawColumnName) {
		if (StringUtils.startsWith(rawColumnName, "\"") && StringUtils.endsWith(rawColumnName, "\"")) {
			// Column name is escaped by "
			return StringUtils.substringBetween(rawColumnName, "\"", "\"");
		} else {
			// Uppercase it!
			return StringUtils.upperCase(rawColumnName);
		}
	}

	/**
	 * 
	 * @param struct
	 * @param resultSet
	 * @return true if columnValue from DB was not null
	 * @throws SQLException
	 */
	public boolean setValueFromResultSet(
			final Struct struct, final ResultSet resultSet) throws SQLException  {
		switch (jdbcType) {
		case Types.DATE:
		case Types.TIMESTAMP:
		case Types.TIMESTAMP_WITH_TIMEZONE:
			struct.put(columnName, resultSet.getTimestamp(columnName));
			break;
		case Types.BOOLEAN:
			struct.put(columnName, resultSet.getBoolean(columnName));
			break;
		case Types.TINYINT:
			struct.put(columnName, resultSet.getByte(columnName));
			break;
		case Types.SMALLINT:
			struct.put(columnName, resultSet.getShort(columnName));
			break;
		case Types.INTEGER:
			struct.put(columnName, resultSet.getInt(columnName));
			break;
		case Types.BIGINT:
			struct.put(columnName, resultSet.getLong(columnName));
			break;
		case Types.FLOAT:
			struct.put(columnName, resultSet.getFloat(columnName));
			break;
		case Types.DOUBLE:
			struct.put(columnName, resultSet.getDouble(columnName));
			break;
		case Types.DECIMAL:
			struct.put(columnName, resultSet.getBigDecimal(columnName));
			break;
		case Types.NUMERIC:
			struct.put(columnName, ((OracleResultSet) resultSet).getNUMBER(columnName).getBytes());
			break;
		case Types.BINARY:
			struct.put(columnName, resultSet.getBytes(columnName));
			break;
		case Types.VARCHAR:
			struct.put(columnName, resultSet.getString(columnName));
			break;
		default:
			LOGGER.error("Unsupported data type {} for column {}.",
					JdbcTypes.getTypeName(jdbcType), columnName);
			throw new SQLException("Unsupported data type: " + JdbcTypes.getTypeName(jdbcType));
		}
		return !resultSet.wasNull();
	}

	public String getValueAsString(final Struct keyStruct, final Struct valueStruct) {
		return partOfKeyStruct ?
					keyStruct.get(columnName).toString() :
						valueStruct.get(columnName).toString();
	}

	public boolean isNumber() {
		return number;
	}

}
