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

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NULL;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.SQLXML;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static oracle.jdbc.OracleTypes.INTERVALDS;
import static oracle.jdbc.OracleTypes.INTERVALYM;
import static oracle.jdbc.OracleTypes.JSON;
import static oracle.jdbc.OracleTypes.TIMESTAMPLTZ;
import static oracle.jdbc.OracleTypes.VECTOR;
import static org.apache.commons.lang3.StringUtils.trim;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_17027;
import static solutions.a2.cdc.oracle.data.JdbcTypes.getTypeName;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.sql.NUMBER;
import solutions.a2.cdc.oracle.internals.OraCdcTdeColumnDecrypter;
import solutions.a2.cdc.oracle.utils.KafkaUtils;
import solutions.a2.kafka.Column;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class OraCdcColumn extends Column {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcColumn.class);

	public static final String ROWID_KEY = "ORA_ROW_ID";
	public static final String MVLOG_SEQUENCE = "SEQUENCE$$";
	public static final String ORA_ROWSCN = "ORA_ROWSCN";

	static final Pattern GUARD_COLUMN = Pattern.compile("^SYS_NC\\d{5}\\$$");
	static final Pattern UNUSED_COLUMN = Pattern.compile("^SYS_C\\d{5}(?:_\\d{8}:\\d{2}:\\d{2})?\\$$");

	private static final String TYPE_VARCHAR2 = "VARCHAR2";
	private static final String TYPE_NVARCHAR2 = "NVARCHAR2";
	private static final String TYPE_CHAR = "CHAR";
	private static final String TYPE_NCHAR = "NCHAR";
	private static final String TYPE_FLOAT = "FLOAT";
	private static final String TYPE_RAW = "RAW";
	private static final String TYPE_DATE = "DATE";
	private static final String TYPE_TIMESTAMP = "TIMESTAMP";
	private static final String TYPE_NUMBER = "NUMBER";
	private static final String TYPE_INTEGER = "INTEGER";
	private static final String TYPE_INT = "INT";
	private static final String TYPE_SMALLINT = "SMALLINT";
	private static final String TYPE_BINARY_FLOAT = "BINARY_FLOAT";
	private static final String TYPE_BINARY_DOUBLE = "BINARY_DOUBLE";
	private static final String TYPE_BLOB = "BLOB";
	private static final String TYPE_CLOB = "CLOB";
	private static final String TYPE_NCLOB = "NCLOB";
	private static final String TYPE_XMLTYPE = "XMLTYPE";
	private static final String TYPE_JSON = "JSON";
	private static final String TYPE_BOOLEAN = "BOOLEAN";
	private static final String TYPE_VECTOR = "VECTOR";

	private static final int CHAR_MAX = 4000;
	private static final int RAW_MAX = 2000;

	private int columnId;
	private String nameFromId;
	private String defaultValue;
	private Object typedDefaultValue;
	private String oracleName;
	private int dataLength;
	private OraCdcDecoder decoder;

	/**
	 * 
	 * Used in Source Connector
	 * 
	 * @param mviewSource         for MView log or archived redo log
	 * @param useOracdcSchemas    true for extended schemas
	 * @param processLobs         when true and useOracdcSchemas eq true BLOB/CLOB/NCLOB columns are processed
	 * @param resultSet
	 * @param pkColsSet
	 * @param decrypter
	 * @param rdbmsInfo
	 * @param suppLogAll
	 * @param noLongInDict
	 * @throws SQLException
	 * @throws UnsupportedColumnDataTypeException 
	 */
	public OraCdcColumn(
			final boolean mviewSource,
			final boolean useOracdcSchemas,
			final boolean processLobs,
			final ResultSet resultSet,
			final Set<String> pkColsSet,
			final OraCdcTdeColumnDecrypter decrypter,
			final OraRdbmsInfo rdbmsInfo,
			final boolean suppLogAll,
			final boolean noLongInDict) throws SQLException, UnsupportedColumnDataTypeException {
		oracleName = resultSet.getString("COLUMN_NAME");
		if (!KafkaUtils.validAvroFieldName(oracleName)) {
			columnName = KafkaUtils.fixAvroFieldName(oracleName, "_");
			LOGGER.warn(
					"""
					
					=====================
					Column name '{}' is incompatible with Avro/Protobuf naming standard.
					This column name is changed to '{}'.
					=====================
					
					""", oracleName, columnName);
		} else {
			columnName = oracleName;
		}
		if (Strings.CI.equals("Y", resultSet.getString("NULLABLE")))
			flags |= FLG_NULLABLE;
		else
			flags |= FLG_MANDATORY;
		this.setColumnId(resultSet.getInt("COLUMN_ID"));
		try {
			defaultValue = trim(resultSet.getString(noLongInDict ? "DATA_DEFAULT_VC" : "DATA_DEFAULT"));
		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == ORA_17027) {
				LOGGER.warn(
						"""
						
						=====================
						ORA-17027 on reading default value from LONG column on pre-26ai database!
						DATA_DEFAULT for column {} is set to NULL!
						=====================
						
						""", oracleName);

				defaultValue = null;
			} else
				throw sqle;
		}
		if (StringUtils.isEmpty(defaultValue) || Strings.CI.equals(defaultValue, "NULL"))
			defaultValue = null;
		else
			flags |= FLG_DEFAULT_VALUE;

		if (mviewSource) {
			if (Strings.CI.equals("Y", resultSet.getString("PK")))
				flags |= (FLG_PART_OF_PK | FLG_MANDATORY);
		} else {
			if (pkColsSet != null && pkColsSet.contains(this.columnName))
				flags |= (FLG_PART_OF_PK | FLG_MANDATORY);
		}

		final String oraType = resultSet.getString("DATA_TYPE");
		dataScale = resultSet.getInt("DATA_SCALE");
		if (resultSet.wasNull()) {
			dataScale = -1;
		}
		Integer dataPrecision = resultSet.getInt("DATA_PRECISION");
		if (resultSet.wasNull()) {
			dataPrecision = null;
		}
		dataLength = resultSet.getInt("DATA_LENGTH");
		if (Strings.CI.equals("YES", resultSet.getString("ENCRYPTED")))
			flags |= FLG_ENCRYPTED;
		if (Strings.CI.equals("YES", resultSet.getString("SALT")))
			flags |= FLG_SALT;
		detectTypeAndSchema(oraType, mviewSource, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);

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
	public OraCdcColumn(
			final boolean useOracdcSchemas,
			final String columnName,
			final String columnAttributes,
			final String originalDdl,
			final int columnId,
			final OraCdcTdeColumnDecrypter decrypter,
			final OraRdbmsInfo rdbmsInfo,
			final boolean suppLogAll)
			throws UnsupportedColumnDataTypeException {
		this.columnName = columnName;
		this.setColumnId(columnId);
		Integer dataPrecision = null;
		this.dataScale = -1;
	
		if (Strings.CI.startsWith(columnAttributes, TYPE_VARCHAR2)) {
			// Ignore SIZE for VARCHAR2/NVARCHAR2/CHAR/NCHAR/RAW
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_VARCHAR2, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_NVARCHAR2)) {
			// Ignore SIZE for VARCHAR2/NVARCHAR2/CHAR/NCHAR/RAW
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_NVARCHAR2, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_CHAR)) {
			// Ignore SIZE for VARCHAR2/NVARCHAR2/CHAR/NCHAR/RAW
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_CHAR, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_NCHAR)) {
			// Ignore SIZE for VARCHAR2/NVARCHAR2/CHAR/NCHAR/RAW
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_NCHAR, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_RAW)) {
			// Ignore SIZE for VARCHAR2/NVARCHAR2/CHAR/NCHAR/RAW
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_RAW, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_FLOAT)) {
			// Ignore PRECISION for FLOAT
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_FLOAT, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_DATE)) {
			// No PRECISION for DATE
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_DATE, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_TIMESTAMP)) {
			// Ignore fractional seconds!!!
			if (Strings.CI.contains(columnAttributes, "LOCAL")) {
				// 231: TIMESTAMP [(fractional_seconds_precision)] WITH LOCAL TIME ZONE
				detectIsNullAndDefault(columnAttributes);
				detectTypeAndSchema("TIMESTAMP WITH LOCAL TIME ZONE", false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
			} else if (Strings.CI.contains(columnAttributes, "ZONE")) {
				// 181: TIMESTAMP [(fractional_seconds_precision)] WITH TIME ZONE
				detectIsNullAndDefault(columnAttributes);
				detectTypeAndSchema("TIMESTAMP WITH TIME ZONE", false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
			} else {
				// 180: TIMESTAMP [(fractional_seconds_precision)]
				detectIsNullAndDefault(columnAttributes);
				detectTypeAndSchema(TYPE_TIMESTAMP, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
			}
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_NUMBER)) {
			detectIsNullAndDefault(columnAttributes);
			final String precisionAndScale = trim(
					StringUtils.substringBetween(columnAttributes, "(", ")")); 
			if (precisionAndScale != null) {
				try {
					if (Strings.CS.contains(precisionAndScale, "|")) {
						final String[] tokens = StringUtils.split(precisionAndScale, "|");
						dataPrecision = Integer.parseInt(trim(tokens[0]));
						dataScale = Integer.parseInt(trim(tokens[1]));
					} else {
						dataPrecision = Integer.parseInt(precisionAndScale);
					}
				} catch (NumberFormatException nfe) {
					LOGGER.error("Unable to detect PRECISION and SCALE for column {} while parsing DDL command\n'{}'\nusing pre-processed from DDL '{}'",
							columnName, originalDdl, columnAttributes);
					LOGGER.error("Both PRECISION and SCALE are reset to NULL!!!");
					dataPrecision = null;
					dataScale = -1;
				}
			}
			detectTypeAndSchema(TYPE_NUMBER, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_BINARY_FLOAT)) {
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_BINARY_FLOAT, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_BINARY_DOUBLE)) {
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_BINARY_DOUBLE, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_BLOB)) {
			flags |= FLG_NULLABLE;
			detectTypeAndSchema(TYPE_BLOB, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_CLOB)) {
			flags |= FLG_NULLABLE;
			detectTypeAndSchema(TYPE_CLOB, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_NCLOB)) {
			flags |= FLG_NULLABLE;
			detectTypeAndSchema(TYPE_NCLOB, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_XMLTYPE)) {
			flags |= FLG_NULLABLE;
			detectTypeAndSchema(TYPE_XMLTYPE, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_JSON)) {
			flags |= FLG_NULLABLE;
			detectTypeAndSchema(TYPE_JSON, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_BOOLEAN)) {
			detectIsNullAndDefault(columnAttributes);
			detectTypeAndSchema(TYPE_BOOLEAN, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
		} else if (Strings.CI.startsWith(columnAttributes, TYPE_VECTOR)) {
			flags |= FLG_NULLABLE;
			detectTypeAndSchema(TYPE_VECTOR, false, useOracdcSchemas, dataPrecision, decrypter, rdbmsInfo, suppLogAll);
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
			if (Strings.CI.equals(tokens[i], "null")) {
				posNull = i;
				if (Strings.CI.equals(tokens[i - 1], "not")) {
					posNot = i - 1;
				}
			}
			if (Strings.CI.equals(tokens[i], "default")) {
				posDefault = i;
			}
		}
		if (posNull == -1) {
			// By default - NULL
			flags |= FLG_NULLABLE;
		} else if (posNot == -1) {
			// NULL explicitly set
			flags |= FLG_NULLABLE;
		} else {
			flags |= FLG_MANDATORY;
		}
		if (posDefault != -1 && posDefault + 1 <= tokens.length - 1) {
			flags |= FLG_DEFAULT_VALUE;
			defaultValue = tokens[posDefault + 1]; 
		}
	}

	private void detectTypeAndSchema(
			final String oraType,
			final boolean mviewSource,
			final boolean useOracdcSchemas,
			Integer dataPrecision,
			final OraCdcTdeColumnDecrypter decrypter,
			final OraRdbmsInfo rdbmsInfo,
			final boolean suppLogAll) throws UnsupportedColumnDataTypeException {
		if (Strings.CS.equals(oraType, TYPE_DATE) || Strings.CS.startsWith(oraType, TYPE_TIMESTAMP)) {
			if (suppLogAll) {
				if (useOracdcSchemas) {
					if (Strings.CS.endsWith(oraType, "WITH LOCAL TIME ZONE")) {
						// 231:
						// TIMESTAMP [(fractional_seconds)] WITH LOCAL TIME ZONE
						flags |= FLG_LOCAL_TIME_ZONE;
						jdbcType = TIMESTAMP_WITH_TIMEZONE;
					} else if (Strings.CS.endsWith(oraType, "WITH TIME ZONE")) {
						// 181: TIMESTAMP [(fractional_seconds)] WITH TIME ZONE
						jdbcType = TIMESTAMP_WITH_TIMEZONE;
					} else {
						// 12: DATE, 180: TIMESTAMP [(fractional_seconds_precision)]
						jdbcType = Strings.CS.equals(oraType, TYPE_DATE) ? DATE : TIMESTAMP;
					}
				} else {
					jdbcType = Strings.CS.equals(oraType, TYPE_DATE) ? DATE : TIMESTAMP;
				}
			} else {
				if (Strings.CS.endsWith(oraType, "WITH LOCAL TIME ZONE")) {
					// 231:
					// TIMESTAMP [(fractional_seconds)] WITH LOCAL TIME ZONE
					flags |= FLG_LOCAL_TIME_ZONE;
					jdbcType = TIMESTAMPLTZ;
				} else if (Strings.CS.endsWith(oraType, "WITH TIME ZONE")) {
					// 181: TIMESTAMP [(fractional_seconds)] WITH TIME ZONE
					jdbcType = TIMESTAMP_WITH_TIMEZONE;
				} else {
					// 12: DATE, 180: TIMESTAMP [(fractional_seconds_precision)]
					jdbcType = TIMESTAMP;
				}
			}
		} else if (Strings.CS.startsWith(oraType, "INTERVAL")) {
			if (suppLogAll) {
				if (Strings.CS.contains(oraType, "TO MONTH")) {
					if (useOracdcSchemas) {
						jdbcType = INTERVALYM;
					} else {
						jdbcType = JAVA_SQL_TYPE_INTERVALYM_STRING;
					}
				} else {
					// 'TO SECOND'
					if (useOracdcSchemas) {
						jdbcType = INTERVALDS;
					} else {
						jdbcType = JAVA_SQL_TYPE_INTERVALDS_STRING;
					}
				}
			} else {
				if (Strings.CS.contains(oraType, "TO MONTH")) {
					jdbcType = INTERVALYM;
				} else {
					jdbcType = INTERVALDS;
				}
			}
		} else {
			switch (oraType) {
				case TYPE_FLOAT -> {
					// A subtype of the NUMBER datatype having precision p.
					// A FLOAT value is represented internally as NUMBER.
					// The precision p can range from 1 to 126 binary digits.
					// A FLOAT value requires from 1 to 22 bytes.
					flags |= FLG_NUMBER;
					if (suppLogAll) {
						if (useOracdcSchemas) {
							jdbcType = NUMERIC;
						} else {
							jdbcType = DOUBLE;
						}
					} else {
						jdbcType = NUMERIC;
					}
				}
				case TYPE_NUMBER -> {
					flags |= FLG_NUMBER;
					if (dataScale > -1 && dataPrecision == null) {
						//DATA_SCALE set but DATA_PRECISION is unknown....
						//Set it to MAX
						dataPrecision = 38;
					}
					if (dataPrecision == null && dataScale == -1) {
						// NUMBER w/out precision and scale
						// OEBS and other legacy systems specific
						// Can be Integer or decimal or float....
						if (suppLogAll) {
							if (useOracdcSchemas) {
								jdbcType = NUMERIC;
							} else {
								jdbcType = DOUBLE;
							}
						} else {
							jdbcType = NUMERIC;
						}
					} else if (dataScale == -1 || dataScale == 0) {
						// Integer 
						if (dataPrecision < 3) {
							jdbcType = TINYINT;
						} else if (dataPrecision < 5) {
							jdbcType = SMALLINT;
						} else if (dataPrecision < 10) {
							jdbcType = INTEGER;
						} else if (dataPrecision < 19) {
							jdbcType = BIGINT;
						} else {
							// Too big for BIGINT...
							jdbcType = DECIMAL;
							dataScale = 0;
						}
					} else {
						// Decimal values
						jdbcType = DECIMAL;
					}
				}
				case TYPE_INTEGER, TYPE_INT, TYPE_SMALLINT -> {
					// NUMBER(38, 0)
					if (suppLogAll && useOracdcSchemas) {
						jdbcType = NUMERIC;
					} else {
						jdbcType = DECIMAL;
						dataScale = 0;
					}
				}
				case TYPE_BINARY_FLOAT -> {
					jdbcType = FLOAT;
					flags |= FLG_BINARY_FLOAT_DOUBLE;
				}
				case TYPE_BINARY_DOUBLE -> {
					jdbcType = DOUBLE;
					flags |= FLG_BINARY_FLOAT_DOUBLE;
				}
				case TYPE_CHAR -> jdbcType = CHAR;
				case TYPE_NCHAR -> jdbcType = NCHAR;
				case TYPE_VARCHAR2 ->
					jdbcType = dataLength > CHAR_MAX ? LONGVARCHAR : VARCHAR;
				case TYPE_NVARCHAR2 ->
					jdbcType = dataLength > CHAR_MAX ? LONGNVARCHAR : NVARCHAR;
				case TYPE_CLOB -> {
					jdbcType = CLOB;
					flags |= FLG_LARGE_OBJECT;
				}
				case TYPE_NCLOB -> {
					jdbcType = NCLOB;
					flags |= FLG_LARGE_OBJECT;
				}
				case TYPE_RAW ->
					jdbcType = dataLength > RAW_MAX ? LONGVARBINARY : BINARY;
				case TYPE_BLOB -> {
					jdbcType = BLOB;
					flags |= FLG_LARGE_OBJECT;
				}
				case TYPE_XMLTYPE -> {
					jdbcType = SQLXML;
					flags |= FLG_LARGE_OBJECT;
				}
				case TYPE_JSON -> {
					jdbcType = JSON;
					flags |= FLG_LARGE_OBJECT;
				}
				case TYPE_BOOLEAN -> jdbcType = BOOLEAN;	//252					
				case TYPE_VECTOR -> {
					// 127
					jdbcType = VECTOR;
					flags |= FLG_LARGE_OBJECT;
				}
				default -> {
					LOGGER.warn("Datatype {} for column {} is not supported!",
							oraType, this.columnName);
					throw new UnsupportedColumnDataTypeException(this.columnName);
				}
			}
		}
	}

	/**
	 * Used internally for mapping support
	 * 
	 * @param columnName
	 * @param jdbcType
	 * @param scale
	 */
	public OraCdcColumn(
			final String columnName,
			final int jdbcType,
			final int scale) {
		this.columnName = columnName;
		this.jdbcType = jdbcType;
		this.dataScale = scale;
		
	}

	public void remap(final OraCdcColumn newDef, final OraCdcTdeColumnDecrypter decrypter, final boolean suppLogAll) {
		if ((newDef.jdbcType != NULL && jdbcType != newDef.jdbcType) ||
				(newDef.jdbcType != NULL && jdbcType == newDef.jdbcType && jdbcType == DECIMAL && dataScale != newDef.dataScale)) {
			jdbcType = newDef.jdbcType;
			dataScale = newDef.dataScale;
		}
	}

	public static OraCdcColumn getRowIdKey() {
		OraCdcColumn rowIdColumn = new OraCdcColumn(ROWID_KEY, ROWID, 0);
		rowIdColumn.flags |= (FLG_PART_OF_PK | FLG_MANDATORY);
		return rowIdColumn;
	}

	int getColumnId() {
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

	public boolean IEEE754() {
		return (flags & FLG_BINARY_FLOAT_DOUBLE) > 0;
	}

	public boolean localTimeZone() {
		return (flags & FLG_LOCAL_TIME_ZONE) > 0;
	}

	public boolean getSecureFile() {
		return (flags & FLG_SECURE_FILE) > 0;
	}

	public void setSecureFile(boolean secureFile) {
		if (secureFile)
			flags |= FLG_SECURE_FILE;
		else
			flags &= (~FLG_SECURE_FILE);
	}

	public boolean defaultValuePresent() {
		return (flags & FLG_DEFAULT_VALUE) > 0;
	}

	public String defaultValue() {
		return defaultValue;
	}

	//TODO
	//TODO This will be splitted to appropriate type handler.....
	//TODO and this will be replaced with simple getter and setter for typedDefaultValue
	//TODO
	public Object typedDefaultValue() {
		if ((flags & FLG_DEFAULT_VALUE) == 0) {
			return null;
		} else if (typedDefaultValue == null) {
			//TODO
			//TODO Currently only Oracle VARCHAR2/NVARCHAR2 and NUMBER supported
			//TODO
			try {
				switch (jdbcType) {
				case CHAR:
				case VARCHAR:
				case NCHAR:
				case NVARCHAR:
					if (Strings.CS.startsWith(defaultValue, "'") &&
							Strings.CS.endsWith(defaultValue, "'")) {
						typedDefaultValue = StringUtils.substringBetween(defaultValue, "'", "'");
					} else {
						LOGGER.warn("Default value for CHAR/NCHAR/VARCHAR2/NVARCHAR2 must be inside single quotes!");
						typedDefaultValue = defaultValue;
					}
					break;
				case TINYINT:
					typedDefaultValue = Byte.parseByte(defaultValue);
					break;
				case SMALLINT:
					typedDefaultValue = Short.parseShort(defaultValue);
					break;
				case INTEGER:
					typedDefaultValue = Integer.parseInt(defaultValue);
					break;
				case BIGINT:
					typedDefaultValue = Long.parseLong(defaultValue);
					break;
				case FLOAT:
					typedDefaultValue = Float.parseFloat(defaultValue);
					break;
				case DOUBLE:
					typedDefaultValue = Double.parseDouble(defaultValue);
					break;
				case DECIMAL:
					typedDefaultValue = (new BigDecimal(defaultValue)).setScale(dataScale);
					break;
				case NUMERIC:
					try {
						typedDefaultValue = (new NUMBER(defaultValue, 10)).getBytes();
					} catch (SQLException sqle) {
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
						throw new NumberFormatException(sqle.getMessage());
					}
					break;
				default:
					LOGGER.error("Default value {} for column {} with type {} currently is not supported!",
						defaultValue, columnName, getTypeName(jdbcType));
					typedDefaultValue = null;
				}
			} catch (NumberFormatException nfe) {
				LOGGER.error("Invalid number value {} for column {} with type {}!\nSetting it to null!!!",
						defaultValue, columnName, getTypeName(jdbcType));
				typedDefaultValue = null;
			}
			return typedDefaultValue;
		} else {
			return typedDefaultValue;
		}
	}

	public void typedDefaultValue(Object typedDefaultValue) {
		this.typedDefaultValue = typedDefaultValue;
	}

	public String getOracleName() {
		return oracleName;
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

	@Override
	public int hashCode() {
		return Objects.hash(
				columnName, columnId, jdbcType, flags,
				dataScale, dataLength, defaultValue);
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
		OraCdcColumn other = (OraCdcColumn) obj;
		return
				Objects.equals(columnName, other.columnName) &&
				Objects.equals(columnId, other.columnId) &&
				flags == other.flags &&
				jdbcType == other.jdbcType &&
				Objects.equals(dataScale, other.dataScale) && 
				Objects.equals(dataLength, other.dataLength) &&
				Objects.equals(defaultValue, other.defaultValue);
	}

	public static String canonicalColumnName(final String rawColumnName) {
		if (Strings.CS.startsWith(rawColumnName, "\"") && Strings.CS.endsWith(rawColumnName, "\""))
			// Column name is escaped by "
			return StringUtils.substringBetween(StringUtils.remove(rawColumnName, CHAR_0), "\"", "\"");
		else
			return StringUtils.upperCase(StringUtils.remove(rawColumnName, CHAR_0));
	}

	public boolean isNumber() {
		return (flags & FLG_NUMBER) > 0;
	}

	public boolean encrypted() {
		return (flags & FLG_ENCRYPTED) > 0;
	}

	public boolean largeObject() {
		return (flags & FLG_LARGE_OBJECT) > 0;
	}

	OraCdcDecoder decoder() {
		return decoder;
	}

	public void decoder(OraCdcDecoder decoder) {
		this.decoder = decoder;
	}

	boolean decodeWithoutTrans() {
		return (flags & FLG_DECODE_WITH_TRANS) == 0;
	}

	public void transformLob(final boolean lobTransform) {
		if (lobTransform)
			flags |= FLG_LOB_TRANSFORM;
		else
			flags &= (~FLG_LOB_TRANSFORM);
	}

	boolean transformLob() {
		return (flags & FLG_LOB_TRANSFORM) > 0;
	}

	public boolean mandatory() {
		return (flags & FLG_MANDATORY) > 0;
	}

	public boolean salted() {
		return (flags & FLG_SALT) > 0;
	}

}
