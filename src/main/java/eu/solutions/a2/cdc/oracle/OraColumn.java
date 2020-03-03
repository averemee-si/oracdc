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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

/**
 * 
 * @author averemee
 *
 */
public class OraColumn {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraColumn.class);
	private final static float LOG_2 = 0.30103f; 

	public static final String ROWID_KEY = "ORA_ROW_ID";
	public static final String MVLOG_SEQUENCE = "SEQUENCE$$";
	public static final String ORA_ROWSCN = "ORA_ROWSCN";

	private final String columnName;
	private final String nameFromId;
	private final boolean partOfPk;
	private final int jdbcType;
	private final boolean nullable;
	private int dataScale = 0;


	/**
	 * 
	 * Used in Source Connector
	 * 
	 * @param resultSet
	 * @param keySchemaBuilder
	 * @param valueSchemaBuilder
	 * @param schemaType
	 * @param pkColsSet
	 * @throws SQLException
	 */
	public OraColumn(final boolean mviewSource, final ResultSet resultSet,
			final SchemaBuilder keySchema, final SchemaBuilder valueSchema, final int schemaType,
			final Set<String> pkColsSet) throws SQLException {
		this.columnName = resultSet.getString("COLUMN_NAME");
		this.nullable = "Y".equals(resultSet.getString("NULLABLE")) ? true : false;
		this.nameFromId = "\"COL " + resultSet.getInt("COLUMN_ID") + "\"";

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
		switch (oraType) {
			case "DATE":
				// Oracle Date holds time too...
				// So here we use Timestamp
				jdbcType = Types.TIMESTAMP;
				if (this.nullable)
					valueSchema.field(this.columnName, Timestamp.builder().optional().build());
				else
					if (this.partOfPk)
						keySchema.field(this.columnName, Timestamp.builder().required().build());
					else
						valueSchema.field(this.columnName, Timestamp.builder().required().build());
				break;
			case "FLOAT":
				final int floatDataPrecision = resultSet.getInt("DATA_PRECISION");
				if (resultSet.wasNull() || floatDataPrecision > 22) {
					jdbcType = Types.DOUBLE;
					if (this.nullable)
						valueSchema.field(this.columnName, Schema.OPTIONAL_FLOAT64_SCHEMA);
					else
						if (this.partOfPk)
							keySchema.field(this.columnName, Schema.FLOAT64_SCHEMA);
						else
							valueSchema.field(this.columnName, Schema.FLOAT64_SCHEMA);
				} else if (floatDataPrecision > 10) {
					jdbcType = Types.FLOAT;
					if (this.nullable)
						valueSchema.field(this.columnName, Schema.OPTIONAL_FLOAT32_SCHEMA);
					else
						if (this.partOfPk)
							keySchema.field(this.columnName, Schema.FLOAT32_SCHEMA);
						else
							valueSchema.field(this.columnName, Schema.FLOAT32_SCHEMA);
				} else {
					dataScale = (int) Math.ceil((float) floatDataPrecision * LOG_2);
					jdbcType = Types.DECIMAL;
					if (this.nullable)
						valueSchema.field(this.columnName, Decimal.builder(dataScale).optional().build());
					else
						if (this.partOfPk)
							keySchema.field(this.columnName, Decimal.builder(dataScale).required().build());
						else
							valueSchema.field(this.columnName, Decimal.builder(dataScale).required().build());
				}
				break;
			case "NUMBER":
				final int dataPrecision = resultSet.getInt("DATA_PRECISION");
				final boolean precisionIsNull = resultSet.wasNull();
				dataScale = resultSet.getInt("DATA_SCALE");
				final boolean scaleIsNull = resultSet.wasNull();
				if (precisionIsNull && scaleIsNull) {
					// NUMBER w/out precision and scale
					// OEBS and other legacy systems specific
					// Can be Integer or decimal or float....
					jdbcType = Types.DOUBLE;
					if (this.nullable)
						valueSchema.field(this.columnName, Schema.OPTIONAL_FLOAT64_SCHEMA);
					else
						if (this.partOfPk)
							keySchema.field(this.columnName, Schema.FLOAT64_SCHEMA);
						else
							valueSchema.field(this.columnName, Schema.FLOAT64_SCHEMA);
				} else if (dataScale == 0) {
					if (dataPrecision < 3) {
						jdbcType = Types.TINYINT;
						if (this.nullable)
							valueSchema.field(this.columnName, Schema.OPTIONAL_INT8_SCHEMA);
						else
							if (this.partOfPk)
								keySchema.field(this.columnName, Schema.INT8_SCHEMA);
							else
								valueSchema.field(this.columnName, Schema.INT8_SCHEMA);
					}
					else if (dataPrecision < 5) {
						jdbcType = Types.SMALLINT;
						if (this.nullable)
							valueSchema.field(this.columnName, Schema.OPTIONAL_INT16_SCHEMA);
						else
							if (this.partOfPk)
								keySchema.field(this.columnName, Schema.INT16_SCHEMA);
							else
								valueSchema.field(this.columnName, Schema.INT16_SCHEMA);
					}
					else if (dataPrecision < 10) {
						jdbcType = Types.INTEGER;
						if (this.nullable)
							valueSchema.field(this.columnName, Schema.OPTIONAL_INT32_SCHEMA);
						else
							if (this.partOfPk)
								keySchema.field(this.columnName, Schema.INT32_SCHEMA);
							else
								valueSchema.field(this.columnName, Schema.INT32_SCHEMA);
					}
					else if (dataPrecision < 19) {
						jdbcType = Types.BIGINT;
						if (this.nullable)
							valueSchema.field(this.columnName, Schema.OPTIONAL_INT64_SCHEMA);
						else
							if (this.partOfPk)
								keySchema.field(this.columnName, Schema.INT64_SCHEMA);
							else
								valueSchema.field(this.columnName, Schema.INT64_SCHEMA);
					} else {
						jdbcType = Types.DECIMAL;
						if (this.nullable)
							valueSchema.field(this.columnName, Decimal.builder(0).optional().build());
						else
							if (this.partOfPk)
								keySchema.field(this.columnName, Decimal.builder(0).required().build());
							else
								valueSchema.field(this.columnName, Decimal.builder(0).required().build());
					}
				} else {
					// Decimal values
					jdbcType = Types.DECIMAL;
					if (this.nullable)
						valueSchema.field(this.columnName, Decimal.builder(dataScale).optional().build());
					else
						if (this.partOfPk)
							keySchema.field(this.columnName, Decimal.builder(dataScale).required().build());
						else
							valueSchema.field(this.columnName, Decimal.builder(dataScale).required().build());
				}
				break;
			case "RAW":
				jdbcType = Types.BINARY;
				if (this.nullable)
					valueSchema.field(this.columnName, Schema.OPTIONAL_BYTES_SCHEMA);
				else
					if (this.partOfPk)
						keySchema.field(this.columnName, Schema.BYTES_SCHEMA);
					else
						valueSchema.field(this.columnName, Schema.BYTES_SCHEMA);
				break;
			case "CHAR":
				jdbcType = Types.CHAR;
				if (this.nullable)
					valueSchema.field(this.columnName, Schema.OPTIONAL_STRING_SCHEMA);
				else
					if (this.partOfPk)
						keySchema.field(this.columnName, Schema.STRING_SCHEMA);
					else
						valueSchema.field(this.columnName, Schema.STRING_SCHEMA);
				break;
			case "NCHAR":
				jdbcType = Types.NCHAR;
				if (this.nullable)
					valueSchema.field(this.columnName, Schema.OPTIONAL_STRING_SCHEMA);
				else
					if (this.partOfPk)
						keySchema.field(this.columnName, Schema.STRING_SCHEMA);
					else
						valueSchema.field(this.columnName, Schema.STRING_SCHEMA);
				break;
			case "VARCHAR2":
				jdbcType = Types.VARCHAR;
				if (this.nullable)
					valueSchema.field(this.columnName, Schema.OPTIONAL_STRING_SCHEMA);
				else
					if (this.partOfPk)
						keySchema.field(this.columnName, Schema.STRING_SCHEMA);
					else
						valueSchema.field(this.columnName, Schema.STRING_SCHEMA);
				break;
			case "NVARCHAR2":
				jdbcType = Types.NVARCHAR;
				if (this.nullable)
					valueSchema.field(this.columnName, Schema.OPTIONAL_STRING_SCHEMA);
				else
					if (this.partOfPk)
						keySchema.field(this.columnName, Schema.STRING_SCHEMA);
					else
						valueSchema.field(this.columnName, Schema.STRING_SCHEMA);
				break;
			case "BLOB":
				jdbcType = Types.BLOB;
				if (this.nullable)
					valueSchema.field(this.columnName, Schema.OPTIONAL_BYTES_SCHEMA);
				else
					if (this.partOfPk)
						keySchema.field(this.columnName, Schema.BYTES_SCHEMA);
					else
						valueSchema.field(this.columnName, Schema.BYTES_SCHEMA);
				break;
			case "CLOB":
				jdbcType = Types.CLOB;
				if (this.nullable)
					valueSchema.field(this.columnName, Schema.OPTIONAL_STRING_SCHEMA);
				else
					if (this.partOfPk)
						keySchema.field(this.columnName, Schema.STRING_SCHEMA);
					else
						valueSchema.field(this.columnName, Schema.STRING_SCHEMA);
				break;
			case "TIMESTAMP":
			case "TIMESTAMP(0)":
			case "TIMESTAMP(1)":
			case "TIMESTAMP(3)":
			case "TIMESTAMP(6)":
			case "TIMESTAMP(9)":
				jdbcType = Types.TIMESTAMP;
				if (this.nullable)
					valueSchema.field(this.columnName, Timestamp.builder().optional().build());
				else
					if (this.partOfPk)
						keySchema.field(this.columnName, Timestamp.builder().required().build());
					else
						valueSchema.field(this.columnName, Timestamp.builder().required().build());
				break;
			default:
				jdbcType = Types.VARCHAR;
				if (this.nullable)
					valueSchema.field(this.columnName, Schema.OPTIONAL_STRING_SCHEMA);
				else
					if (this.partOfPk)
						keySchema.field(this.columnName, Schema.STRING_SCHEMA);
					else
						valueSchema.field(this.columnName, Schema.STRING_SCHEMA);
				break;
		}
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM && this.partOfPk) {
			valueSchema.field(this.columnName,
					keySchema.build().field(this.columnName).schema());
		}
	}

	/**
	 * Used in Sink connector
	 * 
	 * @param avroSchema
	 * @param partOfPk
	 */
	public OraColumn(final Field field, final boolean partOfPk) {
		this.columnName = field.name();
		this.partOfPk = partOfPk;
		this.nullable = field.schema().isOptional();
		this.nameFromId = null;
		switch (field.schema().type().getName().toUpperCase()) {
		case "INT8":
			jdbcType = Types.TINYINT;
			break;
		case "INT16":
			jdbcType = Types.SMALLINT;
			break;
		case "INT32":
			if (field.schema().name() != null && Date.LOGICAL_NAME.equals(field.schema().name()))
				jdbcType = Types.DATE;
			else
				jdbcType = Types.INTEGER;
			break;
		case "INT64":
			if (field.schema().name() != null && Timestamp.LOGICAL_NAME.equals(field.schema().name()))
				jdbcType = Types.TIMESTAMP;
			else
				jdbcType = Types.BIGINT;
			break;
		case "FLOAT32":
			jdbcType = Types.FLOAT;
			break;
		case "FLOAT64":
			jdbcType = Types.DOUBLE;
			break;
		case "BYTES":
			if (field.schema().name() != null && Decimal.LOGICAL_NAME.equals(field.schema().name())) {
				jdbcType = Types.DECIMAL;
				try {
					dataScale = Integer.valueOf(field.schema().parameters().get(Decimal.SCALE_FIELD));
				} catch (Exception e) {
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
				}
			} else
				jdbcType = Types.BINARY;
			break;
		case "STRING":
		default:
			jdbcType = Types.VARCHAR;
			break;

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

	public String getNameFromId() {
		return nameFromId;
	}

	public boolean isPartOfPk() {
		return partOfPk;
	}

	public int getJdbcType() {
		return jdbcType;
	}

	public boolean isNullable() {
		return nullable;
	}

	public int getDataScale() {
		return dataScale;
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
		switch (jdbcType) {
		case Types.DATE:
			//TODO Timezone support!!!!
			if (columnValue == null)
				statement.setNull(columnNo, Types.DATE);
			else
				statement.setDate(columnNo, new java.sql.Date((Long) columnValue));
			break;
		case Types.TIMESTAMP:
			//TODO Timezone support!!!!
			if (columnValue == null)
				statement.setNull(columnNo, Types.TIMESTAMP);
			else
				statement.setTimestamp(columnNo, new java.sql.Timestamp(((java.util.Date) columnValue).getTime()));
			break;
		case Types.BOOLEAN:
			if (columnValue == null)
				statement.setNull(columnNo, Types.BOOLEAN);
			else
				statement.setBoolean(columnNo, (boolean) columnValue);
			break;
		case Types.TINYINT:
			if (columnValue == null)
				statement.setNull(columnNo, Types.TINYINT);
			else
				statement.setByte(columnNo, (Byte) columnValue);
			break;
		case Types.SMALLINT:
			if (columnValue == null)
				statement.setNull(columnNo, Types.SMALLINT);
			else
				statement.setShort(columnNo, (Short) columnValue);
			break;
		case Types.INTEGER:
			if (columnValue == null)
				statement.setNull(columnNo, Types.INTEGER);
			else
				statement.setInt(columnNo, (Integer) columnValue);
			break;
		case Types.BIGINT:
			if (columnValue == null)
				statement.setNull(columnNo, Types.BIGINT);
			else
				try {
					statement.setLong(columnNo, (Long) columnValue);
				} catch (ClassCastException cce) {
					statement.setLong(columnNo, (Integer) columnValue);
				}
			break;
		case Types.FLOAT:
			if (columnValue == null)
				statement.setNull(columnNo, Types.FLOAT);
			else
				statement.setFloat(columnNo, (float) columnValue);
			break;
		case Types.DOUBLE:
			if (columnValue == null)
				statement.setNull(columnNo, Types.DOUBLE);
			else
				statement.setDouble(columnNo, (double) columnValue);
			break;
		case Types.DECIMAL:
			if (columnValue == null)
				statement.setNull(columnNo, Types.DECIMAL);
			else
				statement.setBigDecimal(columnNo, (BigDecimal) columnValue);
			break;
		case Types.BINARY:
			if (columnValue == null)
				statement.setNull(columnNo, Types.BINARY);
			else
				statement.setBytes(columnNo, (byte[]) columnValue);
			break;
		case Types.VARCHAR:
			if (columnValue == null)
				statement.setNull(columnNo, Types.VARCHAR);
			else
				// 0x00 PostgreSQL problem
				if (HikariPoolConnectionFactory.getDbType() == HikariPoolConnectionFactory.DB_TYPE_POSTGRESQL)
					statement.setString(columnNo, ((String) columnValue).replace("\0", ""));
				else
					statement.setString(columnNo, (String) columnValue);
			break;
		default:
			throw new SQLException("Unsupported data type!!!");
		}
	}
}
