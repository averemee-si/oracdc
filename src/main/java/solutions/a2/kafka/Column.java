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

package solutions.a2.kafka;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static solutions.a2.cdc.oracle.data.JdbcTypes.getTypeName;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.data.OraNumber;
import solutions.a2.cdc.oracle.data.OraTimestamp;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public abstract class Column {

	private static final Logger LOGGER = LoggerFactory.getLogger(Column.class);
	protected static final char CHAR_0 = (char)0;

	public static final int JAVA_SQL_TYPE_INTERVALYM_STRING = -2_000_000_001;
	public static final int JAVA_SQL_TYPE_INTERVALDS_STRING = -2_000_000_003;

	protected String columnName;
	protected int jdbcType;
	protected int dataScale;
	protected short flags = 0;

	protected static final short FLG_NULLABLE            = (short)0x0001;
	protected static final short FLG_PART_OF_PK          = (short)0x0002;
	protected static final short FLG_MANDATORY           = (short)0x0004;
	protected static final short FLG_LARGE_OBJECT        = (short)0x0008;
	protected static final short FLG_SECURE_FILE         = (short)0x0010;
	protected static final short FLG_SALT                = (short)0x0020;
	protected static final short FLG_ENCRYPTED           = (short)0x0040;
	protected static final short FLG_NUMBER              = (short)0x0080;
	protected static final short FLG_BINARY_FLOAT_DOUBLE = (short)0x0100;
	protected static final short FLG_LOCAL_TIME_ZONE     = (short)0x0200;
	protected static final short FLG_DEFAULT_VALUE       = (short)0x0400;
	protected static final short FLG_LOB_TRANSFORM       = (short)0x0800;
	protected static final short FLG_WRAPPED             = (short)0x1000;
	protected static final short FLG_DECODE_WITH_TRANS   = (short) (FLG_LARGE_OBJECT | FLG_SECURE_FILE);

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public boolean isPartOfPk() {
		return (flags & FLG_PART_OF_PK) > 0;
	}

	public int getJdbcType() {
		return jdbcType;
	}

	public boolean isNullable() {
		return (flags & FLG_NULLABLE) > 0;
	}

	public int getDataScale() {
		return dataScale;
	}

	/**
	 * 
	 * @param dbType
	 * @param statement
	 * @param columnNo
	 * @param columnValue
	 * @throws SQLException
	 */
	protected void bindWithPrepStmt(
			final int dbType,
			final PreparedStatement statement,
			final int columnNo,
			final Object columnValue) throws SQLException  {
		if (columnValue == null) {
			statement.setNull(columnNo, jdbcType);
		} else {
			switch (jdbcType) {
			case DATE:
				statement.setDate(columnNo, new java.sql.Date(((java.util.Date) columnValue).getTime()));
				break;
			case TIMESTAMP:
				statement.setTimestamp(columnNo, new java.sql.Timestamp(((java.util.Date) columnValue).getTime()));
				break;
			case TIMESTAMP_WITH_TIMEZONE:
				statement.setObject(columnNo, OraTimestamp.toLogical((String) columnValue));
				break;
			case BOOLEAN:
				statement.setBoolean(columnNo, (boolean) columnValue);
				break;
			case TINYINT:
				statement.setByte(columnNo, (Byte) columnValue);
				break;
			case SMALLINT:
				statement.setShort(columnNo, (Short) columnValue);
				break;
			case INTEGER:
				statement.setInt(columnNo, (Integer) columnValue);
				break;
			case BIGINT:
				try {
					statement.setLong(columnNo, (Long) columnValue);
				} catch (ClassCastException cce) {
					statement.setLong(columnNo, (Integer) columnValue);
				}
				break;
			case FLOAT:
				if (!handleFloatNegativeInfinity(statement, columnNo, columnValue)) {
					statement.setFloat(columnNo, (float) columnValue);
				}
				break;
			case DOUBLE:
				if (!handleFloatNegativeInfinity(statement, columnNo, columnValue)) {
					statement.setDouble(columnNo, (double) columnValue);
				}
				break;
			case DECIMAL:
				statement.setBigDecimal(columnNo, (BigDecimal) columnValue);
				break;
			case NUMERIC:
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
				BigDecimal bd = null;
				bd = OraNumber.toLogical(ba);
				if (bd == null) {
					setNull = true;
				}
				if (setNull) {
					statement.setNull(columnNo, NUMERIC);
				} else {
					statement.setBigDecimal(columnNo, bd);
				}
				break;
			case BINARY:
				statement.setBytes(columnNo, ((ByteBuffer) columnValue).array());
				break;
			case VARCHAR:
				statement.setString(columnNo, (String) columnValue);
				break;
			default:
				LOGGER.error("Unsupported data type {} for column {}.",
						getTypeName(jdbcType), columnName);
				throw new SQLException("Unsupported data type: " + getTypeName(jdbcType));
			}
		}
	}

	public boolean handleFloatNegativeInfinity(PreparedStatement statement, int columnNo, Object value) throws SQLException {
		if ((jdbcType == FLOAT && Float.NEGATIVE_INFINITY == (float) value) ||
				(jdbcType == DOUBLE && Double.NEGATIVE_INFINITY == (double) value)) {
			if ((flags & FLG_NULLABLE) > 0) {
				statement.setNull(columnNo, jdbcType);
				LOGGER.error(
						"""
						
						=====================
						Negative {} infinity value for nullable column '{}' at position # {}!
						Column value is set to NULL !
						=====================
						
						""", getTypeName(jdbcType), columnName, columnNo);
			} else {
				if (jdbcType == FLOAT) statement.setFloat(columnNo, Float.MIN_VALUE);
				else statement.setDouble(columnNo, Double.MIN_VALUE);
				LOGGER.error(
						"""
						
						=====================
						Negative {} infinity value for column '{}' at position # {}!
						Column value is set to {}.MIN_VALUE = '{}'!
						=====================
						
						""", getTypeName(jdbcType), columnName, columnNo, getTypeName(jdbcType),
							jdbcType == FLOAT ? Float.MIN_VALUE : Double.MIN_VALUE);
			}
			return true;
		} else
			return false;
	}

}
