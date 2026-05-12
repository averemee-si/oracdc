/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc;

import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static solutions.a2.cdc.oracle.data.JdbcTypes.getTypeName;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public abstract class Column {

	private static final Logger LOGGER = LoggerFactory.getLogger(Column.class);

	public static final char CHAR_0 = (char)0;
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
	private Object extension;

	public String name() {
		return columnName;
	}

	public void name(String columnName) {
		this.columnName = columnName;
	}

	public boolean partOfPk() {
		return (flags & FLG_PART_OF_PK) > 0;
	}

	public int jdbcType() {
		return jdbcType;
	}

	public boolean nullable() {
		return (flags & FLG_NULLABLE) > 0;
	}

	public int dataScale() {
		return dataScale;
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

	public void extension(Object extension) {
		this.extension = extension;
	}

	public Object extension() {
		return extension;
	}


}
