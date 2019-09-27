/**
 * Copyright (c) 2018-present, http://a2-solutions.eu
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

package eu.solutions.a2.cdc.oracle.standalone;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import eu.solutions.a2.cdc.oracle.standalone.avro.AvroSchema;

public class OraColumn {

	private final String columnName;
	private final boolean partOfPk;
	private final String oraType;
	private final int jdbcType;
	private final boolean nullable;
//	private final Schema schemaType;
	private final AvroSchema avroSchema;
	

	public OraColumn(final ResultSet resultSet) throws SQLException {
		this.columnName = resultSet.getString("COLUMN_NAME");
		final String partOfPkString = resultSet.getString("PK");
		if (!resultSet.wasNull() && "Y".equals(partOfPkString))
			this.partOfPk = true;
		else
			this.partOfPk = false;
		this.nullable = "Y".equals(resultSet.getString("NULLABLE")) ? true : false;
		this.oraType = resultSet.getString("DATA_TYPE");
		switch (this.oraType) {
			case "DATE":
				jdbcType = Types.DATE;
				if (this.nullable)
					this.avroSchema = AvroSchema.STRING_OPTIONAL();
				else
					this.avroSchema = AvroSchema.STRING_MANDATORY();
				break;
			case "FLOAT":
				jdbcType = Types.FLOAT;
				if (this.nullable)
					this.avroSchema = AvroSchema.FLOAT64_OPTIONAL();
				else
					this.avroSchema = AvroSchema.FLOAT64_MANDATORY();
				break;
			case "NUMBER":
				final int dataPrecision = resultSet.getInt("DATA_PRECISION"); 
				final int dataScale = resultSet.getInt("DATA_SCALE");
				if (dataScale == 0) {
					// Integer!!!
					if (dataPrecision < 3) {
						jdbcType = Types.TINYINT;
						if (this.nullable)
							this.avroSchema = AvroSchema.INT8_OPTIONAL();
						else
							this.avroSchema = AvroSchema.INT8_MANDATORY();
					}
					else if (dataPrecision < 5) {
						jdbcType = Types.SMALLINT;
						if (this.nullable)
							this.avroSchema = AvroSchema.INT16_OPTIONAL();
						else
							this.avroSchema = AvroSchema.INT16_MANDATORY();
					}
					else if (dataPrecision < 10) {
						jdbcType = Types.INTEGER;
						if (this.nullable)
							this.avroSchema = AvroSchema.INT32_OPTIONAL();
						else
							this.avroSchema = AvroSchema.INT32_MANDATORY();
					}
					else {
						jdbcType = Types.BIGINT;
						if (this.nullable)
							this.avroSchema = AvroSchema.INT64_OPTIONAL();
						else
							this.avroSchema = AvroSchema.INT64_MANDATORY();
					}
				} else {
					jdbcType = Types.DOUBLE;
					if (this.nullable)
						this.avroSchema = AvroSchema.FLOAT64_OPTIONAL();
					else
						this.avroSchema = AvroSchema.FLOAT64_MANDATORY();
				}
				break;
			case "RAW":
				jdbcType = Types.BINARY;
				// We'll use Base64 for this
				if (this.nullable)
					this.avroSchema = AvroSchema.STRING_OPTIONAL();
				else
					this.avroSchema = AvroSchema.STRING_MANDATORY();
				break;
			case "CHAR":
				jdbcType = Types.CHAR;
				if (this.nullable)
					this.avroSchema = AvroSchema.STRING_OPTIONAL();
				else
					this.avroSchema = AvroSchema.STRING_MANDATORY();
				break;
			case "NCHAR":
				jdbcType = Types.NCHAR;
				if (this.nullable)
					this.avroSchema = AvroSchema.STRING_OPTIONAL();
				else
					this.avroSchema = AvroSchema.STRING_MANDATORY();
				break;
			case "VARCHAR2":
				jdbcType = Types.VARCHAR;
				if (this.nullable)
					this.avroSchema = AvroSchema.STRING_OPTIONAL();
				else
					this.avroSchema = AvroSchema.STRING_MANDATORY();
				break;
			case "NVARCHAR2":
				jdbcType = Types.NVARCHAR;
				if (this.nullable)
					this.avroSchema = AvroSchema.STRING_OPTIONAL();
				else
					this.avroSchema = AvroSchema.STRING_MANDATORY();
				break;
			case "BLOB":
				jdbcType = Types.BLOB;
				// We'll use Base64 for this
				if (this.nullable)
					this.avroSchema = AvroSchema.STRING_OPTIONAL();
				else
					this.avroSchema = AvroSchema.STRING_MANDATORY();
				break;
			case "CLOB":
				jdbcType = Types.CLOB;
				if (this.nullable)
					this.avroSchema = AvroSchema.STRING_OPTIONAL();
				else
					this.avroSchema = AvroSchema.STRING_MANDATORY();
				break;
			case "TIMESTAMP":
			case "TIMESTAMP(0)":
			case "TIMESTAMP(1)":
			case "TIMESTAMP(3)":
			case "TIMESTAMP(6)":
			case "TIMESTAMP(9)":
				if (this.nullable)
					this.avroSchema = AvroSchema.STRING_OPTIONAL();
				else
					this.avroSchema = AvroSchema.STRING_MANDATORY();
				jdbcType = Types.TIMESTAMP;
				break;
			default:
				jdbcType = Types.VARCHAR;
				if (this.nullable)
					this.avroSchema = AvroSchema.STRING_OPTIONAL();
				else
					this.avroSchema = AvroSchema.STRING_MANDATORY();
				break;
		}
		this.avroSchema.setField(columnName);
	}

	public String getColumnName() {
		return columnName;
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

	public AvroSchema getAvroSchema() {
		return avroSchema;
	}

}
