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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleResultSet;
import oracle.sql.NUMBER;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;
import solutions.a2.cdc.oracle.data.OraTimestamp;
import solutions.a2.cdc.oracle.utils.Lz4Util;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.utils.ExceptionUtils;


/**
 * 
 * @author averemee
 *
 */
public abstract class OraTable4SourceConnector extends OraTableDefinition {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraTable4SourceConnector.class);

	protected Map<String, String> sourcePartition;
	protected Schema schema;
	protected Schema keySchema;
	protected Schema valueSchema;
	private boolean rowLevelScn;
	protected OraRdbmsInfo rdbmsInfo;

	protected OraTable4SourceConnector(String tableOwner, String tableName, int schemaType) {
		super(tableOwner, tableName, schemaType);
	}

	/**
	 * 
	 * @param rsColumns
	 * @param sourceOffset
	 * @param snapshotLog           Snapshot log only!
	 * @param mViewSelect           Snapshot log only!
	 * @param masterSelect          Snapshot log only!
	 * @param snapshotDelete        Snapshot log only!
	 * @param logWithRowIds         Snapshot log only!
	 * @param logWithPrimaryKey     Snapshot log only!
	 * @param logWithSequence       Snapshot log only!
	 * @throws SQLException
	 */
	protected void buildColumnList(
			final ResultSet rsColumns, 
			final Map<String, Object> sourceOffset,
			final String snapshotLog,
			final StringBuilder mViewSelect,
			final StringBuilder masterSelect,
			final StringBuilder snapshotDelete,
			final boolean logWithRowIds,
			final boolean logWithPrimaryKey,
			final boolean logWithSequence,
			final boolean protobufSchemaNames
			) throws SQLException {
		boolean mViewFirstColumn = true;
		boolean masterFirstColumn = true;

		final String snapshotFqn = "\"" + this.tableOwner + "\"" + ".\"" + snapshotLog + "\"";
		snapshotDelete.append("delete from ");
		snapshotDelete.append(snapshotFqn);
		snapshotDelete.append(" where ROWID = ?");
			
		masterSelect.append("select ");
		mViewSelect.append("select ");
		final StringBuilder masterWhere = new StringBuilder(256);
		if (logWithRowIds) {
			// ROWID access is always faster that any other
			masterWhere.append("ROWID=?");
			// Add M_ROW$$ column for snapshot logs with ROWID
			LOGGER.trace("Adding {} to column list.", OraColumn.ROWID_KEY);
			mViewFirstColumn = false;
			mViewSelect.append("chartorowid(M_ROW$$) ");
			mViewSelect.append(OraColumn.ROWID_KEY);
		}

		final String tableFqn = this.tableOwner + "." + this.tableName;
		// Schema init
		final SchemaBuilder keySchemaBuilder = SchemaBuilder
					.struct()
					.required()
					.name(protobufSchemaNames ?
							tableOwner + "_" + tableName + "_Key" : tableFqn + ".Key")
					.version(version);
		final SchemaBuilder valueSchemaBuilder = SchemaBuilder
					.struct()
					.optional()
					.name(protobufSchemaNames ?
							tableOwner + "_" + tableName + "_Value" : tableFqn + ".Value")
					.version(version);
		// Substitute missing primary key with ROWID value
		if (!logWithPrimaryKey && logWithRowIds) {
			addPseudoKey(keySchemaBuilder, valueSchemaBuilder);
		}

		while (rsColumns.next()) {
			
			boolean columnAdded = false;
			OraColumn column = null;
			try {
				column = new OraColumn(true, false, false, rsColumns, null);
				columnAdded = true;
			} catch (UnsupportedColumnDataTypeException ucdte) {
				LOGGER.warn("Column {} not added to definition of table {}.{}",
						ucdte.getColumnName(), this.tableOwner, this.tableName);
			}

			if (columnAdded) {
				allColumns.add(column);
				if (masterFirstColumn) {
					masterFirstColumn = false;
				} else {
					masterSelect.append(", ");
				}
				masterSelect.append("\"");
				masterSelect.append(column.getColumnName());
				masterSelect.append("\"");

				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("New column {} added to table definition {}.",
							column.getColumnName(), tableFqn);
				}

				if (column.isPartOfPk()) {
					pkColumns.put(column.getColumnName(), column);
					// Schema addition
					keySchemaBuilder.field(column.getColumnName(), column.getSchema());
					if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
						valueSchemaBuilder.field(column.getColumnName(), column.getSchema());
					}
					if (mViewFirstColumn) {
						mViewFirstColumn = false;
					} else {
						mViewSelect.append(", ");
						if (!logWithRowIds)
						// We need this only when snapshot log don't contains M_ROW$$ 
							masterWhere.append(" and ");
					}
					mViewSelect.append("\"");
					mViewSelect.append(column.getColumnName());
					mViewSelect.append("\"");
					if (!logWithRowIds) {
						// We need this only when snapshot log don't contains M_ROW$$ 
						masterWhere.append("\"");
						masterWhere.append(column.getColumnName());
						masterWhere.append("\"=?");
					}
				} else {
					// Just add to value schema
					valueSchemaBuilder.field(column.getColumnName(), column.getSchema());
				}
			}
		}
		// Schema
		schemaEiplogue(tableFqn, keySchemaBuilder, valueSchemaBuilder);

		masterSelect.append(" from \"");
		masterSelect.append(this.tableOwner);
		masterSelect.append("\".\"");
		masterSelect.append(this.tableName);
		masterSelect.append("\" where ");
		masterSelect.append(masterWhere);

		if (logWithSequence) {
			mViewSelect.append(", ");
			mViewSelect.append(OraColumn.MVLOG_SEQUENCE);
		}
		mViewSelect.append(", case DMLTYPE$$ when 'I' then 'c' when 'U' then 'u' else 'd' end as OPTYPE$$, ORA_ROWSCN, SYSTIMESTAMP at time zone 'GMT' as TIMESTAMP$$, ROWID from ");
		mViewSelect.append(snapshotFqn);
		if (logWithSequence) {
			LOGGER.trace("BEGIN: mvlog with sequence specific.");
			if (sourceOffset != null && sourceOffset.get(OraColumn.MVLOG_SEQUENCE) != null) {
				long lastProcessedSequence = (long) sourceOffset.get(OraColumn.MVLOG_SEQUENCE);
				mViewSelect.append("\nwhere ");
				mViewSelect.append(OraColumn.MVLOG_SEQUENCE);
				mViewSelect.append(" > ");
				mViewSelect.append(lastProcessedSequence);
				mViewSelect.append("\n");
				LOGGER.debug("Will read mvlog with {} greater than {}.",
							OraColumn.MVLOG_SEQUENCE, lastProcessedSequence);
			}
			mViewSelect.append(" order by ");
			mViewSelect.append(OraColumn.MVLOG_SEQUENCE);
			LOGGER.trace("END: mvlog with sequence specific.");
		}
		LOGGER.trace("End of column list and SQL statements preparation for table {}.{}", this.tableOwner, this.tableName);
	}

	protected void schemaEiplogue(final String tableFqn,
			final SchemaBuilder keySchemaBuilder, final SchemaBuilder valueSchemaBuilder) throws SQLException {
		if (keySchemaBuilder == null) {
			keySchema = null;
		} else {
			keySchema = keySchemaBuilder.build();
		}
		schemaEiplogue(tableFqn, valueSchemaBuilder);
	}

	protected void schemaEiplogue(final String tableFqn,
			final SchemaBuilder valueSchemaBuilder) throws SQLException {
		valueSchema = valueSchemaBuilder.build();
		if (this.schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
			final SchemaBuilder schemaBuilder = SchemaBuilder
					.struct()
					.name(tableFqn + ".Envelope");
			schemaBuilder.field("op", Schema.STRING_SCHEMA);
			schemaBuilder.field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA);
			schemaBuilder.field("before", keySchema);
			schemaBuilder.field("after", valueSchema);
			if (rdbmsInfo != null) {
				schemaBuilder.field("source", rdbmsInfo.getSchema());
			}
			schema = schemaBuilder.build();
		}
	}

	protected void addPseudoKey(
			final SchemaBuilder keySchemaBuilder, final SchemaBuilder valueSchemaBuilder) {
		// Add ROWID (ORA$ROWID) - this column is not in dictionary!!!
		OraColumn rowIdColumn = OraColumn.getRowIdKey();
		allColumns.add(rowIdColumn);
		pkColumns.put(rowIdColumn.getColumnName(), rowIdColumn);
		keySchemaBuilder.field(rowIdColumn.getColumnName(), Schema.STRING_SCHEMA);
		if (this.schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
			valueSchemaBuilder.field(rowIdColumn.getColumnName(), Schema.STRING_SCHEMA);
		}
	}

	public boolean isRowLevelScn() {
		return rowLevelScn;
	}

	public void setRowLevelScn(boolean rowLevelScn) {
		this.rowLevelScn = rowLevelScn;
	}


	protected void processAllColumns(
			OracleResultSet rsMaster, final Struct keyStruct, final Struct valueStruct) throws SQLException {
		for (int i = 0; i < allColumns.size(); i++) {
			final OraColumn oraColumn = allColumns.get(i);
			final String columnName = oraColumn.getColumnName();
			Object columnValue = null; 
			switch (oraColumn.getJdbcType()) {
				case Types.DATE:
				case Types.TIMESTAMP:
					columnValue = rsMaster.getTimestamp(columnName);
					break;
				case Types.TIMESTAMP_WITH_TIMEZONE:
					final Connection connection = rsMaster.getStatement().getConnection();
					if (oraColumn.isLocalTimeZone()) {
						TIMESTAMPLTZ ltz = rsMaster.getTIMESTAMPLTZ(columnName);
						columnValue = rsMaster.wasNull() ?
							null :
							OraTimestamp.ISO_8601_FMT.format(ltz.offsetDateTimeValue(connection));
					} else {
						TIMESTAMPTZ tz = rsMaster.getTIMESTAMPTZ(columnName);
						columnValue = rsMaster.wasNull() ?
							null :
							OraTimestamp.ISO_8601_FMT.format(tz.offsetDateTimeValue(connection));
					}
					break;
				case Types.TINYINT:
					final byte byteColumnValue = rsMaster.getByte(columnName);
					columnValue = rsMaster.wasNull() ?  null : byteColumnValue;
					break;
				case Types.SMALLINT:
					final short shortColumnValue = rsMaster.getShort(columnName); 
					columnValue = rsMaster.wasNull() ?  null : shortColumnValue;
					break;
				case Types.INTEGER:
					final int intColumnValue = rsMaster.getInt(columnName);
					columnValue = rsMaster.wasNull() ?  null : intColumnValue;
					break;
				case Types.BIGINT:
					final long longColumnValue = rsMaster.getLong(columnName);
					columnValue = rsMaster.wasNull() ?  null : longColumnValue;
					break;
				case Types.DECIMAL:
					final BigDecimal bdColumnValue = rsMaster.getBigDecimal(columnName);
					columnValue = rsMaster.wasNull() ?  null : bdColumnValue.setScale(oraColumn.getDataScale());
					break;
				case Types.NUMERIC:
					final NUMBER numberValue = rsMaster.getNUMBER(columnName);
					columnValue = rsMaster.wasNull() ?  null : numberValue.getBytes();
					break;
				case Types.FLOAT:
					final float floatColumnValue = rsMaster.getFloat(columnName); 
					columnValue = rsMaster.wasNull() ?  null : floatColumnValue;
					break;
				case Types.DOUBLE:
					final double doubleColumnValue = rsMaster.getDouble(columnName); 
					columnValue = rsMaster.wasNull() ?  null : doubleColumnValue;
					break;
				case Types.BINARY:
					columnValue = rsMaster.getBytes(columnName);
					break;
				case Types.CHAR:
				case Types.VARCHAR:
					columnValue = rsMaster.getString(columnName);
					break;
				case Types.NCHAR:
				case Types.NVARCHAR:
					columnValue = rsMaster.getNString(columnName);
					break;
				case Types.ROWID:
					final RowId rowIdColumnValue = rsMaster.getRowId(columnName);
					columnValue = rsMaster.wasNull() ?  null : rowIdColumnValue.toString();
					break;
				case Types.BLOB:
					final Blob blobColumnValue = rsMaster.getBlob(columnName);
					if (rsMaster.wasNull() || blobColumnValue.length() < 1) {
						columnValue = null;
					} else {
						try (InputStream is = blobColumnValue.getBinaryStream();
								ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
							final byte[] data = new byte[16384];
							int bytesRead;
							while ((bytesRead = is.read(data, 0, data.length)) != -1) {
								baos.write(data, 0, bytesRead);
							}
							columnValue = baos.toByteArray();
						} catch (IOException ioe) {
							LOGGER.error("IO Error while processing BLOB column {}.{}({})", 
									tableOwner, tableName, columnName);
							LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
						}
					}
					break;
				case Types.CLOB:
				case Types.NCLOB:
					final Clob clobColumnValue;
					if (oraColumn.getJdbcType() == Types.CLOB) {
						clobColumnValue = rsMaster.getClob(columnName);
					} else {
						// Types.NCLOB
						clobColumnValue = rsMaster.getNClob(columnName);
					}
					if (rsMaster.wasNull() || clobColumnValue.length() < 1) {
						columnValue = null;
					} else {
						try (Reader reader = clobColumnValue.getCharacterStream()) {
							final char[] data = new char[8192];
							final StringBuilder sbClob = new StringBuilder(8192);
							int charsRead;
							while ((charsRead = reader.read(data, 0, data.length)) != -1) {
								sbClob.append(data, 0, charsRead);
							}
							columnValue = Lz4Util.compress(sbClob.toString());
						} catch (IOException ioe) {
							LOGGER.error("IO Error while processing {} column {}.{}({})", 
									oraColumn.getJdbcType() == Types.CLOB ? "CLOB" : "NCLOB",
									tableOwner, tableName, columnName);
							LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
						}
					}
					break;
				case Types.SQLXML:
					final SQLXML xmlColumnValue = rsMaster.getSQLXML(columnName);
					if (rsMaster.wasNull()) {
						columnValue = null;
					} else {
						try (Reader reader = xmlColumnValue.getCharacterStream()) {
							final char[] data = new char[8192];
							final StringBuilder sbSqlXml = new StringBuilder(8192);
							int charsRead;
							while ((charsRead = reader.read(data, 0, data.length)) != -1) {
								sbSqlXml.append(data, 0, charsRead);
							}
							columnValue = Lz4Util.compress(sbSqlXml.toString());
						} catch (IOException ioe) {
							LOGGER.error("IO Error while processing XML column {}.{}({})", 
									tableOwner, tableName, columnName);
							LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
						}
					}
					break;
				default:
					columnValue = oraColumn.unsupportedTypeValue();
					break;
			}
			if (keyStruct != null && pkColumns.containsKey(columnName)) {
				keyStruct.put(columnName, columnValue);
			}
			// Don't process PK again in case of SCHEMA_TYPE_INT_KAFKA_STD
			if ((schemaType == ConnectorParams.SCHEMA_TYPE_INT_KAFKA_STD && !pkColumns.containsKey(columnName)) ||
					schemaType == ConnectorParams.SCHEMA_TYPE_INT_SINGLE ||
					schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
				valueStruct.put(columnName, columnValue);
			}
		}
	}

}
