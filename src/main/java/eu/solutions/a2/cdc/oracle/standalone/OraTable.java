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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import eu.solutions.a2.cdc.oracle.ConnectionFactory;
import eu.solutions.a2.cdc.oracle.standalone.avro.AvroSchema;
import eu.solutions.a2.cdc.oracle.standalone.avro.Envelope;
import eu.solutions.a2.cdc.oracle.standalone.avro.Payload;
import eu.solutions.a2.cdc.oracle.standalone.avro.Source;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

public class OraTable implements Runnable {

	private static final Logger LOGGER = Logger.getLogger(OraTable.class);

	private final int batchSize;
	private final SendMethodIntf sendMethod;
	private final String tableOwner;
	private final String masterTable;
	private final String masterTableSelSql;
	private final String snapshotLog;
	private final String snapshotLogSelSql;
	private final String snapshotLogDelSql;
	private final List<OraColumn> allColumns;
	private final List<OraColumn> pkColumns;
	private final AvroSchema schema;
	private final SimpleDateFormat iso8601DateFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	private final SimpleDateFormat iso8601TimestampFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

	public OraTable(ResultSet resultSet, final int batchSize, final SendMethodIntf sendMethod) throws SQLException {
		this.batchSize = batchSize;
		this.sendMethod = sendMethod;
		this.tableOwner = resultSet.getString("LOG_OWNER");

		this.masterTable = resultSet.getString("MASTER");

		this.snapshotLog = resultSet.getString("LOG_TABLE");
		final String snapshotFqn = "\"" + this.tableOwner + "\"" + ".\"" + this.snapshotLog + "\"";
		this.snapshotLogDelSql = "delete from " + snapshotFqn + " where ROWID=?";

		Connection connection = resultSet.getStatement().getConnection();
		/*
select C.COLUMN_NAME, C.DATA_TYPE, C.DATA_LENGTH, C.DATA_PRECISION, C.DATA_SCALE, C.NULLABLE,
 (select 'Y' from ALL_IND_COLUMNS I
  where I.TABLE_OWNER=C.OWNER and I.TABLE_NAME=C.TABLE_NAME and I.INDEX_NAME='PK_DEPT' and C.COLUMN_NAME=I.COLUMN_NAME) PK
from   ALL_TAB_COLUMNS C
where  C.OWNER='SCOTT' and C.TABLE_NAME='DEPT'
  and  (C.DATA_TYPE in ('DATE', 'FLOAT', 'NUMBER', 'RAW', 'CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2', 'BLOB', 'CLOB') or C.DATA_TYPE like 'TIMESTAMP%');
		 */
		PreparedStatement statement = connection.prepareStatement(
				"select C.COLUMN_NAME, C.DATA_TYPE, C.DATA_LENGTH, C.DATA_PRECISION, C.DATA_SCALE, C.NULLABLE,\n" +
				"(select 'Y' from ALL_IND_COLUMNS I\n" +
				"  where I.TABLE_OWNER=C.OWNER and I.TABLE_NAME=C.TABLE_NAME and I.INDEX_NAME=? and C.COLUMN_NAME=I.COLUMN_NAME) PK\n" +
				"from   ALL_TAB_COLUMNS C\n" +
				"where  C.OWNER=? and C.TABLE_NAME=?\n" +
				"  and  (C.DATA_TYPE in ('DATE', 'FLOAT', 'NUMBER', 'RAW', 'CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2', 'BLOB', 'CLOB') or C.DATA_TYPE like 'TIMESTAMP%')");
		statement.setString(1, resultSet.getString("CONSTRAINT_NAME"));
		statement.setString(2, this.tableOwner);
		statement.setString(3, this.masterTable);
		ResultSet rsColumns = statement.executeQuery();
		StringBuilder masterSelect = new StringBuilder(512);
		boolean masterFirstColumn = true;
		masterSelect.append("select ");
		StringBuilder masterWhere = new StringBuilder(128);
		StringBuilder mViewSelect = new StringBuilder(128);
		boolean mViewFirstColumn = true;
		mViewSelect.append("select ");
		allColumns = new ArrayList<>();
		pkColumns = new ArrayList<>();
		// Schema
		final String tableNameWithOwner = this.tableOwner + "." + this.masterTable;
		final AvroSchema schemaBefore = AvroSchema.STRUCT_OPTIONAL();
		schemaBefore.setName(tableNameWithOwner + ".PK");
		schemaBefore.setField("before");
		schemaBefore.initFields();
		final AvroSchema schemaAfter = AvroSchema.STRUCT_OPTIONAL();
		schemaAfter.setName(tableNameWithOwner + ".Data");
		schemaAfter.setField("after");
		schemaAfter.initFields();

		while (rsColumns .next()) {
			OraColumn column = new OraColumn(rsColumns);
			allColumns.add(column);
			schemaAfter.getFields().add(column.getAvroSchema());

			if (masterFirstColumn) {
				masterFirstColumn = false;
			} else {
				masterSelect.append(", ");
			}
			masterSelect.append("\"");
			masterSelect.append(column.getColumnName());
			masterSelect.append("\"");

			if (column.isPartOfPk()) {
				pkColumns.add(column);
				schemaBefore.getFields().add(column.getAvroSchema());
				if (mViewFirstColumn) {
					mViewFirstColumn = false;
				} else {
					mViewSelect.append(", ");
					masterWhere.append(" and ");
				}
				mViewSelect.append("\"");
				mViewSelect.append(column.getColumnName());
				mViewSelect.append("\"");
				masterWhere.append("\"");
				masterWhere.append(column.getColumnName());
				masterWhere.append("\"=?");
			}
		}
		rsColumns.close();
		rsColumns = null;
		statement.close();
		statement = null;
		// Schema
		final AvroSchema op = AvroSchema.STRING_MANDATORY();
		op.setField("op");
		final AvroSchema ts_ms = AvroSchema.INT64_MANDATORY();
		ts_ms.setField("ts_ms");
		schema = AvroSchema.STRUCT_MANDATORY();
		schema.setName(tableNameWithOwner + ".Envelope");
		schema.initFields();
		schema.getFields().add(schemaBefore);
		schema.getFields().add(schemaAfter);
		schema.getFields().add(Source.schema());
		schema.getFields().add(op);
		schema.getFields().add(ts_ms);

		masterSelect.append(", ORA_ROWSCN, SYSTIMESTAMP at time zone 'GMT' as TIMESTAMP$$ from ");
		masterSelect.append("\"");
		masterSelect.append(this.tableOwner);
		masterSelect.append("\".\"");
		masterSelect.append(this.masterTable);
		masterSelect.append("\" where ");
		masterSelect.append(masterWhere);
		
		this.masterTableSelSql = masterSelect.toString();

		mViewSelect.append(", SEQUENCE$$, case DMLTYPE$$ when 'I' then 'c' when 'U' then 'u' else 'd' end as OPTYPE$$, ORA_ROWSCN, SYSTIMESTAMP at time zone 'GMT' as TIMESTAMP$$, ROWID from ");
		mViewSelect.append(snapshotFqn);
		mViewSelect.append(" order by SEQUENCE$$");
		this.snapshotLogSelSql = mViewSelect.toString();
	}

	public void run() {
		// Poll data (standalone mode)
		try (Connection connection = ConnectionFactory.getConnection();
				PreparedStatement stmtLog = connection.prepareStatement(snapshotLogSelSql);
				PreparedStatement stmtMaster = connection.prepareStatement(masterTableSelSql);
				PreparedStatement stmtDeleteLog = connection.prepareStatement(snapshotLogDelSql)) {
			final List<RowId> logRows2Delete = new ArrayList<>();
			//TODO - SEQUENCE$$ for start!!!
			// Read materialized view log and get PK values
			ResultSet rsLog = stmtLog.executeQuery();
			int recordCount = 0;
			while (rsLog.next() && recordCount < batchSize) {
				recordCount++;
				final String opType = rsLog.getString("OPTYPE$$");
				final Map<String, Object> columnValues = new LinkedHashMap<>();
				final boolean deleteOp = "d".equals(opType);
				// process primary key information from materialized view log
				processPkColumns(deleteOp, rsLog, columnValues, stmtMaster);
				// Add ROWID to list for delete after sending data to queue
				logRows2Delete.add(rsLog.getRowId("ROWID"));

				boolean success = true;
				final Envelope envelope = new Envelope(
						this.getSchema(),
						new Payload(new Source(tableOwner, masterTable), opType));

				if (deleteOp) {
					// For DELETE we have only "before" data
					envelope.getPayload().setBefore(columnValues);
					// For delete we need to get TS & SCN from snapshot log
					envelope.getPayload().getSource().setTs_ms(
							rsLog.getTimestamp("TIMESTAMP$$").getTime());
					envelope.getPayload().getSource().setScn(
							rsLog.getBigDecimal("ORA_ROWSCN").toBigInteger());					
				} else {
					// Get data from master table
					ResultSet rsMaster = stmtMaster.executeQuery();
					// We're working with PK
					if (rsMaster.next()) {
						processAllColumns(rsMaster, columnValues);
						// For INSERT/UPDATE  we have only "after" data 
						envelope.getPayload().setAfter(columnValues);
						// For delete we need to get TS & SCN from snapshot log
						envelope.getPayload().getSource().setTs_ms(
								rsMaster.getTimestamp("TIMESTAMP$$").getTime());
						envelope.getPayload().getSource().setScn(
								rsMaster.getBigDecimal("ORA_ROWSCN").toBigInteger());					
					} else {
						success = false;
						LOGGER.error("Primary key = " + nonExistentPk(rsLog) + " not found in " + tableOwner + "." + masterTable);
						LOGGER.error("\twhile executing\n\t\t" + masterTableSelSql);
					}
					// Close unneeded ResultSet
					rsMaster.close();
					rsMaster = null;
				}
				// Ready to process message
				if (success) {
					final StringBuilder messageKey = new StringBuilder(64);
					messageKey.append(tableOwner);
					messageKey.append(".");
					messageKey.append(masterTable);
					messageKey.append("-");
					messageKey.append(rsLog.getLong("SEQUENCE$$"));
					sendMethod.sendData(messageKey.toString(), envelope);
				}
			}
			rsLog.close();
			rsLog = null;
			// Perform deletion
			//TODO - success check of send!!!
			for (RowId rowId : logRows2Delete) {
				stmtDeleteLog.setRowId(1, rowId);
				stmtDeleteLog.executeUpdate();
			}
			connection.commit();
		} catch (SQLException e) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
		}
	}

	public AvroSchema getSchema() {
		return schema;
	}

	@Override
	public String toString() {
		final String masterFqn = "\"" + this.tableOwner + "\"" + ".\"" + this.masterTable + "\"";
		final String snapshotFqn = "\"" + this.tableOwner + "\"" + ".\"" + this.snapshotLog + "\"";		
		return snapshotFqn + " on " + masterFqn;
	}

	private void processPkColumns(final boolean deleteOp, ResultSet rsLog,
			final Map<String, Object> columnValues, PreparedStatement stmtMaster) throws SQLException {
		for (int i = 0; i < pkColumns.size(); i++) {
			final OraColumn oraColumn = pkColumns.get(i);
			final String columnName = oraColumn.getColumnName();
			switch (oraColumn.getJdbcType()) {
			case Types.DATE:
				if (deleteOp)
					//TODO Timezone support!!!!
					columnValues.put(
							columnName,
							iso8601DateFmt.format(new Date(rsLog.getDate(columnName).getTime())));
				else
					stmtMaster.setDate(i + 1, rsLog.getDate(columnName));
				break;
			case Types.TINYINT:
				if (deleteOp)
					columnValues.put(columnName, rsLog.getByte(columnName));
				else
					stmtMaster.setByte(i + 1, rsLog.getByte(columnName));
				break;
			case Types.SMALLINT:
				if (deleteOp)
					columnValues.put(columnName, rsLog.getShort(columnName));
				else
					stmtMaster.setShort(i + 1, rsLog.getShort(columnName));
				break;
			case Types.INTEGER:
				if (deleteOp)
					columnValues.put(columnName, rsLog.getInt(columnName));
				else
					stmtMaster.setInt(i + 1, rsLog.getInt(columnName));
				break;
			case Types.BIGINT:
				if (deleteOp)
					columnValues.put(columnName, rsLog.getLong(columnName));
				else
					stmtMaster.setLong(i + 1, rsLog.getLong(columnName));
				break;
			case Types.BINARY:
				if (deleteOp)
					// Encode BINARY to Base64
					columnValues.put(
							columnName,
							Base64.getEncoder().encodeToString(rsLog.getBytes(columnName)));
				else
					stmtMaster.setBytes(i + 1, rsLog.getBytes(columnName));
				break;
			case Types.CHAR:
			case Types.VARCHAR:
				if (deleteOp)
					columnValues.put(columnName, rsLog.getString(columnName));
				else
					stmtMaster.setString(i + 1, rsLog.getString(columnName));
				break;
			case Types.NCHAR:
			case Types.NVARCHAR:
				if (deleteOp)
					columnValues.put(columnName, rsLog.getNString(columnName));
				else
					stmtMaster.setNString(i + 1, rsLog.getNString(columnName));
				break;
			case Types.TIMESTAMP:
				if (deleteOp)
					//TODO Timezone support!!!!
					columnValues.put(
							columnName,
							iso8601TimestampFmt.format(new Date(rsLog.getTimestamp(columnName).getTime())));
				else
					stmtMaster.setTimestamp(i + 1, rsLog.getTimestamp(columnName));
				break;
			default:
				// Types.FLOAT, Types.DOUBLE, Types.BLOB, Types.CLOB 
				// TODO - is it possible?
				if (deleteOp)
					columnValues.put(columnName, rsLog.getString(columnName));
				else
					stmtMaster.setString(i + 1, rsLog.getString(columnName));
				break;
			}
		}
	}

	private void processAllColumns(
			ResultSet rsMaster, final Map<String, Object> columnValues) throws SQLException {
		for (int i = 0; i < allColumns.size(); i++) {
			final OraColumn oraColumn = allColumns.get(i);
			final String columnName = oraColumn.getColumnName();
			switch (oraColumn.getJdbcType()) {
			case Types.DATE:
				//TODO Timezone support!!!!
				final java.sql.Date dateColumnValue = rsMaster.getDate(columnName);
				if (rsMaster.wasNull())
					columnValues.put(columnName, null);
				else
					columnValues.put(
							columnName,
							iso8601DateFmt.format(new Date(dateColumnValue.getTime())));
				break;
			case Types.TINYINT:
				final byte byteColumnValue = rsMaster.getByte(columnName);
				if (rsMaster.wasNull())
					columnValues.put(columnName, null);
				else
					columnValues.put(columnName, byteColumnValue);									
				break;
			case Types.SMALLINT:
				final short shortColumnValue = rsMaster.getShort(columnName); 
				if (rsMaster.wasNull())
					columnValues.put(columnName, null);
				else
					columnValues.put(columnName, shortColumnValue);
				break;
			case Types.INTEGER:
				final int intColumnValue = rsMaster.getInt(columnName); 
				if (rsMaster.wasNull())
					columnValues.put(columnName, null);
				else
					columnValues.put(columnName, intColumnValue);
				break;
			case Types.BIGINT:
				final long longColumnValue = rsMaster.getLong(columnName); 
				if (rsMaster.wasNull())
					columnValues.put(columnName, null);
				else
					columnValues.put(columnName, longColumnValue);
				break;
			case Types.BINARY:
				// Encode BINARY to Base64
				final byte[] binaryColumnValue = rsMaster.getBytes(columnName);
				if (rsMaster.wasNull())
					columnValues.put(columnName, null);
				else
					columnValues.put(
							columnName,
							Base64.getEncoder().encodeToString(binaryColumnValue));
				break;
			case Types.CHAR:
			case Types.VARCHAR:
				final String charColumnValue = rsMaster.getString(columnName); 
				if (rsMaster.wasNull())
					columnValues.put(columnName, null);
				else
					columnValues.put(columnName, charColumnValue);
				break;
			case Types.NCHAR:
			case Types.NVARCHAR:
				final String nCharColumnValue = rsMaster.getNString(columnName); 
				if (rsMaster.wasNull())
					columnValues.put(columnName, null);
				else
					columnValues.put(columnName, nCharColumnValue);
				break;
			case Types.TIMESTAMP:
				//TODO Timezone support!!!!
				final Timestamp tsColumnValue = rsMaster.getTimestamp(columnName);
				if (rsMaster.wasNull())
					columnValues.put(columnName, null);
				else
					columnValues.put(
							columnName,
							iso8601TimestampFmt.format(new Date(tsColumnValue.getTime())));
				break;
			case Types.FLOAT:
				final float floatColumnValue = rsMaster.getFloat(columnName); 
				if (rsMaster.wasNull())
					columnValues.put(columnName, null);
				else
					columnValues.put(columnName, floatColumnValue);
				break;
			case Types.DOUBLE:
				final double doubleColumnValue = rsMaster.getDouble(columnName); 
				if (rsMaster.wasNull())
					columnValues.put(columnName, null);
				else
					columnValues.put(columnName, doubleColumnValue);
				break;
			case Types.BLOB:
				final Blob blobColumnValue = rsMaster.getBlob(columnName);
				if (rsMaster.wasNull() || blobColumnValue.length() < 1) {
					columnValues.put(columnName, null);
				} else {
					try (InputStream is = blobColumnValue.getBinaryStream();
							ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
						final byte[] data = new byte[16384];
						int bytesRead;
						while ((bytesRead = is.read(data, 0, data.length)) != -1) {
							baos.write(data, 0, bytesRead);
						}
						columnValues.put(columnName, Base64.getEncoder().encodeToString(baos.toByteArray()));
					} catch (IOException ioe) {
						LOGGER.error("IO Error while processing BLOB column " + 
									 tableOwner + "." + masterTable + "(" + columnName + ")");
						LOGGER.error("\twhile executing\n\t\t" + masterTableSelSql);
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
					}
				}
				break;
			case Types.CLOB:
				final Clob clobColumnValue = rsMaster.getClob(columnName);
				if (rsMaster.wasNull() || clobColumnValue.length() < 1) {
					columnValues.put(columnName, null);
				} else {
					try (Reader reader = clobColumnValue.getCharacterStream()) {
						final char[] data = new char[8192];
						StringBuilder sbClob = new StringBuilder(8192);
						int charsRead;
						while ((charsRead = reader.read(data, 0, data.length)) != -1) {
							sbClob.append(data, 0, charsRead);
						}
						columnValues.put(columnName, sbClob.toString());
					} catch (IOException ioe) {
						LOGGER.error("IO Error while processing CLOB column " + 
								 tableOwner + "." + masterTable + "(" + columnName + ")");
						LOGGER.error("\twhile executing\n\t\t" + masterTableSelSql);
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
					}
				}
				break;
			default:
				break;
			}
		}
	}

	private String nonExistentPk(ResultSet rsLog) throws SQLException {
		StringBuilder sbPrimaryKey = new StringBuilder(128);
		for (int i = 0; i < pkColumns.size(); i++) {
			final OraColumn oraColumn = pkColumns.get(i);
			final String columnName = oraColumn.getColumnName();
			if (i > 0)
				sbPrimaryKey.append(" and ");
			sbPrimaryKey.append(columnName);
			sbPrimaryKey.append("=");
			switch (oraColumn.getJdbcType()) {
			case Types.DATE:
				sbPrimaryKey.append("'");
				sbPrimaryKey.append(
						iso8601DateFmt.format(new Date(rsLog.getDate(columnName).getTime())));
				sbPrimaryKey.append("'");
				break;
			case Types.TINYINT:
				sbPrimaryKey.append(rsLog.getByte(columnName));
				break;
			case Types.SMALLINT:
				sbPrimaryKey.append(rsLog.getShort(columnName));
				break;
			case Types.INTEGER:
				sbPrimaryKey.append(rsLog.getInt(columnName));
				break;
			case Types.BIGINT:
				sbPrimaryKey.append(rsLog.getLong(columnName));
				break;
			case Types.BINARY:
				// Encode binary to Base64
				sbPrimaryKey.append("'");
				sbPrimaryKey.append(
						Base64.getEncoder().encodeToString(rsLog.getBytes(columnName)));
				sbPrimaryKey.append("'");
				break;
			case Types.CHAR:
			case Types.VARCHAR:
				sbPrimaryKey.append("'");
				sbPrimaryKey.append(rsLog.getString(columnName));
				sbPrimaryKey.append("'");
				break;
			case Types.NCHAR:
			case Types.NVARCHAR:
				sbPrimaryKey.append("'");
				sbPrimaryKey.append(rsLog.getNString(columnName));
				sbPrimaryKey.append("'");
				break;
			case Types.TIMESTAMP:
				sbPrimaryKey.append("'");
				sbPrimaryKey.append(
						iso8601TimestampFmt.format(new Date(rsLog.getTimestamp(columnName).getTime())));
				sbPrimaryKey.append("'");
				break;
			default:
				// Types.FLOAT, Types.DOUBLE, Types.BLOB, Types.CLOB
				// TODO - is it possible?
				sbPrimaryKey.append("'");
				sbPrimaryKey.append(rsLog.getString(columnName));
				sbPrimaryKey.append("'");
				break;
			}
		}
		return sbPrimaryKey.toString();
	}

}
