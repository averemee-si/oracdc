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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import oracle.jdbc.OracleResultSet;

/**
 * 
 * @author averemee
 *
 */
public class OraTable extends OraTable4SourceConnector {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraTable.class);

	private int batchSize;
	private boolean logWithRowIds = false;
	private boolean logWithPrimaryKey = false;
	private boolean logWithSequence = false;

	private String masterTableSelSql;
	private String snapshotLog;
	private String snapshotLogSelSql;
	private String snapshotLogDelSql;
	private Map<String, String> sourcePartition;

	private final SimpleDateFormat iso8601DateFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	private final SimpleDateFormat iso8601TimestampFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

	/**
	 * Constructor for OraTable object based on snapshot log and master table
	 * 
	 * @param tableOwner
	 * @param masterTable
	 * @param snapshotLog
	 * @param logWithRowIds
	 * @param logWithPrimaryKey
	 * @param logWithSequence
	 * @param batchSize
	 * @param schemaType type of schema
	 * @throws SQLException
	 */
	public OraTable(
			final String tableOwner, final String masterTable, final String snapshotLog,
			final boolean logWithRowIds, final boolean logWithPrimaryKey, final boolean logWithSequence,
			final int batchSize, final int schemaType,
			final Map<String, String> sourcePartition, Map<String, Object> sourceOffset) throws SQLException {
		super(tableOwner, masterTable, schemaType);
		LOGGER.trace("Creating OraTable object for materialized view log...");
		this.logWithRowIds = logWithRowIds;
		this.logWithPrimaryKey = logWithPrimaryKey;
		this.logWithSequence = logWithSequence;
		this.batchSize = batchSize;
		this.snapshotLog = snapshotLog;
		this.sourcePartition = sourcePartition;

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Table owner -> {}, master table -> {}", this.tableOwner, this.tableName);
			LOGGER.debug("\tMaterialized view log name -> {}", this.snapshotLog);
			LOGGER.debug("\t\tMView log with ROWID's -> {}, Primary Key -> {}, Sequence -> {}.",
					this.logWithRowIds, this.logWithPrimaryKey, this.logWithSequence);
			LOGGER.debug("batchSize -> {}", this.batchSize);
		}

		try (Connection connection = OraPoolConnectionFactory.getConnection()) {
			LOGGER.trace("Preparing column list and SQL statements for table {}.{}", this.tableOwner, this.tableName);
			PreparedStatement statement = connection.prepareStatement(OraDictSqlTexts.COLUMN_LIST_MVIEW,
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			statement.setString(1, this.snapshotLog);
			statement.setString(2, this.tableOwner);
			statement.setString(3, this.tableName);

			ResultSet rsColumns = statement.executeQuery();

			final StringBuilder mViewSelect = new StringBuilder(256);
			final StringBuilder masterSelect = new StringBuilder(512);
			final StringBuilder snapshotDelete = new StringBuilder(128);
			// We always process LOB's for snapshot logs - despite of value processLobs passed
			buildColumnList(true, false, false, null, rsColumns, sourceOffset, null, null,
					snapshotLog, mViewSelect, masterSelect, snapshotDelete,
					logWithRowIds, logWithPrimaryKey, logWithSequence);

			rsColumns.close();
			rsColumns = null;
			statement.close();
			statement = null;

			this.masterTableSelSql = masterSelect.toString();
			this.snapshotLogSelSql = mViewSelect.toString();
			this.snapshotLogDelSql = snapshotDelete.toString();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Table {} -> MView select statement\n{}", tableName, this.snapshotLogSelSql);
				LOGGER.debug("Table {} -> MView delete statement\n{}", tableName, this.tableName, this.snapshotLogDelSql);
				LOGGER.debug("Table {} -> Master table select statement\n{}", tableName, this.masterTableSelSql);
			}

		} catch (SQLException sqle) {
			LOGGER.error("Unable to get table information.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
		}
	}

	public List<SourceRecord> pollMVLog(final Connection connection, final String kafkaConnectTopic) throws SQLException {
		LOGGER.trace("BEGIN: pollMVLog()");
		PreparedStatement stmtLog = connection.prepareStatement(snapshotLogSelSql,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		PreparedStatement stmtMaster = connection.prepareStatement(masterTableSelSql,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		PreparedStatement stmtDeleteLog = connection.prepareStatement(snapshotLogDelSql);
		final List<RowId> logRows2Delete = new ArrayList<>();
		final List<SourceRecord> result = new ArrayList<>();
		// Read materialized view log and get PK values
		ResultSet rsLog = stmtLog.executeQuery();
		int recordCount = 0;
		while (rsLog.next() && recordCount < batchSize) {
			recordCount++;
			final String opType = rsLog.getString("OPTYPE$$");
			final boolean deleteOp = "d".equals(opType);
			final Struct keyStruct = new Struct(keySchema);
			final Struct valueStruct = new Struct(valueSchema);
			processPkColumns(deleteOp, rsLog, keyStruct, valueStruct, stmtMaster);
			// Add ROWID to list for delete after sending data to queue
			logRows2Delete.add(rsLog.getRowId("ROWID"));
			boolean success = true;
			if (!deleteOp) {
				// Get data from master table
				OracleResultSet rsMaster = (OracleResultSet) stmtMaster.executeQuery();
				// We're working with PK
				if (rsMaster.next()) {
					processAllColumns(rsMaster, null, valueStruct);
				} else {
					success = false;
					LOGGER.error("Primary key = {} not found in {}.{}", nonExistentPk(rsLog), tableOwner, tableName);
					LOGGER.error("\twhile executing{}\n\t\t", masterTableSelSql);
				}
				// Close unneeded ResultSet
				rsMaster.close();
				rsMaster = null;
			}
			// Ready to process message
			if (success) {
				final long lastProcessedScn = rsLog.getLong(OraColumn.ORA_ROWSCN);
				Map<String, Object> offset = null;
				if (kafkaConnectTopic != null) {
					LOGGER.trace("BEGIN: Prepare Kafka Connect offset");
					offset = new HashMap<>(2);
					offset.put(OraColumn.ORA_ROWSCN, lastProcessedScn);
					LOGGER.debug("Owner -> {}, table -> {}, last processed {} is {}.",
							tableOwner, tableName, OraColumn.ORA_ROWSCN, lastProcessedScn);
					if (this.logWithSequence) {
						final long lastProcessedSequence = rsLog.getLong(OraColumn.MVLOG_SEQUENCE);
						offset.put(OraColumn.MVLOG_SEQUENCE, lastProcessedSequence);
						LOGGER.debug("Owner -> {}, table -> {}, last processed {} is {}.",
								tableOwner, tableName, OraColumn.MVLOG_SEQUENCE, lastProcessedSequence);
					}
					LOGGER.trace("END: Prepare Kafka Connect offset");
				}

				if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
					final Struct struct = new Struct(schema);
					final Struct source = OraRdbmsInfo.getInstance().getStruct(
							null,
							null,
							tableOwner,
							tableName,
							lastProcessedScn,
							rsLog.getTimestamp("TIMESTAMP$$").getTime(),
							"", lastProcessedScn,"");
					struct.put("source", source);
					struct.put("before", keyStruct);
					if (!deleteOp) {
						struct.put("after", valueStruct);
					}
					struct.put("op", opType);
					struct.put("ts_ms", System.currentTimeMillis());
					final SourceRecord sourceRecord = new SourceRecord(
							(kafkaConnectTopic == null) ? null : sourcePartition,
							(kafkaConnectTopic == null) ? null : offset,
							kafkaConnectTopic,
							schema,
							struct);
					result.add(sourceRecord);
				} else if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
					final SourceRecord sourceRecord = new SourceRecord(
							(kafkaConnectTopic == null) ? null : sourcePartition,
							(kafkaConnectTopic == null) ? null : offset,
							kafkaConnectTopic,
							keySchema,
							keyStruct,
							deleteOp ? null : valueSchema,
							deleteOp ? null : valueStruct);
					sourceRecord.headers().addString("op", opType);
					result.add(sourceRecord);
				}
			}
		}
		rsLog.close();
		rsLog = null;
		// Perform deletion
		LOGGER.trace("Start of materialized view log cleaning.");
		for (RowId rowId : logRows2Delete) {
			stmtDeleteLog.setRowId(1, rowId);
			stmtDeleteLog.executeUpdate();
		}
		LOGGER.trace("End of materialized view log cleaning.");
		stmtLog.close(); stmtLog = null;
		stmtMaster.close(); stmtMaster = null;
		stmtDeleteLog.close(); stmtDeleteLog = null;
		LOGGER.trace("END: pollMVLog()");
		return result;
	}


	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(128);
		if (this.snapshotLog != null) {
			sb.append("\"");
			sb.append(this.tableOwner);
			sb.append("\".\"");
			sb.append(this.snapshotLog);
			sb.append("\" on ");
		}
		sb.append("\"");
		sb.append(this.tableOwner);
		sb.append("\".\"");
		sb.append(this.tableName);
		sb.append("\"");
		return sb.toString();
	}

	private void processPkColumns(final boolean deleteOp, ResultSet rsLog,
			final Struct keyStruct, final Struct valueStruct, PreparedStatement stmtMaster) throws SQLException {
		if (this.logWithPrimaryKey && this.logWithRowIds && !deleteOp)
			stmtMaster.setRowId(1, rsLog.getRowId(OraColumn.ROWID_KEY));
		Iterator<Entry<String, OraColumn>> iterator = pkColumns.entrySet().iterator();
		int bindNo = 1;
		while (iterator.hasNext()) {
			final OraColumn oraColumn = iterator.next().getValue();
			final String columnName = oraColumn.getColumnName();
			switch (oraColumn.getJdbcType()) {
			case Types.ROWID:
				if (!this.logWithPrimaryKey)
					keyStruct.put(columnName, rsLog.getRowId(columnName).toString());
				if (!deleteOp && this.logWithRowIds) {
					stmtMaster.setRowId(bindNo, rsLog.getRowId(columnName));
				}
				break;
			case Types.DATE:
				//TODO Timezone support!!!!
				keyStruct.put(columnName, rsLog.getDate(columnName));
				if (!deleteOp && !this.logWithRowIds)
					stmtMaster.setDate(bindNo, rsLog.getDate(columnName));
				break;
			case Types.TINYINT:
				keyStruct.put(columnName, rsLog.getByte(columnName));
				if (!deleteOp && !this.logWithRowIds)
					stmtMaster.setByte(bindNo, rsLog.getByte(columnName));
				break;
			case Types.SMALLINT:
				keyStruct.put(columnName, rsLog.getShort(columnName));
				if (!deleteOp && !this.logWithRowIds)
					stmtMaster.setShort(bindNo, rsLog.getShort(columnName));
				break;
			case Types.INTEGER:
				keyStruct.put(columnName, rsLog.getInt(columnName));
				if (!deleteOp && !this.logWithRowIds)
					stmtMaster.setInt(bindNo, rsLog.getInt(columnName));
				break;
			case Types.BIGINT:
				keyStruct.put(columnName, rsLog.getLong(columnName));
				if (!deleteOp && !this.logWithRowIds)
					stmtMaster.setLong(bindNo, rsLog.getLong(columnName));
				break;
			case Types.FLOAT:
				keyStruct.put(columnName, rsLog.getFloat(columnName));
				if (!deleteOp && !this.logWithRowIds)
					stmtMaster.setFloat(bindNo, rsLog.getFloat(columnName));
				break;
			case Types.DOUBLE:
				keyStruct.put(columnName, rsLog.getDouble(columnName));
				if (!deleteOp && !this.logWithRowIds)
					stmtMaster.setDouble(bindNo, rsLog.getDouble(columnName));
				break;
			case Types.DECIMAL:
				BigDecimal bdValue = rsLog.getBigDecimal(columnName).setScale(oraColumn.getDataScale());
				keyStruct.put(columnName, bdValue);
				if (!deleteOp && !this.logWithRowIds)
					stmtMaster.setBigDecimal(bindNo, bdValue);
				break;
			case Types.BINARY:
				keyStruct.put(columnName, rsLog.getBytes(columnName));
				if (!deleteOp && !this.logWithRowIds)
					stmtMaster.setBytes(bindNo, rsLog.getBytes(columnName));
				break;
			case Types.CHAR:
			case Types.VARCHAR:
				keyStruct.put(columnName, rsLog.getString(columnName));
				if (!deleteOp && !this.logWithRowIds)
					stmtMaster.setString(bindNo, rsLog.getString(columnName));
				break;
			case Types.NCHAR:
			case Types.NVARCHAR:
				keyStruct.put(columnName, rsLog.getNString(columnName));
				if (!deleteOp && !this.logWithRowIds)
					stmtMaster.setNString(bindNo, rsLog.getNString(columnName));
				break;
			case Types.TIMESTAMP:
				//TODO Timezone support!!!!
				keyStruct.put(columnName, rsLog.getTimestamp(columnName));
				if (!deleteOp && !this.logWithRowIds)
					stmtMaster.setTimestamp(bindNo, rsLog.getTimestamp(columnName));
				break;
			default:
				// Types.BLOB, Types.CLOB, Types.NCLOB - not possible!!! 
				keyStruct.put(columnName, columnName);
				if (!deleteOp && !this.logWithRowIds)
					stmtMaster.setString(bindNo, columnName);
				break;
			}
			if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM)
				valueStruct.put(columnName, keyStruct.get(columnName));
			bindNo++;
		}
	}

	private String nonExistentPk(ResultSet rsLog) throws SQLException {
		StringBuilder sbPrimaryKey = new StringBuilder(128);
		Iterator<Entry<String, OraColumn>> iterator = pkColumns.entrySet().iterator();
		int i = 0;
		while (iterator.hasNext()) {
			final OraColumn oraColumn = iterator.next().getValue();
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
			case Types.DECIMAL:
				sbPrimaryKey.append(rsLog.getBigDecimal(columnName));
				break;
			case Types.FLOAT:
				sbPrimaryKey.append(rsLog.getFloat(columnName));
				break;
			case Types.DOUBLE:
				sbPrimaryKey.append(rsLog.getDouble(columnName));
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
				// Types.BLOB, Types.CLOB, TYPES.NCLOB - not possible!!!
				sbPrimaryKey.append("'->");
				sbPrimaryKey.append(columnName);
				sbPrimaryKey.append("<-'");
				break;
			}
			i++;
		}
		return sbPrimaryKey.toString();
	}

}
