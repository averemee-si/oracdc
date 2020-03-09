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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

/**
 * 
 * @author averemee
 *
 */
public class OraTable extends OraTableDefinition {

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

	private Map<String, OraColumn> idToNameMap;
	private OraCdcSqlRedoParser parser;

	private Schema schema;
	private Schema keySchema;
	private Schema valueSchema;

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
			buildColumnList(true, null, rsColumns, sourceOffset, null);
			rsColumns.close(); rsColumns = null;
			statement.close(); statement = null;
		} catch (SQLException sqle) {
			LOGGER.error("Unable to get table information.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
		}
	}

	/**
	 * 
	 * This constructor is used in LogMiner worker thread
	 * 
	 * @param pdbName
	 * @param conId
	 * @param tableOwner
	 * @param tableName
	 * @param schemaType
	 * @param isCdb
	 * @param odd
	 * @param sourcePartition
	 * @param kafkaTopic
	 */
	public OraTable(
			final String pdbName, final Short conId, final String tableOwner,
			final String tableName, final int schemaType, final boolean isCdb,
			final OraDumpDecoder odd, final Map<String, String> sourcePartition,
			final String kafkaTopic) {
		super(tableOwner, tableName, schemaType);
		LOGGER.trace("BEGIN: Creating OraTable object from LogMiner data...");
		this.sourcePartition = sourcePartition;
		final String tableFqn = ((pdbName == null) ? "" : pdbName + ":") +
				this.tableOwner + "." + this.tableName;
		boolean tableWithPk = true;
		try (Connection connection = OraPoolConnectionFactory.getConnection()) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("Preparing column list and mining SQL statements for table {}.", tableFqn);
			}
			// Detect PK column list...
			Set<String> pkColumns = OraRdbmsInfo.getPkColumnsFromDict(connection,
					isCdb ? conId : null, this.tableOwner, this.tableName);
			if (pkColumns == null) {
				tableWithPk = false;
			}
			PreparedStatement statement = connection.prepareStatement(
					isCdb ? OraDictSqlTexts.COLUMN_LIST_CDB : OraDictSqlTexts.COLUMN_LIST_PLAIN,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.setString(1, this.tableOwner);
			statement.setString(2, this.tableName);
			if (isCdb) {
				statement.setShort(3, conId);
			}

			ResultSet rsColumns = statement.executeQuery();
			buildColumnList(false, pdbName, rsColumns, null, pkColumns);
			rsColumns.close(); rsColumns = null;
			statement.close(); statement = null;
		} catch (SQLException sqle) {
			LOGGER.error("Unable to get table information.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
		}
		parser = new OraCdcSqlRedoParser(
				pdbName, this.tableOwner, this.tableName, tableWithPk,
				schemaType, schema, keySchema, valueSchema,
				odd, pkColumns, idToNameMap,
				kafkaTopic, sourcePartition);
		LOGGER.trace("END: Creating OraTable object from LogMiner data...");
	}

	private void buildColumnList(final boolean mviewSource, final String pdbName, final ResultSet rsColumns, 
			final Map<String, Object> sourceOffset, final Set<String> pkColsSet) throws SQLException {
		final String snapshotFqn;
		final StringBuilder mViewSelect;
		final StringBuilder masterSelect;
		final StringBuilder masterWhere;
		boolean mViewFirstColumn = true;
		boolean masterFirstColumn = true;

		if (mviewSource) {
			snapshotFqn = "\"" + this.tableOwner + "\"" + ".\"" + this.snapshotLog + "\"";
			this.snapshotLogDelSql = "delete from " + snapshotFqn + " where ROWID=?";
			// Init for build SQL for master table select
			masterSelect = new StringBuilder(512);
			masterSelect.append("select ");
			masterWhere = new StringBuilder(256);
			// Init for build SQL for snapshot log select
			mViewSelect = new StringBuilder(256);
			mViewSelect.append("select ");
			if (this.logWithRowIds) {
				// ROWID access is always faster that any other
				masterWhere.append("ROWID=?");
				// Add M_ROW$$ column for snapshot logs with ROWID
				LOGGER.trace("Adding {} to column list.", OraColumn.ROWID_KEY);
				mViewFirstColumn = false;
				mViewSelect.append("chartorowid(M_ROW$$) ");
				mViewSelect.append(OraColumn.ROWID_KEY);
			}
		} else {
			mViewSelect = null;
			masterSelect = null;
			masterWhere = null;
			snapshotFqn = null;
			idToNameMap = new HashMap<>();
		}

		final String tableFqn = ((pdbName == null) ? "" : pdbName + ":") +
				this.tableOwner + "." + this.tableName;
		// Schema init
		final SchemaBuilder keySchemaBuilder = SchemaBuilder
					.struct()
					.required()
					.name(tableFqn + ".Key")
					.version(1);
		final SchemaBuilder valueSchemaBuilder = SchemaBuilder
					.struct()
					.optional()
					.name(tableFqn + ".Value")
					.version(1);
		// Substitute missing primary key with ROWID value
		if ((mviewSource && (!this.logWithPrimaryKey && this.logWithRowIds)) ||
				(!mviewSource && pkColsSet == null)) {
			// Add ROWID (ORA$ROWID) - this column is not in dictionary!!!
			OraColumn rowIdColumn = OraColumn.getRowIdKey();
			allColumns.add(rowIdColumn);
			pkColumns.put(rowIdColumn.getColumnName(), rowIdColumn);
			keySchemaBuilder.field(rowIdColumn.getColumnName(), Schema.STRING_SCHEMA);
			if (this.schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
				valueSchemaBuilder.field(rowIdColumn.getColumnName(), Schema.STRING_SCHEMA);
			}
		}

		while (rsColumns .next()) {
			final OraColumn column = new OraColumn(
					mviewSource, rsColumns, keySchemaBuilder, valueSchemaBuilder, schemaType, pkColsSet);
			allColumns.add(column);
			LOGGER.debug("New column {} added to table definition {}.", column.getColumnName(), tableFqn);
			if (mviewSource) {
				if (masterFirstColumn) {
					masterFirstColumn = false;
				} else {
					masterSelect.append(", ");
				}
				masterSelect.append("\"");
				masterSelect.append(column.getColumnName());
				masterSelect.append("\"");
			} else {
				idToNameMap.put(column.getNameFromId(), column);
			}

			if (column.isPartOfPk()) {
				pkColumns.put(column.getColumnName(), column);
				if (mviewSource) {
					if (mViewFirstColumn) {
						mViewFirstColumn = false;
					} else {
						mViewSelect.append(", ");
						if (!this.logWithRowIds)
							// We need this only when snapshot log don't contains M_ROW$$ 
							masterWhere.append(" and ");
					}
					mViewSelect.append("\"");
					mViewSelect.append(column.getColumnName());
					mViewSelect.append("\"");
					if (!this.logWithRowIds) {
						// We need this only when snapshot log don't contains M_ROW$$ 
						masterWhere.append("\"");
						masterWhere.append(column.getColumnName());
						masterWhere.append("\"=?");
					}
				}
			}
		}
		// Schema
		keySchema = keySchemaBuilder.build(); 
		valueSchema = valueSchemaBuilder.build(); 
		if (this.schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
			SchemaBuilder schemaBuilder = SchemaBuilder
					.struct()
					.name(tableFqn + ".Envelope");
			schemaBuilder.field("op", Schema.STRING_SCHEMA);
			schemaBuilder.field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA);
			schemaBuilder.field("before", keySchema);
			schemaBuilder.field("after", valueSchema);
			schemaBuilder.field("source", OraRdbmsInfo.getInstance().getSchema());
			schema = schemaBuilder.build();
		}

		if (mviewSource) {
			masterSelect.append(" from \"");
			masterSelect.append(this.tableOwner);
			masterSelect.append("\".\"");
			masterSelect.append(this.tableName);
			masterSelect.append("\" where ");
			masterSelect.append(masterWhere);
			this.masterTableSelSql = masterSelect.toString();

			if (this.logWithSequence) {
				mViewSelect.append(", ");
				mViewSelect.append(OraColumn.MVLOG_SEQUENCE);
			}
			mViewSelect.append(", case DMLTYPE$$ when 'I' then 'c' when 'U' then 'u' else 'd' end as OPTYPE$$, ORA_ROWSCN, SYSTIMESTAMP at time zone 'GMT' as TIMESTAMP$$, ROWID from ");
			mViewSelect.append(snapshotFqn);
			if (this.logWithSequence) {
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
			this.snapshotLogSelSql = mViewSelect.toString();
			LOGGER.trace("End of column list and SQL statements preparation for table {}.{}", this.tableOwner, this.tableName);
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Table {} -> MView select statement\n{}", tableFqn, this.snapshotLogSelSql);
				LOGGER.debug("Table {} -> MView delete statement\n{}", tableFqn, this.tableName, this.snapshotLogDelSql);
				LOGGER.debug("Table {} -> Master table select statement\n{}", tableFqn, this.masterTableSelSql);
			}
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
				ResultSet rsMaster = stmtMaster.executeQuery();
				// We're working with PK
				if (rsMaster.next()) {
					processAllColumns(rsMaster, valueStruct);
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
							rsLog.getTimestamp("TIMESTAMP$$").getTime());
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
				// Types.BLOB, Types.CLOB - not possible!!! 
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

	private void processAllColumns(
			ResultSet rsMaster, final Struct valueStruct) throws SQLException {
		for (int i = 0; i < allColumns.size(); i++) {
			final OraColumn oraColumn = allColumns.get(i);
			final String columnName = oraColumn.getColumnName();
			// Don't process PK again in case of SCHEMA_TYPE_INT_KAFKA_STD
			if ((schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD && !pkColumns.containsKey(columnName)) ||
					schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
				switch (oraColumn.getJdbcType()) {
				//case Types.DATE:
				//We always use Timestamps
				case Types.TINYINT:
					final byte byteColumnValue = rsMaster.getByte(columnName);
					if (rsMaster.wasNull())
						valueStruct.put(columnName, null);
					else
						valueStruct.put(columnName, byteColumnValue);									
					break;
				case Types.SMALLINT:
					final short shortColumnValue = rsMaster.getShort(columnName); 
					if (rsMaster.wasNull())
						valueStruct.put(columnName, null);
					else
						valueStruct.put(columnName, shortColumnValue);
					break;
				case Types.INTEGER:
					final int intColumnValue = rsMaster.getInt(columnName); 
					if (rsMaster.wasNull())
						valueStruct.put(columnName, null);
					else
						valueStruct.put(columnName, intColumnValue);
					break;
				case Types.BIGINT:
					final long longColumnValue = rsMaster.getLong(columnName); 
					if (rsMaster.wasNull())
						valueStruct.put(columnName, null);
					else
						valueStruct.put(columnName, longColumnValue);
					break;
				case Types.BINARY:
					final byte[] binaryColumnValue = rsMaster.getBytes(columnName);
					if (rsMaster.wasNull())
						valueStruct.put(columnName, null);
					else
						valueStruct.put(columnName, binaryColumnValue);
					break;
				case Types.CHAR:
				case Types.VARCHAR:
					final String charColumnValue = rsMaster.getString(columnName); 
					if (rsMaster.wasNull())
						valueStruct.put(columnName, null);
					else
						valueStruct.put(columnName, charColumnValue);
					break;
				case Types.NCHAR:
				case Types.NVARCHAR:
					final String nCharColumnValue = rsMaster.getNString(columnName); 
					if (rsMaster.wasNull())
						valueStruct.put(columnName, null);
					else
						valueStruct.put(columnName, nCharColumnValue);
					break;
				case Types.TIMESTAMP:
					//TODO Timezone support!!!!
					final Timestamp tsColumnValue = rsMaster.getTimestamp(columnName);
					if (rsMaster.wasNull())
						valueStruct.put(columnName, null);
					else
						valueStruct.put(columnName, tsColumnValue);
					break;
				case Types.FLOAT:
					final float floatColumnValue = rsMaster.getFloat(columnName); 
					if (rsMaster.wasNull())
						valueStruct.put(columnName, null);
					else
						valueStruct.put(columnName, floatColumnValue);
					break;
				case Types.DOUBLE:
					final double doubleColumnValue = rsMaster.getDouble(columnName); 
					if (rsMaster.wasNull())
						valueStruct.put(columnName, null);
					else
						valueStruct.put(columnName, doubleColumnValue);
					break;
				case Types.DECIMAL:
					final BigDecimal bdColumnValue = rsMaster.getBigDecimal(columnName); 
					if (rsMaster.wasNull())
						valueStruct.put(columnName, null);
					else
						valueStruct.put(columnName, bdColumnValue.setScale(oraColumn.getDataScale()));
					break;
				case Types.BLOB:
					final Blob blobColumnValue = rsMaster.getBlob(columnName);
					if (rsMaster.wasNull() || blobColumnValue.length() < 1) {
						valueStruct.put(columnName, null);
					} else {
						try (InputStream is = blobColumnValue.getBinaryStream();
							ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
							final byte[] data = new byte[16384];
							int bytesRead;
							while ((bytesRead = is.read(data, 0, data.length)) != -1) {
								baos.write(data, 0, bytesRead);
							}
							valueStruct.put(columnName, baos.toByteArray());
						} catch (IOException ioe) {
							LOGGER.error("IO Error while processing BLOB column {}.{}({})", 
									 tableOwner, tableName, columnName);
							LOGGER.error("\twhile executing\n\t\t{}", masterTableSelSql);
							LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
						}
					}
					break;
				case Types.CLOB:
					final Clob clobColumnValue = rsMaster.getClob(columnName);
					if (rsMaster.wasNull() || clobColumnValue.length() < 1) {
						valueStruct.put(columnName, null);
					} else {
						try (Reader reader = clobColumnValue.getCharacterStream()) {
							final char[] data = new char[8192];
							StringBuilder sbClob = new StringBuilder(8192);
							int charsRead;
							while ((charsRead = reader.read(data, 0, data.length)) != -1) {
								sbClob.append(data, 0, charsRead);
							}
							valueStruct.put(columnName, sbClob.toString());
						} catch (IOException ioe) {
							LOGGER.error("IO Error while processing CLOB column {}.{}({})", 
									 tableOwner, tableName, columnName);
							LOGGER.error("\twhile executing\n\t\t{}", masterTableSelSql);
							LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
						}
					}
					break;
				default:
					break;
				}
			}
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
				// Types.BLOB, Types.CLOB - not possible!!!
				sbPrimaryKey.append("'->");
				sbPrimaryKey.append(columnName);
				sbPrimaryKey.append("<-'");
				break;
			}
			i++;
		}
		return sbPrimaryKey.toString();
	}

	public SourceRecord parseRedoRecord(OraCdcLogMinerStatement stmt) throws SQLException {
		return parser.parseRedoRecord(stmt);
	}

}
