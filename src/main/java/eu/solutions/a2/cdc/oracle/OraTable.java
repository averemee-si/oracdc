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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.TargetDbSqlUtils;

public class OraTable {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraTable.class);

	private int batchSize;
	private final String tableOwner;
	private final String masterTable;
	private boolean logWithRowIds = false;
	private boolean logWithPrimaryKey = false;
	private boolean logWithSequence = false;

	private String masterTableSelSql;
	private String snapshotLog;
	private String snapshotLogSelSql;
	private String snapshotLogDelSql;
	private Map<String, String> sourcePartition;
	private final List<OraColumn> allColumns = new ArrayList<>();
	private final HashMap<String, OraColumn> pkColumns = new LinkedHashMap<>();
	private final int schemaType;
	private Schema schema;
	private Schema keySchema;
	private Schema valueSchema;

	private final SimpleDateFormat iso8601DateFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	private final SimpleDateFormat iso8601TimestampFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
	private boolean ready4Ops = false;

	private String sinkInsertSql = null;
	private String sinkUpdateSql = null;
	private String sinkDeleteSql = null;
	private PreparedStatement sinkInsert = null;
	private PreparedStatement sinkUpdate = null;
	private PreparedStatement sinkDelete = null;

	/**
	 * Constructor object based on snapshot log and master table
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
		LOGGER.trace("Creating OraTable object for materialized view log...");
		this.logWithRowIds = logWithRowIds;
		this.logWithPrimaryKey = logWithPrimaryKey;
		this.logWithSequence = logWithSequence;
		this.batchSize = batchSize;
		this.tableOwner = tableOwner;
		this.masterTable = masterTable;
		this.snapshotLog = snapshotLog;
		this.schemaType = schemaType;
		final String snapshotFqn = "\"" + this.tableOwner + "\"" + ".\"" + this.snapshotLog + "\"";
		this.snapshotLogDelSql = "delete from " + snapshotFqn + " where ROWID=?";
		this.sourcePartition = sourcePartition;

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Table owner -> {}, master table -> {}", this.tableOwner, this.masterTable);
			LOGGER.debug("\tMaterialized view log name -> {}", this.snapshotLog);
			LOGGER.debug("\t\tMView log with ROWID's -> {}, Primary Key -> {}, Sequence -> {}.",
					this.logWithRowIds, this.logWithPrimaryKey, this.logWithSequence);
			LOGGER.debug("batchSize -> {}", this.batchSize);
		}

		try (Connection connection = OraPoolConnectionFactory.getConnection()) {
			LOGGER.trace("Preparing column list and SQL statements for table {}.{}", this.tableOwner, this.masterTable);
			PreparedStatement statement = connection.prepareStatement(OraDictSqlTexts.COLUMN_LIST,
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			statement.setString(1, this.snapshotLog);
			statement.setString(2, this.tableOwner);
			statement.setString(3, this.masterTable);
			ResultSet rsColumns = statement.executeQuery();
			// Init for build SQL for master table select
			StringBuilder masterSelect = new StringBuilder(512);
			boolean masterFirstColumn = true;
			masterSelect.append("select ");
			StringBuilder masterWhere = new StringBuilder(256);
			if (this.logWithRowIds)
				// ROWID access is always faster that any other
				masterWhere.append("ROWID=?");
			// Init for build SQL for snapshot log select
			StringBuilder mViewSelect = new StringBuilder(256);
			boolean mViewFirstColumn = true;
			mViewSelect.append("select ");
			if (this.logWithRowIds) {
				// Add M_ROW$$ column for snapshot logs with ROWID
				LOGGER.trace("Adding {} to column list.", OraColumn.ROWID_KEY);
				mViewFirstColumn = false;
				mViewSelect.append("chartorowid(M_ROW$$) ");
				mViewSelect.append(OraColumn.ROWID_KEY);
			}

			final String tableNameWithOwner = this.tableOwner + "." + this.masterTable;
			// Schema init
			final SchemaBuilder keySchemaBuilder = SchemaBuilder
						.struct()
						.required()
						.name(tableNameWithOwner + ".Key")
						.version(1);
			final SchemaBuilder valueSchemaBuilder = SchemaBuilder
						.struct()
						.optional()
						.name(tableNameWithOwner + ".Value")
						.version(1);
			if (!this.logWithPrimaryKey && this.logWithRowIds) {
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
				OraColumn column = null;
				column = new OraColumn(rsColumns, keySchemaBuilder, valueSchemaBuilder, schemaType);
				allColumns.add(column);
				LOGGER.debug("New column {} added to table definition {}.", column.getColumnName(), this.masterTable);
				if (masterFirstColumn) {
					masterFirstColumn = false;
				} else {
					masterSelect.append(", ");
				}
				masterSelect.append("\"");
				masterSelect.append(column.getColumnName());
				masterSelect.append("\"");

				if (column.isPartOfPk()) {
					pkColumns.put(column.getColumnName(), column);
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
			rsColumns.close();
			rsColumns = null;
			statement.close();
			statement = null;
			// Schema
			keySchema = keySchemaBuilder.build(); 
			valueSchema = valueSchemaBuilder.build(); 
			if (this.schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
				SchemaBuilder schemaBuilder = SchemaBuilder
						.struct()
						.name(tableNameWithOwner + ".Envelope");
				schemaBuilder.field("op", Schema.STRING_SCHEMA);
				schemaBuilder.field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA);
				schemaBuilder.field("before", keySchema);
				schemaBuilder.field("after", valueSchema);
				schemaBuilder.field("source", OraRdbmsInfo.getInstance().getSchema());
				schema = schemaBuilder.build();
			}

			masterSelect.append(" from \"");
			masterSelect.append(this.tableOwner);
			masterSelect.append("\".\"");
			masterSelect.append(this.masterTable);
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
			LOGGER.trace("End of column list and SQL statements preparation for table {}.{}", this.tableOwner, this.masterTable);
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Table {}.{} -> MView select statement\n{}", this.tableOwner, this.masterTable, this.snapshotLogSelSql);
				LOGGER.debug("Table {}.{} -> MView delete statement\n{}", this.tableOwner, this.masterTable, this.snapshotLogDelSql);
				LOGGER.debug("Table {}.{} -> Master table select statement\n{}", this.tableOwner, this.masterTable, this.masterTableSelSql);
			}
		} catch (SQLException sqle) {
			LOGGER.error("Unable to get table information.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
		}
	}

	/**
	 * This constructor is used only for Sink connector
	 * 
	 * @param tableName
	 * @param record
	 * @param autoCreateTable
	 * @param schemaType
	 */
	public OraTable(
			final String tableName, final SinkRecord record, final boolean autoCreateTable, final int schemaType) {
		LOGGER.trace("Creating OraTable object from Kafka connect SinkRecord...");
		this.schemaType = schemaType;
		final List<Field> keyFields;
		final List<Field> valueFields;
		if (this.schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			LOGGER.debug("Schema type set to Kafka Connect.");
			// Not exist in Kafka Connect schema - setting it to dummy value
			this.tableOwner = "oracdc";
			this.masterTable = tableName;
			keyFields = record.keySchema().fields();
			valueFields = record.valueSchema().fields();
		} else {	// if (this.schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM)
			LOGGER.debug("Schema type set to Dbezium style.");
			Struct source = (Struct)((Struct) record.value()).get("source");
			this.tableOwner = source.getString("owner");
			if (tableName == null)
				this.masterTable = source.getString("table");
			else
				this.masterTable = tableName;
			keyFields = record.valueSchema().field("before").schema().fields();
			valueFields = record.valueSchema().field("after").schema().fields();
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("tableOwner = {}.", this.tableOwner);
			LOGGER.debug("masterTable = {}.", this.masterTable);
		}
		int pkColCount = 0;
		for (Field field : keyFields) {
			final OraColumn column = new OraColumn(field, true);
			pkColumns.put(column.getColumnName(), column);
			pkColCount++;
		}
		// Only non PK columns!!!
		for (Field field : valueFields) {
			if (!pkColumns.containsKey(field.name())) {
				final OraColumn column = new OraColumn(field, false);
				allColumns.add(column);
			}
		}
		prepareSql(pkColCount, autoCreateTable);
	}

	private void prepareSql(final int pkColCount, final boolean autoCreateTable) {
		// Prepare UPDATE/INSERT/DELETE statements...
		LOGGER.trace("Prepare UPDATE/INSERT/DELETE statements for table {}", this.masterTable);
		final StringBuilder sbDelUpdWhere = new StringBuilder(128);
		sbDelUpdWhere.append(" where ");

		final StringBuilder sbInsSql = new StringBuilder(256);
		sbInsSql.append("insert into ");
		sbInsSql.append(this.masterTable);
		sbInsSql.append("(");
		Iterator<Entry<String, OraColumn>> iterator = pkColumns.entrySet().iterator();
		int pkColumnNo = 0;
		while (iterator.hasNext()) {
			final String columnName = iterator.next().getValue().getColumnName();

			if (pkColumnNo > 0) {
				sbDelUpdWhere.append(" and ");
			}
			sbDelUpdWhere.append(columnName);
			sbDelUpdWhere.append("=?");

			sbInsSql.append(columnName);
			if (pkColumnNo < pkColCount - 1) {
				sbInsSql.append(",");
			}
			pkColumnNo++;
		}

		final StringBuilder sbUpdSql = new StringBuilder(256);
		sbUpdSql.append("update ");
		sbUpdSql.append(this.masterTable);
		sbUpdSql.append(" set ");
		final int nonPkColumnCount = allColumns.size();
		for (int i = 0; i < nonPkColumnCount; i++) {
			sbInsSql.append(",");
			sbInsSql.append(allColumns.get(i).getColumnName());

			sbUpdSql.append(allColumns.get(i).getColumnName());
			if (i < nonPkColumnCount - 1) {
				sbUpdSql.append("=?,");
			} else {
				sbUpdSql.append("=?");
			}
		}
		sbInsSql.append(") values(");
		final int totalColumns = nonPkColumnCount + pkColCount;
		for (int i = 0; i < totalColumns; i++) {
			if (i < totalColumns - 1) {
				sbInsSql.append("?,");
			} else {
				sbInsSql.append("?)");
			}
		}

		final StringBuilder sbDelSql = new StringBuilder(128);
		sbDelSql.append("delete from ");
		sbDelSql.append(this.masterTable);
		sbDelSql.append(sbDelUpdWhere);

		sbUpdSql.append(sbDelUpdWhere);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Table name -> {}, INSERT statement ->\n{}", this.masterTable, sbInsSql.toString());
			LOGGER.debug("Table name -> {}, UPDATE statement ->\n{}", this.masterTable, sbUpdSql.toString());
			LOGGER.debug("Table name -> {}, DELETE statement ->\n{}", this.masterTable, sbDelSql.toString());
		}

		// Check for table existence
		try (Connection connection = HikariPoolConnectionFactory.getConnection()) {
			LOGGER.trace("Check for table {} in database", this.masterTable);
			DatabaseMetaData metaData = connection.getMetaData();
			String tableName = masterTable;
			if (HikariPoolConnectionFactory.getDbType() == HikariPoolConnectionFactory.DB_TYPE_POSTGRESQL) {
				LOGGER.trace("Working with PostgreSQL specific lower case only names");
				// PostgreSQL specific...
				// Also look at https://stackoverflow.com/questions/43111996/why-postgresql-does-not-like-uppercase-table-names
				tableName = tableName.toLowerCase();
			}
			ResultSet resultSet = metaData.getTables(null, null, tableName, null);
			if (resultSet.next()) {
				LOGGER.trace("Table {} already exist.", tableName);
				ready4Ops = true;
			}
			resultSet.close();
			resultSet = null;
		} catch (SQLException sqle) {
			ready4Ops = false;
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
		}
		if (!ready4Ops && autoCreateTable) {
			// Create table in target database
			LOGGER.trace("Prepare to create table {}", this.masterTable);
			String createTableSqlText = TargetDbSqlUtils.createTableSql(
					this.masterTable, this.pkColumns, this.allColumns);
			LOGGER.debug("Create table with:\n{}", createTableSqlText);
			try (Connection connection = HikariPoolConnectionFactory.getConnection()) {
				Statement statement = connection.createStatement();
				statement.executeUpdate(createTableSqlText);
				connection.commit();
				ready4Ops = true;
			} catch (SQLException sqle) {
				ready4Ops = false;
				LOGGER.error("Create table failed! Failed creation statement:");
				LOGGER.error(createTableSqlText);
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			}
		}

		sinkInsertSql = sbInsSql.toString();
		sinkUpdateSql = sbUpdSql.toString();
		sinkDeleteSql = sbDelSql.toString();
		LOGGER.trace("End of SQL and DB preparation for table {}.", this.masterTable);
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
					LOGGER.error("Primary key = {} not found in {}.{}", nonExistentPk(rsLog), tableOwner, masterTable);
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
							tableOwner, masterTable, OraColumn.ORA_ROWSCN, lastProcessedScn);
					if (this.logWithSequence) {
						final long lastProcessedSequence = rsLog.getLong(OraColumn.MVLOG_SEQUENCE);
						offset.put(OraColumn.MVLOG_SEQUENCE, lastProcessedSequence);
						LOGGER.debug("Owner -> {}, table -> {}, last processed {} is {}.",
								tableOwner, masterTable, OraColumn.MVLOG_SEQUENCE, lastProcessedSequence);
					}
					LOGGER.trace("END: Prepare Kafka Connect offset");
				}

				if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
					final Struct struct = new Struct(schema);
					final Struct source = OraRdbmsInfo.getInstance().getStruct(
							null,
							null,
							tableOwner,
							masterTable,
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


	public String getMasterTable() {
		return masterTable;
	}

	public void putData(final Connection connection, final SinkRecord record) throws SQLException {
		LOGGER.trace("BEGIN: putData");
		String opType = "";
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			Iterator<Header> iterator = record.headers().iterator();
			while (iterator.hasNext()) {
				Header header = iterator.next();
				if ("op".equals(header.key())) {
					opType = (String) header.value();
					break;
				}
			}
			LOGGER.debug("Operation type set from headers to {}.", opType);
		} else { // if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM)
			opType = ((Struct) record.value()).getString("op");
			LOGGER.debug("Operation type set payload to {}.", opType);
		}
		switch (opType) {
		case "c":
			processInsert(connection, record);
			break;
		case "u":
			processUpdate(connection, record);
			break;
		case "d":
			processDelete(connection, record);
			break;
		default:
			LOGGER.error("Uncnown or null value for operation type '{}' received in header!", opType);
			if (record.value() == null)
				processDelete(connection, record);
			else
				processUpdate(connection, record);
		}
		LOGGER.trace("END: putData");
	}

	public void closeCursors() throws SQLException {
		LOGGER.trace("BEGIN: closeCursors()");
		if (sinkInsert != null) {
			sinkInsert.close();
			sinkInsert = null;
		}
		if (sinkUpdate != null) {
			sinkUpdate.close();
			sinkUpdate = null;
		}
		if (sinkDelete != null) {
			sinkDelete.close();
			sinkDelete = null;
		}
		LOGGER.trace("END: closeCursors()");
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
		sb.append(this.masterTable);
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
					if (oraColumn.isOracleDate()) {
						Date dateTsColumnValue = rsMaster.getDate(columnName);
						if (rsMaster.wasNull())
							valueStruct.put(columnName, null);
						else
							valueStruct.put(columnName, new Timestamp(dateTsColumnValue.getTime()));
					} else {
						Timestamp tsColumnValue = rsMaster.getTimestamp(columnName);
						if (rsMaster.wasNull())
							valueStruct.put(columnName, null);
						else
							valueStruct.put(columnName, tsColumnValue);
					}
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
									 tableOwner, masterTable, columnName);
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
									 tableOwner, masterTable, columnName);
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

	private void processInsert(
			final Connection connection, final SinkRecord record) throws SQLException {
		LOGGER.trace("BEGIN: processInsert()");
		final Struct keyStruct;
		final Struct valueStruct;
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			keyStruct = (Struct) record.key();
			valueStruct = (Struct) record.value();
		} else { // if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM)
			keyStruct = ((Struct) record.value()).getStruct("before");
			valueStruct = ((Struct) record.value()).getStruct("after");
		}
		if (sinkInsert == null) {
			sinkInsert = connection.prepareStatement(sinkInsertSql);
		}
		int columnNo = 1;
		Iterator<Entry<String, OraColumn>> iterator = pkColumns.entrySet().iterator();
		while (iterator.hasNext()) {
			final OraColumn oraColumn = iterator.next().getValue();
			oraColumn.bindWithPrepStmt(sinkInsert, columnNo, keyStruct.get(oraColumn.getColumnName()));
			columnNo++;
		}
		for (int i = 0; i < allColumns.size(); i++) {
			final OraColumn oraColumn = allColumns.get(i);
			if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD ||
					(schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM && !oraColumn.isPartOfPk())) {
				oraColumn.bindWithPrepStmt(sinkInsert, columnNo, valueStruct.get(oraColumn.getColumnName()));
				columnNo++;
			}
		}
		sinkInsert.executeUpdate();
		LOGGER.trace("END: processInsert()");
	}

	private void processUpdate(
			final Connection connection, final SinkRecord record) throws SQLException {
		LOGGER.trace("BEGIN: processUpdate()");
		final Struct keyStruct;
		final Struct valueStruct;
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			keyStruct = (Struct) record.key();
			valueStruct = (Struct) record.value();
		} else { // if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM)
			keyStruct = ((Struct) record.value()).getStruct("before");
			valueStruct = ((Struct) record.value()).getStruct("after");
		}
		if (sinkUpdate == null) {
			sinkUpdate = connection.prepareStatement(sinkUpdateSql);
		}
		int columnNo = 1;
		for (int i = 0; i < allColumns.size(); i++) {
			final OraColumn oraColumn = allColumns.get(i);
			if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD ||
					(schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM && !oraColumn.isPartOfPk())) {
				oraColumn.bindWithPrepStmt(sinkUpdate, columnNo, valueStruct.get(oraColumn.getColumnName()));
				columnNo++;
			}
		}
		Iterator<Entry<String, OraColumn>> iterator = pkColumns.entrySet().iterator();
		while (iterator.hasNext()) {
			final OraColumn oraColumn = iterator.next().getValue();
			oraColumn.bindWithPrepStmt(sinkUpdate, columnNo, keyStruct.get(oraColumn.getColumnName()));
			columnNo++;
		}
		final int recordCount = sinkUpdate.executeUpdate();
		if (recordCount == 0) {
			LOGGER.warn("Primary key not found, executing insert");
			processInsert(connection, record);
		}
		LOGGER.trace("END: processUpdate()");
	}

	private void processDelete(
			final Connection connection, final SinkRecord record) throws SQLException {
		LOGGER.trace("BEGIN: processDelete()");
		final Struct keyStruct;
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			keyStruct = (Struct) record.key();
		} else { // if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM)
			keyStruct = ((Struct) record.value()).getStruct("before");
		}
		if (sinkDelete == null) {
			sinkDelete = connection.prepareStatement(sinkDeleteSql);
		}
		Iterator<Entry<String, OraColumn>> iterator = pkColumns.entrySet().iterator();
		int columnNo = 1;
		while (iterator.hasNext()) {
			final OraColumn oraColumn = iterator.next().getValue();
			oraColumn.bindWithPrepStmt(sinkDelete, columnNo, keyStruct.get(oraColumn.getColumnName()));
			columnNo++;
		}
		sinkDelete.executeUpdate();
		LOGGER.trace("END: processDelete()");
	}

}
