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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.jmx.OraCdcSinkTableInfo;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.TargetDbSqlUtils;


/**
 * 
 * @author averemee
 *
 */
public class OraTable4SinkConnector extends OraTableDefinition {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraTable4SinkConnector.class);

	private final int dbType;
	private final OraCdcSinkTableInfo metrics;
	private boolean ready4Ops = false;
	private String sinkUpsertSql = null;
	private String sinkDeleteSql = null;
	private PreparedStatement sinkUpsert = null;
	private PreparedStatement sinkDelete = null;
	private int upsertCount;
	private int deleteCount;


	/**
	 * This constructor is used only for Sink connector
	 * 
	 * @param tableName
	 * @param record
	 * @param autoCreateTable
	 * @param schemaType
	 * @throws SQLException 
	 */
	public OraTable4SinkConnector(
			final String tableName, final SinkRecord record, final boolean autoCreateTable, final int schemaType) throws SQLException {
		super(schemaType);
		dbType = HikariPoolConnectionFactory.getDbType();
		LOGGER.trace("Creating OraTable object from Kafka connect SinkRecord...");
		final List<Field> keyFields;
		final List<Field> valueFields;
		if (this.schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			LOGGER.debug("Schema type set to Kafka Connect.");
			this.tableOwner = "oracdc";
			this.tableName = tableName;
			keyFields = record.keySchema().fields();
			valueFields = record.valueSchema().fields();
		} else {	// if (this.schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM)
			LOGGER.debug("Schema type set to Dbezium style.");
			Struct source = (Struct)((Struct) record.value()).get("source");
			this.tableOwner = source.getString("owner");
			if (tableName == null) {
				this.tableName = source.getString("table");
			} else {
				this.tableName = tableName;
			}
			keyFields = record.valueSchema().field("before").schema().fields();
			valueFields = record.valueSchema().field("after").schema().fields();
		}
		LOGGER.debug("tableOwner = {}, tableName = {}.", this.tableOwner, this.tableName);
		for (Field field : keyFields) {
			final OraColumn column = new OraColumn(field, true);
			pkColumns.put(column.getColumnName(), column);
		}
		// Only non PK columns!!!
		for (Field field : valueFields) {
			if (!pkColumns.containsKey(field.name())) {
				final OraColumn column = new OraColumn(field, false);
				allColumns.add(column);
			}
		}
		LOGGER.debug("Create JMX objects...");
		metrics = new OraCdcSinkTableInfo(this.tableName);
		prepareSql(autoCreateTable);
		upsertCount = 0;
		deleteCount = 0;
	}


	private void prepareSql(final boolean autoCreateTable) throws SQLException {
		// Prepare UPDATE/INSERT/DELETE statements...
		LOGGER.trace("Prepare UPDATE/INSERT/DELETE statements for table {}", this.tableName);
		final List<String> sqlTexts = TargetDbSqlUtils.generateSinkSql(
				this.tableName, this.dbType, this.pkColumns, this.allColumns);
		sinkUpsertSql = sqlTexts.get(TargetDbSqlUtils.INSERT);
		sinkDeleteSql = sqlTexts.get(TargetDbSqlUtils.DELETE);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Table name -> {}, UPSERT statement ->\n{}", this.tableName, sinkUpsertSql);
			LOGGER.debug("Table name -> {}, DELETE statement ->\n{}", this.tableName, sinkDeleteSql);
		}

		// Check for table existence
		try (Connection connection = HikariPoolConnectionFactory.getConnection()) {
			LOGGER.trace("Check for table {} in database", this.tableName);
			DatabaseMetaData metaData = connection.getMetaData();
			String tableNameCaseConv = tableName;
			if (dbType == HikariPoolConnectionFactory.DB_TYPE_POSTGRESQL) {
				LOGGER.trace("Working with PostgreSQL specific lower case only names");
				// PostgreSQL specific...
				// Also look at https://stackoverflow.com/questions/43111996/why-postgresql-does-not-like-uppercase-table-names
				tableNameCaseConv = tableName.toLowerCase();
			}
			ResultSet resultSet = metaData.getTables(null, null, tableNameCaseConv, null);
			if (resultSet.next()) {
				LOGGER.trace("Table {} already exist.", tableNameCaseConv);
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
			LOGGER.trace("Prepare to create table {}", this.tableName);
			String createTableSqlText = TargetDbSqlUtils.createTableSql(
					this.tableName, this.dbType, this.pkColumns, this.allColumns);
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
				throw new SQLException(sqle);
			}
		}
		LOGGER.trace("End of SQL and DB preparation for table {}.", this.tableName);
	}

	public String getTableFqn() {
		return tableOwner + "." + tableName;
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
		final long nanosStart = System.nanoTime();
		if ("d".equals(opType)) {
			processDelete(connection, record);			
			metrics.addDelete(System.nanoTime() - nanosStart);
		} else {
			processUpsert(connection, record);
			metrics.addUpsert(System.nanoTime() - nanosStart);
		}
		LOGGER.trace("END: putData");
	}

	public void exec() throws SQLException {
		LOGGER.trace("BEGIN: exec()");
		final long nanosStart = System.nanoTime();
		if (sinkUpsert != null && upsertCount > 0) {
			sinkUpsert.executeBatch();
			sinkUpsert.clearBatch();
			upsertCount = 0;
			metrics.addUpsertExec(System.nanoTime() - nanosStart);
		}
		if (sinkDelete != null && deleteCount > 0) {
			sinkDelete.executeBatch();
			sinkDelete.clearBatch();
			deleteCount = 0;
			metrics.addDeleteExec(System.nanoTime() - nanosStart);
		}
		LOGGER.trace("END: exec()");
	}

	public void execAndCloseCursors() throws SQLException {
		LOGGER.trace("BEGIN: closeCursors()");
		final long nanosStart = System.nanoTime();
		if (sinkUpsert != null) {
			if (upsertCount > 0) {
				sinkUpsert.executeBatch();
				upsertCount = 0;
			}
			sinkUpsert.close();
			sinkUpsert = null;
			metrics.addUpsertExec(System.nanoTime() - nanosStart);
		}
		if (sinkDelete != null) {
			if (deleteCount > 0) {
				sinkDelete.executeBatch();
				deleteCount = 0;
			}
			sinkDelete.close();
			sinkDelete = null;
			metrics.addDeleteExec(System.nanoTime() - nanosStart);
		}
		LOGGER.trace("END: closeCursors()");
	}

	private void processUpsert(
			final Connection connection, final SinkRecord record) throws SQLException {
		LOGGER.trace("BEGIN: processUpsert()");
		final Struct keyStruct;
		final Struct valueStruct;
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			keyStruct = (Struct) record.key();
			valueStruct = (Struct) record.value();
		} else { // if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM)
			keyStruct = ((Struct) record.value()).getStruct("before");
			valueStruct = ((Struct) record.value()).getStruct("after");
		}
		if (sinkUpsert == null) {
			sinkUpsert = connection.prepareStatement(sinkUpsertSql);
		}
		int columnNo = 1;
		Iterator<Entry<String, OraColumn>> iterator = pkColumns.entrySet().iterator();
		while (iterator.hasNext()) {
			final OraColumn oraColumn = iterator.next().getValue();
			oraColumn.bindWithPrepStmt(sinkUpsert, columnNo, keyStruct.get(oraColumn.getColumnName()));
			columnNo++;
		}
		for (int i = 0; i < allColumns.size(); i++) {
			final OraColumn oraColumn = allColumns.get(i);
			if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD ||
					(schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM && !oraColumn.isPartOfPk())) {
				oraColumn.bindWithPrepStmt(sinkUpsert, columnNo, valueStruct.get(oraColumn.getColumnName()));
				columnNo++;
			}
		}
		sinkUpsert.addBatch();
		upsertCount++;
		LOGGER.trace("END: processUpsert()");
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
		sinkDelete.addBatch();
		deleteCount++;
		LOGGER.trace("END: processDelete()");
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(128);
		sb.append("\"");
		sb.append(this.tableOwner);
		sb.append("\".\"");
		sb.append(this.tableName);
		sb.append("\"");
		return sb.toString();
	}

}
