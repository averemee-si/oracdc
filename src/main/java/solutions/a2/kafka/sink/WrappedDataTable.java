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

package solutions.a2.kafka.sink;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.data.OraBlob;
import solutions.a2.cdc.oracle.data.OraClob;
import solutions.a2.cdc.oracle.data.OraJson;
import solutions.a2.cdc.oracle.data.OraNClob;
import solutions.a2.cdc.oracle.data.OraVector;
import solutions.a2.cdc.oracle.data.OraXml;
import solutions.a2.kafka.sink.jmx.WrappedTableInfo;

import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.SQLXML;
import static oracle.jdbc.OracleTypes.JSON;
import static oracle.jdbc.OracleTypes.VECTOR;
import static solutions.a2.cdc.oracle.data.JdbcTypes.getTypeName;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_PREFIX;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_DEBEZIUM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_KAFKA_STD;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_SINGLE;
import static solutions.a2.kafka.sink.JdbcSinkConnectionPool.DB_TYPE_MYSQL;
import static solutions.a2.kafka.sink.JdbcSinkConnectionPool.DB_TYPE_ORACLE;
import static solutions.a2.kafka.sink.JdbcSinkConnectionPool.DB_TYPE_POSTGRESQL;
import static solutions.a2.kafka.sink.JdbcSinkConnectorConfig.CONNECTOR_REPLICATE;
import static solutions.a2.kafka.sink.JdbcSinkConnectorConfig.CONNECTOR_AUDIT_TRAIL;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class WrappedDataTable extends JdbcSinkTableBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(WrappedDataTable.class);

	private final WrappedTableInfo metrics;
	private String sinkInsertSql = null;
	private String sinkDeleteSql = null;
	private PreparedStatement sinkInsert = null;
	private PreparedStatement sinkDelete = null;
	private int insertCount;
	private int deleteCount;
	private long insertTime;
	private long deleteTime;
	private final int pkStringLength;
	private final Set<String> pkInUpsertBatch = new HashSet<>();
	private String updateStmtStart;
	private String updateStmtWhere;
	private final Map<Integer, UpdateStatement> updateStatements = new HashMap<>();
	

	public WrappedDataTable(final JdbcSinkConnectionPool sinkPool,
			final String tableName, final SinkRecord record,
			final JdbcSinkConnectorConfig config) throws SQLException {
		super(config.getSchemaType(), sinkPool.getDbType());
		LOGGER.trace("Creating WrappedDataTable object from Kafka connect SinkRecord...");
		pkStringLength = config.getPkStringLength();
		connectorMode = config.getConnectorMode();

		//TODO
		if (schemaType == SCHEMA_TYPE_INT_DEBEZIUM) {
			LOGGER.debug("Schema type set to Debezium style.");
			Struct source = (Struct)((Struct) record.value()).get("source");
			this.tableOwner = source.getString("owner");
			if (tableName == null) {
				this.tableName = source.getString("table");
			} else {
				this.tableName = tableName;
			}
		} else {
			// ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD
			// ParamConstants.SCHEMA_TYPE_INT_SINGLE
			LOGGER.debug("Schema type set to Kafka Connect.");
			this.tableOwner = "oracdc";
			this.tableName = tableName;
		}
		updateStmtStart = "update " + this.tableName + " set ";
		if (dbType == DB_TYPE_POSTGRESQL) {
			LOGGER.debug("Working with PostgreSQL specific lower case only names");
			// PostgreSQL specific...
			// Also look at https://stackoverflow.com/questions/43111996/why-postgresql-does-not-like-uppercase-table-names
			tableNameCaseConv = tableName.toLowerCase();
		} else {
			tableNameCaseConv = tableName;
		}
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("tableOwner = {}, tableName = {}.", this.tableOwner, this.tableName);

		try (Connection connection = sinkPool.getConnection()) {
			final Entry<Set<String>, ResultSet> tableMetadata = checkPresence(connection);
			if (exists) {
				prepareSql(connection, record, tableMetadata);
			} else {
				if (config.autoCreateTable()) {
					LOGGER.info(
							"""
							
							=====================
							Table '{}' will be created in the target database.
							=====================
							
							""", tableNameCaseConv);
					// Create table in target database
					createTable(connection, record, pkStringLength);
					prepareSql();
				} else {
					LOGGER.error(
							"""
							
							=====================
							Table '{}' does not exist in the target database and a2.autocreate=false!
							=====================
							
							""", tableNameCaseConv);
					throw new ConnectException("Table does not exists!");
				}
			}
		} catch (SQLException sqle) {
			throw new ConnectException(sqle);
		}

		metrics = new WrappedTableInfo(this.tableName);
		insertCount = 0;
		deleteCount = 0;
		insertTime = 0;
		deleteTime = 0;
	}


	private void prepareSql(final Connection connection, final SinkRecord record, final Entry<Set<String>, ResultSet> tableMetadata) throws SQLException {

		final Entry<List<Field>, List<Field>> keyValue = getFieldsFromSinkRecord(record);
		final List<Field> keyFields = keyValue.getKey();
		final List<Field> valueFields = keyValue.getValue();
		final Map<String, Field> allFields = new HashMap<>();
		keyFields.forEach(f -> allFields.put(f.name(), f));
		valueFields.forEach(f -> allFields.put(f.name(), f));
		//TODO - currently - case insensitive columns/fields (((
		final Map<String, Field> topicKeys = new HashMap<>();
		keyFields.forEach(f -> topicKeys.put(StringUtils.upperCase(f.name()), f));
		final Map<String, Field> topicValues = new HashMap<>();
		final Map<String, Field> unnestedValues = new HashMap<>();
		final Map<String, String> unnestedParents = new HashMap<>();
		final Map<String, List<Field>> unnestedColumns = new HashMap<>();
		valueFields.forEach(f -> {
			final var fieldName = StringUtils.upperCase(f.name());
			final var fieldSchema = f.schema().type().getName();
			if (Strings.CS.equals("struct", fieldSchema)) {
				if (Strings.CS.startsWith(f.schema().name(), WRAPPED_PREFIX)) {
					topicValues.put(fieldName, f);
				} else if (!Strings.CS.startsWithAny(f.schema().name(),
								OraBlob.LOGICAL_NAME,
								OraClob.LOGICAL_NAME,
								OraNClob.LOGICAL_NAME,
								OraXml.LOGICAL_NAME,
								OraJson.LOGICAL_NAME,
								OraVector.LOGICAL_NAME)) {
					for (Field unnestField : f.schema().fields()) {
						final String unnestFieldName = StringUtils.upperCase(unnestField.name());
						unnestedValues.put(unnestFieldName, unnestField);
						unnestedParents.put(unnestFieldName, fieldName);
					}
				}
			} else {
				topicValues.put(fieldName, f);
			}
		});
	
		if (!onlyValue) {
			// pkColumns may contain values
			pkColumns.clear();
			for (final String dbPkColumn : tableMetadata.getKey()) {
				final boolean isKey;
				final Field pkField;
				//TODO - case sensitive!
				final String dbPkColumn4M = StringUtils.upperCase(dbPkColumn);
				if (topicKeys.containsKey(dbPkColumn4M)) {
					isKey = true;
					pkField = topicKeys.get(dbPkColumn4M);
				} else if (topicValues.containsKey(dbPkColumn4M)) {
					isKey = false;
					pkField = topicValues.get(dbPkColumn4M);
				} else {
					throw new ConnectException("Database primary key column '" +
							tableName + "." + dbPkColumn + "' is not present in Kafka topic " +
							record.topic() + "!");
				}
				//TODO - currently JDBCType only from Kafka Topic!!!
				final var column = new JdbcSinkColumn(pkField, true);
				pkColumns.put(column.getColumnName(), column);
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Primary key column {}.{} from primary key {} is mapped to {} STRUCT.",
							dbPkColumn, tableName, dbPkColumn, (isKey ? "key" : "value"));
				}
			}
		}

		boolean unnestingRequired = false;
		final ResultSet rsAllColumns = tableMetadata.getValue();
		while (rsAllColumns.next()) {
			final Field valueField;
			final String dbValueColumn = rsAllColumns.getString("COLUMN_NAME");
			//TODO - case sensitive!
			final String dbValueColumn4M = StringUtils.upperCase(dbValueColumn);
			if (allFields.remove(dbValueColumn4M) == null) {
				if (allFields.remove(dbValueColumn) == null)
					LOGGER.warn("Unable to remove field {} from Map named allFields!", dbValueColumn);
			}
			if (!pkColumns.containsKey(dbValueColumn4M)) {
				if (topicKeys.containsKey(dbValueColumn4M)) {
					valueField = topicKeys.get(dbValueColumn4M);
				} else if (topicValues.containsKey(dbValueColumn4M)) {
					valueField = topicValues.get(dbValueColumn4M);
				} else if (unnestedValues.containsKey(dbValueColumn4M)) {
					unnestingRequired = true;
					valueField = null;
					final String parentField = unnestedParents.get(dbValueColumn4M);
					if (!unnestedColumns.containsKey(parentField)) {
						unnestedColumns.put(parentField, new ArrayList<>());
					}
					unnestedColumns.get(parentField)
							.add(unnestedValues.get(dbValueColumn4M));
				} else {
					LOGGER.warn(
							"""
							
							=====================
							{} column {}.{} with type {} is present in the database but not in the Kafka topic!
							=====================
							""",
								Strings.CI.equals("YES", rsAllColumns.getString("IS_NULLABLE")) ?
										"Nullable" : "Not nullable",
								tableName, dbValueColumn, getTypeName(rsAllColumns.getInt("DATA_TYPE")));
					continue;
				}
				if (valueField != null) {
					final var column = new JdbcSinkColumn(valueField, false);
					//TODO - currently JDBCType only from Kafka Topic!!!
					if (column.getJdbcType() == BLOB ||
							column.getJdbcType() == CLOB ||
							column.getJdbcType() == NCLOB ||
							column.getJdbcType() == SQLXML ||
							column.getJdbcType() == JSON ||
							column.getJdbcType() == VECTOR) {
						lobColumns.put(column.getColumnName(), column);
					} else {
							allColumns.add(column);
							allColsMap.put(dbValueColumn4M, column);
					}
				}
			}
		}
		if (unnestingRequired) {
			for (final String parentName : unnestedColumns.keySet()) {
				final List<JdbcSinkColumn> transformation = new ArrayList<>();
				for (final Field unnestField : unnestedColumns.get(parentName)) {
					transformation.add(new JdbcSinkColumn(unnestField, false));
				}
				lobColumns.put(parentName, transformation);
			}
		}

		if (allColumns.size() == 0) {
			onlyPkColumns = true;
			LOGGER.warn("Table {} contains only primary key column(s)!", this.tableName);
			LOGGER.warn("Column list for {}:", this.tableName);
			pkColumns.forEach((k, oraColumn) -> {
				LOGGER.warn("\t{},\t JDBC Type -> {}",
						oraColumn.getColumnName(), getTypeName(oraColumn.getJdbcType()));
			});
		} else {
			onlyPkColumns = false;
		}
		if (!allFields.isEmpty()) {
			for (final String columnName : allFields.keySet()) {
				final Field field = allFields.get(columnName);
				final var column = new JdbcSinkColumn(field, false);
				final String alterTableSql = TargetDbSqlUtils.addColumnSql(tableName, dbType, column);
				if (LOGGER.isDebugEnabled())
					LOGGER.debug("Adding new column ta table {} with command '()'", tableName, alterTableSql);
				//TODO
				//TODO retry with backoff here!!!
				//TODO
				try {
					Statement statement = connection.createStatement();
					statement.executeUpdate(alterTableSql);
					if (dbType == DB_TYPE_POSTGRESQL)
						connection.commit();
					statement.close();
					statement = null;
				} catch (SQLException sqle) {
					LOGGER.error(
							"""
							
							=====================
							Unable to execute '{}'!
							ErrorCode = {}, SQLState = '{}'
							=====================
							""",
								alterTableSql, sqle.getErrorCode(), sqle.getSQLState());
					throw sqle;
				}
				allColumns.add(column);
				allColsMap.put(column.getColumnName(), column);
			}

		}
		prepareSql();
	}

	private void prepareSql() {
		// Prepare INSERT/DELETE statements...
		LOGGER.debug("Prepare INSERT/DELETE statements for table {}", this.tableName);
		final Map<String, String> sqlTexts = TargetDbSqlUtils.generateSinkSql(
				tableName, dbType, pkColumns, allColumns, lobColumns,
				connectorMode == CONNECTOR_AUDIT_TRAIL);
		sinkInsertSql = sqlTexts.get(TargetDbSqlUtils.INSERT);

		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Table name -> {}, INSERT statement ->\n{}", this.tableName, sinkInsertSql);
		if (!onlyValue && connectorMode == CONNECTOR_REPLICATE) {
			sinkDeleteSql = sqlTexts.get(TargetDbSqlUtils.DELETE);
			updateStmtWhere = sqlTexts.get(TargetDbSqlUtils.WHERE);
			buildLobColsSql(sqlTexts);
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Table name -> {}, DELETE statement ->\n{}", this.tableName, sinkDeleteSql);
		}
		LOGGER.debug("End of SQL and DB preparation for table {}.", this.tableName);
	}

	public void putData(final Connection connection, final SinkRecord record) throws SQLException {
		LOGGER.debug("BEGIN: putData");
		final var opType = getOpType(record);
		final var nanosStart = System.nanoTime();
		if (onlyValue ||
				connectorMode == CONNECTOR_AUDIT_TRAIL) {
			processInsert(connection, record);
			insertTime += System.nanoTime() - nanosStart;
		} else {
			switch (opType) {
				case 'd' -> {
					try {
						processDelete(connection, record);
					} catch (Exception e) {
						final Entry<Struct, Struct> structs = getStructsFromSinkRecord(record);
						LOGGER.error(
									"""
									
									=====================
									Unable to execute DELETE statement:
									'{}'
									keyStruct = {}
									valueStruct = {}
									=====================
									""", sinkDeleteSql, structs.getKey().toString(), structs.getValue().toString());
						throw e;
					}
					deleteTime += (System.nanoTime() - nanosStart);
				}
				case 'u' -> {
					try {
						processUpdate(connection, record);
					} catch (Exception e) {
						final Entry<Struct, Struct> structs = getStructsFromSinkRecord(record);
						LOGGER.error(
								"""
								
								=====================
								Unable to execute UPDATE statement.
								keyStruct = {}
								valueStruct = {}
								=====================
								""", structs.getKey().toString(), structs.getValue().toString());
						throw e;
					}
				}
				case 'c' -> {
					try {
						processInsert(connection, record);
					} catch (Exception e) {
						final Entry<Struct, Struct> structs = getStructsFromSinkRecord(record);
						LOGGER.error(
									"""
									
									=====================
									Unable to execute INSERT statement:
									'{}'
									keyStruct = {}
									valueStruct = {}
									=====================
									""", sinkInsertSql, structs.getKey().toString(), structs.getValue().toString());
						throw e;
					}
					insertTime += (System.nanoTime() - nanosStart);
				}
			}
		}
		LOGGER.debug("END: putData");
	}

	void exec() throws SQLException {
		LOGGER.debug("BEGIN: exec()");
		final long nanosStart = System.nanoTime();
		if (sinkInsert != null && insertCount > 0) {
			execInsert();
			sinkInsert.clearBatch();
			pkInUpsertBatch.clear();
			execLobUpdate(false);
			insertTime += (System.nanoTime() - nanosStart);
			metrics.addInsert(insertCount, insertTime);
			insertCount = 0;
			insertTime = 0;
		}
		if (sinkDelete != null && deleteCount > 0) {
			execDelete();
			sinkDelete.clearBatch();
			deleteTime += System.nanoTime() - nanosStart;
			metrics.addDelete(deleteCount, deleteTime);
			deleteCount = 0;
			deleteTime = 0;
		}
		LOGGER.debug("END: exec()");
	}

	void execAndCloseCursors() throws SQLException {
		LOGGER.debug("BEGIN: closeCursors()");
		final long nanosStart = System.nanoTime();
		if (sinkInsert != null) {
			if (insertCount > 0) {
				execInsert();
				execLobUpdate(true);
				insertTime += (System.nanoTime() - nanosStart);
				metrics.addInsert(insertCount, insertTime);

			}
			sinkInsert.close();
			sinkInsert = null;
			insertCount = 0;
			insertTime = 0;
		}
		if (sinkDelete != null) {
			if (deleteCount > 0) {
				execDelete();
				deleteTime += System.nanoTime() - nanosStart;
				metrics.addDelete(deleteCount, deleteTime);
			}
			sinkDelete.close();
			sinkDelete = null;
			deleteCount = 0;
			deleteTime = 0;
		}
		LOGGER.debug("END: closeCursors()");
	}

	private void execInsert() throws SQLException {
		try {
			sinkInsert.executeBatch();
		} catch(SQLException sqle) {
			if (printAndHandleInsertError(sqle)) {
				LOGGER.error(
						"""
						
						=====================
						Error while executing INSERT (with {} statements in batch) statement '{}'
						=====================
						""", insertCount, sinkInsertSql);
				throw sqle;
			}
		}
	}

	private void execDelete() throws SQLException {
		try {
			sinkDelete.executeBatch();
		} catch(SQLException sqle) {
			LOGGER.error(
					"""
					
					=====================
					Error while executing DELETE (with {} statements in batch) statement '{}'
					=====================
					""", deleteCount, sinkDeleteSql);
			throw sqle;
		}
	}

	private void processUpdate(
			final Connection connection, final SinkRecord record) throws SQLException {
		var nanos = System.nanoTime(); 
		final var updateStmtHash = updateStatementHashCode(record);
		var updateStatement = updateStatements.get(updateStmtHash);
		if (updateStatement == null) {
			updateStatement = new UpdateStatement(updateStmtHash, this, connection, record);
			updateStatements.put(updateStmtHash, updateStatement);
		} else {
			updateStatement.exec(connection, record);
		}
		metrics.addUpdate(1, System.nanoTime() - nanos);
	}

	private void processDelete(
			final Connection connection, final SinkRecord record) throws SQLException {
		LOGGER.trace("BEGIN: processDelete()");
		final Entry<Struct, Struct> structs = getStructsFromSinkRecord(record);
		if (sinkDelete == null) {
			sinkDelete = connection.prepareStatement(sinkDeleteSql);
			deleteCount = 0;
			deleteTime = 0;
		}
		var iterator = pkColumns.entrySet().iterator();
		int columnNo = 1;
		while (iterator.hasNext()) {
			final var oraColumn = iterator.next().getValue();
			try {
				oraColumn.binder().bind(dbType, sinkDelete, columnNo, structs.getKey(), structs.getValue());
				columnNo++;
			} catch (DataException de) {
				LOGGER.error(
						"""
						
						=====================
						Data error while performing DELETE statement!
						Table={}, PK column={}, {}.
						=====================
						
						""", tableName, oraColumn.getColumnName(),
							structValueAsString(oraColumn, structs.getKey()));
				throw de;
			}
		}
		sinkDelete.addBatch();
		deleteCount++;
		LOGGER.trace("END: processDelete()");
	}

	private void processInsert(
			final Connection connection, final SinkRecord record) throws SQLException {
		LOGGER.debug("BEGIN: processInsert()");
		final Entry<Struct, Struct> structs = getStructsFromSinkRecord(record);
		if (sinkInsert == null) {
			sinkInsert = connection.prepareStatement(sinkInsertSql);
			insertCount = 0;
			insertTime = 0;
		}
		int columnNo = 1;
		var iterator = pkColumns.entrySet().iterator();
		while (iterator.hasNext()) {
			final var column = iterator.next().getValue();
			column.binder().bind(dbType, sinkInsert, columnNo, structs.getKey(), structs.getValue());
			columnNo++;
		}
		for (int i = 0; i < allColumns.size(); i++) {
			final var oraColumn = allColumns.get(i);
			if (schemaType == SCHEMA_TYPE_INT_KAFKA_STD ||
					schemaType == SCHEMA_TYPE_INT_SINGLE ||
					(schemaType == SCHEMA_TYPE_INT_DEBEZIUM && !oraColumn.isPartOfPk())) {
				try {
					oraColumn.binder().bind(dbType, sinkInsert, columnNo, structs.getKey(), structs.getValue());
					columnNo++;
				} catch (DataException | SQLException de) {
					LOGGER.error("Data error while performing insert! Table={}, column={}, {}.",
							tableName, oraColumn.getColumnName(), structValueAsString(oraColumn, structs.getValue()));
					LOGGER.error("SQL statement:\n\t{}", sinkInsertSql);
					LOGGER.error("PK value(s) for this row in table {} are", tableName);
					int colNo = 1;
					for (final var column : allColumns) {
						LOGGER.error("\t{}) column {}, {}",
								colNo, column.getColumnName(), structValueAsString(column, structs.getValue()));
						colNo++;
					}
					throw new DataException(de);
				}
			}
		}
		sinkInsert.addBatch();
		insertCount++;
		LOGGER.debug("END: processInsert()");
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

	public boolean duplicatedKeyInBatch(final SinkRecord record) {
		if (onlyValue || getOpType(record) == 'd') {
			// No keys at all...
			return false;
		} else {
			if (dbType == DB_TYPE_POSTGRESQL) {
				final StringBuilder keyString = new StringBuilder(0x100);
				Entry<Struct, Struct> structs = getStructsFromSinkRecord(record);
				boolean firstColumn = true;
				var iterator = pkColumns.entrySet().iterator();
				while (iterator.hasNext()) {
					final var oraColumn = iterator.next().getValue();
					if (firstColumn) {
						firstColumn = false;
					} else {
						keyString.append("-");
					}
					keyString.append(oraColumn.getValueAsString(structs.getKey(), structs.getValue()));
				}
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Checking key {} for table {} with upsertCount={}",
							keyString.toString(), tableName, insertCount);
				}
				return !pkInUpsertBatch.add(keyString.toString());
			} else {
				return false;
			}
		}
	}

	private static int updateStatementHashCode(final SinkRecord record) {
		final var valueStruct = (Struct) record.value();
		List<String> columns = new ArrayList<>(record.valueSchema().fields().size());
		for (var field : record.valueSchema().fields())
			if (valueStruct.get(field) == null) continue;
			else columns.add(field.name());
		return Objects.hash((Object[]) columns.toArray(String[]::new));
	}

	private static class UpdateStatement {
		private final String sqlText;
		private final List<JdbcSinkColumn> setColumns = new ArrayList<>();
		private final int hashCode;
		private final WrappedDataTable table;

		UpdateStatement(final int hashCode, final WrappedDataTable table,
				final Connection connection, final SinkRecord record) throws SQLException {
			this.hashCode = hashCode;
			this.table = table;
			final var sb = new StringBuilder(0x80);
			boolean first = true;
			sb.append(table.updateStmtStart);
			final var valueStruct = (Struct) record.value();
			for (var field : record.valueSchema().fields()) {
				if (valueStruct.get(field) == null) continue;
				else {
					final var name = field.name();
					setColumns.add(table.allColsMap.get(name));
					if (first) first = false;
					else sb.append(",");
					sb
						.append(name)
						.append("=?");
				}
			}
			sb.append(table.updateStmtWhere);
			sqlText = sb.toString();
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Created UPDATE statement holder for SQL:\n\t{}", sqlText);
			exec(connection, record);
		}

		void exec(final Connection connection, final SinkRecord record) throws SQLException {
			final var valueStruct = (Struct) record.value();
			final var keyStruct = (Struct) record.key();
			PreparedStatement statement = connection.prepareStatement(sqlText);
			var columnNo = 1;
			for (int i = 0; i < setColumns.size(); i++) {
				final var column = setColumns.get(i);
				try {
					column.binder().bind(table.dbType, statement, columnNo, keyStruct, valueStruct);
					columnNo++;
				} catch (DataException de) {
					printUpdateError(table.tableName, column, valueStruct);
					throw de;
				}
			}
			final var iterator = table.pkColumns.entrySet().iterator();
			while (iterator.hasNext()) {
				final var column = iterator.next().getValue();
				try {
					column.binder().bind(table.dbType, statement, columnNo, keyStruct, valueStruct);
					columnNo++;
				} catch (DataException de) {
					printUpdateError(table.tableName, column, keyStruct);
					throw de;
				}
			}
			//TODO
			//TODO LOBs
			//TODO
			if (statement.executeUpdate() != 1) {
				LOGGER.error(
						"""
						=====================
						The UPDATE statement updated zero rows.
						SQL text: {}
						Key content: {}
						=====================
						
						""", sqlText, keyStruct);
			}
			statement.close();
			statement = null;
		}

		@Override
		public int hashCode() {
			return hashCode;
		}
	}

	private static void printUpdateError(final String tableName, final JdbcSinkColumn column, final Struct struct) {
		LOGGER.error(
				"""
				
				=====================
				Data error while performing UPDATE statement.
				Table={}, PK column={}, {}.
				=====================
				
				""", tableName, column.getColumnName(), structValueAsString(column, struct));
	}

	private static final String DUP_ROW_MESSAGE =
			"""
			
			=====================
			'{}' while executing SQL:
			{}
			Statement ignored
			=====================
			
			""";
	private boolean printAndHandleInsertError(final SQLException sqle) {
		switch (dbType) {
			case DB_TYPE_ORACLE -> {
				if (onlyPkColumns && sqle.getErrorCode() == 1) {
					// ORA-00001: unique constraint %s violated
					// ignore for tables with PK only column(s)
					LOGGER.warn(DUP_ROW_MESSAGE, sqle.getMessage(), sinkInsertSql);
					return false;
				}
			}
			case DB_TYPE_MYSQL -> {
				if (onlyPkColumns && Strings.CS.startsWith(sqle.getMessage(), "Duplicate entry")) {
					// Duplicate entry 'XXX' for key 'YYYYY'
					// ignore for tables with PK only column(s)
					LOGGER.warn(DUP_ROW_MESSAGE, sqle.getMessage(), sinkInsertSql);
					return false;
				}
			}
			case DB_TYPE_POSTGRESQL -> {
				if (Strings.CS.equals(sqle.getSQLState(), "23505")) {
					LOGGER.warn(DUP_ROW_MESSAGE, sqle.getMessage(), sinkInsertSql);
					return false;
				}
			}
		}
		return true;
	}

}
