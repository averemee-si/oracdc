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

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

import solutions.a2.cdc.oracle.OraColumn;
import solutions.a2.cdc.oracle.data.OraBlob;
import solutions.a2.cdc.oracle.data.OraClob;
import solutions.a2.cdc.oracle.data.OraJson;
import solutions.a2.cdc.oracle.data.OraNClob;
import solutions.a2.cdc.oracle.data.OraXml;
import solutions.a2.cdc.oracle.runtime.config.Parameters;
import solutions.a2.kafka.sink.jmx.SinkTableInfo;

import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.SQLXML;
import static oracle.jdbc.OracleTypes.JSON;
import static solutions.a2.cdc.oracle.data.JdbcTypes.getTypeName;
import static solutions.a2.kafka.sink.JdbcSinkConnectionPool.DB_TYPE_MYSQL;
import static solutions.a2.kafka.sink.JdbcSinkConnectionPool.DB_TYPE_ORACLE;
import static solutions.a2.kafka.sink.JdbcSinkConnectionPool.DB_TYPE_POSTGRESQL;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class JdbcSinkTable extends JdbcSinkTableBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkTable.class);

	private final SinkTableInfo metrics;
	private String sinkUpsertSql = null;
	private String sinkDeleteSql = null;
	private PreparedStatement sinkUpsert = null;
	private PreparedStatement sinkDelete = null;
	private int upsertCount;
	private int deleteCount;
	private long upsertTime;
	private long deleteTime;
	private boolean delayedObjectsCreation = false;
	private final int pkStringLength;
	private final Set<String> pkInUpsertBatch = new HashSet<>();
	private boolean ready4Delete = false;

	/**
	 * This constructor is used only for Sink connector
	 *
	 * @param sinkPool
	 * @param tableName
	 * @param record
	 * @param schemaType
	 * @param config
	 * @throws SQLException 
	 */
	public JdbcSinkTable(
			final JdbcSinkConnectionPool sinkPool, final String tableName,
			final SinkRecord record, final int schemaType, 
			final JdbcSinkConnectorConfig config) throws SQLException {
		super(schemaType, sinkPool.getDbType());
		LOGGER.debug("Creating OraTable object from Kafka connect SinkRecord...");
		pkStringLength = config.getPkStringLength();
		connectorMode = config.getConnectorMode();

		if (schemaType == Parameters.SCHEMA_TYPE_INT_DEBEZIUM) {
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
		if (dbType == DB_TYPE_POSTGRESQL) {
			LOGGER.debug("Working with PostgreSQL specific lower case only names");
			// PostgreSQL specific...
			// Also look at https://stackoverflow.com/questions/43111996/why-postgresql-does-not-like-uppercase-table-names
			tableNameCaseConv = tableName.toLowerCase();
		} else {
			tableNameCaseConv = tableName;
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("tableOwner = {}, tableName = {}.", this.tableOwner, this.tableName);
		}


		final char opType = getOpType(record);
		try (Connection connection = sinkPool.getConnection()) {
			final Entry<Set<String>, ResultSet> tableMetadata = checkPresence(connection);
			if (exists) {
				if (opType == 'd' &&
						(!config.useAllColsOnDelete()) &&
						connectorMode == JdbcSinkConnectorConfig.CONNECTOR_REPLICATE) {
					final List<Field> keyFields;
					if (this.schemaType == Parameters.SCHEMA_TYPE_INT_DEBEZIUM) {
						keyFields = record.valueSchema().field("before").schema().fields();
					} else {
						//ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD
						//ParamConstants.SCHEMA_TYPE_INT_SINGLE
						if (record.keySchema() == null) {
							keyFields = null;
						} else {
							keyFields = record.keySchema().fields();
						}
					}
					if (keyFields != null) {
						for (Field field : keyFields) {
							final var column = new JdbcSinkColumn(field, true);
							pkColumns.put(column.getColumnName(), column);
						}

						sinkDeleteSql = TargetDbSqlUtils.generateSinkSql(
								tableName, dbType, pkColumns, allColumns, lobColumns,
								connectorMode == JdbcSinkConnectorConfig.CONNECTOR_AUDIT_TRAIL).get(TargetDbSqlUtils.DELETE);
						ready4Delete = true;
					} else {
						LOGGER.warn(
								"""
								
								=====================
								"data transfer to the  existing table {} will begin after first non-delete operation for it!
								=====================
								""",
									tableName);
					}
					delayedObjectsCreation = true;
				} else {
					prepareSql(connection, record, tableMetadata);
				}
			} else {
				if (config.autoCreateTable()) {
					LOGGER.info(
							"""
							
							=====================
							Table '{}' will be created in the target database.
							=====================
							""",
								tableNameCaseConv);
					if (opType == 'd' && (!config.useAllColsOnDelete())) {
						delayedObjectsCreation = true;
					} else {
						// Create table in target database
						createTable(connection, record, pkStringLength);
						prepareSql();
					}
				} else {
					LOGGER.error(
							"""
							=====================
							Table '{}' does not exist in the target database and a2.autocreate=false!
							=====================
							""",
								tableNameCaseConv);
					throw new ConnectException("Table does not exists!");
				}
			}
		} catch (SQLException sqle) {
			throw new ConnectException(sqle);
		}

		metrics = new SinkTableInfo(this.tableName);
		upsertCount = 0;
		deleteCount = 0;
		upsertTime = 0;
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
			final String fieldName = StringUtils.upperCase(f.name());
			if (Strings.CS.equals("struct", f.schema().type().getName()) &&
					!Strings.CS.startsWithAny(f.schema().name(),
							OraBlob.LOGICAL_NAME,
							OraClob.LOGICAL_NAME,
							OraNClob.LOGICAL_NAME,
							OraXml.LOGICAL_NAME,
							OraJson.LOGICAL_NAME)) {
				for (Field unnestField : f.schema().fields()) {
					final String unnestFieldName = StringUtils.upperCase(unnestField.name());
					unnestedValues.put(unnestFieldName, unnestField);
					unnestedParents.put(unnestFieldName, fieldName);
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
							column.getJdbcType() == JSON) {
						lobColumns.put(column.getColumnName(), column);
					} else {
							allColumns.add(column);
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
			for (final var columnName : allFields.keySet()) {
				final var field = allFields.get(columnName);
				final var column = new JdbcSinkColumn(field, false);
				final String alterTableSql = TargetDbSqlUtils.addColumnSql(tableName, dbType, column);
				if (LOGGER.isDebugEnabled())
					LOGGER.debug("Adding new column ta table {} with command '()'", tableName, alterTableSql);
				//TODO
				//TODO retry with backoff here!!!
				//TODO
				try {
					var statement = connection.createStatement();
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
			}

		}
		prepareSql();
	}

	private void prepareSql() {
		// Prepare UPDATE/INSERT/DELETE statements...
		LOGGER.debug("Prepare UPDATE/INSERT/DELETE statements for table {}", this.tableName);
		final Map<String, String> sqlTexts = TargetDbSqlUtils.generateSinkSql(
				tableName, dbType, pkColumns, allColumns, lobColumns,
				connectorMode == JdbcSinkConnectorConfig.CONNECTOR_AUDIT_TRAIL);
		if (onlyValue ||
				connectorMode == JdbcSinkConnectorConfig.CONNECTOR_AUDIT_TRAIL) {
			sinkUpsertSql = sqlTexts.get(TargetDbSqlUtils.INSERT);
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Table name -> {}, INSERT statement ->\n{}", this.tableName, sinkUpsertSql);
			}
		} else {
			sinkUpsertSql = sqlTexts.get(TargetDbSqlUtils.UPSERT);
			sinkDeleteSql = sqlTexts.get(TargetDbSqlUtils.DELETE);
			buildLobColsSql(sqlTexts);
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Table name -> {}, UPSERT statement ->\n{}", this.tableName, sinkUpsertSql);
				LOGGER.debug("Table name -> {}, DELETE statement ->\n{}", this.tableName, sinkDeleteSql);
			}
		}
		LOGGER.debug("End of SQL and DB preparation for table {}.", this.tableName);
	}

	public String getTableFqn() {
		return tableOwner + "." + tableName;
	}

	public void putData(final Connection connection, final SinkRecord record) throws SQLException {
		LOGGER.debug("BEGIN: putData");
		final char opType = getOpType(record);
		final long nanosStart = System.nanoTime();
		if (onlyValue ||
				connectorMode == JdbcSinkConnectorConfig.CONNECTOR_AUDIT_TRAIL) {
			processInsert(connection, record);
			upsertTime += System.nanoTime() - nanosStart;
		} else {
			if ('d' == opType) {
				if (delayedObjectsCreation) {
					if (exists) {
						if (ready4Delete) {
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
										=====================
										""",
											sinkDeleteSql, structs.getKey().toString());
								throw e;
							}
							deleteTime += System.nanoTime() - nanosStart;
						} else {
							LOGGER.warn(
									"Skipping the delete operation for the table {}. Please check connector and schema settings!",
									tableName);
						}
					} else {
						LOGGER.info(
							"Skipping the delete operation because the table {} has not yet been created",
							tableName);
					}
				} else {
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
								""",
									sinkDeleteSql, structs.getKey().toString(), structs.getValue().toString());
						throw e;
					}
					deleteTime += System.nanoTime() - nanosStart;
				}
			} else {
				if (delayedObjectsCreation) {
					final Entry<Set<String>, ResultSet> tableMetadata = checkPresence(connection);
					if (exists) {
						prepareSql(connection, record, tableMetadata);
					} else {
						createTable(connection, record, pkStringLength);
					}
					prepareSql();
					delayedObjectsCreation = false;
				}
				try {
					processUpsert(connection, record);
				} catch (Exception e) {
					final Entry<Struct, Struct> structs = getStructsFromSinkRecord(record);
					LOGGER.error(
							"""
							
							=====================
							Unable to execute UPSERT statement:
							'{}'
							keyStruct = {}
							valueStruct = {}
							=====================
							""",
								sinkUpsertSql, structs.getKey().toString(), structs.getValue().toString());
					throw e;
				}
				upsertTime += System.nanoTime() - nanosStart;
			}
		}
		LOGGER.debug("END: putData");
	}

	public void exec() throws SQLException {
		LOGGER.debug("BEGIN: exec()");
		final long nanosStart = System.nanoTime();
		if (sinkUpsert != null && upsertCount > 0) {
			execUpsert();
			sinkUpsert.clearBatch();
			pkInUpsertBatch.clear();
			execLobUpdate(false);
			upsertTime += System.nanoTime() - nanosStart;
			metrics.addUpsert(upsertCount, upsertTime);
			upsertCount = 0;
			upsertTime = 0;
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

	public void execAndCloseCursors() throws SQLException {
		LOGGER.debug("BEGIN: closeCursors()");
		final long nanosStart = System.nanoTime();
		if (sinkUpsert != null) {
			if (upsertCount > 0) {
				execUpsert();
				execLobUpdate(true);
				upsertTime += System.nanoTime() - nanosStart;
				metrics.addUpsert(upsertCount, upsertTime);

			}
			sinkUpsert.close();
			sinkUpsert = null;
			upsertCount = 0;
			upsertTime = 0;
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

	private void execUpsert() throws SQLException {
		try {
			sinkUpsert.executeBatch();
		} catch(SQLException sqle) {
			boolean raiseException = true;
			if (dbType == DB_TYPE_ORACLE) {
				if (onlyPkColumns && sqle.getErrorCode() == 1) {
					// ORA-00001: unique constraint %s violated
					// ignore for tables with PK only column(s)
					raiseException = false;
					LOGGER.warn(sqle.getMessage());
				}
			} else if (dbType == DB_TYPE_MYSQL) {
				if (onlyPkColumns && Strings.CS.startsWith(sqle.getMessage(), "Duplicate entry")) {
					// Duplicate entry 'XXX' for key 'YYYYY'
					// ignore for tables with PK only column(s)
					raiseException = false;
					LOGGER.warn(sqle.getMessage());
				}
			}
			if (raiseException) {
				LOGGER.error(
						"""
						
						=====================
						Error while executing UPSERT (with {} statements in batch) statement '{}'
						=====================
						""",
							upsertCount, sinkUpsertSql);
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
					""",
						deleteCount, sinkDeleteSql);
			throw sqle;
		}
	}

	private void processUpsert(
			final Connection connection, final SinkRecord record) throws SQLException {
		LOGGER.trace("BEGIN: processUpsert()");
		final Entry<Struct, Struct> structs = getStructsFromSinkRecord(record);
		if (sinkUpsert == null) {
			sinkUpsert = connection.prepareStatement(sinkUpsertSql);
			upsertCount = 0;
			upsertTime = 0;
		}
		int columnNo = 1;
		var iterator = pkColumns.entrySet().iterator();
		while (iterator.hasNext()) {
			final var oraColumn = iterator.next().getValue();
			try {
				oraColumn.binder().bind(dbType, sinkUpsert, columnNo, structs.getKey(), structs.getValue());
				columnNo++;
			} catch (DataException de) {
				LOGGER.error(
						"""
						
						=====================
						Data error while performing upsert! Table={}, PK column={}, {}.
						=====================
						""",
							tableName, oraColumn.getColumnName(), structValueAsString(oraColumn, structs.getKey()));
				throw de;
			}
		}
		for (int i = 0; i < allColumns.size(); i++) {
			final var oraColumn = allColumns.get(i);
			if (schemaType == Parameters.SCHEMA_TYPE_INT_KAFKA_STD ||
					(schemaType == Parameters.SCHEMA_TYPE_INT_DEBEZIUM && !oraColumn.isPartOfPk())) {
				try {
					oraColumn.binder().bind(dbType, sinkUpsert, columnNo, structs.getKey(), structs.getValue());
					columnNo++;
				} catch (DataException | SQLException de) {
					LOGGER.error("Data error while performing upsert! Table={}, column={}, {}.",
							tableName, oraColumn.getColumnName(), structValueAsString(oraColumn, structs.getValue()));
					LOGGER.error("SQL statement:\n\t{}", sinkUpsertSql);
					LOGGER.error("PK value(s) for this row in table {} are", tableName);
					int colNo = 1;
					var pkIterator = pkColumns.entrySet().iterator();
					while (pkIterator.hasNext()) {
						var pkColumn = pkIterator.next().getValue();
						LOGGER.error("\t{}) PK column {}, {}",
								colNo, pkColumn.getColumnName(), structValueAsString(pkColumn, structs.getKey()));
						colNo++;
					}
					throw new DataException(de);
				}
			}
		}
		sinkUpsert.addBatch();
		upsertCount++;

		if (lobColumns.size() > 0) {
			var lobIterator = lobColsSqlMap.entrySet().iterator();
			while (lobIterator.hasNext()) {
				final LobSqlHolder holder = lobIterator.next().getValue();
				final Object objLobColumn = lobColumns.get(holder.COLUMN);
				final Struct objLobValue = (Struct) structs.getValue().get(holder.COLUMN);
				if (objLobValue != null) {
					//NULL means do not touch LOB!
					if (holder.STATEMENT == null) {
						holder.STATEMENT = connection.prepareStatement(holder.SQL_TEXT);
						holder.EXEC_COUNT = 0;
					}
					if (objLobColumn instanceof OraColumn) {
						final int lobColType = ((OraColumn)objLobColumn).getJdbcType();
						if (lobColType == BLOB) {
							final byte[] columnByteValue = objLobValue.getBytes("V");
							if (columnByteValue == null)
								holder.STATEMENT.setNull(1, lobColType);
							else
								holder.STATEMENT.setBinaryStream(
									1, new ByteArrayInputStream(columnByteValue), columnByteValue.length);
						} else {
							// CLOB || NCLOB || SQLXML || JSON 
							final String columnStringValue = objLobValue.getString("V");
							if (columnStringValue == null)
								holder.STATEMENT.setNull(1, lobColType);
							else
								holder.STATEMENT.setCharacterStream(
									1, new StringReader(columnStringValue));
						}
						try {
							// Bind PK columns...
							columnNo = 2;
							iterator = pkColumns.entrySet().iterator();
							while (iterator.hasNext()) {
								final var oraColumn = iterator.next().getValue();
								oraColumn.binder().bind(
										dbType, holder.STATEMENT, columnNo, structs.getKey(), structs.getValue());
								columnNo++;
							}
							holder.STATEMENT.addBatch();
							holder.EXEC_COUNT++;
						} catch (SQLException sqle) {
							LOGGER.error("Error while preparing LOB update statement {}", holder.SQL_TEXT);
							throw new SQLException(sqle);
						}
					} else {
					// Process transformed field
						final Struct transformedStruct = (Struct) objLobValue;
						@SuppressWarnings("unchecked")
						final List<JdbcSinkColumn> transformedCols = (List<JdbcSinkColumn>) objLobColumn;
						columnNo = 1;
						for (var transformedColumn : transformedCols) {
							transformedColumn.binder().bind(
									dbType, holder.STATEMENT, columnNo, null, transformedStruct);
							columnNo++;
						}
						// Bind PK columns...
						iterator = pkColumns.entrySet().iterator();
						while (iterator.hasNext()) {
							final var oraColumn = iterator.next().getValue();
							oraColumn.binder().bind(
									dbType, holder.STATEMENT, columnNo, structs.getKey(), structs.getValue());
							columnNo++;
						}
						holder.STATEMENT.addBatch();
						holder.EXEC_COUNT++;
					}
				}
			}
		}
		LOGGER.trace("END: processUpsert()");
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
				LOGGER.error("Data error while performing delete! Table {}, PK column {}, {}.",
						tableName, oraColumn.getColumnName(), structValueAsString(oraColumn, structs.getKey()));
				throw new DataException(de);
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
		if (sinkUpsert == null) {
			sinkUpsert = connection.prepareStatement(sinkUpsertSql);
			upsertCount = 0;
			upsertTime = 0;
		}
		int columnNo = 1;
		for (int i = 0; i < allColumns.size(); i++) {
			final var oraColumn = allColumns.get(i);
			if (schemaType == Parameters.SCHEMA_TYPE_INT_KAFKA_STD ||
					schemaType == Parameters.SCHEMA_TYPE_INT_SINGLE ||
					(schemaType == Parameters.SCHEMA_TYPE_INT_DEBEZIUM && !oraColumn.isPartOfPk())) {
				try {
					oraColumn.binder().bind(dbType, sinkUpsert, columnNo, structs.getKey(), structs.getValue());
					columnNo++;
				} catch (DataException | SQLException de) {
					LOGGER.error("Data error while performing insert! Table={}, column={}, {}.",
							tableName, oraColumn.getColumnName(), structValueAsString(oraColumn, structs.getValue()));
					LOGGER.error("SQL statement:\n\t{}", sinkUpsertSql);
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
		sinkUpsert.addBatch();
		upsertCount++;
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
				final StringBuilder keyString = new StringBuilder(256);
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
							keyString.toString(), tableName, upsertCount);
				}
				return !pkInUpsertBatch.add(keyString.toString());
			} else {
				return false;
			}
		}
	}

}
