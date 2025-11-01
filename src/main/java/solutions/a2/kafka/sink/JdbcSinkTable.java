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
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.OraColumn;
import solutions.a2.cdc.oracle.OraTableDefinition;
import solutions.a2.cdc.oracle.data.OraBlob;
import solutions.a2.cdc.oracle.data.OraClob;
import solutions.a2.cdc.oracle.data.OraJson;
import solutions.a2.cdc.oracle.data.OraNClob;
import solutions.a2.cdc.oracle.data.OraXml;
import solutions.a2.cdc.postgres.PgRdbmsInfo;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.kafka.sink.jmx.SinkTableInfo;
import solutions.a2.utils.ExceptionUtils;

import static java.sql.Types.BINARY;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.SQLXML;
import static oracle.jdbc.OracleTypes.JSON;
import static solutions.a2.cdc.oracle.data.JdbcTypes.getTypeName;
import static solutions.a2.kafka.sink.JdbcSinkConnectionPool.DB_TYPE_MYSQL;
import static solutions.a2.kafka.sink.JdbcSinkConnectionPool.DB_TYPE_ORACLE;
import static solutions.a2.kafka.sink.JdbcSinkConnectionPool.DB_TYPE_POSTGRESQL;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class JdbcSinkTable extends OraTableDefinition {

	private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkTable.class);
	private static final Struct DUMMY_STRUCT =
			new Struct(
						SchemaBuilder
							.struct()
							.optional()
							.build());


	private final int dbType;
	private final SinkTableInfo metrics;
	private String sinkUpsertSql = null;
	private String sinkDeleteSql = null;
	private PreparedStatement sinkUpsert = null;
	private PreparedStatement sinkDelete = null;
	private int upsertCount;
	private int deleteCount;
	private long upsertTime;
	private long deleteTime;
	private boolean onlyPkColumns;
	private final Map<String, Object> lobColumns = new HashMap<>();
	private Map<String, LobSqlHolder> lobColsSqlMap;
	private boolean delayedObjectsCreation = false;
	private final int pkStringLength;
	private boolean onlyValue = false;
	private final Set<String> pkInUpsertBatch = new HashSet<>();
	private boolean exists = true;
	private String tableNameCaseConv;
	private boolean ready4Delete = false;
	private int connectorMode = JdbcSinkConnectorConfig.CONNECTOR_REPLICATE;

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
		super(schemaType);
		LOGGER.debug("Creating OraTable object from Kafka connect SinkRecord...");
		pkStringLength = config.getPkStringLength();
		dbType = sinkPool.getDbType();
		connectorMode = config.getConnectorMode();

		if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
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
					if (this.schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
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
							final OraColumn column = new OraColumn(field, true, true);
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
				final OraColumn column = new OraColumn(pkField, true, isKey);
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
			final boolean isKey;
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
					isKey = true;
					valueField = topicKeys.get(dbValueColumn4M);
				} else if (topicValues.containsKey(dbValueColumn4M)) {
					isKey = false;
					valueField = topicValues.get(dbValueColumn4M);
				} else if (unnestedValues.containsKey(dbValueColumn4M)) {
					isKey = false;
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
					final OraColumn column = new OraColumn(valueField, false, isKey);
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
				final List<OraColumn> transformation = new ArrayList<>();
				for (final Field unnestField : unnestedColumns.get(parentName)) {
					transformation.add(new OraColumn(unnestField, false, false));
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
				final OraColumn column = new OraColumn(field, false, false);
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

	private void execLobUpdate(final boolean closeCursor) throws SQLException {
		if (lobColumns.size() > 0) {
			Iterator<Entry<String, LobSqlHolder>> lobIterator = lobColsSqlMap.entrySet().iterator();
			while (lobIterator.hasNext()) {
				final LobSqlHolder holder = lobIterator.next().getValue();
				try {
					if (holder.EXEC_COUNT > 0) {
						LOGGER.debug("Processing LOB update for {}.{} using SQL:\n\t",
								this.tableName, holder.COLUMN, holder.SQL_TEXT);
						holder.STATEMENT.executeBatch();
						holder.STATEMENT.clearBatch();
						//TODO
						//TODO Add metric for counting LOB columns...
						//TODO
						holder.EXEC_COUNT = 0;
						if (closeCursor) {
							holder.STATEMENT.close();
							holder.STATEMENT = null;
						}
					} else if (closeCursor && holder.STATEMENT != null) {
						holder.STATEMENT.close();
						holder.STATEMENT = null;
					}
				} catch(SQLException sqle) {
					LOGGER.error(
							"""
							
							=====================
							Error {} while executing LOB update statement '{}'
							=====================
							""",
								sqle.getMessage(), holder.SQL_TEXT);
					throw new SQLException(sqle);
				}
			}
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
		Iterator<Entry<String, OraColumn>> iterator = pkColumns.entrySet().iterator();
		while (iterator.hasNext()) {
			final OraColumn oraColumn = iterator.next().getValue();
			try {
				oraColumn.bindWithPrepStmt(dbType, sinkUpsert, columnNo, structs.getKey(), structs.getValue());
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
			final OraColumn oraColumn = allColumns.get(i);
			if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_KAFKA_STD ||
					(schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM && !oraColumn.isPartOfPk())) {
				try {
					oraColumn.bindWithPrepStmt(dbType, sinkUpsert, columnNo, structs.getKey(), structs.getValue());
					columnNo++;
				} catch (DataException | SQLException de) {
					LOGGER.error("Data error while performing upsert! Table={}, column={}, {}.",
							tableName, oraColumn.getColumnName(), structValueAsString(oraColumn, structs.getValue()));
					LOGGER.error("SQL statement:\n\t{}", sinkUpsertSql);
					LOGGER.error("PK value(s) for this row in table {} are", tableName);
					int colNo = 1;
					Iterator<Entry<String, OraColumn>> pkIterator = pkColumns.entrySet().iterator();
					while (pkIterator.hasNext()) {
						OraColumn pkColumn = pkIterator.next().getValue();
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
			Iterator<Entry<String, LobSqlHolder>> lobIterator = lobColsSqlMap.entrySet().iterator();
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
								final OraColumn oraColumn = iterator.next().getValue();
								oraColumn.bindWithPrepStmt(
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
						final List<OraColumn> transformedCols = (List<OraColumn>) objLobColumn;
						columnNo = 1;
						for (OraColumn transformedColumn : transformedCols) {
							transformedColumn.bindWithPrepStmt(
									dbType, holder.STATEMENT, columnNo, null, transformedStruct);
							columnNo++;
						}
						// Bind PK columns...
						iterator = pkColumns.entrySet().iterator();
						while (iterator.hasNext()) {
							final OraColumn oraColumn = iterator.next().getValue();
							oraColumn.bindWithPrepStmt(
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
		Iterator<Entry<String, OraColumn>> iterator = pkColumns.entrySet().iterator();
		int columnNo = 1;
		while (iterator.hasNext()) {
			final OraColumn oraColumn = iterator.next().getValue();
			try {
				oraColumn.bindWithPrepStmt(dbType, sinkDelete, columnNo, structs.getKey(), structs.getValue());
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
			final OraColumn oraColumn = allColumns.get(i);
			if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_KAFKA_STD ||
					schemaType == ConnectorParams.SCHEMA_TYPE_INT_SINGLE ||
					(schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM && !oraColumn.isPartOfPk())) {
				try {
					oraColumn.bindWithPrepStmt(dbType, sinkUpsert, columnNo, structs.getKey(), structs.getValue());
					columnNo++;
				} catch (DataException | SQLException de) {
					LOGGER.error("Data error while performing insert! Table={}, column={}, {}.",
							tableName, oraColumn.getColumnName(), structValueAsString(oraColumn, structs.getValue()));
					LOGGER.error("SQL statement:\n\t{}", sinkUpsertSql);
					LOGGER.error("PK value(s) for this row in table {} are", tableName);
					int colNo = 1;
					for (final OraColumn column : allColumns) {
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

	public String structValueAsString(final OraColumn oraColumn, final Struct struct) {
		final StringBuilder sb = new StringBuilder(128);
		sb.append("Column Type =");
		sb.append(getTypeName(oraColumn.getJdbcType()));
		sb.append(", Column Value='");
		switch (oraColumn.getJdbcType()) {
			case NUMERIC:
			case BINARY:
				ByteBuffer bb = (ByteBuffer) struct.get(oraColumn.getColumnName());
				sb.append(rawToHex(bb.array()));
				break;
			default:
				sb.append(struct.get(oraColumn.getColumnName()));
				break;
		}
		sb.append("'");
		return sb.toString();
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

	private char getOpType(final SinkRecord record) {
		char opType = 'c';
		if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
			opType = ((Struct) record.value())
							.getString("op")
							.charAt(0);
			LOGGER.debug("Operation type set payload to {}.", opType);
		} else {
			//ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD
			//ParamConstants.SCHEMA_TYPE_INT_SINGLE
			Iterator<Header> iterator = record.headers().iterator();
			while (iterator.hasNext()) {
				Header header = iterator.next();
				if ("op".equals(header.key())) {
					opType = ((String) header.value())
							.charAt(0);
					break;
				}
			}
			LOGGER.debug("Operation type set from headers to {}.", opType);
		}
		return opType;
	}

	private void buildNonPkColsList(final List<Field> valueFields) throws SQLException {
		for (final Field field : valueFields) {
			if (!pkColumns.containsKey(field.name())) {
				if (Strings.CS.equals("struct", field.schema().type().getName()) &&
						!Strings.CS.startsWithAny(field.schema().name(),
								OraBlob.LOGICAL_NAME,
								OraClob.LOGICAL_NAME,
								OraNClob.LOGICAL_NAME,
								OraXml.LOGICAL_NAME,
								OraJson.LOGICAL_NAME)) {
					final List<OraColumn> transformation = new ArrayList<>();
					for (Field unnestField : field.schema().fields()) {
						transformation.add(new OraColumn(unnestField, false, false));
					}
					lobColumns.put(field.name(), transformation);
				} else {
					final OraColumn column = new OraColumn(field, false, false);
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
	}

	private void buildLobColsSql(final Map<String, String> sqlTexts) {
		if (lobColumns.size() > 0) {
			lobColsSqlMap = new HashMap<>();
			lobColumns.forEach((columnName, v) -> {
				LobSqlHolder holder = new LobSqlHolder();
				holder.COLUMN = columnName;
				holder.EXEC_COUNT = 0;
				holder.SQL_TEXT = sqlTexts.get(columnName);
				lobColsSqlMap.put(columnName, holder);
				LOGGER.debug("\tLOB column {}.{}, UPDATE statement ->\n{}",
						this.tableName, columnName, holder.SQL_TEXT);
			});
		}
	}

	private void createTable(final Connection connection, final SinkRecord record, final int pkStringLength) throws SQLException {
		LOGGER.debug("Prepare to create table {}", this.tableName);

		final Entry<List<Field>, List<Field>> keyValue = getFieldsFromSinkRecord(record);
		final List<Field> keyFields = keyValue.getKey();
		final List<Field> valueFields = keyValue.getValue();
		if (!onlyValue) {
			for (Field field : keyFields) {
				final OraColumn column = new OraColumn(field, true, true);
				pkColumns.put(column.getColumnName(), column);
			}
		}

		// Only non PK columns!!!
		buildNonPkColsList(valueFields);

		List<String> sqlCreateTexts = TargetDbSqlUtils.createTableSql(
				tableName, dbType, pkStringLength, pkColumns, allColumns, lobColumns);
		if (dbType == DB_TYPE_POSTGRESQL &&
				sqlCreateTexts.size() > 1) {
			for (int i = 1; i < sqlCreateTexts.size(); i++) {
				LOGGER.debug("\tPostgreSQL lo trigger:\n\t{}", sqlCreateTexts.get(i));
			}
		}
		boolean createLoTriggerFailed = false;
		try {
			Statement statement = connection.createStatement();
			statement.executeUpdate(sqlCreateTexts.get(0));
			LOGGER.info(
					"""
					
					=====================
					Table '{}' created in the target database using:
					{}
					=====================
					""",
						tableName, sqlCreateTexts.get(0));
			if (dbType == DB_TYPE_POSTGRESQL &&
					sqlCreateTexts.size() > 1) {
				for (int i = 1; i < sqlCreateTexts.size(); i++) {
					try {
						statement.executeUpdate(sqlCreateTexts.get(i));
					} catch (SQLException pge) {
						createLoTriggerFailed = true;
						LOGGER.error("Trigger creation has failed! Failed creation statement:\n");
						LOGGER.error(sqlCreateTexts.get(i));
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(pge));
						throw pge;
					}
				}
			}
			connection.commit();
			exists = true;
		} catch (SQLException sqle) {
			if (!createLoTriggerFailed) {
				LOGGER.error("Table creation has failed! Failed creation statement:\n");
				LOGGER.error(sqlCreateTexts.get(0));
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			}
			throw sqle;
		}
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
				Iterator<Entry<String, OraColumn>> iterator = pkColumns.entrySet().iterator();
				while (iterator.hasNext()) {
					final OraColumn oraColumn = iterator.next().getValue();
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

	private Entry<Set<String>, ResultSet> checkPresence(final Connection connection) throws SQLException {
		LOGGER.debug("Check for table {} in database", this.tableName);
		final DatabaseMetaData metaData = connection.getMetaData();
		final String schema;
		if (dbType == DB_TYPE_POSTGRESQL) {
			final PreparedStatement psSchema = connection.prepareStatement("select CURRENT_SCHEMA",
						ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			final ResultSet rsSchema = psSchema.executeQuery();
			if (rsSchema.next()) {
				schema = rsSchema.getString(1);
			} else {
				throw new SQLException("Unable to execute 'select CURRENT_SCHEMA'!");
			}
			rsSchema.close();
			psSchema.close();
		} else {
			//TODO - Microsoft SQL Server!
			schema = null;
		}
		final Entry<Set<String>, ResultSet> result;
		final String[] tableTypes = {"TABLE", "PARTITIONED TABLE"};
		ResultSet resultSet = metaData.getTables(null, schema, tableNameCaseConv, tableTypes);
		if (resultSet.next()) {
			final String catalog = resultSet.getString("TABLE_CAT");
			final String dbSchema = resultSet.getString("TABLE_SCHEM"); 
			final String dbTable = resultSet.getString("TABLE_NAME"); 
			LOGGER.info(
					"""
					
					=====================
					Table '{}' already exists with type '{}' in catalog '{}', schema '{}'.
					=====================
					""",
						dbTable, resultSet.getString("TABLE_TYPE"), catalog, dbSchema);
			exists = true;
			final Set<String> pkFields;
			if (dbType == DB_TYPE_POSTGRESQL) {
				pkFields = PgRdbmsInfo.getPkColumnsFromDict(connection, dbSchema, dbTable);
			} else {
				//TODO
				//TODO Additional testing required for non PG destinations
				//TODO
				pkFields = new HashSet<>();
				ResultSet rsPk = metaData.getPrimaryKeys(catalog, dbSchema, dbTable); 
				while (rsPk.next()) {
					pkFields.add(rsPk.getString("COLUMN_NAME"));
				}						
			}
			result = Map.entry(pkFields,
					metaData.getColumns(catalog, dbSchema, dbTable, null));
		} else {
			exists = false;
			result = null;
		}
		resultSet.close();
		resultSet = null;
		return result;
	}

	private Entry<List<Field>, List<Field>> getFieldsFromSinkRecord(final SinkRecord record) {
		final List<Field> keyFields;
		final List<Field> valueFields;
		if (this.schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
			LOGGER.debug("Schema type set to Debezium style.");
			keyFields = record.valueSchema().field("before").schema().fields();
			valueFields = record.valueSchema().field("after").schema().fields();
			version = record.valueSchema().version();
		} else {
			//ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD
			//ParamConstants.SCHEMA_TYPE_INT_SINGLE
			LOGGER.debug("Schema type set to Kafka Connect.");
			if (record.keySchema() == null) {
				keyFields = new ArrayList<>();
				onlyValue = true;
			} else {
				keyFields = record.keySchema().fields();
			}
			if (record.valueSchema() != null) {
				valueFields = record.valueSchema().fields();
				version = record.valueSchema().version();
			} else {
				valueFields = new ArrayList<>();
				version = 1;
			}
		}
		return Map.entry(keyFields, valueFields);
	}

	private Entry<Struct, Struct> getStructsFromSinkRecord(final SinkRecord record) {
		final Struct keyStruct;
		final Struct valueStruct;
		if (this.schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
			LOGGER.debug("Schema type set to Debezium style.");
			keyStruct = ((Struct) record.value()).getStruct("before");
			valueStruct = ((Struct) record.value()).getStruct("after");
		} else {
			//ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD
			//ParamConstants.SCHEMA_TYPE_INT_SINGLE
			LOGGER.debug("Schema type set to Kafka Connect.");
			if (record.key() == null) {
				keyStruct = DUMMY_STRUCT;
			} else {
				keyStruct = (Struct) record.key();
			}
			if (record.value() == null) {
				valueStruct = DUMMY_STRUCT;
			} else {
				valueStruct = (Struct) record.value();
			}
		}
		return Map.entry(keyStruct, valueStruct);
	}

	private static class LobSqlHolder {
		private String COLUMN;
		private String SQL_TEXT;
		private PreparedStatement STATEMENT;
		private int EXEC_COUNT;
	}

}
