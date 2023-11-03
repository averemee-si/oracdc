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

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.jmx.OraCdcSinkTableInfo;
import solutions.a2.cdc.oracle.schema.JdbcTypes;
import solutions.a2.cdc.oracle.utils.ExceptionUtils;
import solutions.a2.cdc.oracle.utils.Lz4Util;
import solutions.a2.cdc.oracle.utils.TargetDbSqlUtils;


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
	private long upsertTime;
	private long deleteTime;
	private final boolean onlyPkColumns;
	private final Map<String, Object> lobColumns = new HashMap<>();
	private Map<String, LobSqlHolder> lobColsSqlMap;


	/**
	 * This constructor is used only for Sink connector
	 *
	 * @param sinkPool
	 * @param tableName
	 * @param record
	 * @param pkStringLength
	 * @param autoCreateTable
	 * @param schemaType
	 * @throws SQLException 
	 */
	public OraTable4SinkConnector(
			final OraCdcJdbcSinkConnectionPool sinkPool, final String tableName,
			final SinkRecord record, final int pkStringLength, final boolean autoCreateTable,
			final int schemaType) throws SQLException {
		super(schemaType);
		dbType = sinkPool.getDbType();
		LOGGER.trace("Creating OraTable object from Kafka connect SinkRecord...");
		final List<Field> keyFields;
		final List<Field> valueFields;
		if (this.schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			LOGGER.debug("Schema type set to Kafka Connect.");
			this.tableOwner = "oracdc";
			this.tableName = tableName;
			keyFields = record.keySchema().fields();
			if (record.valueSchema() != null) {
				valueFields = record.valueSchema().fields();
			} else {
				LOGGER.warn("Value schema is NULL for table {}.", this.tableName);
				valueFields = new ArrayList<>();
			}
		} else {	// if (this.schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM)
			LOGGER.debug("Schema type set to Debezium style.");
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
				if (StringUtils.equals("struct", field.schema().type().getName())) {
					final List<OraColumn> transformation = new ArrayList<>();
					for (Field unnestField : field.schema().fields()) {
						transformation.add(new OraColumn(unnestField, true));
					}
					lobColumns.put(field.name(), transformation);
				} else {
					final OraColumn column = new OraColumn(field, false);
					if (column.getJdbcType() == Types.BLOB ||
						column.getJdbcType() == Types.CLOB ||
						column.getJdbcType() == Types.NCLOB ||
						column.getJdbcType() == Types.SQLXML) {
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
						oraColumn.getColumnName(), JdbcTypes.getTypeName(oraColumn.getJdbcType()));
			});
		} else {
			onlyPkColumns = false;
		}
		metrics = new OraCdcSinkTableInfo(this.tableName);
		prepareSql(sinkPool, pkStringLength, autoCreateTable);
		upsertCount = 0;
		deleteCount = 0;
		upsertTime = 0;
		deleteTime = 0;
	}


	private void prepareSql(final OraCdcJdbcSinkConnectionPool sinkPool,
							final int pkStringLength,
							final boolean autoCreateTable) throws SQLException {
		// Prepare UPDATE/INSERT/DELETE statements...
		LOGGER.debug("Prepare UPDATE/INSERT/DELETE statements for table {}", this.tableName);
		final Map<String, String> sqlTexts = TargetDbSqlUtils.generateSinkSql(
				tableName, dbType, pkColumns, allColumns, lobColumns);
		sinkUpsertSql = sqlTexts.get(TargetDbSqlUtils.UPSERT);
		sinkDeleteSql = sqlTexts.get(TargetDbSqlUtils.DELETE);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Table name -> {}, UPSERT statement ->\n{}", this.tableName, sinkUpsertSql);
			LOGGER.debug("Table name -> {}, DELETE statement ->\n{}", this.tableName, sinkDeleteSql);
		}
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

		// Check for table existence
		try (Connection connection = sinkPool.getConnection()) {
			LOGGER.debug("Check for table {} in database", this.tableName);
			DatabaseMetaData metaData = connection.getMetaData();
			String tableNameCaseConv = tableName;
			if (dbType == OraCdcJdbcSinkConnectionPool.DB_TYPE_POSTGRESQL) {
				LOGGER.debug("Working with PostgreSQL specific lower case only names");
				// PostgreSQL specific...
				// Also look at https://stackoverflow.com/questions/43111996/why-postgresql-does-not-like-uppercase-table-names
				tableNameCaseConv = tableName.toLowerCase();
			}
			final String schema;
			if (dbType == OraCdcJdbcSinkConnectionPool.DB_TYPE_POSTGRESQL) {
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
			final String[] tableTypes = {"TABLE"};
			ResultSet resultSet = metaData.getTables(null, schema, tableNameCaseConv, tableTypes);
			if (resultSet.next()) {
				LOGGER.info(
						"\n" +
						"=====================\n" +
						"Table '{}' already exists with type '{}' in catalog '{}', schema '{}'.\n" +
						"=====================",
						resultSet.getString("TABLE_NAME"),
						resultSet.getString("TABLE_TYPE"),
						resultSet.getString("TABLE_CAT"),
						resultSet.getString("TABLE_SCHEM"));
				ready4Ops = true;
			} else {
				if (autoCreateTable) {
					LOGGER.info(
							"\n" +
							"=====================\n" +
							"Table '{}' will be created in the target database.\n" +
							"=====================",
							tableNameCaseConv);
				} else {
					LOGGER.error(
							"\n" +
							"=====================\n" +
							"Table '{}' does not exist in the target database and a2.autocreate=false!\n" +
							"=====================",
							tableNameCaseConv);
				}
			}
			resultSet.close();
			resultSet = null;
		} catch (SQLException sqle) {
			ready4Ops = false;
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
		}
		if (!ready4Ops && autoCreateTable) {
			// Create table in target database
			LOGGER.debug("Prepare to create table {}", this.tableName);
			List<String> sqlCreateTexts = TargetDbSqlUtils.createTableSql(
					tableName, dbType, pkStringLength, pkColumns, allColumns, lobColumns);
			if (dbType == OraCdcJdbcSinkConnectionPool.DB_TYPE_POSTGRESQL &&
					sqlCreateTexts.size() > 1) {
				for (int i = 1; i < sqlCreateTexts.size(); i++) {
					LOGGER.debug("\tPostgreSQL lo trigger:\n\t{}", sqlCreateTexts.get(i));
				}
			}
			boolean createLoTriggerFailed = false;
			try (Connection connection = sinkPool.getConnection()) {
				Statement statement = connection.createStatement();
				statement.executeUpdate(sqlCreateTexts.get(0));
				LOGGER.info(
						"\n" +
						"=====================\n" +
						"Table '{}' created in the target database using:\n{}" +
						"=====================",
						tableName, sqlCreateTexts.get(0));
				if (dbType == OraCdcJdbcSinkConnectionPool.DB_TYPE_POSTGRESQL &&
						sqlCreateTexts.size() > 1) {
					for (int i = 1; i < sqlCreateTexts.size(); i++) {
						try {
							statement.executeUpdate(sqlCreateTexts.get(i));
						} catch (SQLException pge) {
							createLoTriggerFailed = true;
							LOGGER.error("Trigger creation has failed! Failed creation statement:\n");
							LOGGER.error(sqlCreateTexts.get(i));
							LOGGER.error(ExceptionUtils.getExceptionStackTrace(pge));
							throw new SQLException(pge);
						}
					}
				}
				connection.commit();
				ready4Ops = true;
			} catch (SQLException sqle) {
				ready4Ops = false;
				if (!createLoTriggerFailed) {
					LOGGER.error("Table creation has failed! Failed creation statement:\n");
					LOGGER.error(sqlCreateTexts.get(0));
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
				}
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
			deleteTime += System.nanoTime() - nanosStart;
		} else {
			processUpsert(connection, record);
			upsertTime += System.nanoTime() - nanosStart;
		}
		LOGGER.trace("END: putData");
	}

	public void exec() throws SQLException {
		LOGGER.trace("BEGIN: exec()");
		final long nanosStart = System.nanoTime();
		if (sinkUpsert != null && upsertCount > 0) {
			execUpsert();
			sinkUpsert.clearBatch();
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
		LOGGER.trace("END: exec()");
	}

	public void execAndCloseCursors() throws SQLException {
		LOGGER.trace("BEGIN: closeCursors()");
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
		LOGGER.trace("END: closeCursors()");
	}

	private void execUpsert() throws SQLException {
		try {
			sinkUpsert.executeBatch();
		} catch(SQLException sqle) {
			boolean raiseException = true;
			if (dbType == OraCdcJdbcSinkConnectionPool.DB_TYPE_ORACLE) {
				if (onlyPkColumns && sqle.getErrorCode() == 1) {
					// ORA-00001: unique constraint %s violated
					// ignore for tables with PK only column(s)
					raiseException = false;
					LOGGER.warn(sqle.getMessage());
				}
			} else if (dbType == OraCdcJdbcSinkConnectionPool.DB_TYPE_MYSQL) {
				if (onlyPkColumns && StringUtils.startsWith(sqle.getMessage(), "Duplicate entry")) {
					// Duplicate entry 'XXX' for key 'YYYYY'
					// ignore for tables with PK only column(s)
					raiseException = false;
					LOGGER.warn(sqle.getMessage());
				}
			}
			if (raiseException) {
				LOGGER.error("Error while executing UPSERT statement {}", sinkUpsertSql);
				throw new SQLException(sqle);
			}
		}
	}

	private void execDelete() throws SQLException {
		try {
			sinkDelete.executeBatch();
		} catch(SQLException sqle) {
			LOGGER.error("Error while executing DELETE statement {}", sinkUpsertSql);
			throw new SQLException(sqle);
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
					LOGGER.error("Error {} while executing LOB update statement {}",
							sqle.getMessage(), holder.SQL_TEXT);
					throw new SQLException(sqle);
				}
			}
		}
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
			upsertCount = 0;
			upsertTime = 0;
		}
		int columnNo = 1;
		Iterator<Entry<String, OraColumn>> iterator = pkColumns.entrySet().iterator();
		while (iterator.hasNext()) {
			final OraColumn oraColumn = iterator.next().getValue();
			try {
				oraColumn.bindWithPrepStmt(dbType, sinkUpsert, columnNo, keyStruct.get(oraColumn.getColumnName()));
				columnNo++;
			} catch (DataException de) {
				LOGGER.error("Data error while performing upsert! Table={}, PK column={}, {}.",
						tableName, oraColumn.getColumnName(), structValueAsString(oraColumn, keyStruct));
				throw new DataException(de);
			}
		}
		for (int i = 0; i < allColumns.size(); i++) {
			final OraColumn oraColumn = allColumns.get(i);
			if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD ||
					(schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM && !oraColumn.isPartOfPk())) {
				try {
					oraColumn.bindWithPrepStmt(dbType, sinkUpsert, columnNo, valueStruct.get(oraColumn.getColumnName()));
					columnNo++;
				} catch (DataException | SQLException de) {
					LOGGER.error("Data error while performing upsert! Table={}, column={}, {}.",
							tableName, oraColumn.getColumnName(), structValueAsString(oraColumn, valueStruct));
					LOGGER.error("SQL statement:\n\t{}", sinkUpsertSql);
					LOGGER.error("PK value(s) for this row in table {} are", tableName);
					int colNo = 1;
					Iterator<Entry<String, OraColumn>> pkIterator = pkColumns.entrySet().iterator();
					while (pkIterator.hasNext()) {
						OraColumn pkColumn = pkIterator.next().getValue();
						LOGGER.error("\t{}) PK column {}, {}",
								colNo, pkColumn.getColumnName(), structValueAsString(pkColumn, keyStruct));
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
				final Object objLobValue = valueStruct.get(holder.COLUMN);
				if (objLobValue != null) {
					//NULL means do not touch LOB!
					if (holder.STATEMENT == null) {
						holder.STATEMENT = connection.prepareStatement(holder.SQL_TEXT);
						holder.EXEC_COUNT = 0;
					}
					if (objLobColumn instanceof OraColumn) {
//						final byte[] columnByteValue = ((ByteBuffer) objLobValue).array();
						final byte[] columnByteValue = (byte[]) objLobValue;
						final int lobColType = ((OraColumn)objLobColumn).getJdbcType();
						try {
							if (columnByteValue.length == 0) {
								holder.STATEMENT.setNull(1, lobColType);
							} else {
								if (lobColType == Types.BLOB) {
									holder.STATEMENT.setBinaryStream(
											1, new ByteArrayInputStream(columnByteValue), columnByteValue.length);
								} else {
									// Types.CLOB || Types.NCLOB
									holder.STATEMENT.setCharacterStream(
//											1, new StringReader(GzipUtil.decompress(columnByteValue)));
											1, new StringReader(Lz4Util.decompress(columnByteValue)));
								}
							}
							// Bind PK columns...
							columnNo = 2;
							iterator = pkColumns.entrySet().iterator();
							while (iterator.hasNext()) {
								final OraColumn oraColumn = iterator.next().getValue();
								oraColumn.bindWithPrepStmt(
										dbType, holder.STATEMENT, columnNo, keyStruct.get(oraColumn.getColumnName()));
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
									dbType, holder.STATEMENT, columnNo, transformedStruct.get(transformedColumn.getColumnName()));
							columnNo++;
						}
						// Bind PK columns...
						iterator = pkColumns.entrySet().iterator();
						while (iterator.hasNext()) {
							final OraColumn oraColumn = iterator.next().getValue();
							oraColumn.bindWithPrepStmt(
									dbType, holder.STATEMENT, columnNo, keyStruct.get(oraColumn.getColumnName()));
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
		final Struct keyStruct;
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			keyStruct = (Struct) record.key();
		} else { // if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM)
			keyStruct = ((Struct) record.value()).getStruct("before");
		}
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
				oraColumn.bindWithPrepStmt(dbType, sinkDelete, columnNo, keyStruct.get(oraColumn.getColumnName()));
				columnNo++;
			} catch (DataException de) {
				LOGGER.error("Data error while performing delete! Table {}, PK column {}, {}.",
						tableName, oraColumn.getColumnName(), structValueAsString(oraColumn, keyStruct));
				throw new DataException(de);
			}
		}
		sinkDelete.addBatch();
		deleteCount++;
		LOGGER.trace("END: processDelete()");
	}

	public String structValueAsString(final OraColumn oraColumn, final Struct struct) {
		final StringBuilder sb = new StringBuilder(128);
		sb.append("Column Type =");
		sb.append(JdbcTypes.getTypeName(oraColumn.getJdbcType()));
		sb.append(", Column Value='");
		switch (oraColumn.getJdbcType()) {
			case Types.NUMERIC:
			case Types.BINARY:
			case Types.BLOB:
				ByteBuffer bb = (ByteBuffer) struct.get(oraColumn.getColumnName());
				sb.append(OraDumpDecoder.toHexString(bb.array()));
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

	private class LobSqlHolder {
		protected String COLUMN;
		protected String SQL_TEXT;
		protected PreparedStatement STATEMENT;
		protected int EXEC_COUNT;
	}

}
