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

import static java.sql.Types.BINARY;
import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.SQLXML;
import static oracle.jdbc.OracleTypes.JSON;
import static oracle.jdbc.OracleTypes.VECTOR;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM;
import static solutions.a2.kafka.sink.JdbcSinkConnectionPool.DB_TYPE_POSTGRESQL;
import static solutions.a2.cdc.oracle.data.JdbcTypes.getTypeName;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_PREFIX;
import static solutions.a2.kafka.sink.JdbcSinkConnectorConfig.CONNECTOR_REPLICATE;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.data.OraBlob;
import solutions.a2.cdc.oracle.data.OraClob;
import solutions.a2.cdc.oracle.data.OraJson;
import solutions.a2.cdc.oracle.data.OraNClob;
import solutions.a2.cdc.oracle.data.OraVector;
import solutions.a2.cdc.oracle.data.OraXml;
import solutions.a2.cdc.postgres.PgRdbmsInfo;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.utils.ExceptionUtils;


/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public abstract class JdbcSinkTableBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkTableBase.class);
	private static final Struct DUMMY_STRUCT =
			new Struct(SchemaBuilder
							.struct()
							.optional()
							.build());


	String tableOwner;
	String tableName;
	final int schemaType;
	final int dbType;
	int version;
	List<JdbcSinkColumn> allColumns;
	Map<String, JdbcSinkColumn> pkColumns;
	final Map<String, Object> lobColumns = new HashMap<>();
	Map<String, LobSqlHolder> lobColsSqlMap;
	boolean exists = true;
	boolean onlyValue = false;
	boolean onlyPkColumns;
	String tableNameCaseConv;
	int connectorMode = CONNECTOR_REPLICATE;
	final Map<String, JdbcSinkColumn> allColsMap = new HashMap<>();

	JdbcSinkTableBase(final int schemaType, final int dbType) {
		this.pkColumns = new LinkedHashMap<>();
		this.schemaType = schemaType;
		this.allColumns = new ArrayList<>();
		this.version = 1;
		this.dbType = dbType;
	}

	String tableName() {
		return tableName;
	}

	static String structValueAsString(final JdbcSinkColumn column, final Struct struct) {
		final var sb = new StringBuilder(0x80);
		sb
			.append("Column Type =")
			.append(getTypeName(column.getJdbcType()))
			.append(", Column Value='");
		switch (column.getJdbcType()) {
			case NUMERIC, BINARY -> {
				ByteBuffer bb = (ByteBuffer) struct.get(column.getColumnName());
				sb.append(rawToHex(bb.array()));
			}
			default -> sb.append(struct.get(column.getColumnName()));
		}
		sb.append("'");
		return sb.toString();
	}

	char getOpType(final SinkRecord record) {
		var opType = 'c';
		if (schemaType == SCHEMA_TYPE_INT_DEBEZIUM) {
			opType = ((Struct) record.value())
							.getString("op")
							.charAt(0);
			LOGGER.debug("Operation type set payload to {}.", opType);
		} else {
			//SCHEMA_TYPE_INT_KAFKA_STD
			//SCHEMA_TYPE_INT_SINGLE
			var iterator = record.headers().iterator();
			while (iterator.hasNext()) {
				var header = iterator.next();
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

	Entry<Set<String>, ResultSet> checkPresence(final Connection connection) throws SQLException {
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
					
					""", dbTable, resultSet.getString("TABLE_TYPE"), catalog, dbSchema);
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

	void createTable(final Connection connection, final SinkRecord record, final int pkStringLength) throws SQLException {
		LOGGER.debug("Prepare to create table {}", this.tableName);

		final Entry<List<Field>, List<Field>> keyValue = getFieldsFromSinkRecord(record);
		final List<Field> keyFields = keyValue.getKey();
		final List<Field> valueFields = keyValue.getValue();
		if (!onlyValue) {
			for (Field field : keyFields) {
				final var column = new JdbcSinkColumn(field, true);
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
					
					""", tableName, sqlCreateTexts.get(0));
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

	private void buildNonPkColsList(final List<Field> valueFields) throws SQLException {
		for (final Field field : valueFields) {
			if (!pkColumns.containsKey(field.name())) {
				if (Strings.CS.equals("struct", field.schema().type().getName()) &&
						!Strings.CS.startsWithAny(field.schema().name(),
								OraBlob.LOGICAL_NAME,
								OraClob.LOGICAL_NAME,
								OraNClob.LOGICAL_NAME,
								OraXml.LOGICAL_NAME,
								OraJson.LOGICAL_NAME,
								OraVector.LOGICAL_NAME,
								WRAPPED_PREFIX)) {
					final List<JdbcSinkColumn> transformation = new ArrayList<>();
					for (Field unnestField : field.schema().fields()) {
						transformation.add(new JdbcSinkColumn(unnestField, false));
					}
					lobColumns.put(field.name(), transformation);
				} else {
					final var column = new JdbcSinkColumn(field, false);
					if (column.getJdbcType() == BLOB ||
						column.getJdbcType() == CLOB ||
						column.getJdbcType() == NCLOB ||
						column.getJdbcType() == SQLXML ||
						column.getJdbcType() == JSON ||
						column.getJdbcType() == VECTOR) {
						lobColumns.put(column.getColumnName(), column);
					} else {
						allColumns.add(column);
						allColsMap.put(column.getColumnName(), column);
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

	void buildLobColsSql(final Map<String, String> sqlTexts) {
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

	Entry<List<Field>, List<Field>> getFieldsFromSinkRecord(final SinkRecord record) {
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

	Entry<Struct, Struct> getStructsFromSinkRecord(final SinkRecord record) {
		final Struct keyStruct;
		final Struct valueStruct;
		if (this.schemaType == SCHEMA_TYPE_INT_DEBEZIUM) {
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


	void execLobUpdate(final boolean closeCursor) throws SQLException {
		if (lobColumns.size() > 0) {
			var lobIterator = lobColsSqlMap.entrySet().iterator();
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

	static class LobSqlHolder {
		String COLUMN;
		String SQL_TEXT;
		PreparedStatement STATEMENT;
		int EXEC_COUNT;
	}


}
