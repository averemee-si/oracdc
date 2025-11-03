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
import static java.sql.Types.NUMERIC;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM;
import static solutions.a2.kafka.sink.JdbcSinkConnectionPool.DB_TYPE_POSTGRESQL;
import static solutions.a2.cdc.oracle.data.JdbcTypes.getTypeName;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.postgres.PgRdbmsInfo;
import solutions.a2.kafka.ConnectorParams;


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
	String tableNameCaseConv;

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
