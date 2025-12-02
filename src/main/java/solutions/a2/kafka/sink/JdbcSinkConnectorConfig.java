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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.lang3.Strings;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static solutions.a2.kafka.ConnectorParams.CONNECTION_URL_PARAM;
import static solutions.a2.kafka.ConnectorParams.CONNECTION_URL_DOC;
import static solutions.a2.kafka.ConnectorParams.CONNECTION_USER_PARAM;
import static solutions.a2.kafka.ConnectorParams.CONNECTION_USER_DOC;
import static solutions.a2.kafka.ConnectorParams.CONNECTION_PASSWORD_PARAM;
import static solutions.a2.kafka.ConnectorParams.CONNECTION_PASSWORD_DOC;
import static solutions.a2.kafka.ConnectorParams.BATCH_SIZE_PARAM;
import static solutions.a2.kafka.ConnectorParams.BATCH_SIZE_DOC;
import static solutions.a2.kafka.ConnectorParams.BATCH_SIZE_DEFAULT;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_PARAM;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_DOC;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_KAFKA;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_INT_KAFKA_STD;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_DEBEZIUM;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_SINGLE;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_INT_SINGLE;
import static solutions.a2.kafka.ConnectorParams.USE_ALL_COLUMNS_ON_DELETE_PARAM;
import static solutions.a2.kafka.ConnectorParams.USE_ALL_COLUMNS_ON_DELETE_DOC;
import static solutions.a2.kafka.ConnectorParams.USE_ALL_COLUMNS_ON_DELETE_DEFAULT;
import static solutions.a2.kafka.ConnectorParams.TOPIC_PREFIX_PARAM;
import static solutions.a2.kafka.ConnectorParams.TOPIC_PREFIX_DOC;
import static solutions.a2.utils.ExceptionUtils.getExceptionStackTrace;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class JdbcSinkConnectorConfig extends AbstractConfig {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectorConfig.class);

	private static final String AUTO_CREATE_PARAM = "a2.autocreate";
	private static final String AUTO_CREATE_DOC =
			"""
			Automatically create the destination table if missed.
			""";

	private static final int PK_STRING_LENGTH_DEFAULT = 30;
	private static final String PK_STRING_LENGTH_PARAM = "a2.pk.string.length";
	private static final String PK_STRING_LENGTH_DOC =
			"""
			The length of the string by default when it is used as a part of primary key.
			"Derfault - """ + PK_STRING_LENGTH_DEFAULT;

	private static final String TABLE_NAME_PREFIX_PARAM = "a2.table.name.prefix";
	private static final String TABLE_NAME_PREFIX_DOC =
			"""
			Prefix to prepend to table name.
			Default - "" (Empty string - no prefix)
			""";

	private static final String TABLE_NAME_SUFFIX_PARAM = "a2.table.name.suffix";
	private static final String TABLE_NAME_SUFFIX_DOC =
			"""
			Suffix to append to table name.
			Default - "" (Empty string - no suffix)
			""";

	private static final String TABLE_MAPPER_DEFAULT = "solutions.a2.kafka.sink.DefaultTableNameMapper";
	private static final String TABLE_MAPPER_PARAM = "a2.table.mapper";
	private static final String TABLE_MAPPER_DOC =
			"""
			The fully-qualified class name of the class that specifies which table in which to sink the data.
			If value of thee parameter 'a2.shema.type' is set to 'debezium', the default DefaultTableNameMapper uses the 'source'.'table' field value from Sinkrecord,
			otherwise it constructs the table name as the Kafka topic name without the prefix specified by the 'a2.topic.prefix' parameter.
			If the values of the parameters 'a2.table.name.prefix' and/or 'a2.table.name.suffix' are specified, then the values of these parameters are added to the table name, respectively, either at the beginning or at the end.
			Default - """ + TABLE_MAPPER_DEFAULT;

	static final int CONNECTOR_REPLICATE = 1;
	static final int CONNECTOR_AUDIT_TRAIL = 2;
	private static final String CONN_TYPE_PARAM = "a2.sink.connector.mode";
	private static final String CONN_TYPE_REPLICATE = "replicate";
	private static final String CONN_TYPE_AUDIT_TRAIL = "audit_trail";
	private static final String CONN_TYPE_DOC =
			"""
			Connector operating mode - 'replicate' or 'audit_trail'.
			In 'replicate' mode, the connector sends INSERT/UPDATE/DELETE commands to the target database,
			and in 'audit_trail' mode, it only sends INSERT commands to record the change history of the source table.
			Default - """ + CONN_TYPE_REPLICATE;

	private static final String INIT_SQL_PARAM = "a2.connection.init.sql";
	private static final String INIT_SQL_DOC = 
			"""
			Set the SQL statement that will be executed on all new connections when they are created.
			For example, to override the PostgreSQL server setting for the work_mem parameter, use
				"a2.connection.init.sql" : "SET work_mem = '16MB'"
			""";

	private int schemaType = -1;
	private int connectorMode = -1;

	static ConfigDef config() {
		return new ConfigDef()
				.define(CONNECTION_URL_PARAM, Type.STRING,
						Importance.HIGH, CONNECTION_URL_DOC)
				.define(CONNECTION_USER_PARAM, Type.STRING,
						Importance.HIGH, CONNECTION_USER_DOC)
				.define(CONNECTION_PASSWORD_PARAM, Type.PASSWORD,
						Importance.HIGH, CONNECTION_PASSWORD_DOC)
				.define(BATCH_SIZE_PARAM, Type.INT,
						BATCH_SIZE_DEFAULT,
						Importance.HIGH, BATCH_SIZE_DOC)
				.define(SCHEMA_TYPE_PARAM, Type.STRING,
						SCHEMA_TYPE_KAFKA,
						ConfigDef.ValidString.in(SCHEMA_TYPE_KAFKA, SCHEMA_TYPE_DEBEZIUM, SCHEMA_TYPE_SINGLE),
						Importance.HIGH, SCHEMA_TYPE_DOC)
				.define(AUTO_CREATE_PARAM, Type.BOOLEAN, false,
						Importance.HIGH, AUTO_CREATE_DOC)
				.define(PK_STRING_LENGTH_PARAM, Type.INT, PK_STRING_LENGTH_DEFAULT,
						Importance.LOW, PK_STRING_LENGTH_DOC)
				.define(USE_ALL_COLUMNS_ON_DELETE_PARAM,
						Type.BOOLEAN, USE_ALL_COLUMNS_ON_DELETE_DEFAULT,
						Importance.MEDIUM, USE_ALL_COLUMNS_ON_DELETE_DOC)
				.define(TOPIC_PREFIX_PARAM, Type.STRING, "",
						Importance.MEDIUM, TOPIC_PREFIX_DOC)
				.define(TABLE_NAME_PREFIX_PARAM, Type.STRING, "",
						Importance.LOW, TABLE_NAME_PREFIX_DOC)
				.define(TABLE_NAME_SUFFIX_PARAM, Type.STRING, "",
						Importance.LOW, TABLE_NAME_SUFFIX_DOC)
				.define(TABLE_MAPPER_PARAM, Type.STRING,
						TABLE_MAPPER_DEFAULT,
						Importance.MEDIUM, TABLE_MAPPER_DOC)
				.define(CONN_TYPE_PARAM, Type.STRING,
						CONN_TYPE_REPLICATE,
						ConfigDef.ValidString.in(CONN_TYPE_REPLICATE, CONN_TYPE_AUDIT_TRAIL),
						Importance.HIGH, CONN_TYPE_DOC)
				.define(INIT_SQL_PARAM, Type.STRING, "",
						Importance.LOW, INIT_SQL_DOC)
				;
	}

	JdbcSinkConnectorConfig(Map<?, ?> originals) {
		super(config(), originals);
	}

	boolean autoCreateTable() {
		return getBoolean(AUTO_CREATE_PARAM);
	}

	int getPkStringLength() {
		return getInt(PK_STRING_LENGTH_PARAM);
	}

	boolean useAllColsOnDelete() {
		return getBoolean(USE_ALL_COLUMNS_ON_DELETE_PARAM);
	}

	int getSchemaType() {
		if (schemaType == -1) {
			switch (getString(SCHEMA_TYPE_PARAM)) {
				case SCHEMA_TYPE_KAFKA -> schemaType = SCHEMA_TYPE_INT_KAFKA_STD;
				case SCHEMA_TYPE_SINGLE ->  schemaType = SCHEMA_TYPE_INT_SINGLE;
				case SCHEMA_TYPE_DEBEZIUM -> schemaType = SCHEMA_TYPE_INT_DEBEZIUM;
			}
		}
		return schemaType;
	}

	TableNameMapper getTableNameMapper() {
		final TableNameMapper tnm;
		final Class<?> clazz;
		final Constructor<?> constructor;
		try {
			clazz = Class.forName(getString(TABLE_MAPPER_PARAM));
		} catch (ClassNotFoundException nfe) {
			LOGGER.error(
					"""
					
					=====================
					Class '{}' specified as the parameter '{}' value was not found.
					{}
					=====================
					
					""", getString(TABLE_MAPPER_PARAM), TABLE_MAPPER_PARAM, getExceptionStackTrace(nfe));
			throw new ConnectException(nfe);
		}
		try {
			constructor = clazz.getConstructor();
		} catch (NoSuchMethodException nsme) {
			LOGGER.error(
					"""
					
					=====================
					Unable to get default constructor for the class '{}'.
					{}
					=====================
					
					""", getString(TABLE_MAPPER_PARAM), getExceptionStackTrace(nsme));
			throw new ConnectException(nsme);
		} 
		
		try {
			tnm = (TableNameMapper) constructor.newInstance();
		} catch (SecurityException | 
				InvocationTargetException | 
				IllegalAccessException | 
				InstantiationException e) {
			LOGGER.error(
					"""
					
					=====================
					'{}' while instantinating the class '{}'.
					{}
					=====================
					
					""", e.getMessage(), getString(TABLE_MAPPER_PARAM), getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
		return tnm;
	}

	String getTableNamePrefix() {
		return getString(TABLE_NAME_PREFIX_PARAM);
	}

	String getTableNameSuffix() {
		return getString(TABLE_NAME_SUFFIX_PARAM);
	}

	int getConnectorMode() {
		if (connectorMode == -1) {
			if (Strings.CI.equals(CONN_TYPE_REPLICATE, getString(CONN_TYPE_PARAM))) {
				connectorMode = CONNECTOR_REPLICATE;
			} else {
				// CONN_TYPE_AUDIT_TRAIL
				connectorMode = CONNECTOR_AUDIT_TRAIL; 
			}
		}
		return connectorMode;
	}

	String getJdbcUrl() {
		return getString(CONNECTION_URL_PARAM);
	}

	String getJdbcUser() {
		return getString(CONNECTION_USER_PARAM);
	}

	String getJdbcPassword() {
		return getPassword(CONNECTION_PASSWORD_PARAM).value();
	}

	String getInitSql() {
		return getString(INIT_SQL_PARAM);
	}

	int batchSize() {
		return getInt(BATCH_SIZE_PARAM);
	}

}
