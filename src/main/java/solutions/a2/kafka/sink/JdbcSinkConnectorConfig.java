/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.kafka.sink;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Strings;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import solutions.a2.cdc.TargetDbConfig;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.BATCH_SIZE_DEFAULT;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.BATCH_SIZE_DOC;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.BATCH_SIZE_PARAM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.CONNECTION_PASSWORD_DOC;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.CONNECTION_PASSWORD_PARAM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.CONNECTION_URL_DOC;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.CONNECTION_URL_PARAM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.CONNECTION_USER_DOC;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.CONNECTION_USER_PARAM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_DEBEZIUM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_DOC;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_DEBEZIUM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_KAFKA_STD;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_SINGLE;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_KAFKA;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_PARAM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_SINGLE;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.TOPIC_PREFIX_DOC;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.TOPIC_PREFIX_PARAM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.USE_ALL_COLUMNS_ON_DELETE_DEFAULT;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.USE_ALL_COLUMNS_ON_DELETE_DOC;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.USE_ALL_COLUMNS_ON_DELETE_PARAM;
import static solutions.a2.utils.ExceptionUtils.getExceptionStackTrace;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class JdbcSinkConnectorConfig extends AbstractConfig implements TargetDbConfig {
	
	private static final Logger LOGGER = LogManager.getLogger(JdbcSinkConnectorConfig.class);

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

	private static final String INIT_SQL_PARAM = "a2.connection.init.sql";
	private static final String INIT_SQL_DOC = 
			"""
			Set the SQL statement that will be executed on all new connections when they are created.
			For example, to override the PostgreSQL server setting for the work_mem parameter, use
				"a2.connection.init.sql" : "SET work_mem = '16MB'"
			""";

	private static final String SCHEMA_NAME_PREFIX_PARAM = "a2.schema.name.prefix";
	private static final String SCHEMA_NAME_PREFIX_DOC =
			"""
			Prefix of existing Kafka Connect schema.
			Default - "" (Empty string - no prefix)
			""";

	private int schemaType = -1;

	static ConfigDef config() {
		return new ConfigDef()
				.define(CONNECTION_URL_PARAM, STRING, HIGH, CONNECTION_URL_DOC)
				.define(CONNECTION_USER_PARAM, STRING, HIGH, CONNECTION_USER_DOC)
				.define(CONNECTION_PASSWORD_PARAM, PASSWORD, HIGH, CONNECTION_PASSWORD_DOC)
				.define(BATCH_SIZE_PARAM, INT, BATCH_SIZE_DEFAULT, HIGH, BATCH_SIZE_DOC)
				.define(SCHEMA_TYPE_PARAM, STRING, SCHEMA_TYPE_KAFKA,
						ConfigDef.ValidString.in(SCHEMA_TYPE_KAFKA, SCHEMA_TYPE_DEBEZIUM, SCHEMA_TYPE_SINGLE),
						HIGH, SCHEMA_TYPE_DOC)
				.define(AUTO_CREATE_PARAM, BOOLEAN, false, HIGH, AUTO_CREATE_DOC)
				.define(PK_STRING_LENGTH_PARAM, INT, PK_STRING_LENGTH_DEFAULT, LOW, PK_STRING_LENGTH_DOC)
				.define(USE_ALL_COLUMNS_ON_DELETE_PARAM, BOOLEAN, USE_ALL_COLUMNS_ON_DELETE_DEFAULT, MEDIUM, USE_ALL_COLUMNS_ON_DELETE_DOC)
				.define(TOPIC_PREFIX_PARAM, STRING, "", MEDIUM, TOPIC_PREFIX_DOC)
				.define(TABLE_NAME_PREFIX_PARAM, STRING, "", LOW, TABLE_NAME_PREFIX_DOC)
				.define(TABLE_NAME_SUFFIX_PARAM, STRING, "", LOW, TABLE_NAME_SUFFIX_DOC)
				.define(TABLE_MAPPER_PARAM, STRING, TABLE_MAPPER_DEFAULT, MEDIUM, TABLE_MAPPER_DOC)
				.define(CONN_TYPE_PARAM, STRING, CONN_TYPE_REPLICATE,
						ConfigDef.ValidString.in(CONN_TYPE_REPLICATE, CONN_TYPE_AUDIT_TRAIL),
						HIGH, CONN_TYPE_DOC)
				.define(INIT_SQL_PARAM, STRING, "", LOW, INIT_SQL_DOC)
				.define(SCHEMA_NAME_PREFIX_PARAM, LIST, "", LOW, SCHEMA_NAME_PREFIX_DOC)
				;
	}

	JdbcSinkConnectorConfig(Map<?, ?> originals) {
		super(config(), originals);
	}

	@Override
	public boolean autoCreateTable() {
		return getBoolean(AUTO_CREATE_PARAM);
	}

	@Override
	public int pkStringLength() {
		return getInt(PK_STRING_LENGTH_PARAM);
	}

	@Override
	public int batchSize() {
		return getInt(BATCH_SIZE_PARAM);
	}


	private int connectorMode = -1;

	@Override
	public int getConnectorMode() {
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

	String topicPrefix() {
		return getString(TOPIC_PREFIX_PARAM);
	}

	List<String> schemaPrefix() {
		return getList(SCHEMA_NAME_PREFIX_PARAM);
	}

}
