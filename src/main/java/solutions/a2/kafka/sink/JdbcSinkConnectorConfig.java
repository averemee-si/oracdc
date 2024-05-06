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

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.kafka.ConnectorParams;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class JdbcSinkConnectorConfig extends AbstractConfig {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectorConfig.class);

	private static final String AUTO_CREATE_PARAM = "a2.autocreate";
	private static final String AUTO_CREATE_DOC = "Automatically create the destination table if missed";

	private static final int PK_STRING_LENGTH_DEFAULT = 30;
	private static final String PK_STRING_LENGTH_PARAM = "a2.pk.string.length";
	private static final String PK_STRING_LENGTH_DOC =
			"The length of the string by default when it is used as a part of primary key." +
			"\nDerfault - " + PK_STRING_LENGTH_DEFAULT;

	private static final String TABLE_NAME_PREFIX_PARAM = "a2.table.name.prefix";
	private static final String TABLE_NAME_PREFIX_DOC = 
			"Prefix to prepend to table name\n" +
			"Default - \"\" (Empty string - no prefix)";

	private static final String TABLE_NAME_SUFFIX_PARAM = "a2.table.name.suffix";
	private static final String TABLE_NAME_SUFFIX_DOC = 
			"Prefix to append to table name\n" +
			"Default - \"\" (Empty string - no suffix)";

	private static final String TABLE_MAPPER_DEFAULT = "solutions.a2.kafka.sink.DefaultTableNameMapper";
	private static final String TABLE_MAPPER_PARAM = "a2.table.mapper";
	private static final String TABLE_MAPPER_DOC =
			"The fully-qualified class name of the class that specifies which table in which to sink the data.\n" +
			"If value of thee parameter 'a2.shema.type' is set to 'debezium', the default DefaultTableNameMapper uses the 'source'.'table' field value from Sinkrecord,\n" +
			"otherwise it constructs the table name as the Kafka topic name without the prefix specified by the 'a2.topic.prefix' parameter.\n" +
			"If the values of the parameters 'a2.table.name.prefix' and/or 'a2.table.name.suffix' are specified, then the values of these parameters are added to the table name, respectively, either at the beginning or at the end.\n" +
			"Default - " + TABLE_MAPPER_DEFAULT;

	public static final int CONNECTOR_REPLICATE = 1;
	public static final int CONNECTOR_AUDIT_TRAIL = 2;
	private static final String CONN_TYPE_PARAM = "a2.sink.connector.mode";
	private static final String CONN_TYPE_REPLICATE = "replicate";
	private static final String CONN_TYPE_AUDIT_TRAIL = "audit_trail";
	private static final String CONN_TYPE_DOC =
			"Connector operating mode - 'replicate' or 'audit_trail'.\n" +
			"In 'replicate' mode, the connector sends INSERT/UPDATE/DELETE commands to the target database," +
			" and in 'audit_trail' mode, it only sends INSERT commands to record the change history of the source table.\n" +
			"Default - " + CONN_TYPE_REPLICATE;

	private int schemaType = -1;
	private int connectorMode = -1;

	public static ConfigDef config() {
		return new ConfigDef()
				.define(ConnectorParams.CONNECTION_URL_PARAM, Type.STRING,
						Importance.HIGH, ConnectorParams.CONNECTION_URL_DOC)
				.define(ConnectorParams.CONNECTION_USER_PARAM, Type.STRING,
						Importance.HIGH, ConnectorParams.CONNECTION_USER_DOC)
				.define(ConnectorParams.CONNECTION_PASSWORD_PARAM, Type.PASSWORD,
						Importance.HIGH, ConnectorParams.CONNECTION_PASSWORD_DOC)
				.define(ConnectorParams.BATCH_SIZE_PARAM, Type.INT,
						ConnectorParams.BATCH_SIZE_DEFAULT,
						Importance.HIGH, ConnectorParams.BATCH_SIZE_DOC)
				.define(ConnectorParams.SCHEMA_TYPE_PARAM, Type.STRING,
						ConnectorParams.SCHEMA_TYPE_KAFKA,
						ConfigDef.ValidString.in(ConnectorParams.SCHEMA_TYPE_KAFKA, ConnectorParams.SCHEMA_TYPE_DEBEZIUM),
						Importance.HIGH, ConnectorParams.SCHEMA_TYPE_DOC)
				.define(AUTO_CREATE_PARAM, Type.BOOLEAN, false,
						Importance.HIGH, AUTO_CREATE_DOC)
				.define(PK_STRING_LENGTH_PARAM, Type.INT, PK_STRING_LENGTH_DEFAULT,
						Importance.LOW, PK_STRING_LENGTH_DOC)
				.define(ConnectorParams.USE_ALL_COLUMNS_ON_DELETE_PARAM,
						Type.BOOLEAN, ConnectorParams.USE_ALL_COLUMNS_ON_DELETE_DEFAULT,
						Importance.MEDIUM, ConnectorParams.USE_ALL_COLUMNS_ON_DELETE_DOC)
				.define(ConnectorParams.TOPIC_PREFIX_PARAM, Type.STRING, "",
						Importance.MEDIUM, ConnectorParams.TOPIC_PREFIX_DOC)
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
				;
	}

	public JdbcSinkConnectorConfig(Map<?, ?> originals) {
		super(config(), originals);
	}

	public boolean autoCreateTable() {
		return getBoolean(AUTO_CREATE_PARAM);
	}

	public int getPkStringLength() {
		return getInt(PK_STRING_LENGTH_PARAM);
	}

	public boolean useAllColsOnDelete() {
		return getBoolean(ConnectorParams.USE_ALL_COLUMNS_ON_DELETE_PARAM);
	}

	public int getSchemaType() {
		if (schemaType == -1) {
			switch (getString(ConnectorParams.SCHEMA_TYPE_PARAM)) {
			case ConnectorParams.SCHEMA_TYPE_KAFKA:
				schemaType = ConnectorParams.SCHEMA_TYPE_INT_KAFKA_STD;
				break;
			case ConnectorParams.SCHEMA_TYPE_SINGLE:
				schemaType = ConnectorParams.SCHEMA_TYPE_INT_SINGLE;
				break;
			case ConnectorParams.SCHEMA_TYPE_DEBEZIUM:
				schemaType = ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM;
				break;
			}
		}
		return schemaType;
	}

	public TableNameMapper getTableNameMapper() {
		final TableNameMapper tnm;
		final Class<?> clazz;
		final Constructor<?> constructor;
		try {
			clazz = Class.forName(getString(TABLE_MAPPER_PARAM));
		} catch (ClassNotFoundException nfe) {
			LOGGER.error(
					"\n=====================\n" +
					"Class '{}' specified as the parameter '{}' value was not found.\n" +
					ExceptionUtils.getExceptionStackTrace(nfe) +
					"\n" +
					"=====================\n",
					getString(TABLE_MAPPER_PARAM), TABLE_MAPPER_PARAM);
			throw new ConnectException(nfe);
		}
		try {
			constructor = clazz.getConstructor();
		} catch (NoSuchMethodException nsme) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to get default constructor for the class '{}'.\n" +
					ExceptionUtils.getExceptionStackTrace(nsme) +
					"\n" +
					"=====================\n",
					getString(TABLE_MAPPER_PARAM));
			throw new ConnectException(nsme);
		} 
		
		try {
			tnm = (TableNameMapper) constructor.newInstance();
		} catch (SecurityException | 
				InvocationTargetException | 
				IllegalAccessException | 
				InstantiationException e) {
			LOGGER.error(
					"\n=====================\n" +
					"'{}' while instantinating the class '{}'.\n" +
					ExceptionUtils.getExceptionStackTrace(e) +
					"\n" +
					"=====================\n",
					e.getMessage(),getString(TABLE_MAPPER_PARAM));
			throw new ConnectException(e);
		}
		return tnm;
	}

	public String getTableNamePrefix() {
		return getString(TABLE_NAME_PREFIX_PARAM);
	}

	public String getTableNameSuffix() {
		return getString(TABLE_NAME_SUFFIX_PARAM);
	}

	public int getConnectorMode() {
		if (connectorMode == -1) {
			if (StringUtils.equalsIgnoreCase(CONN_TYPE_REPLICATE, getString(CONN_TYPE_PARAM))) {
				connectorMode = CONNECTOR_REPLICATE;
			} else {
				// CONN_TYPE_AUDIT_TRAIL
				connectorMode = CONNECTOR_AUDIT_TRAIL; 
			}
		}
		return connectorMode;
	}

}
