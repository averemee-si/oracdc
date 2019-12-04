/**
 * Copyright (c) 2018-present, http://a2-solutions.eu
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

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.kafka.connect.OraCdcSourceConnectorConfig;
import eu.solutions.a2.cdc.oracle.kafka.connect.OraCdcSourceTask;
import eu.solutions.a2.cdc.oracle.standalone.avro.Source;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.OraSqlUtils;
import eu.solutions.a2.cdc.oracle.utils.Version;

public class OraCdcSourceConnector extends SourceConnector {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcSourceConnector.class);

	private OraCdcSourceConnectorConfig config;
	private boolean validConfig = true;
	private int tableCount = 0;
	private String whereExclude = null;
	private String whereInclude = null;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc Source Connector");
		config = new OraCdcSourceConnectorConfig(props);

		// Initialize connection pool
		try {
			if (!"".equals(config.getString(OraCdcSourceConnectorConfig.CONNECTION_URL_PARAM))) {
				OraPoolConnectionFactory.init(
					config.getString(OraCdcSourceConnectorConfig.CONNECTION_URL_PARAM),
					config.getString(OraCdcSourceConnectorConfig.CONNECTION_USER_PARAM),
					config.getString(OraCdcSourceConnectorConfig.CONNECTION_PASSWORD_PARAM));
			} else if (!"".equals(config.getString(OraCdcSourceConnectorConfig.CONNECTION_WALLET_PARAM))) {
				OraPoolConnectionFactory.init4Wallet(
						config.getString(OraCdcSourceConnectorConfig.CONNECTION_WALLET_PARAM),
						config.getString(OraCdcSourceConnectorConfig.CONNECTION_TNS_ADMIN_PARAM),
						config.getString(OraCdcSourceConnectorConfig.CONNECTION_TNS_ALIAS_PARAM));
			} else {
				validConfig = false;
				LOGGER.error("Database connection parameters are not properly set\n. Both {}, and {} are not set",
						OraCdcSourceConnectorConfig.CONNECTION_URL_PARAM,
						OraCdcSourceConnectorConfig.CONNECTION_WALLET_PARAM);
				throw new RuntimeException("Database connection parameters are not properly set!");
			}
		} catch (SQLException | ClassNotFoundException | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException pe) {
			validConfig = false;
			LOGGER.error("Unable to initialize database connection.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(pe));
			LOGGER.error("{} will not run!", OraCdcSourceConnector.class.getCanonicalName());
		}

		if (validConfig) {
			try (Connection connection = OraPoolConnectionFactory.getConnection()) {
				String sqlStatementText = OraDictSqlTexts.MVIEW_COUNT_PK_SEQ_NOSCN_NONV_NOOI;
				final List<String> excludeList = config.getList(OraCdcSourceConnectorConfig.TABLE_EXCLUDE_PARAM);
				if (excludeList.size() > 0) {
					whereExclude = OraSqlUtils.parseTableSchemaList(true, false, excludeList);
					sqlStatementText += whereExclude;
				}
				final List<String> includeList = config.getList(OraCdcSourceConnectorConfig.TABLE_INCLUDE_PARAM);
				if (includeList.size() > 0) {
					whereInclude = OraSqlUtils.parseTableSchemaList(false, false, includeList);
					sqlStatementText += whereInclude;
				}

				// Count number of materialized view logs
				final PreparedStatement statement = connection.prepareStatement(sqlStatementText);
				final ResultSet rsCount = statement.executeQuery();
				if (rsCount.next())
					tableCount = rsCount.getInt(1);
				rsCount.close();
				statement.close();

				if (tableCount == 0) {
					final String message =
							"Nothing to do with user " + 
							config.getString(OraCdcSourceConnectorConfig.CONNECTION_USER_PARAM) +
							"."; 
					LOGGER.error(message);
					LOGGER.error("Stopping {}", OraCdcSourceConnector.class.getName());
					throw new RuntimeException(message);
				}

				if (OraCdcSourceConnectorConfig.SCHEMA_TYPE_STANDALONE.equals(
						config.getString(OraCdcSourceConnectorConfig.SCHEMA_TYPE_PARAM)))
					Source.init(Source.SCHEMA_TYPE_STANDALONE);
				else
					// config.getString(OraCdcSourceConnectorConfig.SCHEMA_TYPE_PARAM)
					Source.init(Source.SCHEMA_TYPE_KAFKA_CONNECT_STD);

			} catch (SQLException sqle) {
				validConfig = false;
				LOGGER.error("Unable to get table information.");
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
				LOGGER.error("Exiting!");
			}
		}

		if (!validConfig) {
			throw new RuntimeException("Unable to validate configuration.");
		}

	}

	@Override
	public void stop() {
		//TODO Do we need more here?
	}

	@Override
	public Class<? extends Task> taskClass() {
		return OraCdcSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		if (maxTasks != tableCount) {
			final String message = 
					"To run " +
					OraCdcSourceConnector.class.getName() +
					" against " +
					config.getString(OraCdcSourceConnectorConfig.CONNECTION_URL_PARAM) +
					" with username " +
					config.getString(OraCdcSourceConnectorConfig.CONNECTION_USER_PARAM) +
					" parameter tasks.max must set to " +
					tableCount;
			LOGGER.error(message);
			LOGGER.error("Stopping {}", OraCdcSourceConnector.class.getName());
			throw new RuntimeException(message);
		}

		List<Map<String, String>> configs = new ArrayList<>(maxTasks);
		try (Connection connection = OraPoolConnectionFactory.getConnection()) {
			String sqlStatementText = OraDictSqlTexts.MVIEW_LIST_PK_SEQ_NOSCN_NONV_NOOI;
			if (whereExclude != null) {
				sqlStatementText += whereExclude;
			}
			if (whereInclude != null) {
				sqlStatementText += whereExclude;
			}
			final PreparedStatement statement = connection.prepareStatement(sqlStatementText);
			final ResultSet resultSet = statement.executeQuery();

			for (int i = 0; i < maxTasks; i++) {
				resultSet.next();
				final Map<String, String> taskParam = new HashMap<>(6);

				taskParam.put(OraCdcSourceConnectorConfig.BATCH_SIZE_PARAM,
						config.getInt(OraCdcSourceConnectorConfig.BATCH_SIZE_PARAM).toString());
				taskParam.put(OraCdcSourceConnectorConfig.POLL_INTERVAL_MS_PARAM,
						config.getInt(OraCdcSourceConnectorConfig.POLL_INTERVAL_MS_PARAM).toString());
				taskParam.put(OraCdcSourceConnectorConfig.TASK_PARAM_MASTER,
						resultSet.getString("MASTER"));
				taskParam.put(OraCdcSourceConnectorConfig.TASK_PARAM_MV_LOG,
						resultSet.getString("LOG_TABLE"));
				taskParam.put(OraCdcSourceConnectorConfig.TASK_PARAM_OWNER,
						resultSet.getString("LOG_OWNER"));

				if (Source.schemaType() == Source.SCHEMA_TYPE_KAFKA_CONNECT_STD)
					taskParam.put(OraCdcSourceConnectorConfig.TOPIC_PREFIX_PARAM,
							config.getString(OraCdcSourceConnectorConfig.TOPIC_PREFIX_PARAM));
				else
					// Source.SCHEMA_TYPE_STANDALONE
					taskParam.put(OraCdcSourceConnectorConfig.KAFKA_TOPIC_PARAM,
							config.getString(OraCdcSourceConnectorConfig.KAFKA_TOPIC_PARAM));

				configs.add(taskParam);
			}
		} catch (SQLException sqle) {
			validConfig = false;
			LOGGER.error("Unable to get table information.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			LOGGER.error("Exiting!");
		}

		if (!validConfig) {
			throw new RuntimeException("Unable to validate configuration.");
		}

		return configs;
	}

	@Override
	public ConfigDef config() {
		return OraCdcSourceConnectorConfig.config();
	}

}
