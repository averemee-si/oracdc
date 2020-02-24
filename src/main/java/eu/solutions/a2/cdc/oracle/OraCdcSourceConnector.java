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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.OraSqlUtils;
import eu.solutions.a2.cdc.oracle.utils.Version;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcSourceConnector extends SourceConnector {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcSourceConnector.class);
	private static final int MAX_TABLES = 256;

	private OraCdcSourceConnectorConfig config;
	private boolean validConfig = true;
	private boolean mvLogPre11gR2 = false;
	private int tableCount = 0;
	private String whereExclude = null;
	private String whereInclude = null;
	private int schemaType;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc materialized view log source connector");
		config = new OraCdcSourceConnectorConfig(props);

		// Initialize connection pool
		try {
			if (!"".equals(config.getString(ParamConstants.CONNECTION_URL_PARAM))) {
				LOGGER.trace("Connecting to Oracle RDBMS using JDBC URL, username, and password.");
				OraPoolConnectionFactory.init(
					config.getString(ParamConstants.CONNECTION_URL_PARAM),
					config.getString(ParamConstants.CONNECTION_USER_PARAM),
					config.getString(ParamConstants.CONNECTION_PASSWORD_PARAM));
			} else if (!"".equals(config.getString(ParamConstants.CONNECTION_WALLET_PARAM))) {
				LOGGER.trace("Connecting to Oracle RDBMS using Oracle Wallet");
				OraPoolConnectionFactory.init4Wallet(
						config.getString(ParamConstants.CONNECTION_WALLET_PARAM),
						config.getString(ParamConstants.CONNECTION_TNS_ADMIN_PARAM),
						config.getString(ParamConstants.CONNECTION_TNS_ALIAS_PARAM));
			} else {
				validConfig = false;
				LOGGER.error("Database connection parameters are not properly set\n. Both {}, and {} are not set",
						ParamConstants.CONNECTION_URL_PARAM,
						ParamConstants.CONNECTION_WALLET_PARAM);
				throw new ConnectException("Database connection parameters are not properly set!");
			}
			LOGGER.trace("Oracle UCP successfully created.");
		} catch (SQLException | ClassNotFoundException | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException pe) {
			validConfig = false;
			LOGGER.error("Unable to initialize database connection.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(pe));
			LOGGER.error("{} will not run!", OraCdcSourceConnector.class.getCanonicalName());
		}

		if (validConfig) {
			try (Connection connection = OraPoolConnectionFactory.getConnection()) {
				OraRdbmsInfo rdbmsInfo = new OraRdbmsInfo(connection);
				LOGGER.info("Connected to $ORACLE_SID={}, version={}, running on {}, OS {}.",
						rdbmsInfo.getInstanceName(), rdbmsInfo.getVersionString(), rdbmsInfo.getHostName(), rdbmsInfo.getPlatformName());

				String sqlStatementText = null;
				if (rdbmsInfo.getVersionMajor() < 11) {
					mvLogPre11gR2 = true;
					sqlStatementText = OraDictSqlTexts.MVIEW_COUNT_PK_SEQ_NOSCN_NONV_NOOI_PRE11G;
				} else {
					sqlStatementText = OraDictSqlTexts.MVIEW_COUNT_PK_SEQ_NOSCN_NONV_NOOI;
				}

				final List<String> excludeList = config.getList(ParamConstants.TABLE_EXCLUDE_PARAM);
				if (excludeList.size() > 0) {
					LOGGER.trace("Exclude table list set.");
					whereExclude = OraSqlUtils.parseTableSchemaList(true, OraSqlUtils.MODE_WHERE_ALL_MVIEW_LOGS, excludeList);
					LOGGER.debug("Excluded table list where clause:\n{}", whereExclude);
					sqlStatementText += whereExclude;
				}
				final List<String> includeList = config.getList(ParamConstants.TABLE_INCLUDE_PARAM);
				if (includeList.size() > 0) {
					LOGGER.trace("Include table list set.");
					whereInclude = OraSqlUtils.parseTableSchemaList(false, OraSqlUtils.MODE_WHERE_ALL_MVIEW_LOGS, includeList);
					LOGGER.debug("Included table list where clause:\n{}", whereInclude);
					sqlStatementText += whereInclude;
				}

				LOGGER.trace("Multithreading will be used.");
				LOGGER.debug("Will use\n{}\nfor counting number of tables to process.", sqlStatementText);
				// Count number of materialized view logs/available tables for mining
				final PreparedStatement statement = connection.prepareStatement(sqlStatementText);
				final ResultSet rsCount = statement.executeQuery();
				if (rsCount.next()) {
					tableCount = rsCount.getInt(1);
				}
				rsCount.close();
				statement.close();

				LOGGER.debug("Will work with {} tables.", tableCount);
				if (tableCount == 0) {
					final String message = "Nothing to do with user " + 
								connection.getMetaData().getUserName() + "."; 
					LOGGER.error(message);
					LOGGER.error("Stopping {}", OraCdcSourceConnector.class.getName());
					throw new ConnectException(message);
				}

				if (tableCount > MAX_TABLES) {
					final String message = "Too much tables with user " + 
								connection.getMetaData().getUserName() +
								".\nReduce table count from " + tableCount + " and try again."; 
					LOGGER.error(message);
					LOGGER.error("Stopping {}", OraCdcSourceConnector.class.getName());
					throw new ConnectException(message);
				}

				final String schemaTypeString = config.getString(ParamConstants.SCHEMA_TYPE_PARAM);
				LOGGER.debug("a2.schema.type set to {}.", schemaTypeString);
				if (ParamConstants.SCHEMA_TYPE_DEBEZIUM.equals(schemaTypeString))
					schemaType = ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM;
				else
					schemaType = ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD;

			} catch (SQLException sqle) {
				validConfig = false;
				LOGGER.error("Unable to get table information.");
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
				LOGGER.error("Exiting!");
			}
		}

		if (!validConfig) {
			throw new ConnectException("Unable to validate configuration.");
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
		LOGGER.trace("BEGIN: taskConfigs(int maxTasks)");
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("maxTasks set to -> {}.", maxTasks);
			LOGGER.debug("tableCount set to -> {}.", tableCount);
		}
		if (maxTasks != tableCount) {
			final String message = 
					"To run " + OraCdcSourceConnector.class.getName() +
					" against " + (
					"".equals(config.getString(ParamConstants.CONNECTION_WALLET_PARAM)) ?
								config.getString(ParamConstants.CONNECTION_URL_PARAM) +
								" with username " +
								config.getString(ParamConstants.CONNECTION_USER_PARAM)
							:
								config.getString(ParamConstants.CONNECTION_TNS_ALIAS_PARAM) +
								" using wallet " +
								config.getString(ParamConstants.CONNECTION_WALLET_PARAM)) +
					" parameter tasks.max must set to " + tableCount;
			LOGGER.error(message);
			LOGGER.error("Stopping {}", OraCdcSourceConnector.class.getName());
			throw new ConnectException(message);
		}

		final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
		try (Connection connection = OraPoolConnectionFactory.getConnection()) {
			String sqlStatementText = null;
			if (mvLogPre11gR2) {
				sqlStatementText = OraDictSqlTexts.MVIEW_LIST_PK_SEQ_NOSCN_NONV_NOOI_PRE11G;
			} else {
				sqlStatementText = OraDictSqlTexts.MVIEW_LIST_PK_SEQ_NOSCN_NONV_NOOI;
			}

			if (whereExclude != null) {
				sqlStatementText += whereExclude;
			}
			if (whereInclude != null) {
				sqlStatementText += whereInclude;
			}
			LOGGER.debug("Will use\n{}\nto collect table information.", sqlStatementText);

			final PreparedStatement statement = connection.prepareStatement(sqlStatementText);
			final ResultSet resultSet = statement.executeQuery();

			for (int i = 0; i < maxTasks; i++) {
				resultSet.next();
				final Map<String, String> taskParam = new HashMap<>();

				taskParam.put(ParamConstants.BATCH_SIZE_PARAM,
					config.getInt(ParamConstants.BATCH_SIZE_PARAM).toString());
				taskParam.put(ParamConstants.POLL_INTERVAL_MS_PARAM,
					config.getInt(ParamConstants.POLL_INTERVAL_MS_PARAM).toString());
				taskParam.put(OraCdcSourceConnectorConfig.TASK_PARAM_MASTER,
					resultSet.getString("MASTER"));
				taskParam.put(OraCdcSourceConnectorConfig.TASK_PARAM_MV_LOG,
					resultSet.getString("LOG_TABLE"));
				taskParam.put(OraCdcSourceConnectorConfig.TASK_PARAM_OWNER,
					resultSet.getString("LOG_OWNER"));
				taskParam.put(OraCdcSourceConnectorConfig.TASK_PARAM_MV_ROWID,
					resultSet.getString("ROWIDS"));
				taskParam.put(OraCdcSourceConnectorConfig.TASK_PARAM_MV_PK,
					resultSet.getString("PRIMARY_KEY"));
				taskParam.put(OraCdcSourceConnectorConfig.TASK_PARAM_MV_SEQUENCE,
					resultSet.getString("SEQUENCE"));
				taskParam.put(ParamConstants.SCHEMA_TYPE_PARAM,
					Integer.toString(schemaType));

				if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
					taskParam.put(OraCdcSourceConnectorConfig.TOPIC_PREFIX_PARAM,
							config.getString(OraCdcSourceConnectorConfig.TOPIC_PREFIX_PARAM));
				} else {
					// ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM
					taskParam.put(OraCdcSourceConnectorConfig.KAFKA_TOPIC_PARAM,
							config.getString(OraCdcSourceConnectorConfig.KAFKA_TOPIC_PARAM));
				}

				configs.add(taskParam);
			}
			return configs;
		} catch (SQLException sqle) {
			validConfig = false;
			LOGGER.error("Unable to get table information.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			LOGGER.error("Exiting!");
			throw new ConnectException("Unable to validate configuration.");
		}

	}

	@Override
	public ConfigDef config() {
		return OraCdcSourceConnectorConfig.config();
	}

}
