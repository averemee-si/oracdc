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
	private boolean useLogMiner = false;
	private boolean isCdb = false;
	private boolean mvLogPre11gR2 = false;
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
			if (!"".equals(config.getString(ParamConstants.CONNECTION_URL_PARAM))) {
				OraPoolConnectionFactory.init(
					config.getString(ParamConstants.CONNECTION_URL_PARAM),
					config.getString(ParamConstants.CONNECTION_USER_PARAM),
					config.getString(ParamConstants.CONNECTION_PASSWORD_PARAM));
			} else if (!"".equals(config.getString(ParamConstants.CONNECTION_WALLET_PARAM))) {
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
		} catch (SQLException | ClassNotFoundException | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException pe) {
			validConfig = false;
			LOGGER.error("Unable to initialize database connection.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(pe));
			LOGGER.error("{} will not run!", OraCdcSourceConnector.class.getCanonicalName());
		}

		if (validConfig) {
			try (Connection connection = OraPoolConnectionFactory.getConnection()) {
				String sqlStatementText = null;
				int modeWhere = OraSqlUtils.MODE_WHERE_ALL_MVIEW_LOGS;
				// Detect CDC source
				OraRdbmsInfo rdbmsInfo = new OraRdbmsInfo(connection);
				if (ParamConstants.ORACDC_MODE_LOGMINER.equalsIgnoreCase(config.getString(ParamConstants.ORACDC_MODE_PARAM))) {
					if (rdbmsInfo.isCdb() && !rdbmsInfo.isCdbRoot()) {
						validConfig = false;
						throw new SQLException("Must connected to CDB$ROOT while using oracdc for mining data using LogMiner!!!");
					} else {
						useLogMiner = true;
						isCdb = rdbmsInfo.isCdb();
						modeWhere = OraSqlUtils.MODE_WHERE_ALL_TABLES;
						if (isCdb) {
							sqlStatementText = OraDictSqlTexts.TABLE_COUNT_CDB;
						} else {
							sqlStatementText = OraDictSqlTexts.TABLE_COUNT_PLAIN;
						}
					}
				} else {
					if (rdbmsInfo.getVersionMajor() < 11)
						mvLogPre11gR2 = true;
					if (mvLogPre11gR2)
						sqlStatementText = OraDictSqlTexts.MVIEW_COUNT_PK_SEQ_NOSCN_NONV_NOOI_PRE11G;
					else
						sqlStatementText = OraDictSqlTexts.MVIEW_COUNT_PK_SEQ_NOSCN_NONV_NOOI;
				}

				final List<String> excludeList = config.getList(ParamConstants.TABLE_EXCLUDE_PARAM);
				if (excludeList.size() > 0) {
					whereExclude = OraSqlUtils.parseTableSchemaList(true, modeWhere, excludeList);
					sqlStatementText += whereExclude;
				}
				final List<String> includeList = config.getList(ParamConstants.TABLE_INCLUDE_PARAM);
				if (includeList.size() > 0) {
					whereInclude = OraSqlUtils.parseTableSchemaList(false, modeWhere, includeList);
					sqlStatementText += whereInclude;
				}

				// Count number of materialized view logs/available tables for mining
				final PreparedStatement statement = connection.prepareStatement(sqlStatementText);
				final ResultSet rsCount = statement.executeQuery();
				if (rsCount.next())
					tableCount = rsCount.getInt(1);
				rsCount.close();
				statement.close();

				if (tableCount == 0) {
					final String message =
							"Nothing to do with user " + 
							connection.getMetaData().getUserName() +
							"."; 
					LOGGER.error(message);
					LOGGER.error("Stopping {}", OraCdcSourceConnector.class.getName());
					throw new ConnectException(message);
				}
				//TODO
				//TODO
				//TODO
				if (useLogMiner && tableCount > 150) {
					final String message =
							"Too much tables with user " + 
							connection.getMetaData().getUserName() +
							".\nReduce table count from " +
							tableCount +
							" and try again."; 
					LOGGER.error(message);
					LOGGER.error("Stopping {}", OraCdcSourceConnector.class.getName());
					throw new ConnectException(message);
				}

				if (ParamConstants.SCHEMA_TYPE_STANDALONE.equals(
						config.getString(ParamConstants.SCHEMA_TYPE_PARAM)))
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
		if (maxTasks != tableCount) {
			final String message = 
					"To run " +
					OraCdcSourceConnector.class.getName() +
					" against " + (
					"".equals(config.getString(ParamConstants.CONNECTION_WALLET_PARAM)) ?
								config.getString(ParamConstants.CONNECTION_URL_PARAM) +
								" with username " +
								config.getString(ParamConstants.CONNECTION_USER_PARAM)
							:
								config.getString(ParamConstants.CONNECTION_TNS_ALIAS_PARAM) +
								" using wallet " +
								config.getString(ParamConstants.CONNECTION_WALLET_PARAM)) +
					" parameter tasks.max must set to " +
					tableCount;
			LOGGER.error(message);
			LOGGER.error("Stopping {}", OraCdcSourceConnector.class.getName());
			throw new ConnectException(message);
		}

		List<Map<String, String>> configs = new ArrayList<>(maxTasks);
		try (Connection connection = OraPoolConnectionFactory.getConnection()) {
			String sqlStatementText = null;
			if (useLogMiner)
				if (isCdb)
					sqlStatementText = OraDictSqlTexts.TABLE_LIST_CDB;
				else
					sqlStatementText = OraDictSqlTexts.TABLE_LIST_PLAIN;
			else
				if (mvLogPre11gR2)
					sqlStatementText = OraDictSqlTexts.MVIEW_LIST_PK_SEQ_NOSCN_NONV_NOOI_PRE11G;
				else
					sqlStatementText = OraDictSqlTexts.MVIEW_LIST_PK_SEQ_NOSCN_NONV_NOOI;

			if (whereExclude != null) {
				sqlStatementText += whereExclude;
			}
			if (whereInclude != null) {
				sqlStatementText += whereInclude;
			}
			final PreparedStatement statement = connection.prepareStatement(sqlStatementText);
			final ResultSet resultSet = statement.executeQuery();

			for (int i = 0; i < maxTasks; i++) {
				resultSet.next();
				final Map<String, String> taskParam = new HashMap<>(9);

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
			throw new ConnectException("Unable to validate configuration.");
		}

		return configs;
	}

	@Override
	public ConfigDef config() {
		return OraCdcSourceConnectorConfig.config();
	}

}
