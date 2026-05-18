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

package solutions.a2.cdc.oracle.runtime.thread;

import static solutions.a2.cdc.oracle.runtime.config.Parameters.POLL_INTERVAL_MS_PARAM;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import solutions.a2.cdc.oracle.OraCdcSourceBaseConfig;
import solutions.a2.cdc.oracle.OraDictSqlTexts;
import solutions.a2.cdc.oracle.OraPoolConnectionFactory;
import solutions.a2.cdc.oracle.OraRdbmsInfo;
import solutions.a2.cdc.oracle.runtime.config.KafkaSourceBaseConfig;
import solutions.a2.cdc.oracle.runtime.config.Parameters;
import solutions.a2.cdc.oracle.utils.OraSqlUtils;
import solutions.a2.cdc.oracle.utils.Version;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class KafkaSourceSnapshotLogConnector extends SourceConnector {

	private static final Logger LOGGER = LogManager.getLogger(KafkaSourceSnapshotLogConnector.class);
	private static final int MAX_TABLES = 256;

	static final String TASK_PARAM_MASTER = "master";
	static final String TASK_PARAM_MV_LOG = "mv.log";
	static final String TASK_PARAM_OWNER = "owner";
	static final String TASK_PARAM_MV_ROWID = "mvlog.rowid";
	static final String TASK_PARAM_MV_PK = "mvlog.pk";
	static final String TASK_PARAM_MV_SEQUENCE = "mvlog.seq";

	private OraCdcSourceBaseConfig config;
	private boolean validConfig = true;
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
		LOGGER.info("Starting oracdc materialized view log source connector");
		config = new KafkaSourceBaseConfig(props);

		if (StringUtils.isBlank(config.rdbmsUrl())) {
			LOGGER.error("Database connection parameters are not properly set!\n'{}' must be set for running connector!",
					Parameters.CONNECTION_URL_PARAM);
			throw new ConnectException("Database connection parameters are not properly set!");
		}

		// Initialize connection pool
		try {
			if (StringUtils.isNotBlank(config.walletLocation())) {
				LOGGER.info("Connecting to Oracle RDBMS using Oracle Wallet");
				OraPoolConnectionFactory.init(
						config.rdbmsUrl(),
						config.walletLocation());
			} else if (StringUtils.isNotBlank(config.rdbmsUser()) &&
					StringUtils.isNotBlank(config.rdbmsPassword())) {
				LOGGER.info("Connecting to Oracle RDBMS using JDBC URL, username, and password.");
				OraPoolConnectionFactory.init(
					config.rdbmsUrl(), config.rdbmsUser(), config.rdbmsPassword());
			} else {
				validConfig = false;
				LOGGER.error("Database connection parameters are not properly set\n. Or wallet.location, or pair of {}/{} are not set",
						Parameters.CONNECTION_USER_PARAM,
						Parameters.CONNECTION_PASSWORD_PARAM);
				throw new ConnectException("Database connection parameters are not properly set!");
			}
			LOGGER.trace("Oracle UCP successfully created.");
		} catch (SQLException e) {
			validConfig = false;
			LOGGER.error("Unable to initialize database connection.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			LOGGER.error("{} will not run!", KafkaSourceSnapshotLogConnector.class.getCanonicalName());
		}

		if (validConfig) {
			try (Connection connection = OraPoolConnectionFactory.getConnection()) {
				OraRdbmsInfo rdbmsInfo = new OraRdbmsInfo(connection);
				LOGGER.info("Connected to {}\n{}\n\t$ORACLE_SID={}, running on {}, OS {}.",
						rdbmsInfo.getRdbmsEdition(), rdbmsInfo.getVersionString(),
						rdbmsInfo.getInstanceName(), rdbmsInfo.getHostName(), rdbmsInfo.getPlatformName());

				String sqlStatementText = null;
				if (rdbmsInfo.getVersionMajor() < 11) {
					mvLogPre11gR2 = true;
					sqlStatementText = OraDictSqlTexts.MVIEW_COUNT_PK_SEQ_NOSCN_NONV_NOOI_PRE11G;
				} else {
					sqlStatementText = OraDictSqlTexts.MVIEW_COUNT_PK_SEQ_NOSCN_NONV_NOOI;
				}

				final List<String> excludeList = config.excludeObj();
				if (excludeList.size() > 0) {
					LOGGER.trace("Exclude table list set.");
					whereExclude = OraSqlUtils.parseTableSchemaList(true, OraSqlUtils.MODE_WHERE_ALL_MVIEW_LOGS, excludeList);
					LOGGER.debug("Excluded table list where clause:\n{}", whereExclude);
					sqlStatementText += whereExclude;
				}
				final List<String> includeList = config.includeObj();
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
					LOGGER.error("Stopping {}", KafkaSourceSnapshotLogConnector.class.getName());
					throw new ConnectException(message);
				}

				if (tableCount > MAX_TABLES) {
					final String message = "Too much tables with user " + 
								connection.getMetaData().getUserName() +
								".\nReduce table count from " + tableCount + " and try again."; 
					LOGGER.error(message);
					LOGGER.error("Stopping {}", KafkaSourceSnapshotLogConnector.class.getName());
					throw new ConnectException(message);
				}

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
		return KafkaSourceSnapshotLogTask.class;
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
					"To run " + KafkaSourceSnapshotLogConnector.class.getName() +
					" against " + (
					StringUtils.isBlank(config.walletLocation()) ?
								config.rdbmsUrl() +
								" with username " +
								config.rdbmsUser()
							:
								config.rdbmsUrl() +
								" using wallet " +
								config.walletLocation()) +
					" parameter tasks.max must set to " + tableCount;
			LOGGER.error(message);
			LOGGER.error("Stopping {}", KafkaSourceSnapshotLogConnector.class.getName());
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

				taskParam.put(Parameters.BATCH_SIZE_PARAM,
					((Integer)config.batchSize()).toString());
				taskParam.put(POLL_INTERVAL_MS_PARAM,
						Integer.toString(config.pollIntervalMs()));
				taskParam.put(TASK_PARAM_MASTER,
					resultSet.getString("MASTER"));
				taskParam.put(TASK_PARAM_MV_LOG,
					resultSet.getString("LOG_TABLE"));
				taskParam.put(TASK_PARAM_OWNER,
					resultSet.getString("LOG_OWNER"));
				taskParam.put(TASK_PARAM_MV_ROWID,
					resultSet.getString("ROWIDS"));
				taskParam.put(TASK_PARAM_MV_PK,
					resultSet.getString("PRIMARY_KEY"));
				taskParam.put(TASK_PARAM_MV_SEQUENCE,
					resultSet.getString("SEQUENCE"));
				config.schemaType(taskParam);
				taskParam.put(Parameters.TOPIC_PREFIX_PARAM,
							config.topicOrPrefix());

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
		return KafkaSourceBaseConfig.config();
	}

}
