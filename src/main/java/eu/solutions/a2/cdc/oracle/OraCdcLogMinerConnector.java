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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.Version;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcLogMinerConnector extends SourceConnector {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerConnector.class);

	private OraCdcSourceConnectorConfig config;
	private boolean validConfig = true;
	private int schemaType;
	private String tmpdir;
	private String stateFileName;
	private String connectorName;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		connectorName = props.get("name");
		LOGGER.info("Starting oracdc '{}' logminer source connector", connectorName);
		config = new OraCdcSourceConnectorConfig(props);

		// Initialize connection pool
		try {
			if (!"".equals(config.getString(ParamConstants.CONNECTION_URL_PARAM))) {
				LOGGER.trace("Connecting to Oracle RDBMS using JDBC URL, username, and password.");
				OraPoolConnectionFactory.init(
					config.getString(ParamConstants.CONNECTION_URL_PARAM),
					config.getString(ParamConstants.CONNECTION_USER_PARAM),
					config.getPassword(ParamConstants.CONNECTION_PASSWORD_PARAM).value());
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
		} catch (SQLException e) {
			validConfig = false;
			LOGGER.error("Unable to initialize database connection.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			LOGGER.error("{} will not run!", OraCdcLogMinerConnector.class.getCanonicalName());
		}

		if (validConfig) {
			try (Connection connection = OraPoolConnectionFactory.getConnection()) {
				OraRdbmsInfo rdbmsInfo = new OraRdbmsInfo(connection);
				LOGGER.info("Connected to {}, {}\n\t$ORACLE_SID={}, running on {}, OS {}.",
						rdbmsInfo.getRdbmsEdition(), rdbmsInfo.getVersionString(),
						rdbmsInfo.getInstanceName(), rdbmsInfo.getHostName(), rdbmsInfo.getPlatformName());

				if (rdbmsInfo.isCdb() && !rdbmsInfo.isCdbRoot() && !rdbmsInfo.isPdbConnectionAllowed()) {
					validConfig = false;
					throw new SQLException("Must connected to CDB$ROOT while using oracdc for mining data using LogMiner!!!");
				} else {
					LOGGER.trace("Oracle connection information:\n{}", rdbmsInfo.toString());
				}

				if (config.getBoolean(ParamConstants.MAKE_STANDBY_ACTIVE_PARAM)) {
					if (StringUtils.isAllBlank(ParamConstants.STANDBY_WALLET_PARAM)) {
						validConfig = false;
						throw new SQLException("Parameter " + ParamConstants.STANDBY_WALLET_PARAM + " not set!!!");
					}
					if (StringUtils.isAllBlank(ParamConstants.STANDBY_TNS_ADMIN_PARAM)) {
						validConfig = false;
						throw new SQLException("Parameter " + ParamConstants.STANDBY_TNS_ADMIN_PARAM + " not set!!!");
					}
					if (StringUtils.isAllBlank(ParamConstants.STANDBY_TNS_ALIAS_PARAM)) {
						validConfig = false;
						throw new SQLException("Parameter " + ParamConstants.STANDBY_TNS_ALIAS_PARAM + " not set!!!");
					}
					if (validConfig) {
						OraPoolConnectionFactory.init4Standby(
							config.getString(ParamConstants.STANDBY_WALLET_PARAM),
							config.getString(ParamConstants.STANDBY_TNS_ADMIN_PARAM),
							config.getString(ParamConstants.STANDBY_TNS_ALIAS_PARAM));
						LOGGER.info("Connection to PHYSICAL STANDBY will be used for LogMiner calls");
					}
					if (config.getBoolean(ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM)) {
						LOGGER.warn("When {} set to true {} must set to false!\n",
								ParamConstants.MAKE_STANDBY_ACTIVE_PARAM,
								ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM);
					}
				}
				if (config.getBoolean(ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM)) {
					if (StringUtils.isAllBlank(ParamConstants.DISTRIBUTED_WALLET_PARAM)) {
						validConfig = false;
						throw new SQLException("Parameter " + ParamConstants.DISTRIBUTED_WALLET_PARAM + " not set!!!");
					}
					if (StringUtils.isAllBlank(ParamConstants.DISTRIBUTED_TNS_ADMIN_PARAM)) {
						validConfig = false;
						throw new SQLException("Parameter " + ParamConstants.DISTRIBUTED_TNS_ADMIN_PARAM + " not set!!!");
					}
					if (StringUtils.isAllBlank(ParamConstants.DISTRIBUTED_TNS_ALIAS_PARAM)) {
						validConfig = false;
						throw new SQLException("Parameter " + ParamConstants.DISTRIBUTED_TNS_ALIAS_PARAM + " not set!!!");
					}
					if (validConfig) {
						OraPoolConnectionFactory.initDistributed(
							config.getString(ParamConstants.DISTRIBUTED_WALLET_PARAM),
							config.getString(ParamConstants.DISTRIBUTED_TNS_ADMIN_PARAM),
							config.getString(ParamConstants.DISTRIBUTED_TNS_ALIAS_PARAM));
						LOGGER.info("oracdc will run in distributed configuration.");
					}
				}
			} catch (SQLException sqle) {
				validConfig = false;
				LOGGER.error("Unable to validate connection information.");
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
				LOGGER.error("Exiting!");
			}

			final String schemaTypeString = config.getString(ParamConstants.SCHEMA_TYPE_PARAM);
			LOGGER.debug("{} set to {}.", ParamConstants.SCHEMA_TYPE_PARAM, schemaTypeString);
			if (ParamConstants.SCHEMA_TYPE_DEBEZIUM.equals(schemaTypeString)) {
				schemaType = ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM;
			} else {
				schemaType = ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD;
			}

			tmpdir = config.getString(ParamConstants.TEMP_DIR_PARAM);
			if ("".equals(tmpdir) || tmpdir == null) {
				tmpdir = System.getProperty("java.io.tmpdir");
			}
			if (Files.isDirectory(Paths.get(tmpdir))) {
				if (!Files.isWritable(Paths.get(tmpdir))) {
					LOGGER.error("Parameter {} points to non-writable directory {}.",
							ParamConstants.TEMP_DIR_PARAM, tmpdir);
					validConfig = false;
				} else {
					LOGGER.trace("Parameter {} points to valid temp directory {}.",
							ParamConstants.TEMP_DIR_PARAM, tmpdir);
				}
			} else {
				LOGGER.error("Parameter {} set to non-existent directory {}.",
						ParamConstants.TEMP_DIR_PARAM, tmpdir);
				validConfig = false;
			}

			stateFileName = config.getString(ParamConstants.PERSISTENT_STATE_FILE_PARAM);
			if ("".equals(stateFileName) || stateFileName == null) {
				final String tmpDir = System.getProperty("java.io.tmpdir");
				stateFileName = tmpDir +  
						(StringUtils.endsWith(tmpDir, File.separator) ? "" : File.separator) +
						"oracdc.state";				
			}
			LOGGER.debug("{} set to {}.", ParamConstants.PERSISTENT_STATE_FILE_PARAM, stateFileName);
			//TODO
			//TODO Perform more file checks and related... ... ...
			//TODO

		}

		if (!validConfig) {
			throw new ConnectException("Unable to validate configuration.");
		}
	}

	@Override
	public void stop() {
		LOGGER.info("Stopping oracdc logminer source connector");
	}

	@Override
	public Class<? extends Task> taskClass() {
		return OraCdcLogMinerTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		LOGGER.trace("BEGIN: taskConfigs(int maxTasks)");
		final Map<String, String> taskParam = new HashMap<>();
		taskParam.put("name", connectorName);
		final Boolean oracdcSchemas = config.getBoolean(ParamConstants.ORACDC_SCHEMAS_PARAM);
		taskParam.put(ParamConstants.ORACDC_SCHEMAS_PARAM, oracdcSchemas.toString());
		if (oracdcSchemas) {
			taskParam.put(ParamConstants.DICTIONARY_FILE_PARAM,
					config.getString(ParamConstants.DICTIONARY_FILE_PARAM));
		}
		taskParam.put(ParamConstants.BATCH_SIZE_PARAM,
				config.getInt(ParamConstants.BATCH_SIZE_PARAM).toString());
		taskParam.put(ParamConstants.POLL_INTERVAL_MS_PARAM,
				config.getInt(ParamConstants.POLL_INTERVAL_MS_PARAM).toString());
		taskParam.put(ParamConstants.SCHEMA_TYPE_PARAM,
				Integer.toString(schemaType));
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			taskParam.put(OraCdcSourceConnectorConfig.TOPIC_PREFIX_PARAM,
				config.getString(OraCdcSourceConnectorConfig.TOPIC_PREFIX_PARAM));
			taskParam.put(ParamConstants.TOPIC_NAME_STYLE_PARAM,
					config.getString(ParamConstants.TOPIC_NAME_STYLE_PARAM));
			taskParam.put(ParamConstants.TOPIC_NAME_DELIMITER_PARAM,
					config.getString(ParamConstants.TOPIC_NAME_DELIMITER_PARAM));
		} else {
			// ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM
			taskParam.put(OraCdcSourceConnectorConfig.KAFKA_TOPIC_PARAM,
				config.getString(OraCdcSourceConnectorConfig.KAFKA_TOPIC_PARAM));
		}
		final List<String> excludeList = config.getList(ParamConstants.TABLE_EXCLUDE_PARAM);
		if (excludeList.size() > 0) {
			LOGGER.trace("Exclude table list set.");
			taskParam.put(ParamConstants.TABLE_EXCLUDE_PARAM,
					config.originalsStrings().get(ParamConstants.TABLE_EXCLUDE_PARAM));
		}
		final List<String> includeList = config.getList(ParamConstants.TABLE_INCLUDE_PARAM);
		if (includeList.size() > 0) {
			LOGGER.trace("Include table list set.");
			taskParam.put(ParamConstants.TABLE_INCLUDE_PARAM,
					config.originalsStrings().get(ParamConstants.TABLE_INCLUDE_PARAM));
		}
		taskParam.put(ParamConstants.TABLE_LIST_STYLE_PARAM,
				config.getString(ParamConstants.TABLE_LIST_STYLE_PARAM));
		
		final long redoSize = config.getLong(ParamConstants.REDO_FILES_SIZE_PARAM);
		if (redoSize > 0) {
			LOGGER.trace("Redo size threshold will be used instead of count of redo files.");
			taskParam.put(ParamConstants.REDO_FILES_SIZE_PARAM, Long.toString(redoSize));
		} else {
			LOGGER.trace("Count of redo files will be used instead of size threshold.");
			int fileCount = config.getShort(ParamConstants.REDO_FILES_COUNT_PARAM);
			if (fileCount < 1) {
				fileCount = 1;
			}
			taskParam.put(ParamConstants.REDO_FILES_COUNT_PARAM, Integer.toString(fileCount));
		}
		final long firstScn = config.getLong(ParamConstants.LGMNR_START_SCN_PARAM);
		if (firstScn > 0) {
			taskParam.put(ParamConstants.LGMNR_START_SCN_PARAM, Long.toString(firstScn));
		}
		taskParam.put(ParamConstants.TEMP_DIR_PARAM, tmpdir);
		taskParam.put(ParamConstants.PERSISTENT_STATE_FILE_PARAM, stateFileName);
		// Just pass...
		taskParam.put(ParamConstants.INITIAL_LOAD_PARAM, config.getString(ParamConstants.INITIAL_LOAD_PARAM));
		taskParam.put(ParamConstants.PROCESS_LOBS_PARAM,
				config.getBoolean(ParamConstants.PROCESS_LOBS_PARAM).toString());
		if (config.getBoolean(ParamConstants.PROCESS_LOBS_PARAM)) {
			taskParam.put(ParamConstants.LOB_TRANSFORM_CLASS_PARAM,
					config.getString(ParamConstants.LOB_TRANSFORM_CLASS_PARAM));
		}
		taskParam.put(ParamConstants.CONNECTION_BACKOFF_PARAM, 
				config.getInt(ParamConstants.CONNECTION_BACKOFF_PARAM).toString());
		if (config.getBoolean(ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM)) {
			// When this set we need explicitly value of  a2.archived.log.catalog parameter
			if (!OraCdcDistributedV$ArchivedLogImpl.class.getCanonicalName()
					.equals(config.getString(ParamConstants.ARCHIVED_LOG_CAT_PARAM))) {
				LOGGER.warn("When {} set to true value of {} must be {}.", 
						ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM,
						ParamConstants.ARCHIVED_LOG_CAT_PARAM,
						OraCdcDistributedV$ArchivedLogImpl.class.getCanonicalName());
				LOGGER.warn("Setting {} value to {}.",
						ParamConstants.ARCHIVED_LOG_CAT_PARAM,
						OraCdcDistributedV$ArchivedLogImpl.class.getCanonicalName());
			}
			taskParam.put(ParamConstants.ARCHIVED_LOG_CAT_PARAM, 
					OraCdcDistributedV$ArchivedLogImpl.class.getCanonicalName());
		} else {
			taskParam.put(ParamConstants.ARCHIVED_LOG_CAT_PARAM, 
					config.getString(ParamConstants.ARCHIVED_LOG_CAT_PARAM));
		}
		final Integer fetchSize = config.getInt(ParamConstants.FETCH_SIZE_PARAM);
		taskParam.put(ParamConstants.FETCH_SIZE_PARAM, fetchSize.toString());
		taskParam.put(ParamConstants.DISTRIBUTED_TARGET_HOST,
				config.getString(ParamConstants.DISTRIBUTED_TARGET_HOST));
		taskParam.put(ParamConstants.DISTRIBUTED_TARGET_PORT,
				config.getInt(ParamConstants.DISTRIBUTED_TARGET_PORT).toString());

		final List<Map<String, String>> configs = new ArrayList<>(1);
		configs.add(taskParam);
		LOGGER.trace("END: taskConfigs(int maxTasks)");
		return configs;
	}

	@Override
	public ConfigDef config() {
		return OraCdcSourceConnectorConfig.config();
	}

}
