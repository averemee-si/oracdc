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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.utils.Version;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcLogMinerConnector extends SourceConnector {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerConnector.class);
	private static final String DB_PARAM_ERROR_GENERIC = "Database connection parameters are not properly set!";
	private static final String TMP_PARAM_ERROR_GENERIC = "Temp directory is not properly set!";
	private static final String DB_PARAM_MUST_SET_WHEN = "Parameter value '{}' must be set when parameter value '{}' is set!";
	private static final String DB_PARAM_MUST_SET_WHEN_TRUE = "Parameter '{}' must be set when '{}' set to true!";
	private static final String VALUE_SET_TO = "Value of parameter '{}' is set to '{}'";

	private Map<String, String> connectorProperties;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc '{}' logminer source connector", props.get("name"));
		OraCdcSourceConnectorConfig config;
		try {
			config = new OraCdcSourceConnectorConfig(props);
			connectorProperties = new HashMap<>();
			connectorProperties.putAll(props);
		} catch (ConfigException ce) {
			throw new ConnectException("Couldn't start oracdc due to coniguration error", ce);
		}

		if (StringUtils.isNotBlank(config.getString(ParamConstants.CONNECTION_URL_PARAM))) {
			if (StringUtils.isBlank(ParamConstants.CONNECTION_USER_PARAM)) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN,
						ParamConstants.CONNECTION_USER_PARAM,
						ParamConstants.CONNECTION_URL_PARAM);
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
			if (StringUtils.isBlank(ParamConstants.CONNECTION_PASSWORD_PARAM)) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN,
						ParamConstants.CONNECTION_PASSWORD_PARAM,
						ParamConstants.CONNECTION_URL_PARAM);
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Connection to RDBMS will be performed using JDBC URL, username, and password.");
			}
		} else if (StringUtils.isNotBlank(config.getString(ParamConstants.CONNECTION_WALLET_PARAM))) {
			if (StringUtils.isBlank(ParamConstants.CONNECTION_TNS_ADMIN_PARAM)) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN,
						ParamConstants.CONNECTION_TNS_ADMIN_PARAM,
						ParamConstants.CONNECTION_WALLET_PARAM);
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
			if (StringUtils.isBlank(ParamConstants.CONNECTION_TNS_ALIAS_PARAM)) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN,
						ParamConstants.CONNECTION_TNS_ALIAS_PARAM,
						ParamConstants.CONNECTION_WALLET_PARAM);
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Connection to RDBMS will be performed using Oracle Wallet.");
			}
		} else {
			LOGGER.error("Database connection parameters are not properly set!\nYou must set in connector properties or {} or {} parameter to connect to RDBMS!",
					ParamConstants.CONNECTION_URL_PARAM,
					ParamConstants.CONNECTION_WALLET_PARAM);
			throw new ConnectException(DB_PARAM_ERROR_GENERIC);
		}

		if (config.getBoolean(ParamConstants.MAKE_STANDBY_ACTIVE_PARAM)) {
			if (StringUtils.isBlank(ParamConstants.STANDBY_WALLET_PARAM)) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN_TRUE,
						ParamConstants.STANDBY_WALLET_PARAM,
						ParamConstants.MAKE_STANDBY_ACTIVE_PARAM);
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
			if (StringUtils.isBlank(ParamConstants.STANDBY_TNS_ADMIN_PARAM)) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN_TRUE,
						ParamConstants.STANDBY_TNS_ADMIN_PARAM,
						ParamConstants.MAKE_STANDBY_ACTIVE_PARAM);
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
			if (StringUtils.isBlank(ParamConstants.STANDBY_TNS_ALIAS_PARAM)) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN_TRUE,
						ParamConstants.STANDBY_TNS_ADMIN_PARAM,
						ParamConstants.MAKE_STANDBY_ACTIVE_PARAM);
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
			if (config.getBoolean(ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM)) {
				LOGGER.warn("When the '{}' parameter is set to true, the '{}' parameter must be set to false!",
						ParamConstants.MAKE_STANDBY_ACTIVE_PARAM,
						ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM);
			}
		}
		if (config.getBoolean(ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM)) {
			if (StringUtils.isBlank(ParamConstants.DISTRIBUTED_WALLET_PARAM)) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN_TRUE,
						ParamConstants.DISTRIBUTED_WALLET_PARAM,
						ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM);
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
			if (StringUtils.isBlank(ParamConstants.DISTRIBUTED_TNS_ADMIN_PARAM)) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN_TRUE,
						ParamConstants.DISTRIBUTED_TNS_ADMIN_PARAM,
						ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM);
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
			if (StringUtils.isAllBlank(ParamConstants.DISTRIBUTED_TNS_ALIAS_PARAM)) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN_TRUE,
						ParamConstants.DISTRIBUTED_TNS_ALIAS_PARAM,
						ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM);
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
		}
		if (StringUtils.isBlank(config.getString(ParamConstants.TEMP_DIR_PARAM))) {
			connectorProperties.put(ParamConstants.TEMP_DIR_PARAM, System.getProperty("java.io.tmpdir"));
			LOGGER.info(VALUE_SET_TO,
					ParamConstants.TEMP_DIR_PARAM,
					System.getProperty("java.io.tmpdir"));
		}
		if (Files.isDirectory(Paths.get(config.getString(ParamConstants.TEMP_DIR_PARAM)))) {
			if (!Files.isWritable(Paths.get(config.getString(ParamConstants.TEMP_DIR_PARAM)))) {
				LOGGER.error(
						"Parameter '{}' points to non-writable directory '{}'.",
						ParamConstants.TEMP_DIR_PARAM,
						config.getString(ParamConstants.TEMP_DIR_PARAM));
				throw new ConnectException(TMP_PARAM_ERROR_GENERIC);
			}
		} else {
			LOGGER.error(
					"Parameter {} set to non-existent or invalid directory {}.",
					ParamConstants.TEMP_DIR_PARAM,
					config.getString(ParamConstants.TEMP_DIR_PARAM));
			throw new ConnectException(TMP_PARAM_ERROR_GENERIC);
		}

		if (StringUtils.isBlank(config.getString(ParamConstants.PERSISTENT_STATE_FILE_PARAM))) {
			connectorProperties.put(
					ParamConstants.PERSISTENT_STATE_FILE_PARAM,
					System.getProperty("java.io.tmpdir") +  
						(StringUtils.endsWith(
								System.getProperty("java.io.tmpdir"), File.separator) ? "" : File.separator) +
						"oracdc.state");
			LOGGER.info(VALUE_SET_TO,
					ParamConstants.PERSISTENT_STATE_FILE_PARAM,
					connectorProperties.get(ParamConstants.PERSISTENT_STATE_FILE_PARAM));
		}

		if (config.getLong(ParamConstants.LGMNR_START_SCN_PARAM) < 1) {
			connectorProperties.remove(ParamConstants.LGMNR_START_SCN_PARAM);
		}

		if (config.getList(ParamConstants.TABLE_EXCLUDE_PARAM).size() < 1) {
			connectorProperties.remove(ParamConstants.TABLE_EXCLUDE_PARAM);
		}
		if (config.getList(ParamConstants.TABLE_INCLUDE_PARAM).size() < 1) {
			connectorProperties.remove(ParamConstants.TABLE_INCLUDE_PARAM);
		}

		if (config.getLong(ParamConstants.REDO_FILES_SIZE_PARAM) > 0) {
			LOGGER.info("Redo size threshold will be used instead of count of redo files.");
			connectorProperties.remove(ParamConstants.REDO_FILES_COUNT_PARAM);
		} else {
			LOGGER.info("Count of redo files will be used instead of size threshold.");
			connectorProperties.remove(ParamConstants.REDO_FILES_SIZE_PARAM);
			if (config.getShort(ParamConstants.REDO_FILES_COUNT_PARAM) < 1) {
				connectorProperties.put(ParamConstants.REDO_FILES_COUNT_PARAM, "1");
			}
		}

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
			connectorProperties.put(ParamConstants.ARCHIVED_LOG_CAT_PARAM, 
					OraCdcDistributedV$ArchivedLogImpl.class.getCanonicalName());
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
		final List<Map<String, String>> configs = new ArrayList<>(1);
		configs.add(connectorProperties);
		return configs;
	}

	@Override
	public ConfigDef config() {
		return OraCdcSourceConnectorConfig.config();
	}

}
