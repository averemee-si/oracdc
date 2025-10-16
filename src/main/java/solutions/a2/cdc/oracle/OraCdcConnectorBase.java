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

package solutions.a2.cdc.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleConnection;
import solutions.a2.cdc.oracle.utils.Version;
import solutions.a2.kafka.ConnectorParams;

import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.INTERNAL_DG4RAC_THREAD_PARAM;
import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.INTERNAL_RAC_URLS_PARAM;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public abstract class OraCdcConnectorBase extends SourceConnector {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcConnectorBase.class);
	private static final String DB_PARAM_ERROR_GENERIC = "Database connection parameters are not properly set!";
	private static final String DB_PARAM_MUST_SET_WHEN = 
			"""
			
			=====================
			Parameter value '{}' must be set when parameter value '{}' is set!
			=====================
			
			""";
	private static final String DB_PARAM_MUST_SET_WHEN_TRUE =
			"""
			
			=====================
			Parameter '{}' must be set when '{}' set to true!
			=====================
			
			""";

	// Generated using 	https://patorjk.com/software/taag/#p=display&f=Ogre&t=A2%20oracdc
	private static final String LOGO =
		"""
			
		   _   ____                            _      
		  /_\\ |___ \\    ___  _ __ __ _  ___ __| | ___ 
		 //_\\\\  __) |  / _ \\| '__/ _` |/ __/ _` |/ __|
		/  _  \\/ __/  | (_) | | | (_| | (_| (_| | (__ 
		\\_/ \\_/_____|  \\___/|_|  \\__,_|\\___\\__,_|\\___|
		
		
		""";

	private Map<String, String> connectorProperties;
	private OraCdcSourceConnectorConfig config;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info(LOGO);
		try {
			config = new OraCdcSourceConnectorConfig(props);
			connectorProperties = new HashMap<>();
			connectorProperties.putAll(config.originalsStrings());
			// Copy rest of params...
			config.values().forEach((k, v) -> {
				if (!connectorProperties.containsKey(k) && v != null) {
					if (v instanceof Password) {
						connectorProperties.put(k, "");
					} else if (v instanceof Boolean) {
						connectorProperties.put(k, ((Boolean) v).toString());
					} else if (v instanceof Short) {
						connectorProperties.put(k, ((Short) v).toString());
					} else if (v instanceof Integer) {
						connectorProperties.put(k, ((Integer) v).toString());
					} else if (v instanceof Long) {
						connectorProperties.put(k, ((Long) v).toString());
					} else if (Strings.CS.equals("java.util.Collections$EmptyList", v.getClass().getName())) {
						connectorProperties.put(k, "");
					} else {
						connectorProperties.put(k, (String) v);
					}
				}
			});
		} catch (ConfigException ce) {
			throw new ConnectException("Couldn't start oracdc due to coniguration error", ce);
		}

		if (StringUtils.isBlank(config.getString(ConnectorParams.CONNECTION_URL_PARAM))) {
			LOGGER.error("Parameter '{}' must be set for running connector!",
					ConnectorParams.CONNECTION_URL_PARAM);
					throw new ConnectException(DB_PARAM_ERROR_GENERIC);
		}

		// V1.1.0 - a2.jdbc.url is mandatory parameter! No more separate a2.tns.admin and a2.tns.alias!!!
		checkDeprecatedTnsParameters(props,
				ParamConstants.CONNECTION_TNS_ADMIN_PARAM,
				ParamConstants.CONNECTION_TNS_ALIAS_PARAM,
				ConnectorParams.CONNECTION_URL_PARAM);
		checkDeprecatedTnsParameters(props,
				ParamConstants.STANDBY_TNS_ADMIN_PARAM,
				ParamConstants.STANDBY_TNS_ALIAS_PARAM,
				ParamConstants.STANDBY_URL_PARAM);
		checkDeprecatedTnsParameters(props,
				ParamConstants.DISTRIBUTED_TNS_ADMIN_PARAM,
				ParamConstants.DISTRIBUTED_TNS_ALIAS_PARAM,
				ParamConstants.DISTRIBUTED_URL_PARAM);

		if (StringUtils.isBlank(config.walletLocation())) {
			if (StringUtils.isBlank(config.getString(ConnectorParams.CONNECTION_USER_PARAM))) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN,
						ConnectorParams.CONNECTION_USER_PARAM,
						ConnectorParams.CONNECTION_URL_PARAM);
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
			if (StringUtils.isBlank(
					config.getPassword(ConnectorParams.CONNECTION_PASSWORD_PARAM).value())) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN,
						ConnectorParams.CONNECTION_PASSWORD_PARAM,
						ConnectorParams.CONNECTION_URL_PARAM);
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
			LOGGER.info("Connection to RDBMS will be performed using Oracle username '{}'",
					config.getString(ConnectorParams.CONNECTION_USER_PARAM));
		} else {
			LOGGER.info("Connection to RDBMS will be performed using Oracle Wallet '{}'",
					config.walletLocation());
		}

		if (config.activateStandby()) {
			if (StringUtils.isBlank(config.getString(ParamConstants.STANDBY_URL_PARAM))) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN_TRUE,
						ParamConstants.STANDBY_URL_PARAM,
						config.activateStandbyParamName());
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
			if (StringUtils.isBlank(config.getString(ParamConstants.STANDBY_WALLET_PARAM))) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN_TRUE,
						ParamConstants.STANDBY_WALLET_PARAM,
						config.activateStandbyParamName());
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
			if (config.getBoolean(ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM)) {
				LOGGER.warn(
						"""
						
						=====================
						When the '{}' parameter is set to true, the '{}' parameter must be set to false!
						=====================
						
						""", config.activateStandbyParamName(),
							ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM);
			}
		}
		if (config.getBoolean(ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM)) {
			if (StringUtils.isBlank(config.getString(ParamConstants.DISTRIBUTED_WALLET_PARAM))) {
				LOGGER.error(DB_PARAM_MUST_SET_WHEN_TRUE,
						ParamConstants.DISTRIBUTED_WALLET_PARAM,
						ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM);
				throw new ConnectException(DB_PARAM_ERROR_GENERIC);
			}
		}

		if (config.getBoolean(ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM)) {
			// When this set we need explicitly value of  a2.archived.log.catalog parameter
			if (!OraCdcDistributedV$ArchivedLogImpl.class.getCanonicalName()
					.equals(config.getString(ParamConstants.ARCHIVED_LOG_CAT_PARAM))) {
				LOGGER.warn(
						"""
						
						=====================
						When {} set to true value of {} must be {}.
						Setting {} value to {}.
						=====================
						
						""",
						ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM,
						ParamConstants.ARCHIVED_LOG_CAT_PARAM,
						OraCdcDistributedV$ArchivedLogImpl.class.getCanonicalName(),
						ParamConstants.ARCHIVED_LOG_CAT_PARAM,
						OraCdcDistributedV$ArchivedLogImpl.class.getCanonicalName());
			}
			connectorProperties.put(ParamConstants.ARCHIVED_LOG_CAT_PARAM, 
					OraCdcDistributedV$ArchivedLogImpl.class.getCanonicalName());
		}
		try {
			config.getOraRowScnField();
			config.getOraRowTsField();
			config.getOraCommitScnField();
			config.getOraRowOpField();

			config.getOraUsernameField();
			config.getOraOsUsernameField();
			config.getOraHostnameField();
			config.getOraAuditSessionIdField();
			config.getOraSessionInfoField();
			config.getOraClientIdField();
		} catch (IllegalArgumentException iae) {
			throw new ConnectException(iae);
		}
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		final List<Map<String, String>> configs = new ArrayList<>();
		List<String> instances = null;
		List<String> urls = null;
		List<String> threads = null;
		boolean isRac = false;
		boolean isSingleInstDg4Rac = false;
		if (config.useRac()) {
			try (OracleConnection connection = (OracleConnection) OraConnectionObjects.getConnection(config)) {
				instances = OraRdbmsInfo.getInstances(connection);
				if (instances.size() > 0) {
					if (instances.size() > maxTasks) {
						LOGGER.error(
								"""
								
								=====================
								Number of Oracle RAC instances for connection '{}'
									is {}, but Kafka Connect 'tasks.max' parameter is set to {}!
								Please set value of 'tasks.max' parameter to {} and restart connector!
								=====================
								
								""",
								config.getString(ConnectorParams.CONNECTION_URL_PARAM),
								instances.size(), maxTasks, instances.size());
						throw new ConnectException("Please increase value of 'tasks.max' parameter!");
					}
					isRac = true;
					urls = OraRdbmsInfo.generateRacJdbcUrls(
							(String )connection.getProperties().get(OracleConnection.CONNECTION_PROPERTY_DATABASE),
							instances);
					final StringBuilder sb = new StringBuilder(0x100);
					urls.forEach(url -> sb.append("\n\t").append(url));
					LOGGER.info(
							"""
							
							=====================
							'{}' Oracle RAC instances found.
							To connect to them, the JDBC URLs listed below will be used:{}
							=====================
							
							""", instances.size(), sb.toString());
				} else {
					LOGGER.warn(
							"""
							
							=====================
							The '{}' parameter is set to 'true', but Oracle RAC is not detected!
							The connector continues to operate with the '{}'='false' parameter.
							=====================
							
							""", config.useRacParamName(), config.useRacParamName());
					connectorProperties.put(config.useRacParamName(), Boolean.FALSE.toString());
				}
			} catch (SQLException sqle) {
				LOGGER.error(
						"""
						
						=====================
						'{}'
						errorCode={}, SQLState = '{}'
						=====================
						
						""", sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
				throw new ConnectException(sqle);
			}
		} else if (config.activateStandby()) {
			try (OracleConnection connection = (OracleConnection) OraConnectionObjects.getStandbyConnection(
					config.getString(ParamConstants.STANDBY_URL_PARAM),
					config.getString(ParamConstants.STANDBY_WALLET_PARAM))) {
				threads = OraRdbmsInfo.getStandbyThreads(connection);
				isSingleInstDg4Rac = threads.size() > 1; 
				if (isSingleInstDg4Rac) {
					if (threads.size() > maxTasks) {
						LOGGER.error(
								"""
								
								=====================
								Number of Oracle RAC instances for connection '{}'
									is {}, but Kafka Connect 'tasks.max' parameter is set to {}!
								Please set value of 'tasks.max' parameter to {} and restart connector!
								=====================
								
								""",
								config.getString(ParamConstants.STANDBY_URL_PARAM), threads.size(), maxTasks, threads.size());
						throw new ConnectException("Please increase value of 'tasks.max' parameter!");
					}
					LOGGER.info(
							"""
							
							=====================
							'{}' Oracle Sigle Instance DataGuard for RAC redo threads detected.
							=====================
							
							""", threads.size());
				}
			} catch (SQLException sqle) {
				if (sqle.getErrorCode() == OraRdbmsInfo.ORA_12514) {
					//ORA-12514, TNS:listener does not currently know of service requested in connect descriptor
					LOGGER.error(
							"""
							
							=====================
							'{}'
							errorCode={}, SQLState = '{}'
							Unable to connect to:
								{}!
							Please check Oracle DataGuard connection parameters!
							=====================
							
							""",
							sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState(),
							config.getString(ParamConstants.STANDBY_URL_PARAM));
				} else if (sqle.getErrorCode() == OraRdbmsInfo.ORA_1017) {
					//ORA-01017: invalid username/password; logon denied
					LOGGER.error(
							"""
							
							=====================
							'{}'
							errorCode={}, SQLState = '{}'
							Unable to connect to:
								'{}' using wallet at '{}'!
							Please review Oracle Support Services Note 
								"java.sql.SQLException: ORA-01017: invalid username/password; logon denied" While Trying To Run The Program With Stored Credentials In The Wallet (Doc ID 2438265.1)!
							on https://support.oracle.com/rs?type=doc&id=2438265.1
							=====================
							
							""",
							sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState(),
							config.getString(ParamConstants.STANDBY_URL_PARAM),
							config.getString(ParamConstants.STANDBY_WALLET_PARAM));
				} else {
					LOGGER.error(
							"""
							
							=====================
							'{}'
							errorCode={}, SQLState = '{}'
							=====================
							
							""", sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
				}
				throw new ConnectException(sqle);
			}
		}
		if (isRac) {
			// We need to replace JDBC URL...
			connectorProperties.put(INTERNAL_RAC_URLS_PARAM, String.join(",", urls));
			for (int i = 0; i < instances.size(); i++) {
				configs.add(connectorProperties);
				LOGGER.info("Done with configuration of task #{} for Oracle RAC instance '{}'",
						i, instances.get(i));
			}
		} else if (isSingleInstDg4Rac) {
			connectorProperties.put(INTERNAL_DG4RAC_THREAD_PARAM, String.join(",", threads));
			for (int i = 0; i < instances.size(); i++) {
				configs.add(connectorProperties);
				LOGGER.info("Done with configuration of task#{} for Oracle Single Instance DataGuard for RAC thread# '{}'",
						i, threads.get(i));
			}
		} else {
			configs.add(connectorProperties);
		}
		return configs;
	}

	@Override
	public ConfigDef config() {
		return OraCdcSourceConnectorConfig.config();
	}

	private void checkDeprecatedTnsParameters(final Map<String, String> props,
			final String tnsAdminParam, final String tnsAliasParam, final String jdbcUrlParam) {
		if (props.containsKey(tnsAdminParam) || props.containsKey(tnsAliasParam)) {
			LOGGER.error(
					"""
					
					=====================
					Parameters '{}' and '{}' are deprecated!!!
					To connect using TNS alias please set '{}' with JDBC URL format below:
						jdbc:oracle:thin:@<alias_name>?TNS_ADMIN=<directory_with_tnsnames_sqlnet>
					For example:
						jdbc:oracle:thin:@prod_db?TNS_ADMIN=/u01/app/oracle/product/21.3.0/dbhome_1/network/admin/
					For more information on JDBC URL format please see Oracle® Database JDBC Java API Reference, Release 26ai -
						https://docs.oracle.com/en/database/oracle/oracle-database/26/jajdb/
					=====================
					
					""",
					tnsAdminParam, tnsAliasParam, jdbcUrlParam);
			throw new ConnectException(DB_PARAM_ERROR_GENERIC);
		}
	}

}
