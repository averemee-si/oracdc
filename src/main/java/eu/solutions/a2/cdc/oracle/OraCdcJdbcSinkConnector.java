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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.kafka.connect.OraCdcJdbcSinkConnectorConfig;
import eu.solutions.a2.cdc.oracle.kafka.connect.OraCdcJdbcSinkTask;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.Version;

public class OraCdcJdbcSinkConnector extends SinkConnector {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcJdbcSinkConnector.class);

	private Map<String, String> props;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc Sink Connector");
		this.props = props;


		try {
			LOGGER.trace("BEGIN: Hikari Connection Pool init.");
			HikariPoolConnectionFactory.init(
					props.get(ParamConstants.CONNECTION_URL_PARAM),
					props.get(ParamConstants.CONNECTION_USER_PARAM),
					props.get(ParamConstants.CONNECTION_PASSWORD_PARAM));
			LOGGER.trace("END: Hikari Connection Pool init.");
		} catch (SQLException sqle) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			throw new ConnectException("Unable to start oracdc Sink Connector.");
		}
	}

	@Override
	public void stop() {
	}

	@Override
	public Class<? extends Task> taskClass() {
		return OraCdcJdbcSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
		for (int i = 0; i < maxTasks; i++) {
			configs.add(props);
		}
		return configs;
	}

	@Override
	public ConfigDef config() {
		return OraCdcJdbcSinkConnectorConfig.config();
	}

}
