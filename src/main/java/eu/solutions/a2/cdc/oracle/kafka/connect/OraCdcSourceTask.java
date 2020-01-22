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

package eu.solutions.a2.cdc.oracle.kafka.connect;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.OraPoolConnectionFactory;
import eu.solutions.a2.cdc.oracle.OraTable;
import eu.solutions.a2.cdc.oracle.ParamConstants;
import eu.solutions.a2.cdc.oracle.standalone.avro.Source;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.Version;

public class OraCdcSourceTask extends SourceTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcSourceTask.class);

	private OraTable oraTable;
	private int batchSize;
	private int pollInterval;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc Source Task for {}", props.get(OraCdcSourceConnectorConfig.TASK_PARAM_MASTER));

		batchSize = Integer.parseInt(props.get(ParamConstants.BATCH_SIZE_PARAM));
		pollInterval = Integer.parseInt(props.get(ParamConstants.POLL_INTERVAL_MS_PARAM));

		try {
			oraTable = new OraTable(
					props.get(OraCdcSourceConnectorConfig.TASK_PARAM_OWNER),
					props.get(OraCdcSourceConnectorConfig.TASK_PARAM_MASTER),
					props.get(OraCdcSourceConnectorConfig.TASK_PARAM_MV_LOG),
					"YES".equalsIgnoreCase(props.get(OraCdcSourceConnectorConfig.TASK_PARAM_MV_ROWID)),
					"YES".equalsIgnoreCase(props.get(OraCdcSourceConnectorConfig.TASK_PARAM_MV_PK)),
					"YES".equalsIgnoreCase(props.get(OraCdcSourceConnectorConfig.TASK_PARAM_MV_SEQUENCE)),
					batchSize);
			if (Source.schemaType() == Source.SCHEMA_TYPE_KAFKA_CONNECT_STD)
				oraTable.setKafkaConnectTopic(
						props.get(OraCdcSourceConnectorConfig.TOPIC_PREFIX_PARAM) + 
						props.get(OraCdcSourceConnectorConfig.TASK_PARAM_MASTER));
			else
				// Source.SCHEMA_TYPE_STANDALONE
				oraTable.setKafkaConnectTopic(props.get(OraCdcSourceConnectorConfig.KAFKA_TOPIC_PARAM));
		} catch (SQLException sqle) {
			LOGGER.error("Unable to get table information.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
		}
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		synchronized (this) {
			this.wait(pollInterval);
		}
		try (Connection connection = OraPoolConnectionFactory.getConnection()) {
			final List<SourceRecord> result = oraTable.poll(connection);
			this.commit();
			connection.commit();
			return result;
		} catch (SQLException sqle) {
			LOGGER.error("Unable to poll data from Oracle RDBMS. Oracle error code: {}.\n", sqle.getErrorCode());
			LOGGER.error("Oracle error message: {}.\n", sqle.getMessage());
			if (sqle.getSQLState() != null)
				LOGGER.error("Oracle SQL State: {}\n", sqle.getSQLState());
			if (sqle instanceof SQLRecoverableException) {
				// Recoverable... Just wait and do it again...
				//TODO - separate timeout???
				synchronized (this) {
					this.wait(pollInterval);
				}
			} else {
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			}
		}
		return null;
	}

	@Override
	public void stop() {
		LOGGER.info("Stopping task.");
		// TODO Auto-generated method stub
	}

}
