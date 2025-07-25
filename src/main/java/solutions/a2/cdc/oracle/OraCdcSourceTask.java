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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Strings;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.ucp.UniversalConnectionPoolException;
import solutions.a2.cdc.oracle.utils.Version;
import solutions.a2.utils.ExceptionUtils;

import static solutions.a2.kafka.ConnectorParams.BATCH_SIZE_PARAM;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_INT_KAFKA_STD;

import static solutions.a2.cdc.oracle.OraCdcSourceConnector.TASK_PARAM_MASTER;
import static solutions.a2.cdc.oracle.OraCdcSourceConnector.TASK_PARAM_MV_LOG;
import static solutions.a2.cdc.oracle.OraCdcSourceConnector.TASK_PARAM_OWNER;
import static solutions.a2.cdc.oracle.OraCdcSourceConnector.TASK_PARAM_MV_ROWID;
import static solutions.a2.cdc.oracle.OraCdcSourceConnector.TASK_PARAM_MV_PK;
import static solutions.a2.cdc.oracle.OraCdcSourceConnector.TASK_PARAM_MV_SEQUENCE;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcSourceTask extends SourceTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcSourceTask.class);
	private static final String PARTITION_FIELD = "mvlog";

	private OraTable oraTable;
	private int batchSize;
	private int pollInterval;
	private int schemaType;
	private String topic;
	private OraCdcSourceBaseConfig config;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {

		LOGGER.info("Starting oracdc Source Task for {}", props.get(TASK_PARAM_MASTER));

		try {
			config = new OraCdcSourceBaseConfig(props);
		} catch (ConfigException ce) {
			throw new ConnectException("Couldn't start oracdc due to coniguration error", ce);
		}

		batchSize = config.getInt(BATCH_SIZE_PARAM);
		LOGGER.debug("batchSize = {} records.", batchSize);
		pollInterval = config.pollIntervalMs();
		LOGGER.debug("pollInterval = {} ms.", pollInterval);
		schemaType = config.schemaType();
		LOGGER.debug("schemaType (Integer value 1 for Debezium, 2 for Kafka STD) = {} .", schemaType);
		if (schemaType == SCHEMA_TYPE_INT_KAFKA_STD) {
			topic = config.topicOrPrefix() + 
					props.get(TASK_PARAM_MASTER);
		} else {
			// ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM
			topic = config.kafkaTopic();
		}
		LOGGER.debug("topic set to {}.", topic);

		try (Connection connDictionary = OraPoolConnectionFactory.getConnection()) {
			LOGGER.trace("Checking for stored offset...");
			final String tableName = props.get(TASK_PARAM_MASTER);
			final String tableOwner = props.get(TASK_PARAM_OWNER); 
			OraRdbmsInfo rdbmsInfo = new OraRdbmsInfo(connDictionary);
			LOGGER.trace("Setting source partition name for processing snapshot log");
			final String sourcePartitionName = rdbmsInfo.getInstanceName() + "_" + rdbmsInfo.getHostName() + ":" +
						tableName + "." + tableOwner;
			LOGGER.debug("Source Partition {} set to {}.", PARTITION_FIELD,  sourcePartitionName);
			final Map<String, String> partition = Collections.singletonMap(PARTITION_FIELD, sourcePartitionName);
			Map<String, Object> offset = context.offsetStorageReader().offset(partition);
			if (offset != null && LOGGER.isDebugEnabled()) {
				if (offset.get(OraColumn.ORA_ROWSCN) != null)
					LOGGER.debug("Last record SCN(from {} pseudocolumn) for {} in offset file = {}.",
							OraColumn.ORA_ROWSCN, sourcePartitionName, (long) offset.get(OraColumn.ORA_ROWSCN));
				if (offset.get(OraColumn.MVLOG_SEQUENCE) != null)
					LOGGER.debug("Last processed {} for {} in offset file = {}.",
							OraColumn.MVLOG_SEQUENCE, sourcePartitionName, (long) offset.get(OraColumn.MVLOG_SEQUENCE));
			}

			oraTable = new OraTable(
					tableOwner, tableName,
					props.get(TASK_PARAM_MV_LOG),
					"YES".equalsIgnoreCase(props.get(TASK_PARAM_MV_ROWID)),
					"YES".equalsIgnoreCase(props.get(TASK_PARAM_MV_PK)),
					"YES".equalsIgnoreCase(props.get(TASK_PARAM_MV_SEQUENCE)),
					batchSize, schemaType, partition, offset, rdbmsInfo, config);
		} catch (SQLException sqle) {
			LOGGER.error("Unable to get table information.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			throw new ConnectException(sqle);
		}
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		LOGGER.trace("BEGIN: poll()");
		synchronized (this) {
			LOGGER.trace("Waiting {} ms", pollInterval);
			this.wait(pollInterval);
		}
		try (Connection connection = OraPoolConnectionFactory.getConnection()) {
			final List<SourceRecord> result = oraTable.pollMVLog(connection, topic);
			LOGGER.trace("Before commit at Kafka side.");
			this.commit();
			LOGGER.trace("After commit at Kafka side & before commit at RDBMS side.");
			connection.commit();
			LOGGER.trace("END: poll()");
			return result;
		} catch (SQLException sqle) {
			LOGGER.error("Unable to poll data from Oracle RDBMS. Oracle error code: {}.\n", sqle.getErrorCode());
			LOGGER.error("Oracle error message: {}.\n", sqle.getMessage());
			if (sqle.getSQLState() != null)
				LOGGER.error("Oracle SQL State: {}\n", sqle.getSQLState());
			if (sqle instanceof SQLRecoverableException) {
				// Recoverable... Just wait and do it again...
				//TODO - separate timeout???
				LOGGER.trace("Recoverable RDBMS exception, waiting {} ms to retry.", pollInterval);
				LOGGER.debug(ExceptionUtils.getExceptionStackTrace(sqle));
				synchronized (this) {
					this.wait(pollInterval);
				}
			} else if (sqle.getCause() != null &&
					sqle.getCause() instanceof UniversalConnectionPoolException &&
					Strings.CS.contains(sqle.getCause().getMessage(), "Universal Connection Pool is about to shutdown")) {
				LOGGER.warn("Got '{}' while stopping task.", sqle.getCause().getMessage());
			} else {
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
				throw new ConnectException(sqle);
			}
		}
		return null;
	}

	@Override
	public void stop() {
		LOGGER.info("Stopping task.");
	}

}