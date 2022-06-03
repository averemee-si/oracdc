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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.utils.ExceptionUtils;
import solutions.a2.cdc.oracle.utils.Version;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcJdbcSinkTask extends SinkTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcJdbcSinkTask.class);

	private final Map<String, OraTable4SinkConnector> tablesInProcessing = new HashMap<>();
	private OraCdcJdbcSinkConnectorConfig config;
	private int batchSize = 1000;
	private boolean autoCreateTable = false;
	private int schemaType;
	private OraCdcJdbcSinkConnectionPool sinkPool;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc '{}' Sink Task", props.get("name"));
		config = new OraCdcJdbcSinkConnectorConfig(props);

		try {
			LOGGER.trace("BEGIN: Hikari Connection Pool initialization.");
			sinkPool = new OraCdcJdbcSinkConnectionPool(
					props.get("name"),
					config.getString(ParamConstants.CONNECTION_URL_PARAM),
					config.getString(ParamConstants.CONNECTION_USER_PARAM),
					config.getPassword(ParamConstants.CONNECTION_PASSWORD_PARAM).value());
			LOGGER.trace("END: Hikari Connection Pool initialization.");
		} catch (SQLException sqle) {
			LOGGER.error("Unable to connect to {}", config.getString(ParamConstants.CONNECTION_URL_PARAM));
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			throw new ConnectException("Unable to start oracdc Sink Connector Task.");
		}

		batchSize = config.getInt(ParamConstants.BATCH_SIZE_PARAM);
		LOGGER.debug("batchSize = {} records.", batchSize);
		autoCreateTable = config.getBoolean(OraCdcJdbcSinkConnectorConfig.AUTO_CREATE_PARAM);
		LOGGER.debug("autoCreateTable set to {}.", autoCreateTable);
		final String schemaTypeString = props.get(ParamConstants.SCHEMA_TYPE_PARAM);
		LOGGER.debug("a2.schema.type set to {}.", schemaTypeString);
		if (ParamConstants.SCHEMA_TYPE_DEBEZIUM.equals(schemaTypeString))
			schemaType = ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM;
		else
			schemaType = ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD;
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		LOGGER.trace("BEGIN: put()");
		final Set<String> tablesInProcess = new HashSet<>();
		try (Connection connection = sinkPool.getConnection()) {
			int processedRecords = 0;
			final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
			for (SinkRecord record : records) {
				final String tableName;
				if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
					tableName = record.topic();
					LOGGER.debug("Table name from Kafka topic = {}.", tableName);
				} else { //schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM
					tableName = ((Struct) record.value()).getStruct("source").getString("table");
					LOGGER.debug("Table name from 'source' field = {}.", tableName);
				}
				OraTable4SinkConnector oraTable = tablesInProcessing.get(tableName);
				if (oraTable == null) {
					LOGGER.debug("Create new table definition for {} and add it to processing map,", tableName);
					oraTable = new OraTable4SinkConnector(
								sinkPool, tableName, record, autoCreateTable, schemaType);
					tablesInProcessing.put(tableName, oraTable);
				}
				if (!tablesInProcess.contains(tableName)) {
					LOGGER.debug("Adding {} to current batch set.", tableName);
					tablesInProcess.add(tableName);
				}
				oraTable.putData(connection, record);
				currentOffsets.put(
						new TopicPartition(record.topic(), record.kafkaPartition()),
						new OffsetAndMetadata(record.kafkaOffset()));
				processedRecords++;
				if (processedRecords == batchSize) {
					for (String tableInProgress : tablesInProcess) {
						LOGGER.debug("Executing batch for table {}.", tableInProgress);
						tablesInProcessing.get(tableInProgress).exec();
					}
					this.flush(currentOffsets);
					connection.commit();
					currentOffsets.clear();
					processedRecords = 0;
				}
			}
			LOGGER.debug("Execute and close cursors");
			for (String tableInProgress : tablesInProcess) {
				LOGGER.debug("Last batch execution and statements closing for table {}.", tableInProgress);
				tablesInProcessing.get(tableInProgress).execAndCloseCursors();
			}
			connection.commit();
		} catch (SQLException sqle) {
			LOGGER.error("Error '{}' when put to target system, SQL errorCode = {}, SQL state = '{}'",
					sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			throw new ConnectException(sqle);
		}
		LOGGER.trace("BEGIN: put()");
	}

	@Override
	public void stop() {
		sinkPool = null;
	}

}
