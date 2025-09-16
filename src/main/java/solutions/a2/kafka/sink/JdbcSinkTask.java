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

package solutions.a2.kafka.sink;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.Strings;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.utils.Version;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class JdbcSinkTask extends SinkTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkTask.class);

	private final Map<String, JdbcSinkTable> tablesInProcessing = new HashMap<>();
	private JdbcSinkConnectorConfig config;
	private int batchSize = 1000;
	private int schemaType;
	private JdbcSinkConnectionPool sinkPool;
	private TableNameMapper tableNameMapper;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc '{}' Sink Task", props.get("name"));
		config = new JdbcSinkConnectorConfig(props);

		try {
			LOGGER.debug("BEGIN: Hikari Connection Pool initialization.");
			sinkPool = new JdbcSinkConnectionPool(props.get("name"), config);
			LOGGER.debug("END: Hikari Connection Pool initialization.");
		} catch (SQLException sqle) {
			LOGGER.error("Unable to connect to {}", config.getString(ConnectorParams.CONNECTION_URL_PARAM));
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			throw new ConnectException("Unable to start oracdc Sink Connector Task.");
		}

		batchSize = config.getInt(ConnectorParams.BATCH_SIZE_PARAM);
		schemaType = config.getSchemaType();
		tableNameMapper = config.getTableNameMapper();
		tableNameMapper.configure(config);
	}

	private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
	private final Set<String> tablesInProcess = new HashSet<>();
	@Override
	public void put(Collection<SinkRecord> records) {
		LOGGER.debug("BEGIN: put()");
		tablesInProcess.clear();
		currentOffsets.clear();
		try (Connection connection = sinkPool.getConnection()) {
			int processedRecords = 0;
			for (SinkRecord record : records) {
				final int version = record.valueSchema() != null
						? record.valueSchema().version()
						: 1;
				final String tableNameWithoutVersion = tableNameMapper.getTableName(record);
				final String tableName = tableNameWithoutVersion + ":" + Integer.toString(version);
				JdbcSinkTable oraTable = tablesInProcessing.get(tableName);
				if (oraTable != null && version != oraTable.getVersion()) {
					LOGGER.info(
							"\n=====================\n" +
							"New version {} for table {} with existing version {} detected.\n" +
							"Table definition will be updated to new version." +
							"\n=====================\n",
							version, oraTable.getTableFqn(), oraTable.getVersion());
					tablesInProcessing.remove(tableName);
					tablesInProcess.remove(tableName);
					return;
				}
				if (oraTable == null) {
					LOGGER.info(
							"\n=====================\n" +
							"Creating definition for table {} with version {}" +
							"\n=====================\n",
							tableNameWithoutVersion, version);
					oraTable = new JdbcSinkTable(
								sinkPool, tableNameWithoutVersion, record, schemaType, config);
					tablesInProcessing.put(tableName, oraTable);
				}
				if (!tablesInProcess.contains(tableName)) {
					LOGGER.debug("Adding {} to current batch set.", tableName);
					tablesInProcess.add(tableName);
				}
				if (oraTable.duplicatedKeyInBatch(record)) {
					// Prevent from "ON CONFLICT DO UPDATE command cannot affect row a second time"
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Executing batch due to duplicate key for table {} .", oraTable.getTableName());
					}
					for (String tableInProgress : tablesInProcess) {
						LOGGER.debug("Executing batch for table {}.", tableInProgress);
						if (Strings.CS.equals(tableInProgress, tableName)) {
							oraTable.execAndCloseCursors();
						} else {
							tablesInProcessing.get(tableInProgress).exec();
						}
					}
					this.flush(currentOffsets);
					connection.commit();
					currentOffsets.clear();
					processedRecords = 0;
				}
				oraTable.putData(connection, record);
				currentOffsets.put(
						new TopicPartition(record.topic(), record.kafkaPartition()),
						new OffsetAndMetadata(record.kafkaOffset()));
				processedRecords++;
				if (processedRecords == batchSize) {
					for (String tableInProgress : tablesInProcess) {
						tablesInProcessing.get(tableInProgress).exec();
						LOGGER.debug("Executing batch for table {}.", tableInProgress);
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
			if (records != null && records.size() > 0) {
				LOGGER.info("{} changes processed successfully.", records.size());
			}
		} catch (SQLException sqle) {
			LOGGER.error("Error '{}' when put to target system, SQL errorCode = {}, SQL state = '{}'",
					sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			throw new ConnectException(sqle);
		}
		LOGGER.debug("END: put()");
	}

	@Override
	public void stop() {
		sinkPool = null;
	}

}
