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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import static solutions.a2.utils.ExceptionUtils.getExceptionStackTrace;
import static solutions.a2.cdc.oracle.utils.Version.getVersion;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class WrappedDataJdbcSinkTask extends SinkTask {

	private static final Logger LOGGER = LogManager.getLogger(WrappedDataJdbcSinkTask.class);

	private final Map<String, WrappedDataTable> tablesInProcessing = new HashMap<>();
	private JdbcSinkConnectorConfig config;
	private int batchSize = 1000;
	private JdbcSinkConnectionPool sinkPool;
	private TableNameMapper tableNameMapper;

	@Override
	public String version() {
		return getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc '{}' Sink Task for wrapped column data", props.get("name"));
		config = new JdbcSinkConnectorConfig(props);

		try {
			LOGGER.debug("BEGIN: Hikari Connection Pool initialization.");
			sinkPool = new JdbcSinkConnectionPool(props.get("name"), config);
			LOGGER.debug("END: Hikari Connection Pool initialization.");
		} catch (SQLException sqle) {
			LOGGER.error(
					"""
					
					=====================
					'{}' on attempt to connect to ''.
					SQL errorCode = {}, SQL state = '{}'. Stack trace:
					{}
					=====================
					
					""", sqle.getMessage(), config.getJdbcUrl(), sqle.getErrorCode(),
						sqle.getSQLState(), getExceptionStackTrace(sqle));
			throw new ConnectException("Unable to start oracdc Sink Connector Task.");
		}

		batchSize = config.batchSize();
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
				final var version = record.valueSchema() != null
						? record.valueSchema().version()
						: 1;
				final var tableNameWithoutVersion = tableNameMapper.getTableName(record);
				final var tableName = tableNameWithoutVersion + ":" + Integer.toString(version);
				var oraTable = tablesInProcessing.get(tableName);
				if (oraTable == null) {
					LOGGER.info(
							"""
							
							=====================
							Creating definition for table {} with version {}
							=====================
							
							""", tableNameWithoutVersion, version);
					oraTable = new WrappedDataTable(sinkPool, tableNameWithoutVersion, record, config);
					tablesInProcessing.put(tableName, oraTable);
				}
				if (!tablesInProcess.contains(tableName)) {
					LOGGER.debug("Adding {} to current batch set.", tableName);
					tablesInProcess.add(tableName);
				}
				if (oraTable.duplicatedKeyInBatch(record)) {
					// Prevent from "ON CONFLICT DO UPDATE command cannot affect row a second time"
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Executing batch due to duplicate key for table {} .", oraTable.tableName());
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
			LOGGER.error(
					"""
					
					=====================
					'{}' when put to target system.
					SQL errorCode = {}, SQL state = '{}'. Stack trace:
					{}
					=====================
					
					""", sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState(), getExceptionStackTrace(sqle));
			throw new ConnectException(sqle);
		}
		LOGGER.debug("END: put()");
	}

	@Override
	public void stop() {
		sinkPool = null;
	}

}
