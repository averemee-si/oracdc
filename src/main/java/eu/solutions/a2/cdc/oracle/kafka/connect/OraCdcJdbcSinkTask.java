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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.HikariPoolConnectionFactory;
import eu.solutions.a2.cdc.oracle.OraTable;
import eu.solutions.a2.cdc.oracle.ParamConstants;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.Version;

public class OraCdcJdbcSinkTask extends SinkTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcJdbcSinkTask.class);

	private final Map<String, OraTable> tablesInProcessing = new HashMap<>(); 
	private OraCdcJdbcSinkConnectorConfig config;
	int batchSize = 1000;
	boolean autoCreateTable = false;
	private int schemaType;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc Sink Task");
		config = new OraCdcJdbcSinkConnectorConfig(props);
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
		try (Connection connection = HikariPoolConnectionFactory.getConnection()) {
			for (SinkRecord record : records) {
				LOGGER.debug("Processing key:\t" + record.key());
				final String tableName;
				if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
					tableName = record.topic();
					LOGGER.debug("Table name from Kafka topic = {}.", tableName);
				} else { //schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM
					tableName = ((Struct) record.value()).getStruct("source").getString("table");
					LOGGER.debug("Table name from 'source' field = {}.", tableName);
				}
				OraTable oraTable = tablesInProcessing.get(tableName);
				if (oraTable == null) {
					LOGGER.trace("Create new table definition for {} and add it to processing map,", tableName);
					oraTable = new OraTable(
							tableName, record, autoCreateTable, schemaType);
					tablesInProcessing.put(tableName, oraTable);
				}
				if (!tablesInProcess.contains(oraTable.getMasterTable())) {
					LOGGER.debug("Adding {} to current batch set.", tableName);
					tablesInProcess.add(oraTable.getMasterTable());
				}
				oraTable.putData(connection, record);
			}
			LOGGER.trace("Close cursors");
			Iterator<String> iterator = tablesInProcess.iterator();
			while (iterator.hasNext()) {
				final String tableName = iterator.next();
				tablesInProcessing.get(tableName).closeCursors();
			}
			connection.commit();
		} catch (SQLException sqle) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
		}
		LOGGER.trace("BEGIN: put()");
	}

	@Override
	public void stop() {
	}

}
