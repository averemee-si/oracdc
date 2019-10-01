/**
 * Copyright (c) 2018-present, http://a2-solutions.eu
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import eu.solutions.a2.cdc.oracle.HikariPoolConnectionFactory;
import eu.solutions.a2.cdc.oracle.standalone.OraTable;
import eu.solutions.a2.cdc.oracle.standalone.avro.Envelope;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.Version;

public class OraCdcJdbcSinkTask extends SinkTask {

	private static final Logger LOGGER = Logger.getLogger(OraCdcJdbcSinkTask.class);
	private static final ObjectReader reader = new ObjectMapper().readerFor(Envelope.class);

	final Map<String, OraTable> tablesInProcessing = new HashMap<>(); 
	OraCdcJdbcSinkConnectorConfig config;
	int batchSize = 1000;
	boolean autoCreateTable = false;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc Sink Task");
		config = new OraCdcJdbcSinkConnectorConfig(props);
		batchSize = config.getInt(OraCdcJdbcSinkConnectorConfig.BATCH_SIZE_PARAM);
		autoCreateTable = config.getBoolean(OraCdcJdbcSinkConnectorConfig.AUTO_CREATE_PARAM);
		try {
			HikariPoolConnectionFactory.init(
					config.getString(OraCdcJdbcSinkConnectorConfig.CONNECTION_URL_PARAM),
					config.getString(OraCdcJdbcSinkConnectorConfig.CONNECTION_USER_PARAM),
					config.getString(OraCdcJdbcSinkConnectorConfig.CONNECTION_PASSWORD_PARAM));
		} catch (SQLException sqle) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
		}
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		final Set<String> tablesInProcess = new HashSet<>();
		try (Connection connection = HikariPoolConnectionFactory.getConnection()) {
			for (SinkRecord record : records) {
				LOGGER.debug("Processing key:\t" + record.key());
				try {
					final Envelope envelope =  reader.readValue((String)record.value());
					final String tableName = envelope.getPayload().getSource().getTable();
					OraTable oraTable = tablesInProcessing.get(tableName);
					if (oraTable == null) {
						oraTable = new OraTable(
								envelope.getPayload().getSource(),
								envelope.getSchema(),
								autoCreateTable);
						tablesInProcessing.put(tableName, oraTable);
					}
					if (!tablesInProcess.contains(oraTable.getMasterTable())) {
						tablesInProcess.add(oraTable.getMasterTable());
					}
					oraTable.putData(connection, envelope.getPayload());
				} catch (IOException ioe) {
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
				}
			}
			Iterator<String> iterator = tablesInProcess.iterator();
			while (iterator.hasNext()) {
				final String tableName = iterator.next();
				tablesInProcessing.get(tableName).closeCursors();
			}
			connection.commit();
		} catch (SQLException sqle) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
		}
	}

	@Override
	public void stop() {
	}

}
