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

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.standalone.CommonJobSingleton;
import eu.solutions.a2.cdc.oracle.standalone.KafkaSingleton;
import eu.solutions.a2.cdc.oracle.standalone.KinesisSingleton;
import eu.solutions.a2.cdc.oracle.standalone.SendMethodIntf;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.JsonConverterException;
import eu.solutions.a2.cdc.oracle.utils.JsonOraCdcConverter;
import eu.solutions.a2.cdc.oracle.utils.OraSqlUtils;

public class OraCdcProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcProducer.class);

	private static final Properties props = new Properties();
	/** Supported target systems */
	private static final int TARGET_KAFKA = 0;
	private static final int TARGET_KINESIS = 1;
	/** Set default target system to Apache Kafka */
	private static int targetSystem = TARGET_KAFKA;

	public static void main(String[] argv) {
		// Configure log4j
		initLog4j(1);
		if (argv.length == 0) {
			printUsage(OraCdcProducer.class.getCanonicalName(), 2);
		}
		// Load program properties
		loadProps(argv[0], 1);

		final String targetBroker = props.getProperty("a2.target.broker", "kafka").trim();
		if ("kafka".equalsIgnoreCase(targetBroker)) {
			targetSystem = TARGET_KAFKA;
		} else if ("kinesis".equalsIgnoreCase(targetBroker)) {
			targetSystem = TARGET_KINESIS;
		} else {
			LOGGER.warn("Wrong target broker type '{}' specified in configuration file {}", targetBroker, argv[0]);
			LOGGER.warn("Setting target broker type to kafka");
		}

		// Read and check JDBC connection properties
		boolean useWallet = true;
		final String jdbcUrl = props.getProperty(ParamConstants.CONNECTION_URL_PARAM);
		final String username = props.getProperty(ParamConstants.CONNECTION_USER_PARAM);
		final String password = props.getProperty(ParamConstants.CONNECTION_PASSWORD_PARAM);
		final String walletLocation = props.getProperty(ParamConstants.CONNECTION_WALLET_PARAM);
		final String tnsAdmin = props.getProperty(ParamConstants.CONNECTION_TNS_ADMIN_PARAM);
		final String alias = props.getProperty(ParamConstants.CONNECTION_TNS_ALIAS_PARAM);
		if (jdbcUrl == null || "".equals(jdbcUrl.trim())) {
			if (walletLocation == null || "".equals(walletLocation)) {
				LOGGER.error("{} or {} not specified in configuration file {}",
						ParamConstants.CONNECTION_URL_PARAM,
						ParamConstants.CONNECTION_WALLET_PARAM,
						argv[0]);
				LOGGER.error("Exiting.");
				System.exit(1);
			} else {
				if (tnsAdmin == null || "".equals(tnsAdmin.trim())) {
					LOGGER.error("{} not specified in configuration file {}",
							ParamConstants.CONNECTION_TNS_ADMIN_PARAM,
							argv[0]);
					LOGGER.error("Exiting.");
					System.exit(1);
				}
				if (alias == null || "".equals(alias.trim())) {
					LOGGER.error("{} not specified in configuration file {}",
							ParamConstants.CONNECTION_TNS_ALIAS_PARAM,
							argv[0]);
					LOGGER.error("Exiting.");
					System.exit(1);
				}
				useWallet = true;
			}
		} else {
			if (username == null || "".equals(username.trim())) {
				LOGGER.error("{} not specified in configuration file {}",
						ParamConstants.CONNECTION_USER_PARAM, argv[0]);
				LOGGER.error("Exiting.");
				System.exit(1);
			}
			if (password == null || "".equals(password.trim())) {
				LOGGER.error("{} not specified in configuration file {}",
						ParamConstants.CONNECTION_PASSWORD_PARAM, argv[0]);
				LOGGER.error("Exiting.");
				System.exit(1);
			}
			useWallet = false;
		}


		// Initialize connection pool
		try {
			if (useWallet)
				OraPoolConnectionFactory.init4Wallet(walletLocation, tnsAdmin, alias);
			else
				OraPoolConnectionFactory.init(jdbcUrl.trim(), username, password);
		} catch (SQLException | ClassNotFoundException | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException pe) {
			LOGGER.error("Unable to initialize database connection.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(pe));
			LOGGER.error("Exiting!");
			System.exit(1);
		}

		int pollInterval = ParamConstants.POLL_INTERVAL_MS_DEFAULT;
		final String pollIntervalString = props.getProperty(ParamConstants.POLL_INTERVAL_MS_PARAM);
		if (pollIntervalString != null && !"".equals(pollIntervalString)) {
			try {
				pollInterval = Integer.parseInt(pollIntervalString);
			} catch (Exception e) {
				LOGGER.warn("Incorrect value for {} -> {}", ParamConstants.POLL_INTERVAL_MS_PARAM, pollIntervalString);
				LOGGER.warn("Setting it to {}", ParamConstants.POLL_INTERVAL_MS_DEFAULT);
			}
		}
		int batchSize = ParamConstants.BATCH_SIZE_DEFAULT;
		final String batchSizeString = props.getProperty(ParamConstants.BATCH_SIZE_PARAM);
		if (batchSizeString != null && !"".equals(batchSizeString)) {
			try {
				batchSize = Integer.parseInt(batchSizeString);
			} catch (Exception e) {
				LOGGER.warn("Incorrect value for {} -> {}", ParamConstants.BATCH_SIZE_PARAM, batchSizeString);
				LOGGER.warn("Setting it to {}", ParamConstants.BATCH_SIZE_DEFAULT);
			}
		}
		String excludeList = props.getProperty(ParamConstants.TABLE_EXCLUDE_PARAM);
		if (excludeList != null && !"".equals(excludeList)) {
			try {
				List<String> nameList = Arrays.asList(excludeList.split(","));
				if (nameList.size() > 0) {
					excludeList = OraSqlUtils.parseTableSchemaList(true, OraSqlUtils.MODE_WHERE_ALL_MVIEW_LOGS, nameList);
				} else {
					excludeList = null;
				}

			} catch (Exception e) {
				LOGGER.error("Unable to parse {} parameter set to -> {}",
						ParamConstants.TABLE_EXCLUDE_PARAM,
						excludeList);
				LOGGER.error("Ignoring it.....");
				excludeList = null;
			}
		} else {
			// Explicitly set to null
			excludeList = null;
		}

		String includeList = props.getProperty(ParamConstants.TABLE_INCLUDE_PARAM);
		if (includeList != null && !"".equals(includeList)) {
			try {
				List<String> nameList = Arrays.asList(includeList.split(","));
				if (nameList.size() > 0) {
					includeList = OraSqlUtils.parseTableSchemaList(false, OraSqlUtils.MODE_WHERE_ALL_MVIEW_LOGS, nameList);
				} else {
					includeList = null;
				}

			} catch (Exception e) {
				LOGGER.error("Unable to parse {} parameter set to -> {}",
						ParamConstants.TABLE_INCLUDE_PARAM,
						includeList);
				LOGGER.error("Ignoring it.....");
				excludeList = null;
			}
		} else {
			// Explicitly set to null
			includeList = null;
		}

		// Init CommonJob MBean
		CommonJobSingleton.getInstance();

		String osName = System.getProperty("os.name").toUpperCase();
		LOGGER.info("Running on " + osName);
		SendMethodIntf sendMethod = null;
		if (targetSystem == TARGET_KAFKA) {
			sendMethod = KafkaSingleton.getInstance(); 
		} else if (targetSystem == TARGET_KINESIS) {
			sendMethod = KinesisSingleton.getInstance();
		}
		sendMethod.parseSettings(props, argv[0], 1);

		final List<OraTableTask> oraTableTasks = new ArrayList<>();
		try (Connection connection = OraPoolConnectionFactory.getConnection()) {
			PreparedStatement statement = null;
			String sqlStatement = null;

			// Count number of materialized view logs
			int tableCount = 0;
			sqlStatement = OraDictSqlTexts.MVIEW_COUNT_PK_SEQ_NOSCN_NONV_NOOI;

			if (excludeList != null) {
				// Add excluded tables
				sqlStatement += excludeList;
			}
			if (includeList != null) {
				// Add excluded tables
				sqlStatement += includeList;
			}
			statement = connection.prepareStatement(sqlStatement);
			ResultSet rsCount = statement.executeQuery();
			if (rsCount.next()) {
				tableCount = rsCount.getInt(1);
			}
			rsCount.close();
			rsCount = null;
			statement.close();
			statement = null;
			if (tableCount < 1) {
				LOGGER.error("Nothing to do with user {}.", username);
				LOGGER.error("Exiting.");
				System.exit(1);
			}
			CommonJobSingleton.getInstance().setTableCount(tableCount);
			//TODO Adjust pool size/or max processing threads?

			// Read table information
			sqlStatement = OraDictSqlTexts.MVIEW_LIST_PK_SEQ_NOSCN_NONV_NOOI; 	
			if (excludeList != null) {
				// Add excluded tables
				sqlStatement += excludeList;
			}
			statement = connection.prepareStatement(sqlStatement);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				OraTable oraTable = new OraTable(
						rs.getString("LOG_OWNER"),
						rs.getString("MASTER"),
						rs.getString("LOG_TABLE"),
						"YES".equalsIgnoreCase(rs.getString("ROWIDS")),
						"YES".equalsIgnoreCase(rs.getString("PRIMARY_KEY")),
						"YES".equalsIgnoreCase(rs.getString("SEQUENCE")),
						batchSize, ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM, null, null);
				oraTableTasks.add(new OraTableTask(oraTable, pollInterval, sendMethod));
				LOGGER.info("Adding " + oraTable);
			}
		} catch (SQLException e) {
			LOGGER.error("Unable to get table information.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			LOGGER.error("Exiting!");
			System.exit(1);
		}

		ScheduledExecutorService executor = Executors.newScheduledThreadPool(
				oraTableTasks.size(),
				new ThreadFactory() {
					public Thread newThread(Runnable r) {
						Thread t = Executors.defaultThreadFactory().newThread(r);
						t.setDaemon(true);
						return t;
					}
				});

		// Add special shutdown thread
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					executor.shutdown();
					// Wait.....
					final int timeoutSeconds = 120;
					final boolean done = executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
					if (! done) {
						LOGGER.error("Incorrect shutdown after {} seconds wait!", timeoutSeconds);
					}
				} catch (InterruptedException e) {
					LOGGER.error("Problems while shutting down main thread pool!");
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
				}
				// Unfortunately sendMethod is not final.....
				if (targetSystem == TARGET_KAFKA) {
					KafkaSingleton.getInstance().shutdown();
				} else if (targetSystem == TARGET_KINESIS) {
					KinesisSingleton.getInstance().shutdown();
				}
				LOGGER.info("Shutting down...");
				//TODO - more information about processing
			}
		});

		for (OraTableTask tableTask : oraTableTasks) {
			//TODO poll time
			executor.scheduleWithFixedDelay(tableTask, 0, pollInterval, TimeUnit.MILLISECONDS);
		}

		while (true) {
			try {
				Thread.sleep(60000);
				LOGGER.info("Total records processed -> {}", CommonJobSingleton.getInstance().getProcessedRecordCount());
			} catch (Exception e) {}
		}
	}

	private static class OraTableTask implements Runnable {
		private final OraTable oraTable;
		private final int pollInterval;
		private final SendMethodIntf sendMethod;

		OraTableTask(final OraTable oraTable, final int pollInterval, final SendMethodIntf sendMethod) {
			this.oraTable = oraTable;
			this.pollInterval = pollInterval;
			this.sendMethod = sendMethod;
		}

		@Override
		public void run() {
			try (Connection connection = OraPoolConnectionFactory.getConnection()) {
				final List<SourceRecord> dbData = oraTable.pollMVLog(connection, null);
				for (SourceRecord record : dbData) {
					Schema schema = record.valueSchema();
					Struct struct = (Struct) record.value();
					try {
						String messagedata = new String(JsonOraCdcConverter.fromConnectData(schema, struct));
						final StringBuilder messageKey = new StringBuilder(64);
						messageKey.append(oraTable.getMasterTable());
						messageKey.append("-");
						messageKey.append(System.currentTimeMillis());
						//TODO
						//TODO Kinesis batching!!!
						//TODO
						sendMethod.sendData(messageKey.toString(), messagedata);
					} catch (JsonConverterException e) {
						LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
					}
				}
				//TODO
				//TODO commit!!!
				//TODO
				connection.commit();
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
						try {
							this.wait(pollInterval);
						} catch (InterruptedException e) {}
					}
				} else {
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
				}
			}
		}

	}

	private static void initLog4j(int exitCode) {
		BasicConfigurator.configure();
		// Check for valid log4j configuration
		String log4jConfig = System.getProperty("a2.log4j.configuration");
		if (log4jConfig == null || "".equals(log4jConfig)) {
			System.err.println("JVM argument -Da2.log4j.configuration must set and point to valid log4j config file!");
			System.err.println("Exiting.");
			System.exit(exitCode);
		}

		// Check that log4j configuration file exist
		Path path = Paths.get(log4jConfig);
		if (!Files.exists(path) || Files.isDirectory(path)) {
			System.err.println("JVM argument -Da2.log4j.configuration points to unknown file " + log4jConfig + "!");
			System.err.println("Exiting.");
			System.exit(exitCode);
		}
		// Initialize log4j
		PropertyConfigurator.configure(log4jConfig);

	}

	private static void printUsage(String className, int exitCode) {
		LOGGER.error("Usage:\njava {} <full path to configuration file>", className);
		LOGGER.error("Exiting.");
		System.exit(exitCode);
	}

	private static void loadProps(String configPath, int exitCode) {
		try {
			props.load(new FileInputStream(configPath));
		} catch (IOException eoe) {
			LOGGER.error("Unable to open configuration file {}", configPath);
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(eoe));
			LOGGER.error("Exiting.");
			System.exit(exitCode);
		}
	}

}
