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

package eu.solutions.a2.cdc.oracle;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import eu.solutions.a2.cdc.oracle.standalone.CommonJobSingleton;
import eu.solutions.a2.cdc.oracle.standalone.KafkaSingleton;
import eu.solutions.a2.cdc.oracle.standalone.KinesisSingleton;
import eu.solutions.a2.cdc.oracle.standalone.OraTable;
import eu.solutions.a2.cdc.oracle.standalone.SendMethodIntf;
import eu.solutions.a2.cdc.oracle.standalone.avro.Source;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

public class OraCdcProducer {

	private static final Logger LOGGER = Logger.getLogger(OraCdcProducer.class);

	/** Default interval in milliseconds between DB query */
	private static final int POLL_INTERVAL = 1000;
	/** Default number of records to read from table */
	private static final int BATCH_SIZE = 100;

	private static final Properties props = new Properties();
	/** Supported target systems */
	private static final int TARGET_KAFKA = 0;
	private static final int TARGET_KINESIS = 1;
	/** Set default target system to Apache Kafka */
	private static int targetSystem = TARGET_KAFKA;


	public static final String MVIEW_LOG_WHERE = 
			"where  L.ROWIDS='NO' and L.PRIMARY_KEY='YES' and L.OBJECT_ID='NO' and L.SEQUENCE='YES' and L.COMMIT_SCN_BASED='NO' and L.INCLUDE_NEW_VALUES='NO'\n";


	public static void main(String[] argv) {
		// Configure log4j
		BasicConfigurator.configure();
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
			LOGGER.warn("Wrong target broker type '" + targetBroker + "' specified in configuration file " + argv[0]);
			LOGGER.warn("Setting target broker type to kafka");
		}

		// Read and check JDBC connection properties
		final String jdbcUrl = props.getProperty("a2.jdbc.url");
		if (jdbcUrl == null || "".equals(jdbcUrl.trim())) {
			LOGGER.fatal("a2.jdbc.url not specified in configuration file " + argv[0]);
			LOGGER.fatal("Exiting.");
			System.exit(1);
		}
		final String username = props.getProperty("a2.jdbc.username");
		if (username == null || "".equals(username.trim())) {
			LOGGER.fatal("a2.jdbc.username not specified in configuration file " + argv[0]);
			LOGGER.fatal("Exiting.");
			System.exit(1);
		}
		final String password = props.getProperty("a2.jdbc.password");
		if (password == null || "".equals(password.trim())) {
			LOGGER.fatal("a2.jdbc.password not specified in configuration file " + argv[0]);
			LOGGER.fatal("Exiting.");
			System.exit(1);
		}

		int pollInterval = POLL_INTERVAL;
		final String pollIntervalString = props.getProperty("a2.poll.interval");
		if (pollIntervalString != null && !"".equals(pollIntervalString)) {
			try {
				pollInterval = Integer.parseInt(pollIntervalString);
			} catch (Exception e) {
				LOGGER.warn("Incorrect value for a2.poll.interval -> " + pollIntervalString);
				LOGGER.warn("Setting it to " + POLL_INTERVAL);
			}
		}
		int batchSize = BATCH_SIZE;
		final String batchSizeString = props.getProperty("a2.batch.size");
		if (batchSizeString != null && !"".equals(batchSizeString)) {
			try {
				batchSize = Integer.parseInt(batchSizeString);
			} catch (Exception e) {
				LOGGER.warn("Incorrect value for a2.batch.size -> " + batchSizeString);
				LOGGER.warn("Setting it to " + BATCH_SIZE);
			}
		}
		String excludeList = props.getProperty("a2.exclude");
		if (excludeList != null && !"".equals(excludeList)) {
			try {
				// Remove all quotes, whitespaces and convert to upper case
				excludeList = excludeList
						.replace("\"", "")
						.replace("'", "")
						.replace(" ", "")
						.replace("\t", "")
						.toUpperCase();
				String[] nameList = excludeList.split(",");
				StringBuilder sb = new StringBuilder(128);
				if (nameList.length > 0) {
					sb.append(" and L.MASTER not in (");
					for (int i = 0; i < nameList.length; i++) {
						sb.append("'");
						sb.append(nameList[i]);
						sb.append("'");
						if (i < nameList.length - 1) {
							sb.append(",");
						}
					}
					sb.append(")");
					excludeList = sb.toString();
				} else {
					excludeList = null;
				}

			} catch (Exception e) {
				LOGGER.error("Unable to parse a2.exclude parameter set to -> " + excludeList);
				LOGGER.error("Ignoring it.....");
				excludeList = null;
			}
		} else {
			// Explicitly set to null
			excludeList = null;
		}

		// Initialize connection pool
		try {
			ConnectionFactory.init(jdbcUrl.trim(), username, password);
		} catch (SQLException sqle) {
			LOGGER.fatal("Unable to initialize database connection.");
			LOGGER.fatal(ExceptionUtils.getExceptionStackTrace(sqle));
			LOGGER.fatal("Exiting!");
			System.exit(1);
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

		final List<OraTable> oraTables = new ArrayList<>();
		try (Connection connection = ConnectionFactory.getConnection()) {
			PreparedStatement statement = null;
			String sqlStatement = null;

			// Count number of materialized view logs
			/*
				select count(*)
				from   ALL_MVIEW_LOGS L
				where  L.ROWIDS='NO' and L.PRIMARY_KEY='YES' and L.OBJECT_ID='NO'
			 		and L.SEQUENCE='YES' and L.COMMIT_SCN_BASED='NO' and L.INCLUDE_NEW_VALUES='NO';
			 */
			int tableCount = 0;
			sqlStatement =
					"select count(*)\n" +
					"from   ALL_MVIEW_LOGS L\n" +
					MVIEW_LOG_WHERE;
			if (excludeList != null) {
				// Add excluded tables
				sqlStatement += excludeList;
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
				LOGGER.fatal("Nothing to do with user " + username + ".");
				LOGGER.fatal("Exiting.");
				System.exit(1);
			}
			CommonJobSingleton.getInstance().setTableCount(tableCount);
			// Adjust pool size
			ConnectionFactory.adjustPoolSize(tableCount + 4);

			// Read database information
			Source.init();

			// Read table information
			/*
				select L.LOG_OWNER, L.MASTER, L.LOG_TABLE, C.CONSTRAINT_NAME
				from   ALL_MVIEW_LOGS L, ALL_CONSTRAINTS C
				where  L.ROWIDS='NO' and L.PRIMARY_KEY='YES' and L.OBJECT_ID='NO'
					and L.SEQUENCE='YES' and L.COMMIT_SCN_BASED='NO' and L.INCLUDE_NEW_VALUES='NO'
  					and  C.CONSTRAINT_TYPE='P' and C.STATUS='ENABLED'
  					and  L.LOG_OWNER=C.OWNER and L.MASTER=C.TABLE_N	AME;
			 */
			sqlStatement = 	
					"select L.LOG_OWNER, L.MASTER, L.LOG_TABLE, C.CONSTRAINT_NAME\n" +
					"from   ALL_MVIEW_LOGS L, ALL_CONSTRAINTS C\n" +
					MVIEW_LOG_WHERE +
					"  and  C.CONSTRAINT_TYPE='P' and C.STATUS='ENABLED'\n" +
					"  and  L.LOG_OWNER=C.OWNER and L.MASTER=C.TABLE_NAME"; 
			if (excludeList != null) {
				// Add excluded tables
				sqlStatement += excludeList;
			}
			statement = connection.prepareStatement(sqlStatement);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				OraTable oraTable = new OraTable(rs, batchSize, sendMethod);
				oraTables.add(oraTable);
				LOGGER.info("Adding " + oraTable);
			}
		} catch (SQLException e) {
			LOGGER.fatal("Unable to get table information.");
			LOGGER.fatal(ExceptionUtils.getExceptionStackTrace(e));
			LOGGER.fatal("Exiting!");
			System.exit(1);
		}

		ScheduledExecutorService executor = Executors.newScheduledThreadPool(
				oraTables.size(),
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
						LOGGER.error("Incorrect shutdown after " + timeoutSeconds + " seconds wait!");
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

		for (OraTable oraTable : oraTables) {
			//TODO poll time
			executor.scheduleWithFixedDelay(oraTable, 0, pollInterval, TimeUnit.MILLISECONDS);
		}

		while (true) {
			try {
				Thread.sleep(60000);
				LOGGER.info("Total records processed -> " + CommonJobSingleton.getInstance().getProcessedRecordCount());
			} catch (Exception e) {}
		}
	}

	private static void initLog4j(int exitCode) {
		// Check for valid log4j configuration
		String log4jConfig = System.getProperty("a2.log4j.configuration");
		if (log4jConfig == null || "".equals(log4jConfig)) {
			System.err.println("JVM argument -Da2.log4j.configuration must set!");
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
		LOGGER.fatal("Usage:\njava " + className + " <full path to configuration file>");
		LOGGER.fatal("Exiting.");
		System.exit(exitCode);
	}

	private static void loadProps(String configPath, int exitCode) {
		try {
			props.load(new FileInputStream(configPath));
		} catch (IOException eoe) {
			LOGGER.fatal("Unable to open configuration file " + configPath);
			LOGGER.fatal(ExceptionUtils.getExceptionStackTrace(eoe));
			LOGGER.fatal("Exiting.");
			System.exit(exitCode);
		}
	}

}
