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

package solutions.a2.cdc.oracle.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.OraConnectionObjects;
import solutions.a2.cdc.oracle.OraRdbmsInfo;

/**
 * 
 * Oracle Setup Check Utility
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class OracleSetupCheck {

	private static final Logger LOGGER = LoggerFactory.getLogger(OracleSetupCheck.class);
	private static final String PRIV_CREATE_SESSION = "CREATE SESSION";
	private static final String PRIV_SELECT_ANY_TRANSACTION = "SELECT ANY TRANSACTION";
	private static final String PRIV_SELECT_ANY_DICTIONARY = "SELECT ANY DICTIONARY";
	private static final String PRIV_LOGMINING = "LOGMINING";
	private static final String PRIV_SET_CONTAINER = "SET CONTAINER";
	private static final String ROLE_EXECUTE_CATALOG_ROLE = "EXECUTE_CATALOG_ROLE";

	private static int errorCount = 0;
	private static final StringBuilder sb = new StringBuilder(4096);
	private static OraConnectionObjects dbPool = null;
	private static OraRdbmsInfo rdbmsInfo = null;

	public static void main(String[] argv) {
		BasicConfigurator.configure();
		org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);

		LOGGER.info("Starting...");

		// Command line options
		final Options options = new Options();
		setupCliOptions(options);

		final CommandLineParser parser = new DefaultParser();
		final HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, argv);
		} catch (ParseException pe) {
			LOGGER.error(pe.getMessage());
			formatter.printHelp(OracleSetupCheck.class.getCanonicalName(), options);
			System.exit(1);
		}

		final String url = cmd.getOptionValue("jdbc-url");
		final String user = cmd.getOptionValue("user");
		final String password = cmd.getOptionValue("password");
		
		try {
			dbPool = OraConnectionObjects.get4UserPassword("oracdc-setup-check", url, user, password);
		} catch (SQLException sqle) {
			LOGGER.error("Unable to connect to Oracle database {} as user {}!", url, user);
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			System.exit(1);
		}
		LOGGER.info("Connected to Oracle database '{}' as user '{}'.", url, user);

		try {
			rdbmsInfo = new OraRdbmsInfo(dbPool.getConnection(), false);
		} catch (SQLException sqle) {
			LOGGER.error("Unable to collect Oracle database {} information as user {}!", url, user);
			if (sqle.getErrorCode() != OraRdbmsInfo.ORA_942) {
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			}
			System.exit(1);
		}

		sb.append("\n=====================\n");

		// STEP 1 - ARCHIVELOG
		if (!StringUtils.equalsIgnoreCase(rdbmsInfo.getLogMode(), "ARCHIVELOG")) {
			errorCount++;
			sb
				.append("\n\n")
				.append(errorCount)
				.append(") ARCHIVELOG mode is not set.\n")
				.append("Please put your database '")
				.append(rdbmsInfo.getDatabaseName())
				.append("' into ARCHIVELOG mode.\n")
				.append("To do this, connect as SYSDBA and run the following commands:\n")
				.append("\tshutdown immediate\n")
				.append("\tstartup mount\n")
				.append("\talter database archivelog;\n")
				.append("\talter database open;\n");
		}

		// STEP 2 - SUPPLEMENTAL LOGGING
		if (!StringUtils.equalsIgnoreCase(rdbmsInfo.getSupplementalLogDataAll(), "YES") &&
				StringUtils.equalsIgnoreCase(rdbmsInfo.getSupplementalLogDataMin(), "NO")) {
			errorCount++;
			sb
				.append("\n\n")
				.append(errorCount)
				.append(") SUPPLEMENTAL LOGGING is not set.\n")
				.append("Both V$DATABASE.SUPPLEMENTAL_LOG_DATA_ALL and V$DATABASE.SUPPLEMENTAL_LOG_DATA_MIN are set to 'NO'!\n")
				.append("For the connector to work properly, you need to set connecting Oracle RDBMS as SYSDBA:\n")
				.append("\talter database add supplemental log data (ALL) columns;\n")
				.append("OR recommended but more time consuming settings for every table participating in CDC\n")
				.append("\talter database add supplemental log data;\n")
				.append("and then enable supplemental only for required tables:\n")
				.append("\talter table <OWNER>.<TABLE_NAME> add supplemental log data (ALL) columns;\n");

		}

		// STEP 3 - REQUIRED PRIVILEGES and ROLES
		try {
			final Connection connection = dbPool.getConnection();

			PreparedStatement statement = connection.prepareStatement(
					"select 1 from USER_ROLE_PRIVS where USERNAME=USER and GRANTED_ROLE='EXECUTE_CATALOG_ROLE'",
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				errorCount++;
				printPrivMessage(false, user, ROLE_EXECUTE_CATALOG_ROLE);
			}
			rs.close();
			rs = null;
			statement.close();
			statement = null;

			statement = connection.prepareStatement(
					"select 1 from USER_SYS_PRIVS where USERNAME=USER and PRIVILEGE=?",
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			
			checkSysPriv(statement, user, PRIV_CREATE_SESSION);
			checkSysPriv(statement, user, PRIV_SELECT_ANY_TRANSACTION);
			checkSysPriv(statement, user, PRIV_SELECT_ANY_DICTIONARY);
			if (rdbmsInfo.isCdb()) {
				checkSysPriv(statement, user, PRIV_SET_CONTAINER);
			}

			if (rdbmsInfo.getVersionMajor() >= 12) {
				/*
					From $ORACLE_HOME/rdbms/admin/e1102000.sql :
					Rem LOGMINING privilege is new in 12.1
				*/
				checkSysPriv(statement, user, PRIV_LOGMINING);
			} else {
				if (statement != null) {
					statement.close();
					statement = null;
				}
				statement = connection.prepareStatement(
						"select 1\n" + 
						"from   USER_TAB_PRIVS\n" + 
						"where  GRANTEE=USER and TYPE='PACKAGE' and PRIVILEGE='EXECUTE' and OWNER='SYS' and TABLE_NAME='DBMS_LOGMNR'",
						ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				rs = statement.executeQuery();
				if (!rs.next()) {
					errorCount++;
					printPrivMessage(true, user, "execute on DBMS_LOGMNR");
				}
				rs.close();
				rs = null;
			}
			statement.close();
			statement = null;

		} catch (SQLException sqle) {
			LOGGER.error("Unable to check required privileges in database {} for user {}!", rdbmsInfo.getDatabaseName(), user);
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			System.exit(1);
		}

		if (errorCount > 0) {
			sb.append("\n\n=====================\n");
			LOGGER.error(sb.toString());
		} else {
			
		}

		try {
			dbPool.destroy();
		} catch (SQLException sqle) {}

		if (errorCount > 0) {
			System.exit(1);
		} else {
			System.exit(0);
		}

	}

	private static void checkSysPriv(
			final PreparedStatement statement, final String user, final String privilege) throws SQLException {
		statement.setString(1, privilege);
		final ResultSet rs = statement.executeQuery();
		if (!rs.next()) {
			errorCount++;
			printPrivMessage(true, user, privilege);
		}
		rs.close();
	}

	private static void printPrivMessage(final boolean isPriv, final String user, final String privilege) {
		sb
			.append("\n\n")
			.append(errorCount)
			.append(") The user '")
			.append(user)
			.append("' does not have the '")
			.append(privilege)
			.append(isPriv ? "' privilege.\n" : "' role.\\n")
			.append("To fix, please connect as SYSDBA and execute:\n")
			.append("\tgrant ")
			.append(privilege)
			.append(" to ")
			.append(user)
			.append(";\n");
	}

	private static void setupCliOptions(final Options options) {
		// Source connection
		final Option sourceJdbcUrl = new Option("url", "jdbc-url", true,
				"Oracle Database JDBC URL");
		sourceJdbcUrl.setRequired(true);
		options.addOption(sourceJdbcUrl);
		final Option sourceUser = new Option("u", "user", true,
				"Oracle Database user");
		sourceUser.setRequired(true);
		options.addOption(sourceUser);
		final Option sourcePassword = new Option("p", "password", true,
				"Password for Oracle connection");
		sourcePassword.setRequired(true);
		options.addOption(sourcePassword);
	}


}
