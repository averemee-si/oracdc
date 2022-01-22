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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

/**
 * 
 * 
 * @author averemee
 */
public class OraConnectionObjects {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraConnectionObjects.class);
	private static final int INITIAL_SIZE = 4;

	private PoolDataSource pds;
	private boolean standby = false;
	private boolean distributed = false;
	private Connection connection4LogMiner;
	private String auxWallet, auxTnsAdmin, auxAlias;

	private OraConnectionObjects(final String poolName, final String dbUrl) throws SQLException {
		PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
		pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
		pds.setConnectionPoolName(poolName);
		pds.setConnectionFactoryClassName(dbUrl);
		pds.setInitialPoolSize(INITIAL_SIZE);
		pds.setMinPoolSize(INITIAL_SIZE);
	}

	public static OraConnectionObjects get4JdbcUrl(final String poolName,
			final String dbUrl, final String dbUser, final String dbPassword)
					throws SQLException {
		OraConnectionObjects oco = new OraConnectionObjects(poolName, dbUrl);
		oco.setUserPassword(dbUser, dbPassword);
		return oco;
	}

	public static OraConnectionObjects get4OraWallet(final String poolName,
			final String wallet, final String tnsAdmin, final String alias)
					throws SQLException {
		System.setProperty(OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION, wallet);
		System.setProperty("oracle.net.tns_admin", tnsAdmin);
		OraConnectionObjects oco = new OraConnectionObjects(poolName, "jdbc:oracle:thin:/@" + alias);
		return oco;
	}

	private void setUserPassword(final String dbUser, final String dbPassword) throws SQLException {
		pds.setUser(dbUser);
		pds.setPassword(dbPassword);
	}

	public void addStandbyConnection(
			final String wallet, final String tnsAdmin, final String alias)
					throws SQLException {
		initConnection4LogMiner(true, wallet, tnsAdmin, alias);
	}

	public void addDistributedConnection(
			final String wallet, final String tnsAdmin, final String alias)
					throws SQLException {
		initConnection4LogMiner(false, wallet, tnsAdmin, alias);
	}

	private void initConnection4LogMiner(
			final boolean init4Standby,
			final String wallet, final String tnsAdmin, final String alias)
					throws SQLException {
		final String dbType = init4Standby ? "standby" : "target mining";
		final String dbUrl = "jdbc:oracle:thin:/@" + alias;
		final Properties props = new Properties();
		if (init4Standby) {
			props.setProperty(OracleConnection.CONNECTION_PROPERTY_INTERNAL_LOGON, "sysdba");
		}
		props.setProperty(OracleConnection.CONNECTION_PROPERTY_THIN_VSESSION_PROGRAM, "oracdc");
		System.setProperty(OracleConnection.CONNECTION_PROPERTY_WALLET_LOCATION, wallet);
		System.setProperty("oracle.net.tns_admin", tnsAdmin);
		auxWallet = wallet;
		auxTnsAdmin = tnsAdmin;
		auxAlias = alias;
		LOGGER.info("Initializing connection to {} database {}...", dbType, dbUrl);
		final OracleDataSource ods = new OracleDataSource();
		ods.setConnectionProperties(props);
		ods.setURL(dbUrl);
		connection4LogMiner = ods.getConnection();
		connection4LogMiner.setAutoCommit(false);

		if (init4Standby) {
			standby = true;
		} else {
			distributed = true;
		}
		LOGGER.info("Connection to {} database {} successfully established.",
				dbType, dbUrl);
	}

	public Connection getLogMinerConnection() throws SQLException {
		return getLogMinerConnection(false);
	}

	public Connection getLogMinerConnection(final boolean trace) throws SQLException {
		final Connection logMinerConnection;
		if (standby || distributed) {
			try {
				((OracleConnection)connection4LogMiner).getDefaultTimeZone(); 
			} catch (SQLException sqle) {
				// Connection is not alive...
				// Need to reinitilize!
				LOGGER.warn("Connection to {} is broken. Trying to reinitialize..",
						standby ? "standby" : "target mining");
				//TODO
				//TODO - better error handling required here!!!
				//TODO
				initConnection4LogMiner(standby, auxWallet, auxTnsAdmin, auxAlias);
			}
			logMinerConnection = connection4LogMiner;
		} else {
			logMinerConnection = getConnection();
			logMinerConnection.setClientInfo("OCSID.CLIENTID","LogMiner Read-only");
		}
		if (trace) {
			try {
				final OracleStatement alterSession = (OracleStatement) logMinerConnection.createStatement();
				alterSession.execute("alter session set max_dump_file_size=unlimited");
				alterSession.execute("alter session set tracefile_identifier='oracdc'");
				alterSession.execute("alter session set events '10046 trace name context forever, level 8'");
			} catch (SQLException sqle) {
				LOGGER.error("Unble to set trace parameters (max_dump_file_size, tracefile_identifier, and event 10046 level 8)!");
				LOGGER.error("To fix please run:");
				LOGGER.error("\tgrant alter session to {};",
						((OracleConnection)logMinerConnection).getUserName());
			}
		}
		return logMinerConnection;
	}

	public Connection getConnection() throws SQLException {
		Connection connection = pds.getConnection();
		connection.setClientInfo("OCSID.MODULE","oracdc");
		connection.setClientInfo("OCSID.CLIENTID","Generic R/W");
		connection.setAutoCommit(false);
		return connection;
	}

}
