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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.pool.OracleDataSource;
import oracle.ucp.UniversalConnectionPoolAdapter;
import oracle.ucp.UniversalConnectionPoolException;
import oracle.ucp.admin.UniversalConnectionPoolManager;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

/**
 * 
 * @author averemee
 *
 */
public class OraPoolConnectionFactory {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraPoolConnectionFactory.class);
	private static final int INITIAL_SIZE = 4;
	private static final int POOL_INIT_WAIT_MS = 100;
	private static final String ORACDC_POOL_NAME = "oracdc-ucp-pool-1";

	private static UniversalConnectionPoolManager mgr = null;

	private static PoolDataSource pds = null;
	private static String walletLocation = null;
	private static String tnsAdminLocation = null;
	private static String dbUrl = null;
	private static String dbUser = null;
	private static String dbPassword = null;
	private static boolean activateStandby = false;
	private static boolean activateDistributed = false;
	private static Connection connection4LogMiner = null;
	private static String conn4LogMinerWalletLocation = null;
	private static String conn4LogMinerTnsAdmin = null;
	private static String conn4LogMinerDbUrl = null;
	private static Properties conn4LogMinerProps = null;

	private static AtomicBoolean poolInitialized = new AtomicBoolean(false);


	public static synchronized void init(final String url, final String user, final String password) throws
									SQLException {
		dbUrl = url;
		dbUser = user;
		dbPassword = password;
		init();
	}

	public static synchronized void init4Wallet(final String wallet, final String tnsAdmin, final String alias) throws
									SQLException {
		walletLocation = wallet;
		tnsAdminLocation = tnsAdmin;
		dbUrl = "jdbc:oracle:thin:/@" + alias;
		dbUser = null;
		dbPassword = null;
		init();
	}

	private static synchronized void init() throws SQLException {
		LOGGER.info("Starting connection to database {}...", dbUrl);
		poolInitialized.set(false);
		if (mgr == null) {
			try {
				mgr = UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
			} catch (UniversalConnectionPoolException ucpe) {
				throw new SQLException(ucpe);
			}
		}
		if (walletLocation != null) {
			System.setProperty("oracle.net.wallet_location", walletLocation);
			System.setProperty("oracle.net.tns_admin", tnsAdminLocation);
		}
		pds = PoolDataSourceFactory.getPoolDataSource();
		pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
		pds.setConnectionPoolName(ORACDC_POOL_NAME);
		pds.setURL(dbUrl);
		if (dbUser != null) {
			pds.setUser(dbUser);
			pds.setPassword(dbPassword);
		}
		pds.setInitialPoolSize(INITIAL_SIZE);
		try {
			mgr.createConnectionPool((UniversalConnectionPoolAdapter) pds);
			mgr.startConnectionPool(ORACDC_POOL_NAME);
		} catch (UniversalConnectionPoolException ucpe) {
			throw new SQLException(ucpe);
		}
		poolInitialized.set(true);
		LOGGER.info("Connection to database {} successfully established.", dbUrl);
	}

	public static Connection getConnection() throws SQLException {
		while (!poolInitialized.get()) {
			try {
				LOGGER.warn("Waiting {} ms for UCP initialization...", POOL_INIT_WAIT_MS);
				Thread.sleep(POOL_INIT_WAIT_MS);
			} catch (InterruptedException ie) {
				throw new SQLException(ie);
			}
		}
		Connection connection = pds.getConnection();
		connection.setClientInfo("OCSID.MODULE","oracdc");
		connection.setClientInfo("OCSID.CLIENTID","Generic R/W");
		connection.setAutoCommit(false);
		return connection;
	}

	public static synchronized void init4Standby(
			final String wallet, final String tnsAdmin, final String alias) throws SQLException {
		conn4LogMinerWalletLocation = wallet;
		conn4LogMinerTnsAdmin =  tnsAdmin;
		conn4LogMinerDbUrl = "jdbc:oracle:thin:/@" + alias;
		conn4LogMinerProps = new Properties();
		conn4LogMinerProps.setProperty("internal_logon", "sysdba");
		conn4LogMinerProps.setProperty("v$session.program","oracdc");

		initConnection4LogMiner(true);
	}

	public static synchronized void initDistributed(
			final String wallet, final String tnsAdmin, final String alias) throws SQLException {
		conn4LogMinerWalletLocation = wallet;
		conn4LogMinerTnsAdmin =  tnsAdmin;
		conn4LogMinerDbUrl = "jdbc:oracle:thin:/@" + alias;
		conn4LogMinerProps = new Properties();
		conn4LogMinerProps.setProperty("v$session.program","oracdc");

		initConnection4LogMiner(false);
	}

	public static synchronized void initConnection4LogMiner(final boolean standby) throws SQLException {
		final String dbType;
		if (standby) {
			dbType = "standby";
		} else {
			dbType = "target mining";
		}
		LOGGER.info("Starting connection to {} database {}...",
				dbType, conn4LogMinerDbUrl);
		System.setProperty("oracle.net.wallet_location", conn4LogMinerWalletLocation);
		System.setProperty("oracle.net.tns_admin", conn4LogMinerTnsAdmin);
		final OracleDataSource ods = new OracleDataSource();
		ods.setConnectionProperties(conn4LogMinerProps);
		ods.setURL(conn4LogMinerDbUrl);
		connection4LogMiner = ods.getConnection();
		connection4LogMiner.setAutoCommit(false);

		if (standby) {
			activateStandby = true;
		} else {
			activateDistributed = true;
		}
		LOGGER.info("Connection to {} database {} successfully established.",
				dbType, conn4LogMinerDbUrl);
	}

	public static Connection getLogMinerConnection() throws SQLException {
		return getLogMinerConnection(false);
	}

	public static Connection getLogMinerConnection(final boolean trace) throws SQLException {
		final Connection logMinerConnection;
		if (activateStandby || activateDistributed) {
			try {
				((OracleConnection)connection4LogMiner).getDefaultTimeZone(); 
			} catch (SQLException sqle) {
				// Connection is not alive...
				// Need to reinitilize!
				//TODO
				//TODO - better handling required!!!
				//TODO
				initConnection4LogMiner(activateStandby ? true : false);
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

	public static synchronized void stopPool() throws SQLException {
		stopPool(false);
	}

	public static synchronized void stopPool(boolean stopAll) throws SQLException {
		LOGGER.info("Stopping connection pool {}...", ORACDC_POOL_NAME);
		try {
			poolInitialized.set(false);
			mgr.destroyConnectionPool(ORACDC_POOL_NAME);
			pds = null;
			if ((activateStandby  || activateDistributed) && stopAll) {
				try {
					connection4LogMiner.close();
				} catch (SQLException sqle) {
					LOGGER.warn("Unable to stop connections to standby database");
					LOGGER.warn(ExceptionUtils.getExceptionStackTrace(sqle));
				}
				connection4LogMiner = null;
			}
		} catch (UniversalConnectionPoolException ucpe) {
			throw new SQLException(ucpe);
		}
		LOGGER.info("Connection pool {} successfully stopped.", ORACDC_POOL_NAME);
	}

	public static synchronized void reCreatePool(boolean reCreateAll) throws SQLException {
		init();
		if (activateStandby && reCreateAll) {
			initConnection4LogMiner(true);
		}
		if (activateDistributed && reCreateAll) {
			initConnection4LogMiner(false);
		}
	}

}
