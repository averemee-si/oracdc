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
	private static Connection connection2Standby = null;
	private static Connection connection4Lobs = null;
	private static String standbyWalletLocation = null;
	private static String standbyTnsAdmin = null;
	private static String standbyDbUrl = null;
	private static Properties standbyProps = null;

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

	public static synchronized void init4Standby(final String wallet, final String tnsAdmin, final String alias) throws
									SQLException {
		standbyWalletLocation = wallet;
		standbyTnsAdmin =  tnsAdmin;
		standbyDbUrl = "jdbc:oracle:thin:/@" + alias;
		standbyProps = new Properties();
		standbyProps.setProperty("internal_logon", "sysdba");
		standbyProps.setProperty("v$session.program","oracdc");

		init4Standby();
	}

	private static synchronized void init4Standby() throws SQLException {
		LOGGER.info("Starting connection to standby database {}...", standbyDbUrl);
		System.setProperty("oracle.net.wallet_location", standbyWalletLocation);
		System.setProperty("oracle.net.tns_admin", standbyTnsAdmin);
		final OracleDataSource ods = new OracleDataSource();
		ods.setConnectionProperties(standbyProps);
		ods.setURL(standbyDbUrl);
		connection2Standby = ods.getConnection();
		connection2Standby.setAutoCommit(false);

		connection4Lobs = ods.getConnection();
		connection4Lobs.setAutoCommit(false);

		activateStandby = true;
		LOGGER.info("Connection to standby database {} successfully established.", standbyDbUrl);
	}

	public static Connection getLogMinerConnection() throws SQLException {
		if (activateStandby) {
			return connection2Standby;
		} else {
			Connection connection = getConnection();
			connection.setClientInfo("OCSID.CLIENTID","LogMiner Read-only");
			return connection;
		}
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
			if (activateStandby && stopAll) {
				try {
					connection2Standby.close();
					connection4Lobs.close();
				} catch (SQLException sqle) {
					LOGGER.warn("Unable to stop connections to standby database");
					LOGGER.warn(ExceptionUtils.getExceptionStackTrace(sqle));
				}
				connection2Standby = null;
				connection4Lobs = null;
			}
		} catch (UniversalConnectionPoolException ucpe) {
			throw new SQLException(ucpe);
		}
		LOGGER.info("Connection pool {} successfully stopped.", ORACDC_POOL_NAME);
	}

	public static synchronized void reCreatePool(boolean reCreateAll) throws SQLException {
		init();
		if (activateStandby && reCreateAll) {
			init4Standby();
		}
	}

}
