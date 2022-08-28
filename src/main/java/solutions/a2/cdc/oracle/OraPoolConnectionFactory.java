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

package solutions.a2.cdc.oracle;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private static String dbUrl = null;
	private static String dbUser = null;
	private static String dbPassword = null;

	private static AtomicBoolean poolInitialized = new AtomicBoolean(false);
	private static AtomicBoolean poolInitInProgress = new AtomicBoolean(false);


	public static synchronized void init(final String url, final String user, final String password) throws
									SQLException {
		dbUrl = url;
		dbUser = user;
		dbPassword = password;
		init();
	}

	public static synchronized void init(final String url, final String wallet) throws
									SQLException {
		walletLocation = wallet;
		dbUrl = url;
		dbUser = null;
		dbPassword = null;
		init();
	}

	private static synchronized void init() throws SQLException {
		if (!poolInitInProgress.get() && !poolInitialized.get()) {
			LOGGER.info("Starting connection to database {}...", dbUrl);
			poolInitInProgress.set(true);
			if (mgr == null) {
				try {
					mgr = UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
				} catch (UniversalConnectionPoolException ucpe) {
					throw new SQLException(ucpe);
				}
			}
			if (walletLocation != null) {
				System.setProperty("oracle.net.wallet_location", walletLocation);
			}
			pds = PoolDataSourceFactory.getPoolDataSource();
			pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
			pds.setConnectionPoolName(ORACDC_POOL_NAME);
			pds.setURL(dbUrl);
			if (walletLocation == null && dbUser != null && dbPassword != null) {
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
			poolInitInProgress.set(false);
			LOGGER.info("Connection to database {} successfully established.", dbUrl);
		}
	}

	public static Connection getConnection() throws SQLException {
		while (!poolInitialized.get()) {
			try {
				LOGGER.warn("Waitin	g {} ms for UCP initialization...", POOL_INIT_WAIT_MS);
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

}
