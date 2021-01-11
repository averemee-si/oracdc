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

import oracle.jdbc.pool.OracleDataSource;
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

	private static final int INITIAL_SIZE = 4;
	private static final String ORACDC_POOL_NAME = "oracdc-ucp-pool-1";

	private static UniversalConnectionPoolManager mgr = null;

	private static PoolDataSource pds = null;
	private static boolean activateStandby = false;
	private static Connection connection2Standby = null;
	private static Connection connection4Lobs = null;
	private static AtomicBoolean poolInitialized = new AtomicBoolean(false);


	public static final void init(final String url, final String user, final String password) throws
									SQLException {
		poolInitialized.set(false);
		if (mgr == null) {
			try {
				mgr = UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
			} catch (UniversalConnectionPoolException ucpe) {
				throw new SQLException(ucpe);
			}
		}
		pds = PoolDataSourceFactory.getPoolDataSource();
		pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
		pds.setConnectionPoolName(ORACDC_POOL_NAME);
		pds.setURL(url);
		if (user != null) {
			pds.setUser(user);
			pds.setPassword(password);
		}
		pds.setInitialPoolSize(INITIAL_SIZE);
		poolInitialized.set(true);
	}

	public static final void init4Wallet(final String wallet, final String tnsAdmin, final String alias) throws
									SQLException {
		System.setProperty("oracle.net.wallet_location", wallet);
		System.setProperty("oracle.net.tns_admin", tnsAdmin);
		final String url = "jdbc:oracle:thin:/@" + alias;
		init(url, null, null);
	}

	public static Connection getConnection() throws SQLException {
		while (!poolInitialized.get()) {
			try {
				// Wait for 50 ms
				Thread.sleep(50);
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

	public static final void init4Standby(final String wallet, final String tnsAdmin, final String alias) throws
									SQLException {
		System.setProperty("oracle.net.wallet_location", wallet);
		System.setProperty("oracle.net.tns_admin", tnsAdmin);
		final String url = "jdbc:oracle:thin:/@" + alias;
		Properties props = new Properties();
		props.setProperty("internal_logon", "sysdba");
		props.setProperty("v$session.program","oracdc");
		final OracleDataSource ods = new OracleDataSource();
		ods.setConnectionProperties(props);
		ods.setURL(url);
		connection2Standby = ods.getConnection();
		connection2Standby.setAutoCommit(false);

		connection4Lobs = ods.getConnection();
		connection4Lobs.setAutoCommit(false);

		activateStandby = true;
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

	public static void stopPool() throws SQLException {
		try {
			poolInitialized.set(false);
			mgr.destroyConnectionPool(ORACDC_POOL_NAME);
			pds = null;
		} catch (UniversalConnectionPoolException ucpe) {
			throw new SQLException(ucpe);
		}
	}

}
