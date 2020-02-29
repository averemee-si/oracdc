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

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

/**
 * 
 * @author averemee
 *
 */
public class OraPoolConnectionFactory {

	private static final int INITIAL_SIZE = 4;

	private static PoolDataSource pds = null;

	public static final void init(final String url, final String user, final String password) throws
									SQLException {
		pds = PoolDataSourceFactory.getPoolDataSource();
		pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
		pds.setConnectionPoolName("oracdc-ucp-pool-1");
		pds.setURL(url);
		if (user != null) {
			pds.setUser(user);
			pds.setPassword(password);
		}
		pds.setInitialPoolSize(INITIAL_SIZE);
	}

	public static final void init4Wallet(final String wallet, final String tnsAdmin, final String alias) throws
									SQLException {
		System.setProperty("oracle.net.wallet_location", wallet);
		System.setProperty("oracle.net.tns_admin", tnsAdmin);
		final String url = "jdbc:oracle:thin:/@" + alias;
		init(url, null, null);
	}

	public static Connection getConnection() throws SQLException {
		Connection connection = pds.getConnection();
		connection.setAutoCommit(false);
		return connection;
	}

	//TODO
	//TODO Will be different soon...
	//TODO
	public static Connection getLogMinerConnection() throws SQLException {
		return getConnection();
	}

}
