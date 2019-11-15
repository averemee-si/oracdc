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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class OraPoolConnectionFactory {

	private static final int INITIAL_SIZE = 4;

	private static DataSource pds = null;

	public static final void init(final String url, final String user, final String password) throws
									SQLException,
									ClassNotFoundException,
									NoSuchMethodException,
									SecurityException,
									IllegalAccessException,
									IllegalArgumentException,
									InvocationTargetException {
		final Class<?> klazzFactory = Class.forName("oracle.ucp.jdbc.PoolDataSourceFactory");
		// pds = PoolDataSourceFactory.getPoolDataSource();
		final Method getPoolDataSource = klazzFactory.getMethod("getPoolDataSource");
		pds = (DataSource) getPoolDataSource.invoke(null);

		final Class<?> klazzPds = Class.forName("oracle.ucp.jdbc.PoolDataSource");
		// pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
		final Method setConnectionFactoryClassName = klazzPds.getMethod(
				"setConnectionFactoryClassName", String.class);
		setConnectionFactoryClassName.invoke(pds, "oracle.jdbc.pool.OracleDataSource");
		// pds.setConnectionPoolName("oracdc-ucp-pool-1");
		final Method setConnectionPoolName = klazzPds.getMethod(
				"setConnectionPoolName", String.class);
		setConnectionPoolName.invoke(pds, "oracdc-ucp-pool-1");
		// pds.setURL(url);
		final Method setURL = klazzPds.getMethod("setURL", String.class);
		setURL.invoke(pds, url);
		if (user != null) {
			// pds.setUser(user);
			final Method setUser = klazzPds.getMethod("setUser", String.class);
			setUser.invoke(pds, user);
			// pds.setPassword(password);
			final Method setPassword = klazzPds.getMethod("setPassword", String.class);
			setPassword.invoke(pds, password);
		}
		// pds.setInitialPoolSize(INITIAL_SIZE)
		final Method setInitialPoolSize = klazzPds.getMethod("setInitialPoolSize", int.class);
		setInitialPoolSize.invoke(pds, INITIAL_SIZE);
	}

	public static final void init4Wallet(final String wallet, final String tnsAdmin, final String alias) throws
									SQLException,
									ClassNotFoundException,
									NoSuchMethodException,
									SecurityException,
									IllegalAccessException,
									IllegalArgumentException,
									InvocationTargetException {
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


}
