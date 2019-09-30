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

public class OraPoolConnectionFactory {

	private static final int INITIAL_SIZE = 4;

	private static PoolDataSource pds = null;

	public static final void init(String url, String user, String password) throws SQLException {
		pds = PoolDataSourceFactory.getPoolDataSource();
		pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
		pds.setConnectionPoolName("oracdc-kafka");
		pds.setURL(url);
		pds.setUser(user);
		pds.setPassword(password);
		pds.setInitialPoolSize(INITIAL_SIZE);
	}

	public static Connection getConnection() throws SQLException {
		Connection connection = pds.getConnection();
		connection.setAutoCommit(false);
		return connection;
	}

	public static void adjustPoolSize(final int minSize) throws SQLException {
		if (minSize < INITIAL_SIZE) {
			pds.setMaxPoolSize(INITIAL_SIZE);
			pds.setMinPoolSize(INITIAL_SIZE);
		} else {
			pds.setMaxPoolSize(minSize);
			pds.setMinPoolSize(minSize);
		}
	}

}
