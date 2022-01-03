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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.ucp.UniversalConnectionPoolException;
import oracle.ucp.admin.UniversalConnectionPoolManager;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

/**
 * 
 * 
 * @author averemee
 */
public class OraConnectionObjects {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraConnectionObjects.class);

	private final String poolName;
	private String dbUrl;
	private String dbUser;
	private String dbPassword;
	private PoolDataSource pds;

	public OraConnectionObjects(final String poolNameSuffix, final String dbUrl, final String dbUser, final String dbPassword) throws SQLException {
		poolName = "oracdc-" + poolNameSuffix;
		this.dbUrl = dbUrl;
		this.dbUser = dbUser;
		this.dbPassword = dbPassword;
		init();
	}

	private void init() throws SQLException {
		LOGGER.info("Starting connection to database {}...", dbUrl);
		UniversalConnectionPoolManager mgr = null;
		try {
			mgr = UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
			if (mgr.getConnectionPool(poolName) != null) {
				
			}
		} catch (UniversalConnectionPoolException ucpe) {
			throw new SQLException(ucpe);
		}
//		if (walletLocation != null) {
//			System.setProperty("oracle.net.wallet_location", walletLocation);
//			System.setProperty("oracle.net.tns_admin", tnsAdminLocation);
//		}
		pds = PoolDataSourceFactory.getPoolDataSource();
	}

	public static void main(String[] argv) {
		System.out.println("Starting...");
	}
}
