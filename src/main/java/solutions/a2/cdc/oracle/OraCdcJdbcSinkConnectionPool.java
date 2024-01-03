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

package solutions.a2.cdc.oracle;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

public class OraCdcJdbcSinkConnectionPool {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcJdbcSinkConnectionPool.class);
	private static final int INITIAL_SIZE = 4;

	public static final int DB_TYPE_MYSQL = 1;
	public static final int DB_TYPE_POSTGRESQL = 2;
	public static final int DB_TYPE_ORACLE = 3;
	public static final int DB_TYPE_MSSQL = 4;

	private HikariDataSource dataSource;
	private int dbType = DB_TYPE_MYSQL;

	/**
	 * 
	 * @param connectorName  name of connector
	 * @param url            JDBC URL
	 * @param user           JDBC user
	 * @param password       JDBC password
	 * @throws SQLException
	 */
	public OraCdcJdbcSinkConnectionPool(
			String connectorName, String url, String user, String password) throws SQLException {
		LOGGER.trace("Entered {}.init", OraCdcJdbcSinkConnectionPool.class.getName());
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("JDBC Url = {}", url);
			LOGGER.debug("Initial pool size = {}", INITIAL_SIZE);
		}
		dataSource = new HikariDataSource();
		dataSource.setJdbcUrl(url);
		dataSource.setUsername(user);
		dataSource.setPassword(password);
		dataSource.setAutoCommit(false);
		dataSource.setPoolName("oracdc-hikari-" + connectorName);
		//TODO - ???
		dataSource.setMaximumPoolSize(INITIAL_SIZE);
		if (url.startsWith("jdbc:mariadb:") || url.startsWith("jdbc:mysql:")) {
			if (!StringUtils.contains(url, "cachePrepStmts")) {
				dataSource.addDataSourceProperty("cachePrepStmts", "true");
			}
			if (!StringUtils.contains(url, "prepStmtCacheSize")) {
				dataSource.addDataSourceProperty("prepStmtCacheSize", "256");
			}
			if (!StringUtils.contains(url, "prepStmtCacheSqlLimit")) {
				dataSource.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
			}
			if (!StringUtils.contains(url, "useServerPrepStmts")) {
				dataSource.addDataSourceProperty("useServerPrepStmts", "true");
			}
			if (!StringUtils.contains(url, "tcpKeepAlive")) {
				dataSource.addDataSourceProperty("tcpKeepAlive", "true");
			}
			if (!StringUtils.contains(url, "maintainTimeStats")) {
				dataSource.addDataSourceProperty("maintainTimeStats", "false");
			}
		} else if (url.startsWith("jdbc:postgresql:")) {
			if (!StringUtils.contains(url, "ApplicationName")) {
				dataSource.addDataSourceProperty("ApplicationName", "oracdc");
			}
			if (!StringUtils.contains(url, "prepareThreshold")) {
				dataSource.addDataSourceProperty("prepareThreshold", "1");
			}
			if (!StringUtils.contains(url, "preparedStatementCacheSizeMiB")) {
				dataSource.addDataSourceProperty("preparedStatementCacheSizeMiB", "16");
			}
			if (!StringUtils.contains(url, "tcpKeepAlive")) {
				dataSource.addDataSourceProperty("tcpKeepAlive", "true");
			}
		}

		// Detect database type
		Connection connection = getConnection();
		final String databaseProductName = connection.getMetaData().getDatabaseProductName();
		LOGGER.debug("connection.getMetaData().getDatabaseProductName() returns {}", databaseProductName);
		connection.close();

		if ("MariaDB".equalsIgnoreCase(databaseProductName) ||
				"MySQL".equalsIgnoreCase(databaseProductName)) {
			dbType = DB_TYPE_MYSQL;
		} else if ("PostgreSQL".equalsIgnoreCase(databaseProductName)) {
			dbType = DB_TYPE_POSTGRESQL;
		} else if ("Oracle".equalsIgnoreCase(databaseProductName)) {
			dbType = DB_TYPE_ORACLE;
		} else if (databaseProductName.startsWith("Microsoft")) {
			dbType = DB_TYPE_MSSQL;
		} else {
			//TODO - more?
		}
	}

	public Connection getConnection() throws SQLException {
		Connection connection = null;
		int attempt = 0;
		long waitTimeMillis = 0;
		while (true) {
			try {
				connection = dataSource.getConnection();
				break;
			} catch (SQLException sqle) {
				if (sqle instanceof SQLTransientConnectionException) {
					//TODO - parameterize it!
					if (waitTimeMillis > 300_000) {
						LOGGER.error(
								"\n=====================\n" +
								"Unable to get connection to after {} milliseconds!.\n" +
								"=====================\n",
								dataSource.getJdbcUrl(), waitTimeMillis);
						throw sqle;
					} else {
						long currentWait = (long) Math.pow(10, ++attempt);
						waitTimeMillis += currentWait;
						try {
							LOGGER.debug("Waiting [] ms for connection", currentWait);
							Thread.sleep(currentWait);
						} catch (InterruptedException ie) {}
					}
				} else {
					throw sqle;
				}
			}
		}
		if (connection.getAutoCommit()) {
			connection.setAutoCommit(false);
		}
		return connection;
	}

	public int getDbType() {
		return dbType;
	}

	public void close() {
		if (dataSource != null) {
			dataSource.close();
		}
		dataSource = null;
	}

}
