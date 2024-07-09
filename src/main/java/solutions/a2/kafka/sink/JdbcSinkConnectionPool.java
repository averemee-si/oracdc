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

package solutions.a2.kafka.sink;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Enumeration;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class JdbcSinkConnectionPool {

	private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectionPool.class);
	private static final String DRIVER_POSTGRESQL = "org.postgresql.Driver";
	private static final String PREFIX_POSTGRESQL = "jdbc:postgresql:";

	public static final int DB_TYPE_MYSQL = 1;
	public static final int DB_TYPE_POSTGRESQL = 2;
	public static final int DB_TYPE_ORACLE = 3;
	public static final int DB_TYPE_MSSQL = 4;

	private HikariDataSource dataSource;
	private int dbType = DB_TYPE_MYSQL;

	/**
	 * 
	 * @param connectorName  name of connector
	 * @param config         JdbcSinkConnectorConfig
	 * @throws SQLException
	 */
	public JdbcSinkConnectionPool(
			final String connectorName, final JdbcSinkConnectorConfig config) throws SQLException {
		final String url = config.getJdbcUrl();
		final String user = config.getJdbcUser();
		final String password = config.getJdbcPassword();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("JDBC Url = {}", url);
		}
		dataSource = new HikariDataSource();
		dataSource.setJdbcUrl(url);
		dataSource.setUsername(user);
		dataSource.setPassword(password);
		dataSource.setAutoCommit(false);
		dataSource.setPoolName("oracdc-hikari-" + connectorName);
		dataSource.setMinimumIdle(0);
		final String initSql = config.getInitSql();
		if (StringUtils.isNotBlank(initSql)) {
			dataSource.setConnectionInitSql(initSql);
		}
		if (StringUtils.startsWith(url, "jdbc:mariadb:") ||
				StringUtils.startsWith(url, "jdbc:mysql:")) {
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
		} else if (StringUtils.startsWith(url, PREFIX_POSTGRESQL)) {
			if (!isDriverLoaded(DRIVER_POSTGRESQL)) {
				try {
					Class.forName(DRIVER_POSTGRESQL);
				} catch (ClassNotFoundException cnf) { }
			}
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
			if (!StringUtils.contains(url, "reWriteBatchedInserts")) {
				dataSource.addDataSourceProperty("reWriteBatchedInserts", "true");
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
		boolean tryToGet = true;
		do {
			try {
				connection = dataSource.getConnection();
				tryToGet = false;
			} catch (SQLTransientConnectionException stce) {
				//TODO - parameterize it!
				if (waitTimeMillis > 300_000) {
					LOGGER.error(
							"\n=====================\n" +
							"Unable to get connection to {} after {} milliseconds!.\n" +
							"=====================\n",
							dataSource.getJdbcUrl(), waitTimeMillis);
					throw stce;
				} else {
					long currentWait = (long) Math.pow(10, ++attempt);
					waitTimeMillis += currentWait;
					try {
						LOGGER.debug("Waiting [] ms for connection", currentWait);
						Thread.sleep(currentWait);
					} catch (InterruptedException ie) {}
				}
			} catch (SQLException sqle) {
				LOGGER.error(
						"\n=====================\n" +
						"Unable to get connection to {}.\n" +
						"=====================\n",
						dataSource.getJdbcUrl());
				throw sqle;
			}
		} while (tryToGet);
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

	private boolean isDriverLoaded(final String driverClass) {
		final Enumeration<Driver> availableDrivers = DriverManager.getDrivers();
		while (availableDrivers.hasMoreElements()) {
			final Driver driver = availableDrivers.nextElement();
			if (StringUtils.equals(driverClass, driver.getClass().getCanonicalName())) {
				return true;
			}
		}
		return false;
	}

}
