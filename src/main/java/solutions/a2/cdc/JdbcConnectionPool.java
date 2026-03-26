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

package solutions.a2.cdc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Enumeration;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class JdbcConnectionPool {

	private static final Logger LOGGER = LoggerFactory.getLogger(JdbcConnectionPool.class);
	private static final String DRIVER_POSTGRESQL = "org.postgresql.Driver";
	private static final String PREFIX_POSTGRESQL = "jdbc:postgresql:";
	private static final String DRIVER_ORACLE = "oracle.jdbc.OracleDriver";
	private static final String PREFIX_ORACLE = "jdbc:oracle:";

	public static final int DB_TYPE_MYSQL = 1;
	public static final int DB_TYPE_POSTGRESQL = 2;
	public static final int DB_TYPE_ORACLE = 3;
	public static final int DB_TYPE_MSSQL = 4;

	private HikariDataSource dataSource;
	private int dbType = DB_TYPE_MYSQL;

	/**
	 * 
	 * @param poolName
	 * @param url
	 * @param username
	 * @param password
	 * @param initSql
	 * @throws SQLException
	 */
	public JdbcConnectionPool(
			final String poolName, final String url, final String username, final String password, final String initSql) throws SQLException {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("JDBC Url = {}", url);
		dataSource = new HikariDataSource();
		dataSource.setJdbcUrl(url);
		dataSource.setUsername(username);
		dataSource.setPassword(password);
		dataSource.setAutoCommit(false);
		dataSource.setPoolName("oracdc-hikari-" + poolName);
		dataSource.setMinimumIdle(0);
		if (StringUtils.isNotBlank(initSql)) {
			dataSource.setConnectionInitSql(initSql);
		}
		if (Strings.CS.startsWith(url, "jdbc:mariadb:") ||
				Strings.CS.startsWith(url, "jdbc:mysql:")) {
			if (!Strings.CS.contains(url, "cachePrepStmts")) {
				dataSource.addDataSourceProperty("cachePrepStmts", "true");
			}
			if (!Strings.CS.contains(url, "prepStmtCacheSize")) {
				dataSource.addDataSourceProperty("prepStmtCacheSize", "256");
			}
			if (!Strings.CS.contains(url, "prepStmtCacheSqlLimit")) {
				dataSource.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
			}
			if (!Strings.CS.contains(url, "useServerPrepStmts")) {
				dataSource.addDataSourceProperty("useServerPrepStmts", "true");
			}
			if (!Strings.CS.contains(url, "tcpKeepAlive")) {
				dataSource.addDataSourceProperty("tcpKeepAlive", "true");
			}
			if (!Strings.CS.contains(url, "maintainTimeStats")) {
				dataSource.addDataSourceProperty("maintainTimeStats", "false");
			}
		} else if (Strings.CS.startsWith(url, PREFIX_POSTGRESQL)) {
			if (!isDriverLoaded(DRIVER_POSTGRESQL)) {
				try {
					Class.forName(DRIVER_POSTGRESQL);
				} catch (ClassNotFoundException cnf) { }
			}
			if (!Strings.CS.contains(url, "ApplicationName")) {
				dataSource.addDataSourceProperty("ApplicationName", "oracdc");
			}
			if (!Strings.CS.contains(url, "prepareThreshold")) {
				dataSource.addDataSourceProperty("prepareThreshold", "1");
			}
			if (!Strings.CS.contains(url, "preparedStatementCacheSizeMiB")) {
				dataSource.addDataSourceProperty("preparedStatementCacheSizeMiB", "16");
			}
			if (!Strings.CS.contains(url, "tcpKeepAlive")) {
				dataSource.addDataSourceProperty("tcpKeepAlive", "true");
			}
			if (!Strings.CS.contains(url, "reWriteBatchedInserts")) {
				dataSource.addDataSourceProperty("reWriteBatchedInserts", "true");
			}
		} else if (Strings.CS.startsWith(url, PREFIX_ORACLE)) {
			if (!isDriverLoaded(DRIVER_ORACLE)) {
				try {
					Class.forName(DRIVER_ORACLE);
				} catch (ClassNotFoundException cnf) { }
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
							"""
							
							=====================
							Unable to get connection to {} after {} milliseconds!.
							=====================
							
							""",
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
						"""
						
						=====================
						Unable to get connection to {}.
						=====================
						
						""",
						dataSource.getJdbcUrl());
				throw sqle;
			}
		} while (tryToGet);
		if (connection.getAutoCommit()) {
			connection.setAutoCommit(false);
		}
		return connection;
	}

	public int dbType() {
		return dbType;
	}

	public void poolSize(int degree) {
		dataSource.setMaximumPoolSize(degree + 8);
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
			if (Strings.CS.equals(driverClass, driver.getClass().getCanonicalName())) {
				return true;
			}
		}
		return false;
	}

}
