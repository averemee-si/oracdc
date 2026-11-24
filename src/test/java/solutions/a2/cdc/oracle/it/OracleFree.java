/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle.it;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.testcontainers.oracle.OracleContainer;

/**
 * Oracle Database Free in a container, with SCOTT's DEPT and EMP, for the integration tests:
 * the source they read is a real Oracle, its dictionary and its own byte format included.
 * <p>
 * The default image is the slim faststart one, which comes with the database already
 * created and is ready in seconds rather than minutes; {@code -Dit.oracle.image} picks
 * another.
 * <p>
 * {@link OracleFree4Mining} is the same database in ARCHIVELOG mode, for the CDC connectors.
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class OracleFree implements AutoCloseable {

	public static final String IMAGE = System.getProperty("it.oracle.image", "gvenzl/oracle-free:slim-faststart");
	public static final String USER = "SCOTT";
	public static final String PASSWORD = "tiger";

	/** DEPT and EMP as SCOTT has always had them, EMP with the foreign key to DEPT */
	private static final String[] SCOTT_TABLES = {
			"""
			create table DEPT (
				DEPTNO number(2) constraint PK_DEPT primary key,
				DNAME varchar2(14),
				LOC varchar2(13))""",
			"""
			create table EMP (
				EMPNO number(4) constraint PK_EMP primary key,
				ENAME varchar2(10),
				JOB varchar2(9),
				MGR number(4),
				HIREDATE date,
				SAL number(7,2),
				COMM number(7,2),
				DEPTNO number(2) constraint FK_DEPTNO references DEPT)"""
	};

	/** SCOTT's rows */
	private static final String[] SCOTT_ROWS = {
			"insert into DEPT values (10, 'ACCOUNTING', 'NEW YORK')",
			"insert into DEPT values (20, 'RESEARCH', 'DALLAS')",
			"insert into DEPT values (30, 'SALES', 'CHICAGO')",
			"insert into DEPT values (40, 'OPERATIONS', 'BOSTON')",
			"insert into EMP values (7369, 'SMITH', 'CLERK', 7902, date '1980-12-17', 800, null, 20)",
			"insert into EMP values (7499, 'ALLEN', 'SALESMAN', 7698, date '1981-02-20', 1600, 300, 30)",
			"insert into EMP values (7521, 'WARD', 'SALESMAN', 7698, date '1981-02-22', 1250, 500, 30)",
			"insert into EMP values (7566, 'JONES', 'MANAGER', 7839, date '1981-04-02', 2975, null, 20)",
			"insert into EMP values (7654, 'MARTIN', 'SALESMAN', 7698, date '1981-09-28', 1250, 1400, 30)",
			"insert into EMP values (7698, 'BLAKE', 'MANAGER', 7839, date '1981-05-01', 2850, null, 30)",
			"insert into EMP values (7782, 'CLARK', 'MANAGER', 7839, date '1981-06-09', 2450, null, 10)",
			"insert into EMP values (7788, 'SCOTT', 'ANALYST', 7566, date '1987-04-19', 3000, null, 20)",
			"insert into EMP values (7839, 'KING', 'PRESIDENT', null, date '1981-11-17', 5000, null, 10)",
			"insert into EMP values (7844, 'TURNER', 'SALESMAN', 7698, date '1981-09-08', 1500, 0, 30)",
			"insert into EMP values (7876, 'ADAMS', 'CLERK', 7788, date '1987-05-23', 1100, null, 20)",
			"insert into EMP values (7900, 'JAMES', 'CLERK', 7698, date '1981-12-03', 950, null, 30)",
			"insert into EMP values (7902, 'FORD', 'ANALYST', 7566, date '1981-12-03', 3000, null, 20)",
			"insert into EMP values (7934, 'MILLER', 'CLERK', 7782, date '1982-01-23', 1300, null, 10)",
			"commit"
	};

	/** Rows of SCOTT.EMP */
	public static final int EMP_ROWS = 14;
	/** Rows of SCOTT.DEPT */
	public static final int DEPT_ROWS = 4;

	private final OracleContainer container = new OracleContainer(IMAGE)
			.withUsername(USER)
			.withPassword(PASSWORD);

	/**
	 * Starts the database, lets {@link #prepareInstance()} do what the instance needs, creates
	 * SCOTT's tables, lets {@link #prepareTables()} do what they need before the first row,
	 * and inserts the rows
	 */
	public OracleFree start() throws SQLException {
		container.start();
		prepareInstance();
		try (var system = connectAsSystem(); var statement = system.createStatement()) {
			statement.execute("grant select any dictionary to " + USER);
		}
		execute(SCOTT_TABLES);
		prepareTables();
		execute(SCOTT_ROWS);
		return this;
	}

	/** Runs after the container starts, before SCOTT's tables exist */
	protected void prepareInstance() throws SQLException {}

	/** Runs after SCOTT's tables are created, before any row goes in */
	protected void prepareTables() throws SQLException {}

	protected OracleContainer container() {
		return container;
	}

	/** The URL of the PDB SCOTT's tables are in */
	public String jdbcUrl() {
		return container.getJdbcUrl();
	}

	public Connection connect() throws SQLException {
		return DriverManager.getConnection(jdbcUrl(), USER, PASSWORD);
	}

	public Connection connectAsSystem() throws SQLException {
		return DriverManager.getConnection(jdbcUrl(), "system", PASSWORD);
	}

	/** Runs every statement as SCOTT, one after another; a {@code commit} is a statement too */
	public void execute(final String... statements) throws SQLException {
		try (var connection = connect(); var statement = connection.createStatement()) {
			for (final var sql : statements)
				statement.execute(sql);
		}
	}

	@Override
	public void close() {
		container.stop();
	}

}
