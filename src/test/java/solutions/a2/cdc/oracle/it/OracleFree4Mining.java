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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * {@link OracleFree} ready for the CDC connectors: ARCHIVELOG mode, minimal supplemental logging
 * for the database and (ALL) columns for SCOTT's tables before their first row, the common user
 * the connectors run as, and the two directory objects the RedoMiner reads the online and the
 * archived redo logs through ({@code a2.storage.media=BFILE}), in the root and in the PDB.
 * <p>
 * After the container starts, everything the instance needs is done the way a DBA does it,
 * {@code / as sysdba} in the container: shutdown, startup mount, ARCHIVELOG, open. The archived
 * logs go to a fixed directory next to the online ones, and Oracle Managed Files are disabled when
 * the image uses them, so that a directory object can point at both.
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class OracleFree4Mining extends OracleFree {

	/** The common user the connectors connect as */
	public static final String MINER_USER = "C##ORACDC";
	public static final String MINER_PASSWORD = "ORACDC";
	/** Directory object of the online redo logs, a2.bfile.directory.online */
	public static final String ONLINE_DIR = "CDC_ONLINE";
	/** Directory object of the archived redo logs, a2.bfile.directory.archive */
	public static final String ARCHIVE_DIR = "CDC_ARCHIVE";

	private String onlinePath;
	private String archivePath;

	@Override
	public OracleFree4Mining start() throws SQLException {
		super.start();
		return this;
	}

	@Override
	protected void prepareInstance() throws SQLException {
		onlinePath = sysdbaValue(
				"select substr(MEMBER, 1, instr(MEMBER, '/', -1) - 1) from V$LOGFILE where rownum = 1;");
		archivePath = onlinePath + "/archive";
		final boolean omf = !sysdbaValue(
				"select nvl(VALUE, '-') from V$PARAMETER where NAME = 'db_create_file_dest';").equals("-");
		shell("mkdir -p " + archivePath);
		final var pdb = container().getDatabaseName().toUpperCase();
		sysdba(
				(omf ? "alter system set \"_omf\" = disabled scope=spfile;\n" : "") +
				"alter system set log_archive_dest_1 = 'LOCATION=" + archivePath + "' scope=spfile;\n" +
				"""
				shutdown immediate
				startup mount
				alter database archivelog;
				alter database open;
				alter pluggable database all open;
				alter database add supplemental log data;
				alter system register;
				""" +
				"create user " + MINER_USER + " identified by " + MINER_PASSWORD + "\n" +
				"  default tablespace SYSAUX temporary tablespace TEMP quota unlimited on SYSAUX container=all;\n" +
				"alter user " + MINER_USER + " set container_data=all container=current;\n" +
				"grant CREATE SESSION, SET CONTAINER, SELECT ANY TRANSACTION, SELECT ANY DICTIONARY,\n" +
				"  SELECT ANY TABLE, EXECUTE_CATALOG_ROLE, LOGMINING to " + MINER_USER + " container=all;\n" +
				directories() +
				"alter session set container=" + pdb + ";\n" +
				directories());
	}

	@Override
	protected void prepareTables() throws SQLException {
		addSupplementalLogging("DEPT", "EMP");
	}

	/**
	 * (ALL) columns supplemental logging for SCOTT's tables; a test creating its own table
	 * calls this before the first change it is going to mine
	 */
	public void addSupplementalLogging(final String... tables) throws SQLException {
		final var statements = new String[tables.length];
		for (int i = 0; i < tables.length; i++)
			statements[i] = "alter table " + tables[i] + " add supplemental log data (ALL) columns";
		execute(statements);
	}

	/** The directory objects are local to a container: the same two in each, read by the miner user */
	private String directories() {
		return
				"create or replace directory " + ONLINE_DIR + " as '" + onlinePath + "';\n" +
				"create or replace directory " + ARCHIVE_DIR + " as '" + archivePath + "';\n" +
				"grant read on directory " + ONLINE_DIR + " to " + MINER_USER + ";\n" +
				"grant read on directory " + ARCHIVE_DIR + " to " + MINER_USER + ";\n";
	}

	/** The URL of the CDB root, where the connectors connect */
	public String rootJdbcUrl() {
		return "jdbc:oracle:thin:@" + container().getHost() + ":" + container().getOraclePort() + "/" + container().getSid();
	}

	/** Connects to the CDB root as the user the connectors run as */
	public Connection connectAsMiner() throws SQLException {
		return DriverManager.getConnection(rootJdbcUrl(), MINER_USER, MINER_PASSWORD);
	}

	/** The SCN of the database now */
	public long currentScn() throws SQLException {
		try (var miner = connectAsMiner();
				var statement = miner.createStatement();
				var rs = statement.executeQuery("select CURRENT_SCN from V$DATABASE")) {
			rs.next();
			return rs.getLong(1);
		}
	}

	/** Archives the current online redo log, so the changes made so far are in an archived one */
	public void archiveLogCurrent() throws SQLException {
		try (var system = DriverManager.getConnection(rootJdbcUrl(), "system", PASSWORD);
				var statement = system.createStatement()) {
			statement.execute("alter system archive log current");
		}
	}

	/** Runs SQL*Plus {@code / as sysdba} in the container; the first error stops it and fails */
	private String sysdba(final String script) throws SQLException {
		return shell("sqlplus -s / as sysdba <<'EOF'\nwhenever sqlerror exit failure\n" + script + "\nexit\nEOF");
	}

	/** One value from a query run {@code / as sysdba} */
	private String sysdbaValue(final String query) throws SQLException {
		return sysdba("set heading off feedback off pagesize 0 linesize 4000 trimspool on\n" + query).strip();
	}

	private String shell(final String command) throws SQLException {
		try {
			final var result = container().execInContainer("bash", "-c", command);
			if (result.getExitCode() != 0)
				throw new SQLException("'" + command + "' failed with " + result.getExitCode() + ":\n" +
						result.getStdout() + result.getStderr());
			return result.getStdout();
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new SQLException(ie);
		}
	}

}
