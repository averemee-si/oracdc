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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleConnection;
import solutions.a2.cdc.oracle.utils.ExceptionUtils;

/**
 * 
 * OraRdbmsInfo: Various Oracle Database routines
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class OraRdbmsInfo {

	public static final int ORA_942 = 942;
	public static final int ORA_1291 = 1291;

	public static final String CDB_ROOT = "CDB$ROOT";

	private String versionString;
	private final String rdbmsEdition;
	private final int versionMajor;
	private final int versionMinor;
	private final short instanceNumber;
	private final String instanceName;
	private final String hostName;
	private final int cpuCoreCount;
	private long dbId;
	private String databaseName;
	private String platformName;
	private boolean cdb;
	private boolean cdbRoot;
	private boolean pdbConnectionAllowed;
	private String pdbName;
	private final Schema schema;
	private String dbCharset;
	private String dbNCharCharset;
	private String dbUniqueName;
	private int redoThread;
	private final String supplementalLogDataAll;
	private final String supplementalLogDataMin;
	private final boolean checkSupplementalLogData4Table;
	private final boolean windows;
	private ZoneId dbTimeZone;
	private ZoneId sessionTimeZone;

	private final static int CDB_INTRODUCED = 12;
	private final static int PDB_MINING_INTRODUCED = 21;
	private final static int PDB_MINING_BACKPORT_MAJOR = 19;
	private final static int PDB_MINING_BACKPORT_MINOR = 10;
	private static final String JDBC_ORA_PREFIX = "jdbc:oracle:thin:@";
	private static final Logger LOGGER = LoggerFactory.getLogger(OraRdbmsInfo.class);

	public OraRdbmsInfo(final Connection connection) throws SQLException {
		this(connection, true);
	}

	public OraRdbmsInfo(final Connection connection, final boolean includeSchema) throws SQLException {
		try (final PreparedStatement psInstance = connection.prepareStatement(OraDictSqlTexts.RDBMS_VERSION_AND_MORE,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				final ResultSet rsInstance = psInstance.executeQuery()) {			
			if (rsInstance.next()) {
				versionString = rsInstance.getString("VERSION");
				instanceNumber = rsInstance.getShort("INSTANCE_NUMBER");
				instanceName = rsInstance.getString("INSTANCE_NAME");
				hostName = rsInstance.getString("HOST_NAME");
				cpuCoreCount = rsInstance.getInt("CPU_CORE_COUNT_CURRENT");
				redoThread = rsInstance.getInt("THREAD#");
			} else {
				throw new SQLException("Unable to read data from V$INSTANCE!");
			}
		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == ORA_942) {
				// ORA-00942: table or view does not exist
				LOGGER.error(
						"\n" +
						"=====================\n" +
						"Please run as SYSDBA:\n" +
						"\tgrant select on V_$INSTANCE to {};\n" +
						"\tgrant select on V_$LICENSE to {};\n" +
						"And restart connector!\n" +
						"=====================\n",
						connection.getSchema(), connection.getSchema());
			}
			throw sqle;
		}

		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean versionFullPresent = true;
		try {
			ps = connection.prepareStatement(OraDictSqlTexts.RDBMS_PRODUCT_VERSION,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs= ps.executeQuery();
		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == 904) {
				// ORA-00904: invalid identifier
				ps = connection.prepareStatement(OraDictSqlTexts.RDBMS_PRODUCT_VERSION_PRE18_1,
						ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				rs= ps.executeQuery();
				versionFullPresent = false;
			} else {
				throw new SQLException(sqle);
			}
		}
		if (rs.next()) {
			if (versionFullPresent) {
				versionString = rs.getString("VERSION_FULL");
			}
				rdbmsEdition = rs.getString("PRODUCT");
		} else {
			throw new SQLException("Unable to read data from PRODUCT_COMPONENT_VERSION!");
		}
		rs.close();
		rs = null;
		ps.close();
		ps = null;

		versionMajor = Integer.parseInt(StringUtils.substringBefore(versionString, "."));
		versionMinor = Integer.parseInt(StringUtils.substringBetween(versionString, ".", "."));

		try {
			if (versionMajor < CDB_INTRODUCED) {
				cdb = false;
				cdbRoot = false;
				pdbConnectionAllowed = false;
				pdbName = null;
				ps = connection.prepareStatement(OraDictSqlTexts.DB_INFO_PRE12C,
						ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				rs = ps.executeQuery();
				if (!rs.next()) {
					throw new SQLException("Unable to read data from V$DATABASE!");
				}
			} else {
				ps = connection.prepareStatement(OraDictSqlTexts.DB_CDB_PDB_INFO,
						ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				rs = ps.executeQuery();
				if (rs.next()) {
					pdbName = rs.getString("CON_NAME");
					if (StringUtils.equalsIgnoreCase(rs.getString("CDB"), "YES")) {
						cdb = true;
						if (StringUtils.equalsIgnoreCase(pdbName, CDB_ROOT)) {
							cdbRoot = true;
							pdbConnectionAllowed = false;
						} else {
							cdbRoot = false;
							// LogMiner works with PDB only from 19.10+ 
							if (versionMajor >= PDB_MINING_INTRODUCED ||
									(versionMajor == PDB_MINING_BACKPORT_MAJOR && versionMinor >= PDB_MINING_BACKPORT_MINOR)) {
								pdbConnectionAllowed = true;
							} else {
								pdbConnectionAllowed = false;
							}
						}
					} else {
						cdb = false;
						cdbRoot = false;
						pdbConnectionAllowed = false;
					}
				} else {
					throw new SQLException("Unable to read data from V$DATABASE!");
				}
			}

			dbId = rs.getLong("DBID");
			databaseName = rs.getString("NAME");
			dbUniqueName = rs.getString("DB_UNIQUE_NAME");
			platformName = rs.getString("PLATFORM_NAME");
			if (StringUtils.startsWithAny(platformName, "Microsoft Windows", "Windows")) {
				windows = true;
			} else {
				windows = false;
			}
			supplementalLogDataAll = rs.getString("SUPPLEMENTAL_LOG_DATA_ALL");
			supplementalLogDataMin = rs.getString("SUPPLEMENTAL_LOG_DATA_MIN");
			if (StringUtils.equalsIgnoreCase(supplementalLogDataAll, "YES")) {
				checkSupplementalLogData4Table = false;
			} else {
				checkSupplementalLogData4Table = true;
			}
			dbCharset = rs.getString("NLS_CHARACTERSET");
			dbNCharCharset = rs.getString("NLS_NCHAR_CHARACTERSET");
			rs.close();
			rs = null;
			ps.close();
			ps = null;

			try {
				dbTimeZone = ZoneId.of(
						((oracle.jdbc.internal.OracleConnection) connection).getDatabaseTimeZone());
			} catch (SQLException tze) {
				dbTimeZone = ZoneId.systemDefault();
				LOGGER.error(
						"\n" +
						"=====================\n" +
						"Database timezone is set to {}\n" +
						"Unable to determine database timezone!\n" +
						ExceptionUtils.getExceptionStackTrace(tze) + 
						"\n" +
						"=====================\n", dbTimeZone);
			}
			LOGGER.debug("Database timezone is set to {}", dbTimeZone);

			final String sessionTZName = ((OracleConnection)connection).getSessionTimeZone();
			if (sessionTZName != null) {
				sessionTimeZone = ZoneId.of(sessionTZName);
			} else {
				sessionTimeZone = ZoneId.systemDefault();
			}
			LOGGER.debug("Session timezone is set to {}", sessionTimeZone);

		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == 942) {
				// ORA-00942: table or view does not exist
				LOGGER.error("Please run as SYSDBA:");
				LOGGER.error("\tgrant select on V_$DATABASE to {};", connection.getSchema());
				LOGGER.error("And restart connector!");
			}
			throw sqle;
		}


		if (includeSchema) {
			SchemaBuilder schemaBuilder = SchemaBuilder
				.struct()
				.name("solutions.a2.cdc.oracle.Source");
			schemaBuilder.field("instance_number", Schema.INT16_SCHEMA);
			schemaBuilder.field("version", Schema.STRING_SCHEMA);
			schemaBuilder.field("instance_name", Schema.STRING_SCHEMA);
			schemaBuilder.field("host_name", Schema.STRING_SCHEMA);
			schemaBuilder.field("dbid", Schema.INT64_SCHEMA);
			schemaBuilder.field("database_name", Schema.STRING_SCHEMA);
			schemaBuilder.field("platform_name", Schema.STRING_SCHEMA);
			// Operation specific
			schemaBuilder.field("commit_scn", Schema.INT64_SCHEMA);
			schemaBuilder.field("xid", Schema.STRING_SCHEMA);
			// Table specific
			schemaBuilder.field("query", Schema.OPTIONAL_STRING_SCHEMA);
			schemaBuilder.field("pdb_name", Schema.OPTIONAL_STRING_SCHEMA);
			schemaBuilder.field("owner", Schema.OPTIONAL_STRING_SCHEMA);
			schemaBuilder.field("table", Schema.OPTIONAL_STRING_SCHEMA);
			// Row specific
			schemaBuilder.field("scn", Schema.INT64_SCHEMA);
			schemaBuilder.field("row_id", Schema.STRING_SCHEMA);
			schemaBuilder.field("ts_ms", Schema.INT64_SCHEMA);
			schema = schemaBuilder.build();
		} else {
			schema = null;
		}
	}

	public Struct getStruct(final String query, final String pdbName, final String owner,
			final String table, final long scn, final long ts, final String xid,
			final long commitScn, final String rowId) {
		Struct struct = new Struct(schema);
		struct.put("instance_number", instanceNumber);
		struct.put("version", versionString);
		struct.put("instance_name", instanceName);
		struct.put("host_name", hostName);
		struct.put("dbid", dbId);
		struct.put("database_name", databaseName);
		struct.put("platform_name", platformName);
		// Table/Operation specific
		if (query != null)
			struct.put("query", query);
		if (pdbName != null)
			struct.put("pdb_name", pdbName);
		if (owner != null)
			struct.put("owner", owner);
		if (table != null)
			struct.put("table", table);
		struct.put("scn", scn);
		struct.put("ts_ms", ts);
		struct.put("xid", xid);
		struct.put("commit_scn", commitScn);
		struct.put("row_id", rowId);
		return struct;
	}

	public static boolean supplementalLoggingSet(
			final Connection connection,
			final short conId,
			final String owner,
			final String tableName) throws SQLException {
		final boolean isCdb = (conId > -1);
		boolean result = false;
		PreparedStatement ps = connection.prepareStatement(
				(isCdb) ?
						OraDictSqlTexts.SUPPLEMENTAL_LOGGING_CDB :
						OraDictSqlTexts.SUPPLEMENTAL_LOGGING_NON_CDB,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ps.setString(1, owner);
		ps.setString(2, tableName);
		if (isCdb) {
			ps.setShort(3, conId);			
		}
		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
			if (StringUtils.equalsIgnoreCase(rs.getString("ALWAYS"), "ALWAYS")) {
				result = true;
			}
		}
		rs.close(); rs = null;
		ps.close(); ps = null;
		return result;
	}

	/**
	 * Returns set of column names for primary key or it equivalent (unique with all non-null)
	 * 
	 * @param connection - Connection to data dictionary (db in 'OPEN' state)
	 * @param conId      - CON_ID, if -1 we working with non CDB or pre-12c Oracle Database
	 * @param tableOwner - Table owner
	 * @param tableName  - Table name
	 * @return           - Set with names of primary key columns. null if nothing found
	 * @throws SQLException
	 */
	public static Set<String> getPkColumnsFromDict(
			final Connection connection,
			final short conId,
			final String tableOwner,
			final String tableName) throws SQLException {
		final boolean isCdb = (conId > -1);
		Set<String> result = null;
		PreparedStatement ps = connection.prepareStatement(
				(isCdb) ?
						OraDictSqlTexts.WELL_DEFINED_PK_COLUMNS_CDB :
						OraDictSqlTexts.WELL_DEFINED_PK_COLUMNS_NON_CDB,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ps.setString(1, tableOwner);
		ps.setString(2, tableName);
		if (isCdb) {
			ps.setShort(3, conId);			
		}

		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			if (result == null)
				result = new HashSet<>();
			result.add(rs.getString("COLUMN_NAME"));
		}
		rs.close();
		rs = null;
		ps.close();
		ps = null;
		if (result == null) {
			// Try to find unique index with non-null columns only
			ps = connection.prepareStatement(
					(isCdb) ?
							OraDictSqlTexts.LEGACY_DEFINED_PK_COLUMNS_CDB :
							OraDictSqlTexts.LEGACY_DEFINED_PK_COLUMNS_NON_CDB,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ps.setString(1, tableOwner);
			ps.setString(2, tableName);
			if (isCdb) {
				ps.setShort(3, conId);			
			}
			rs = ps.executeQuery();
			String indexOwner = null;
			String indexName = null;
			while (rs.next()) {
				if (result == null) {
					result = new HashSet<>();
					indexOwner = rs.getString("INDEX_OWNER");
					indexName = rs.getString("INDEX_NAME");
				}
				result.add(rs.getString("COLUMN_NAME"));
			}
			if (result != null) {
				final StringBuilder sb = new StringBuilder(128);
				boolean firstCol = true;
				for (String columnName : result) {
					if (firstCol) {
						firstCol = false;
					} else {
						sb.append(",");
					}
					sb.append(columnName);
				}
				LOGGER.info(
						"\n" +
						"=====================\n" +
						"Table {}.{} does not have a primary key constraint.\n" +
						"Unique index {}.{} with NON-NULL column(s) will be used instead of the missing primary key.\n" +
						"=====================\n",
						tableOwner, tableName,
						indexOwner, indexName);
			}
			rs.close();
			rs = null;
			ps.close();
			ps = null;
		}
		return result;
	}

	/**
	 * 
	 * Returns list of JDBC URLs for connecting to every RAC instance specified
	 *  in instances list
	 * 
	 * @param url        initial connection URL
	 * @param instances  list of available instances in RAC
	 * @return           list of JDBC URLs
	 */
	public static List<String> generateRacJdbcUrls(final String url, List<String> instances) {
		final List<String> changedUrls = new ArrayList<>();
		if (StringUtils.startsWith(StringUtils.trim(url), "(")) {
			// Parse "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)....(CONNECT_DATA=(SERVICE_NAME=.....)))"
			int instanceNameStartPos = StringUtils.indexOfIgnoreCase(url, "INSTANCE_NAME");
			if (instanceNameStartPos < 0) {
				// Add (INSTANCE_NAME=<>) to connect URL...
				int connectDataStartPos = StringUtils.indexOfIgnoreCase(url, "CONNECT_DATA");
				for (int i = connectDataStartPos - 1; i > -1; i--) {
					if (url.charAt(i) == '(') {
						connectDataStartPos = i;
						break;
					}
				}
				int connectDataEndPos = connectDataStartPos;
				int count = 1;
				while (count > 0) {
					char c = url.charAt(++connectDataEndPos);
					if (c == '(') {
						count++;
					} else if (c == ')') {
						count--;
					}
				}
				final String original = StringUtils.substring(url, connectDataStartPos, connectDataEndPos + 1);
				for (String instanceName : instances) {
					changedUrls.add(
							JDBC_ORA_PREFIX +
							StringUtils.replace(url, original,
									StringUtils.substring(url, connectDataStartPos, connectDataEndPos) + "(INSTANCE_NAME=" + 
																	instanceName + "))"));
				}
			} else {
				// Replace instance name in connect URL...
				for (int i = instanceNameStartPos - 1; i > -1; i--) {
					if (url.charAt(i) == '(') {
						instanceNameStartPos = i;
						break;
					}
				}
				int instanceNameEndPos = instanceNameStartPos;
				int count = 1;
				while (count > 0) {
					char c = url.charAt(++instanceNameEndPos);
					if (c == '(') {
						count++;
					} else if (c == ')') {
						count--;
					}
				}
				final String original = StringUtils.substring(url, instanceNameStartPos, instanceNameEndPos + 1);
				for (String instanceName : instances) {
					changedUrls.add(
							JDBC_ORA_PREFIX +
							StringUtils.replace(url, original,
									"(INSTANCE_NAME=" + instanceName + ")"));
				}
			}
		} else  {
			// Parse "//.......:1521/INSTANCE" or ".......:1521/INSTANCE"
			int semicolonPos = StringUtils.indexOf(url, ":");
			if (url.charAt(semicolonPos + 1) == '/') {
				// tcps://.......
				semicolonPos = StringUtils.indexOf(StringUtils.substring(url, semicolonPos + 2), ":");
			}
			// find next slash
			int slashPos = semicolonPos + StringUtils.indexOf(StringUtils.substring(url, semicolonPos), "/");
			boolean hasInstanceName = false;
			boolean hasParams = false;
			int instanceNameSlashPos = -1;
			int paramStartPos = -1;
			for (int i = slashPos + 1; i < url.length(); i++) {
				if (url.charAt(i) == '/') {
					hasInstanceName = true;
					instanceNameSlashPos = i;
				}
				if (url.charAt(i) == '?') {
					hasParams = true;
					paramStartPos = i;
					break;
				}
			}
			if (!hasInstanceName && !hasParams) {
				for (String instanceName : instances) {
					changedUrls.add(
							JDBC_ORA_PREFIX +
							url + "/" + instanceName);
				}
			} else {
				if (hasInstanceName) {
					final String original;
					if (hasParams) {
						original = StringUtils.substring(url, instanceNameSlashPos, paramStartPos);
					} else {
						original = StringUtils.substring(url, instanceNameSlashPos);
					}
					for (String instanceName : instances) {
						changedUrls.add(
								JDBC_ORA_PREFIX +
								StringUtils.replace(
										url, original, "/" + instanceName));
					}
				} else {
					// Just params - need to add INSTANCE_NAME
					final String original = StringUtils.substring(url, paramStartPos);
					for (String instanceName : instances) {
						changedUrls.add(
								JDBC_ORA_PREFIX +
								StringUtils.replace(url,
										original,
										"/" + instanceName + original));
					}
				}
			}
		}
		return changedUrls;
	}

	public static List<String> getInstances(OracleConnection connection) throws SQLException {
		final List<String> instances = new ArrayList<>();
		try (PreparedStatement ps = connection.prepareStatement(OraDictSqlTexts.RAC_INSTANCES);
				ResultSet rs = ps.executeQuery()) {
			while (rs.next()) {
				instances.add(rs.getString("INSTANCE_NAME"));
			}
		} catch (SQLException sqle) {
			throw sqle;
		}
		return instances;
	}

	/**
	 * 
	 * Returns list of threads in physical standby
	 * 
	 * @param connection connection to database
	 * @return           list of threads
	 */
	public static List<String> getStandbyThreads(OracleConnection connection) throws SQLException {
		final List<String> threads = new ArrayList<>();
		try (PreparedStatement ps = connection.prepareStatement(OraDictSqlTexts.DG4RAC_THREADS);
				ResultSet rs = ps.executeQuery()) {
			while (rs.next()) {
				threads.add(Integer.toString(rs.getInt("THREAD#")));
			}
		} catch (SQLException sqle) {
			throw sqle;
		}
		return threads;
	}


	/**
	 * Returns first available SCN from V$ARCHIVED_LOG
	 * 
	 * @param connection - Connection to mining database
	 * @return           - first available SCN
	 * @throws SQLException
	 */
	public long firstScnFromArchivedLogs(final Connection connection) throws SQLException {
		PreparedStatement ps = connection.prepareStatement(OraDictSqlTexts.FIRST_AVAILABLE_SCN_IN_ARCHIVE,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ps.setInt(1, redoThread);
		long firstScn = -1;
		ResultSet rs = ps.executeQuery();
		if (rs.next())
			firstScn = rs.getLong(1);
		else
			throw new SQLException("Something wrong with access to V$ARCHIVED_LOG or no archived log exists!");
		rs.close(); rs = null;
		ps.close(); ps = null;
		return firstScn;
	}

	/**
	 * Returns part of WHERE with OBJECT_ID's to exclude or include
	 * 
	 * @param connection - Connection to dictionary database
	 * @param exclude
	 * @param where
	 * @return
	 * @throws SQLException
	 */
	public String getMineObjectsIds(final Connection connection,
			final boolean exclude, final String where) throws SQLException {
		final StringBuilder sb = new StringBuilder(32768);
		if (exclude) {
			sb.append(" and DATA_OBJ# not in (");
		} else {
			sb.append(" and (DATA_OBJ# in (");
		}
		
		//TODO
		//TODO For CDB - pair required!!!
		//TODO OBJECT_ID is not unique!!!
		//TODO Need to add "a2.static.objects" parameter for using this for predicate
		//TODO
		final String selectObjectIds =
				"select OBJECT_ID\n" +
				((cdb && !pdbConnectionAllowed) ? "from   CDB_OBJECTS O\n" : "from   DBA_OBJECTS O\n") +
				"where  DATA_OBJECT_ID is not null\n" +
				"  and  OBJECT_TYPE like 'TABLE%'\n" +
				"  and  TEMPORARY='N'\n" +
				((cdb && !pdbConnectionAllowed) ? "  and  CON_ID > 2\n" : "") +
				where;
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("SQL for getting object Id's = {}", selectObjectIds);
		}
		PreparedStatement ps = connection.prepareStatement(selectObjectIds,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = ps.executeQuery();
		boolean firstValue = true;
		boolean lastValue = false;
		int recordCount = 0;
		while (rs.next()) {
			lastValue = false;
			if (firstValue) {
				firstValue = false;
			} else {
				sb.append(",");
			}
			sb.append(rs.getInt(1));
			recordCount++;
			if (recordCount > 999) {
				// ORA-01795
				sb.append(")");
				lastValue = true;
				if (exclude) {
					sb.append(" and DATA_OBJ# not in (");
				} else {
					sb.append(" or DATA_OBJ# in (");
				}
				firstValue = true;
				recordCount = 0;
			}
		}
		if (!lastValue) {
			sb.append(")");
		}
		sb.append(")");
		rs.close();
		rs = null;
		ps.close();
		ps = null;
		return sb.toString();
	}

	public String getConUidsList(final Connection connection) throws SQLException {
		if (cdb && !pdbConnectionAllowed) {
			final StringBuilder sb = new StringBuilder(256);
			sb.append(" and SRC_CON_UID in (");
			// We do not need CDB$ROOT and PDB$SEED
			PreparedStatement statement = connection.prepareStatement(
					"select CON_UID from V$CONTAINERS where CON_ID > 2");
			ResultSet rs = statement.executeQuery();
			boolean first = true;
			while (rs.next()) {
				if (first) {
					first = false;
				} else {
					sb.append(",");
				}
				sb.append(rs.getLong(1));
			}
			sb.append("");
			if (first) {
				return "";
			} else {
				return sb.toString() + ")";
			}
		} else {
			return null;
		}
	}

	public String getVersionString() {
		return versionString;
	}

	public String getRdbmsEdition() {
		return rdbmsEdition;
	}

	public int getVersionMajor() {
		return versionMajor;
	}

	public int getVersionMinor() {
		return versionMinor;
	}

	public short getInstanceNumber() {
		return instanceNumber;
	}

	public String getInstanceName() {
		return instanceName;
	}

	public String getHostName() {
		return hostName;
	}

	public int getCpuCoreCount() {
		return cpuCoreCount;
	}

	public long getDbId() {
		return dbId;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getPlatformName() {
		return platformName;
	}

	public boolean isCdb() {
		return cdb;
	}

	public boolean isCdbRoot() {
		return cdbRoot;
	}

	public boolean isPdbConnectionAllowed() {
		return pdbConnectionAllowed;
	}

	public String getPdbName() {
		return pdbName;
	}

	public Schema getSchema() {
		return schema;
	}

	public String getDbCharset() {
		return dbCharset;
	}

	public String getDbNCharCharset() {
		return dbNCharCharset;
	}

	public String getDbUniqueName() {
		return dbUniqueName;
	}

	public int getRedoThread() {
		return redoThread;
	}

	public void setRedoThread(int redoThread) {
		this.redoThread = redoThread;
	}

	public String getSupplementalLogDataAll() {
		return supplementalLogDataAll;
	}

	public String getSupplementalLogDataMin() {
		return supplementalLogDataMin;
	}

	public boolean isCheckSupplementalLogData4Table() {
		return checkSupplementalLogData4Table;
	}

	public boolean isWindows() {
		return windows;
	}

	public ZoneId getDbTimeZone() {
		return dbTimeZone;
	}

	public ZoneId getSessionTimeZone() {
		return sessionTimeZone;
	}

	public int getNegotiatedSDU(final Connection connection) {
		try {
			final oracle.jdbc.internal.OracleConnection ora =
					(oracle.jdbc.internal.OracleConnection) connection;
			return ora.getNegotiatedSDU();
		} catch (SQLException sqle) {
			LOGGER.error(
					"\n" +
					"=====================\n" +
					"Unable to obtain negotiated SDU!\n" +
					ExceptionUtils.getExceptionStackTrace(sqle) + 
					"\n" +
					"=====================\n");
		}
		return 0;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(256);
		sb.append(rdbmsEdition);
		sb.append("\n");
		sb.append(versionString);
		sb.append("\n");

		sb.append("ORACLE_SID=");
		sb.append(instanceName);
		sb.append(", INSTANCE_NUMBER=");
		sb.append(instanceNumber);
		sb.append("\n");

		sb.append("HOST_NAME=");
		sb.append(hostName);
		sb.append(", CPU_CORE_COUNT_CURRENT=");
		sb.append(cpuCoreCount);
		sb.append(", PLATFORM_NAME=");
		sb.append(platformName);
		sb.append("\n");

		sb.append("DBID=");
		sb.append(dbId);
		sb.append(", DATABASE_NAME=");
		sb.append(databaseName);
		sb.append(", DB_UNIQUE_NAME=");
		sb.append(dbUniqueName);
		sb.append("\n");

		sb.append("NLS_CHARACTERSET=");
		sb.append(dbCharset);
		sb.append(", NLS_NCHAR_CHARACTERSET=");
		sb.append(dbNCharCharset);

		if (cdb) {
			sb.append("\nConnected to CDB, PDB=");
			sb.append(pdbName);
		}

		return sb.toString();
	}

}
