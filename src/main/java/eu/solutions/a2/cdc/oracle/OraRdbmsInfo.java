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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class OraRdbmsInfo {

	private final String versionString;
	private final int versionMajor;
	private final short instanceNumber;
	private final String instanceName;
	private final String hostName;
	private final long dbId;
	private final String databaseName;
	private final String platformName;
	private final boolean cdb;
	private final boolean cdbRoot;
	private final Schema schema;

	private final static int CDB_INTRODUCED = 12;

	private static OraRdbmsInfo instance;

	public OraRdbmsInfo(final Connection connection) throws SQLException {
		this(connection, true);
	}

	private OraRdbmsInfo(final Connection connection, final boolean initialCall) throws SQLException {
		PreparedStatement ps = connection.prepareStatement(OraDictSqlTexts.RDBMS_VERSION_AND_MORE,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
			versionString = rs.getString("VERSION");
			instanceNumber = rs.getShort("INSTANCE_NUMBER");
			instanceName = rs.getString("INSTANCE_NAME");
			hostName = rs.getString("HOST_NAME");
		} else
			throw new SQLException("Unable to detect RDBMS version!");
		rs.close();
		rs = null;
		ps.close();
		ps = null;

		versionMajor = Integer.parseInt(
				versionString.substring(0, versionString.indexOf(".")));

		if (versionMajor < CDB_INTRODUCED) {
			cdb = false;
			cdbRoot = false;
			ps = connection.prepareStatement(OraDictSqlTexts.DB_INFO_PRE12C,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = ps.executeQuery();
			if (!rs.next())
				throw new SQLException("Unable to detect database information!");
		} else {
			ps = connection.prepareStatement(OraDictSqlTexts.DB_CDB_PDB_INFO,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = ps.executeQuery();
			if (rs.next()) {
				if ("YES".equalsIgnoreCase(rs.getString("CDB"))) {
					cdb = true;
					if ("CDB$ROOT".equalsIgnoreCase(rs.getString("CON_NAME")))
						cdbRoot = true;
					else
						cdbRoot = false;
				} else {
					cdb = false;
					cdbRoot = false;
				}
			} else
				throw new SQLException("Unable to detect CDB/PDB status!");
		}

		dbId = rs.getLong("DBID");
		databaseName = rs.getString("NAME");
		platformName = rs.getString("PLATFORM_NAME");
		rs.close();
		rs = null;
		ps.close();
		ps = null;

		SchemaBuilder schemaBuilder = SchemaBuilder
				.struct()
				.name("eu.solutions.a2.cdc.oracle.Source");
		schemaBuilder.field("instance_number", Schema.INT16_SCHEMA);
		schemaBuilder.field("version", Schema.STRING_SCHEMA);
		schemaBuilder.field("instance_name", Schema.STRING_SCHEMA);
		schemaBuilder.field("host_name", Schema.STRING_SCHEMA);
		schemaBuilder.field("dbid", Schema.INT64_SCHEMA);
		schemaBuilder.field("database_name", Schema.STRING_SCHEMA);
		schemaBuilder.field("platform_name", Schema.STRING_SCHEMA);
		// Table/Operation specific
		schemaBuilder.field("query", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("pdb_name", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("owner", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("table", Schema.OPTIONAL_STRING_SCHEMA);
		schemaBuilder.field("scn", Schema.INT64_SCHEMA);
		schemaBuilder.field("ts_ms", Schema.INT64_SCHEMA);
		schema = schemaBuilder.build();

		// It's No Good...
		if (instance == null && initialCall)
			instance = this;
	}

	public static OraRdbmsInfo getInstance() throws SQLException {
		// It's No Good...
		if (instance == null) {
			final Connection connection = OraPoolConnectionFactory.getConnection();
			instance = new OraRdbmsInfo(connection, false);
			connection.close();
		}
		return instance;
	}

	public Struct getStruct(String query, String pdbName, String owner, String table, long scn, Long ts) {
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
		if (ts != null)
			struct.put("ts_ms", ts);
		else
			struct.put("ts_ms", 0l);
		return struct;
	}

	public String getVersionString() {
		return versionString;
	}

	public int getVersionMajor() {
		return versionMajor;
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

	public Schema getSchema() {
		return schema;
	}

}
