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

package eu.solutions.a2.cdc.oracle.standalone.avro;

import java.io.Serializable;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import eu.solutions.a2.cdc.oracle.ConnectionFactory;

public class Source implements Serializable {

	private static final long serialVersionUID = 2184010528102467656L;

	private static BigInteger DBID;
	private BigInteger dbId;
	private static String DATABASE_NAME;
	private String databaseName;
	private static String PLATFORM_NAME;
	private String platformName;
	private static int INSTANCE_NUMBER = -1;  
	private int instanceNumber;
	private static String INSTANCE_NAME;
	private String instanceName;
	private static String HOST_NAME;
	private String hostName;
	private static String VERSION;
	private String version;
	private long ts_ms;
	private String owner;
	private String table;
	private BigInteger scn;

	public Source() {
		dbId = DBID;
		databaseName = DATABASE_NAME;
		platformName = PLATFORM_NAME;
		instanceNumber = INSTANCE_NUMBER;
		instanceName = INSTANCE_NAME;
		hostName = HOST_NAME;
		version = VERSION;
	}

	public Source(final String owner, final String table) {
		this();
		this.owner = owner;
		this.table = table;
	}

	public static void init() throws SQLException {
		Connection connection = ConnectionFactory.getConnection();
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		statement = connection.prepareStatement("select DBID, NAME, PLATFORM_NAME from V$DATABASE");
		resultSet = statement.executeQuery();
		if (resultSet.next()) {
			// Exactly one row!
			DBID = resultSet.getBigDecimal("DBID").toBigInteger();
			DATABASE_NAME = resultSet.getString("NAME");
			PLATFORM_NAME = resultSet.getString("PLATFORM_NAME");
		}
		resultSet.close();
		statement.close();

		statement = connection.prepareStatement("select INSTANCE_NUMBER, INSTANCE_NAME, HOST_NAME, VERSION from V$INSTANCE");
		resultSet = statement.executeQuery();
		if (resultSet.next()) {
			// Exactly one row!
			INSTANCE_NUMBER = resultSet.getInt("INSTANCE_NUMBER");
			INSTANCE_NAME = resultSet.getString("INSTANCE_NAME");
			HOST_NAME = resultSet.getString("HOST_NAME");
			VERSION = resultSet.getString("VERSION");
		}
		resultSet.close();
		statement.close();

		resultSet = null;
		statement = null;
		connection.close();
		connection = null;
	}

	public static AvroSchema schema() {
		AvroSchema field = null;
		final AvroSchema source = AvroSchema.STRUCT_MANDATORY();
		source.setName("eu.solutions.a2.cdc.oracle");
		source.setField("source");
		source.initFields();

		field = AvroSchema.INT64_MANDATORY();
		field.setField("dbid");
		source.getFields().add(field);

		field = AvroSchema.STRING_MANDATORY();
		field.setField("database_name");
		source.getFields().add(field);

		field = AvroSchema.STRING_MANDATORY();
		field.setField("platform_name");
		source.getFields().add(field);

		field = AvroSchema.INT16_MANDATORY();
		field.setField("instance_number");
		source.getFields().add(field);

		field = AvroSchema.STRING_MANDATORY();
		field.setField("instance_name");
		source.getFields().add(field);

		field = AvroSchema.STRING_MANDATORY();
		field.setField("host_name");
		source.getFields().add(field);

		field = AvroSchema.STRING_MANDATORY();
		field.setField("version");
		source.getFields().add(field);

		field = AvroSchema.INT64_MANDATORY();
		field.setField("ts_ms");
		source.getFields().add(field);

		field = AvroSchema.STRING_MANDATORY();
		field.setField("owner");
		source.getFields().add(field);

		field = AvroSchema.STRING_MANDATORY();
		field.setField("table");
		source.getFields().add(field);

		field = AvroSchema.INT64_MANDATORY();
		field.setField("scn");
		source.getFields().add(field);

		return source;
	}

	public BigInteger getDbid() {
		return dbId;
	}
	public void setDbid(BigInteger dbId) {
		this.dbId = dbId;
	}

	public String getDatabase_name() {
		return databaseName;
	}
	public void setDatabase_name(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getPlatform_name() {
		return platformName;
	}
	public void setPlatform_name(String platformName) {
		this.platformName = platformName;
	}

	public int getInstance_number() {
		return instanceNumber;
	}
	public void setInstance_number(int instanceNumber) {
		this.instanceNumber = instanceNumber;
	}

	public String getInstance_name() {
		return instanceName;
	}
	public void setInstance_name(String instanceName) {
		this.instanceName = instanceName;
	}

	public String getHost_name() {
		return hostName;
	}
	public void setHost_name(String hostName) {
		this.hostName = hostName;
	}

	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}

	public long getTs_ms() {
		return ts_ms;
	}
	public void setTs_ms(long ts_ms) {
		this.ts_ms = ts_ms;
	}

	public String getOwner() {
		return owner;
	}
	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getTable() {
		return table;
	}
	public void setTable(String table) {
		this.table = table;
	}

	public BigInteger getScn() {
		return scn;
	}
	public void setScn(BigInteger scn) {
		this.scn = scn;
	}

}
