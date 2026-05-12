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

package solutions.a2.cdc.oracle.runtime.data;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import solutions.a2.cdc.oracle.OraCdcStatementBase;
import solutions.a2.cdc.oracle.OraCdcTransaction;
import solutions.a2.cdc.oracle.OraRdbmsInfo;
import solutions.a2.cdc.oracle.OraCdcTableBase;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class KafkaRdbmsInfoStruct {

	private final Schema schema;
	private final OraRdbmsInfo rdbmsInfo;

	public KafkaRdbmsInfoStruct(final OraRdbmsInfo rdbmsInfo) {
		this.rdbmsInfo = rdbmsInfo;
		var schemaBuilder = SchemaBuilder
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
	}

	public Schema schema() {
		return schema;
	}

	public Struct getStruct(final String query, final String pdbName, final String owner,
			final String table, final long scn, final long ts, final String xid,
			final long commitScn, final String rowId) {
		var struct = new Struct(schema);
		struct.put("instance_number", rdbmsInfo.getInstanceNumber());
		struct.put("version", rdbmsInfo.getVersionString());
		struct.put("instance_name", rdbmsInfo.getInstanceName());
		struct.put("host_name", rdbmsInfo.getHostName());
		struct.put("dbid", rdbmsInfo.getDbId());
		struct.put("database_name", rdbmsInfo.getDatabaseName());
		struct.put("platform_name", rdbmsInfo.getPlatformName());
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

	public Struct getStruct(OraCdcTableBase table, OraCdcStatementBase stmt, OraCdcTransaction transaction) {
		var struct = new Struct(schema);
		struct.put("instance_number", rdbmsInfo.getInstanceNumber());
		struct.put("version", rdbmsInfo.getVersionString());
		struct.put("instance_name", rdbmsInfo.getInstanceName());
		struct.put("host_name", rdbmsInfo.getHostName());
		struct.put("dbid", rdbmsInfo.getDbId());
		struct.put("database_name", rdbmsInfo.getDatabaseName());
		struct.put("platform_name", rdbmsInfo.getPlatformName());
		// Table/Operation specific
		struct.put("query", stmt.getSqlRedo());
		if (StringUtils.isNotBlank(table.pdb()))
			struct.put("pdb_name", table.pdb());
		struct.put("owner", table.owner());
		struct.put("table", table.name());
		struct.put("scn", stmt.getScn());
		struct.put("ts_ms", stmt.getTs());
		struct.put("xid", transaction.getXid());
		struct.put("commit_scn", transaction.getCommitScn());
		struct.put("row_id", stmt.getRowId().toString());
		return struct;
	}

}
