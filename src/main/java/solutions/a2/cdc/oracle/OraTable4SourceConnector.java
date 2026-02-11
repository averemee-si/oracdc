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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;


/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public abstract class OraTable4SourceConnector {

	String tableOwner;
	String tableName;
	final int schemaType;
	int version;
	List<OraColumn> allColumns;
	Map<String, OraColumn> pkColumns;
	Map<String, String> sourcePartition;
	Schema schema;
	Schema keySchema;
	Schema valueSchema;
	OraRdbmsInfo rdbmsInfo;
	boolean rowLevelScn;

	OraTable4SourceConnector(String tableOwner, String tableName, int schemaType) {
		this.pkColumns = new LinkedHashMap<>();
		this.schemaType = schemaType;
		this.allColumns = new ArrayList<>();
		this.version = 1;
		this.tableOwner = tableOwner;
		this.tableName = tableName;
	}

	void schemaEiplogue(final String tableFqn,
			final SchemaBuilder keySchemaBuilder, final SchemaBuilder valueSchemaBuilder) throws SQLException {
		if (keySchemaBuilder == null) {
			keySchema = null;
		} else {
			keySchema = keySchemaBuilder.build();
		}
		valueSchema = valueSchemaBuilder.build();
		if (this.schemaType == OraCdcParameters.SCHEMA_TYPE_INT_DEBEZIUM) {
			final SchemaBuilder schemaBuilder = SchemaBuilder
					.struct()
					.name(tableFqn + ".Envelope");
			schemaBuilder.field("op", Schema.STRING_SCHEMA);
			schemaBuilder.field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA);
			schemaBuilder.field("before", keySchema);
			schemaBuilder.field("after", valueSchema);
			if (rdbmsInfo != null) {
				schemaBuilder.field("source", rdbmsInfo.getSchema());
			}
			schema = schemaBuilder.build();
		}
	}

	int version() {
		return version;
	}
}
