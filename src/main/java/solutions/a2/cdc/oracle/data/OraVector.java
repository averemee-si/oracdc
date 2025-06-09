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

package solutions.a2.cdc.oracle.data;

import static org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT8_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * 
 * Representation of <a href="https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Data-Types.html">Oracle Vector</a> for Kafka Connect
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraVector {

	public static final String LOGICAL_NAME = "solutions.a2.OraVector";

	private static SchemaBuilder builder; 

	static {
		builder = SchemaBuilder
				.struct()
				.optional()
				.name(LOGICAL_NAME)
				.version(2)
				.doc("Oracle Vector");
		builder = builder.field("B",
					SchemaBuilder.array(OPTIONAL_BOOLEAN_SCHEMA)
						.optional()
						.doc("BINARY"));
		builder = builder.field("I",
					SchemaBuilder.array(OPTIONAL_INT8_SCHEMA)
						.optional()
						.doc("INT8"));
		builder = builder.field("F",
					SchemaBuilder.array(OPTIONAL_FLOAT32_SCHEMA)
						.optional()
						.doc("FLOAT32"));
		builder = builder.field("D",
					SchemaBuilder.array(OPTIONAL_FLOAT64_SCHEMA)
						.optional()
						.doc("FLOAT64"));
	}

	public static Schema schema() {
		return builder.build();
	}

}
