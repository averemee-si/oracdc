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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;

/**
 * 
 * Representation of <a href="https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Data-Types.html#GUID-E441F541-BA31-4E8C-B7B4-D2FB8C42D0DF">Oracle JSON</a> (ID=119) for Kafka Connect
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraJson {

	public static final String LOGICAL_NAME = "solutions.a2.OraJson";
	public static final Struct NULL = new Struct(schema());

	public static SchemaBuilder builder() {
		final SchemaBuilder builder = SchemaBuilder
				.struct()
				.optional()
				.name(LOGICAL_NAME)
				.version(2)
				.doc("Oracle JSON");
		builder.field("V", OPTIONAL_STRING_SCHEMA);
		return builder;
	}

	public static Schema schema() {
		return builder().build();
	}

}
