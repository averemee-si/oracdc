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

 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
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
