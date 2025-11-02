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

import static org.apache.kafka.connect.data.Schema.INT8_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT16_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.FLOAT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.apache.kafka.connect.data.Schema.BYTES_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT8_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT16_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA;
import static org.apache.kafka.connect.data.Decimal.SCALE_FIELD;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * 
 * Wrapped standard schemas
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class WrappedSchemas {

	public static final String WRAPPED_INT8         = "solutions.a2.int8";
	public static final String WRAPPED_INT16        = "solutions.a2.int16";
	public static final String WRAPPED_INT32        = "solutions.a2.int32";
	public static final String WRAPPED_INT64        = "solutions.a2.int64";
	public static final String WRAPPED_FLOAT32      = "solutions.a2.float32";
	public static final String WRAPPED_FLOAT64      = "solutions.a2.float64";
	public static final String WRAPPED_BOOLEAN      = "solutions.a2.bool";
	public static final String WRAPPED_STRING       = "solutions.a2.string";
	public static final String WRAPPED_BYTES        = "solutions.a2.bytes";
	public static final String WRAPPED_DECIMAL      = "solutions.a2.DECIMAL";
	public static final String WRAPPED_NUMBER       = "solutions.a2.NUMBER";
	public static final String WRAPPED_TIMESTAMP    = "solutions.a2.TIMESTAMP";
	public static final String WRAPPED_TIMESTAMPTZ  = "solutions.a2.TIMESTAMPTZ";
	public static final String WRAPPED_TIMESTAMPLTZ = "solutions.a2.TIMESTAMPLTZ";
	public static final String WRAPPED_INTERVALYM   = "solutions.a2.INTERVALYM";
	public static final String WRAPPED_INTERVALDS   = "solutions.a2.INTERVALDS";

	public static final String WRAPPED_OPT_INT8         = "solutions.a2.int8.opt";
	public static final String WRAPPED_OPT_INT16        = "solutions.a2.int16.opt";
	public static final String WRAPPED_OPT_INT32        = "solutions.a2.int32.opt";
	public static final String WRAPPED_OPT_INT64        = "solutions.a2.int64.opt";
	public static final String WRAPPED_OPT_FLOAT32      = "solutions.a2.float32.opt";
	public static final String WRAPPED_OPT_FLOAT64      = "solutions.a2.float64.opt";
	public static final String WRAPPED_OPT_BOOLEAN      = "solutions.a2.bool.opt";
	public static final String WRAPPED_OPT_STRING       = "solutions.a2.string.opt";
	public static final String WRAPPED_OPT_BYTES        = "solutions.a2.bytes.opt";
	public static final String WRAPPED_OPT_DECIMAL      = "solutions.a2.DECIMAL.opt";
	public static final String WRAPPED_OPT_NUMBER       = "solutions.a2.NUMBER.opt";
	public static final String WRAPPED_OPT_TIMESTAMP    = "solutions.a2.TIMESTAMP.opt";
	public static final String WRAPPED_OPT_TIMESTAMPTZ  = "solutions.a2.TIMESTAMPTZ.opt";
	public static final String WRAPPED_OPT_TIMESTAMPLTZ = "solutions.a2.TIMESTAMPLTZ.opt";
	public static final String WRAPPED_OPT_INTERVALYM   = "solutions.a2.INTERVALYM.opt";
	public static final String WRAPPED_OPT_INTERVALDS   = "solutions.a2.INTERVALDS.opt";

	public static final Schema WRAPPED_INT8_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_INT8)
			.version(1)
			.doc("Wrapped int8 schema")
			.field("V", INT8_SCHEMA)
			.build();
	public static final Schema WRAPPED_INT16_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_INT16)
			.version(1)
			.doc("Wrapped int16 schema")
			.field("V", INT16_SCHEMA)
			.build();
	public static final Schema WRAPPED_INT32_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_INT32)
			.version(1)
			.doc("Wrapped int32 schema")
			.field("V", INT32_SCHEMA)
			.build();
	public static final Schema WRAPPED_INT64_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_INT64)
			.version(1)
			.doc("Wrapped int64 schema")
			.field("V", INT64_SCHEMA)
			.build();
	public static final Schema WRAPPED_FLOAT32_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_FLOAT32)
			.version(1)
			.doc("Wrapped float32 schema")
			.field("V", FLOAT32_SCHEMA)
			.build();
	public static final Schema WRAPPED_FLOAT64_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_FLOAT64)
			.version(1)
			.doc("Wrapped float64 schema")
			.field("V", FLOAT64_SCHEMA)
			.build();
	public static final Schema WRAPPED_BOOLEAN_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_BOOLEAN)
			.version(1)
			.doc("Wrapped boolean schema")
			.field("V", BOOLEAN_SCHEMA)
			.build();
	public static final Schema WRAPPED_STRING_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_STRING)
			.version(1)
			.doc("Wrapped string schema")
			.field("V", STRING_SCHEMA)
			.build();
	public static final Schema WRAPPED_BYTES_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_BYTES)
			.version(1)
			.doc("Wrapped bytes schema")
			.field("V", BYTES_SCHEMA)
			.build();
	public static final Schema WRAPPED_DECIMAL_SCHEMA(final int scale) {
		return SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_DECIMAL)
			.version(1)
			.doc("Wrapped DECIMAL schema")
			.field("V", BYTES_SCHEMA)
			.parameter(SCALE_FIELD, Integer.toString(scale))
			.build();
	}
	public static final Schema WRAPPED_NUMBER_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_NUMBER)
			.version(1)
			.doc("Wrapped NUMBER schema")
			.field("V", BYTES_SCHEMA)
			.build();
	public static final Schema WRAPPED_TIMESTAMP_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_TIMESTAMP)
			.version(1)
			.doc("Wrapped TIMESTAMP schema")
			.field("V", BYTES_SCHEMA)
			.build();
	public static final Schema WRAPPED_TIMESTAMPTZ_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_TIMESTAMPTZ)
			.version(1)
			.doc("Wrapped TIMESTAMPTZ schema")
			.field("V", BYTES_SCHEMA)
			.build();
	public static final Schema WRAPPED_TIMESTAMPLTZ_SCHEMA(final String tz) {
			return SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_TIMESTAMPLTZ)
			.version(1)
			.doc("Wrapped TIMESTAMPLTZ schema")
			.field("V", BYTES_SCHEMA)
			.parameter("tz", tz)
			.build();
	}
	public static final Schema WRAPPED_INTERVALYM_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_INTERVALYM)
			.version(1)
			.doc("Wrapped INTERVALYM schema")
			.field("V", BYTES_SCHEMA)
			.build();
	public static final Schema WRAPPED_INTERVALDS_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_INTERVALDS)
			.version(1)
			.doc("Wrapped INTERVALDS schema")
			.field("V", BYTES_SCHEMA)
			.build();

	public static final Schema WRAPPED_OPT_INT8_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_INT8)
			.version(1)
			.doc("Wrapped optional int8 schema")
			.field("V", OPTIONAL_INT8_SCHEMA)
			.build();
	public static final Schema WRAPPED_OPT_INT16_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_INT16)
			.version(1)
			.doc("Wrapped optional int16 schema")
			.field("V", OPTIONAL_INT16_SCHEMA)
			.build();
	public static final Schema WRAPPED_OPT_INT32_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_INT32)
			.version(1)
			.doc("Wrapped optional int32 schema")
			.field("V", OPTIONAL_INT32_SCHEMA)
			.build();
	public static final Schema WRAPPED_OPT_INT64_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_INT64)
			.version(1)
			.doc("Wrapped optional int64 schema")
			.field("V", OPTIONAL_INT64_SCHEMA)
			.build();
	public static final Schema WRAPPED_OPT_FLOAT32_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_FLOAT32)
			.version(1)
			.doc("Wrapped optional float32 schema")
			.field("V", OPTIONAL_FLOAT32_SCHEMA)
			.build();
	public static final Schema WRAPPED_OPT_FLOAT64_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_FLOAT64)
			.version(1)
			.doc("Wrapped optional float64 schema")
			.field("V", OPTIONAL_FLOAT64_SCHEMA)
			.build();
	public static final Schema WRAPPED_OPT_BOOLEAN_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_BOOLEAN)
			.version(1)
			.doc("Wrapped optional boolean schema")
			.field("V", OPTIONAL_BOOLEAN_SCHEMA)
			.build();
	public static final Schema WRAPPED_OPT_STRING_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_STRING)
			.version(1)
			.doc("Wrapped optional string schema")
			.field("V", OPTIONAL_STRING_SCHEMA)
			.build();
	public static final Schema WRAPPED_OPT_BYTES_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_BYTES)
			.version(1)
			.doc("Wrapped optional bytes schema")
			.field("V", OPTIONAL_BYTES_SCHEMA)
			.build();
	public static final Schema WRAPPED_OPT_DECIMAL_SCHEMA(final int scale) {
			return SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_DECIMAL)
			.version(1)
			.doc("Wrapped DECIMAL schema")
			.field("V", OPTIONAL_BYTES_SCHEMA)
			.parameter(SCALE_FIELD, Integer.toString(scale))
			.build();
	}
	public static final Schema WRAPPED_OPT_NUMBER_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_NUMBER)
			.version(1)
			.doc("Wrapped optional NUMBER schema")
			.field("V", OPTIONAL_BYTES_SCHEMA)
			.build();
	public static final Schema WRAPPED_OPT_TIMESTAMP_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_TIMESTAMP)
			.version(1)
			.doc("Wrapped optional TIMESTAMP schema")
			.field("V", OPTIONAL_BYTES_SCHEMA)
			.build();
	public static final Schema WRAPPED_OPT_TIMESTAMPTZ_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_TIMESTAMPTZ)
			.version(1)
			.doc("Wrapped optional TIMESTAMPTZ schema")
			.field("V", OPTIONAL_BYTES_SCHEMA)
			.build();
	public static final Schema WRAPPED_OPT_TIMESTAMPLTZ_SCHEMA(final String tz) {
			return SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_TIMESTAMPLTZ)
			.version(1)
			.doc("Wrapped optional TIMESTAMPLTZ schema")
			.field("V", OPTIONAL_BYTES_SCHEMA)
			.parameter("tz", tz)
			.build();
	}
	public static final Schema WRAPPED_OPT_INTERVALYM_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_INTERVALYM)
			.version(1)
			.doc("Wrapped optional INTERVALYM schema")
			.field("V", BYTES_SCHEMA)
			.build();
	public static final Schema WRAPPED_OPT_INTERVALDS_SCHEMA = SchemaBuilder
			.struct()
			.optional()
			.name(WRAPPED_OPT_INTERVALDS)
			.version(1)
			.doc("Wrapped optional INTERVALDS schema")
			.field("V", BYTES_SCHEMA)
			.build();

}
