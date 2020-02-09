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

package eu.solutions.a2.cdc.oracle.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonSchema {

	public static final String SCHEMA_TYPE = "type";
	public static final String TYPE_BOOLEAN = "boolean";
	public static final String TYPE_INT8 = "int8";
	public static final String TYPE_INT16 = "int16";
	public static final String TYPE_INT32 = "int32";
	public static final String TYPE_INT64 = "int64";
	public static final String TYPE_FLOAT = "float";
	public static final String TYPE_DOUBLE = "double";
	public static final String TYPE_BYTES = "bytes";
	public static final String TYPE_STRING = "string";

	public static final String TYPE_STRUCT = "struct";

	protected static final ObjectNode SCHEMA_BOOLEAN =
			JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE, TYPE_BOOLEAN);
	protected static final ObjectNode SCHEMA_INT8 =
			JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE, TYPE_INT8);
	protected static final ObjectNode SCHEMA_INT16 =
			JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE, TYPE_INT8);
	protected static final ObjectNode SCHEMA_INT32 =
			JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE, TYPE_INT8);
	protected static final ObjectNode SCHEMA_INT64 =
			JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE, TYPE_INT8);
	protected static final ObjectNode SCHEMA_FLOAT =
			JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE, TYPE_FLOAT);
	protected static final ObjectNode SCHEMA_DOUBLE =
			JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE, TYPE_DOUBLE);
	protected static final ObjectNode SCHEMA_BYTES =
			JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE, TYPE_BYTES);
	protected static final ObjectNode SCHEMA_STRING =
			JsonNodeFactory.instance.objectNode().put(SCHEMA_TYPE, TYPE_STRING);

	public static ObjectNode envelope(JsonNode schema, JsonNode payload) {
		ObjectNode result = JsonNodeFactory.instance.objectNode();
		result.set("schema", schema);
		result.set("payload", payload);
		return result;
	}

	static class Envelope {
		public JsonNode schema;
		public JsonNode payload;

		public Envelope(JsonNode schema, JsonNode payload) {
			this.schema = schema;
			this.payload = payload;
		}

		public ObjectNode toJsonNode() {
			return envelope(schema, payload);
		}
	}
	
}