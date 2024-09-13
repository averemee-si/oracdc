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

package solutions.a2.kafka.transforms;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */

public class SchemaAndStructUtils {

	public static Struct requireStruct(final Object value, final String purpose) {
		if ((value instanceof Struct)) {
			return (Struct) value;
		} else {
			throw new DataException("Only Struct objects supported for [" + purpose + "], found: " + value == null ? "null" : value.getClass().getName());
		}
	}

	public static Struct requireStructOrNull(final Object value, final String purpose) {
		if (value == null) {
			return null;
		} else {
			return requireStruct(value, purpose);
		}
	}

	public static SchemaBuilder copySchemaBasics(final Schema source, final SchemaBuilder builder) {
		builder.name(source.name());
		builder.version(source.version());
		builder.doc(source.doc());

		if (source.parameters() != null) {
			builder.parameters(source.parameters());
		}
		return builder;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> requireMap(Object value, String purpose) {
		if (value instanceof Map) {
			return (Map<String, Object>) value;
		} else {
			throw new DataException("Only Map objects supported in absence of schema for [" + purpose + "], found: " + value == null ? "null" : value.getClass().getName());
		}
	}
}
