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
