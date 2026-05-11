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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class KeyToValueConverterTest {

	@Test
	public void test() {

		final Schema keySchema = SchemaBuilder.struct()
				.field("DELIVERY_DETAIL_ID", Schema.INT32_SCHEMA)
				.build();
		final Schema valueSchema = SchemaBuilder.struct()
				.field("SOURCE_HEADER_TYPE_NAME", Schema.OPTIONAL_STRING_SCHEMA)
				.field("CUSTOMER_ID", Schema.INT32_SCHEMA)
				.build();
		final Struct keyStruct = new Struct(keySchema);
		final Struct valueStruct = new Struct(valueSchema);
		
		keyStruct.put("DELIVERY_DETAIL_ID", 12);
		valueStruct.put("SOURCE_HEADER_TYPE_NAME", "ORDER_LINE");
		valueStruct.put("CUSTOMER_ID", 1);
		final SinkRecord original = new SinkRecord("test_topic", 0, keySchema, keyStruct, valueSchema, valueStruct, 0);

		final Map<String, String> props = new HashMap<>();
		props.put("fields", "DELIVERY_DETAIL_ID");
		KeyToValueConverter<SinkRecord> k2v = new KeyToValueConverter<>();
		k2v.configure(props);
		SinkRecord updated = k2v.apply(original);
		final Struct updValue = (Struct) updated.value();
		
		assertEquals(updValue.get("DELIVERY_DETAIL_ID"), keyStruct.get("DELIVERY_DETAIL_ID"));
		
		k2v.close();

	}
}
