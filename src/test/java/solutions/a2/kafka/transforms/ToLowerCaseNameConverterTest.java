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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
public class ToLowerCaseNameConverterTest {

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
		SinkRecord updated = null;

		ToLowerCaseNameConverter<SinkRecord> lcKey = new ToLowerCaseNameConverter.Key<>();
		lcKey.configure(props);
		updated = lcKey.apply(original);

		final Struct updKey = (Struct) updated.key();
		assertNull(updKey.schema().field("DELIVERY_DETAIL_ID"));
		assertNotNull(updKey.schema().field("delivery_detail_id"));
		assertEquals(updKey.get("delivery_detail_id"), keyStruct.get("DELIVERY_DETAIL_ID"));

		ToLowerCaseNameConverter<SinkRecord> lcValue = new ToLowerCaseNameConverter.Value<>();
		lcValue.configure(props);
		updated = lcValue.apply(original);

		final Struct updValue = (Struct) updated.value();
		assertNull(updValue.schema().field("SOURCE_HEADER_TYPE_NAME"));
		assertNotNull(updValue.schema().field("source_header_type_name"));
		assertEquals(updValue.get("source_header_type_name"), valueStruct.get("SOURCE_HEADER_TYPE_NAME"));
		assertNull(updValue.schema().field("CUSTOMER_ID"));
		assertNotNull(updValue.schema().field("customer_id"));
		assertEquals(updValue.get("customer_id"), valueStruct.get("CUSTOMER_ID"));

		lcKey.close();
		lcValue.close();

	}
}
