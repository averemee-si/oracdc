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
