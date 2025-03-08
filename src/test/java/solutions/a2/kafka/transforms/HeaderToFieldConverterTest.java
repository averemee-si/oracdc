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
public class HeaderToFieldConverterTest {

	@Test
	public void test() {

		

		final Map<String, String> props = new HashMap<>();
		props.put("header", "op");
		props.put("field", "_oracdcp");
		props.put("map", "c:I,u:U,d:D");
	
		final HeaderToFieldConverter<SinkRecord> h2f = new HeaderToFieldConverter.Value<>();
		h2f.configure(props);

		final Schema schema = SchemaBuilder.struct()
				.field("INVOICE_ID", Schema.INT64_SCHEMA)
				.field("INVOICE_NUMBER", Schema.STRING_SCHEMA)
				.build();

		final Struct struct = new Struct(schema);
		struct.put("INVOICE_ID", 1L);
		struct.put("INVOICE_NUMBER", "20250308/001");

		final SinkRecord originalC = new SinkRecord("test_topic", 0, null, null, schema, struct, 0);
		originalC.headers().addString("op", "c");
		final SinkRecord updatedC = h2f.apply(originalC);
		final Struct structC = (Struct) updatedC.value();
		assertEquals("I", structC.get("_oracdcp"));

		final SinkRecord originalU = new SinkRecord("test_topic", 0, null, null, schema, struct, 0);
		originalU.headers().addString("op", "u");
		final SinkRecord updatedU = h2f.apply(originalU);
		final Struct structU = (Struct) updatedU.value();
		assertEquals("U", structU.get("_oracdcp"));

		final SinkRecord originalD = new SinkRecord("test_topic", 0, null, null, schema, struct, 0);
		originalD.headers().addString("op", "d");
		final SinkRecord updatedD = h2f.apply(originalD);
		final Struct structD = (Struct) updatedD.value();
		assertEquals("D", structD.get("_oracdcp"));

		h2f.close();
	}
}
