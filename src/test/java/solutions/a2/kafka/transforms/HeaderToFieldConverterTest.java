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
 *
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
