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

import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import solutions.a2.cdc.oracle.data.OraIntervalDS;
import solutions.a2.cdc.oracle.data.OraIntervalYM;

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
public class OraIntervalConverterTest {

	@Test
	public void test() {

		// Period of 3 years and 7 months...
		final INTERVALYM ora3Y7M = new INTERVALYM("3-7");
		// Duration of 77 days, 14 hours, 33 minutes, and 15 seconds...
		final INTERVALDS ora77D14H33M15D = new INTERVALDS("77 14:33:15.0");

		final Map<String, String> props = new HashMap<>();
		OraIntervalConverter<SinkRecord> fromOra;
		final Schema schema = SchemaBuilder.struct()
				.field("DURATION_Y2M", OraIntervalYM.builder().build())
				.field("DURATION_D2S", OraIntervalDS.builder().build())
				.build();
		final Struct struct = new Struct(schema);
		struct.put("DURATION_Y2M", ora3Y7M.getBytes());
		struct.put("DURATION_D2S", ora77D14H33M15D.getBytes());
		final SinkRecord original = new SinkRecord("test_topic", 0, null, null, schema, struct, 0);
		Struct updated;

		// oracle.sql.INTERVALYM -> ISO08601 String
		props.clear();
		fromOra = null;
		props.put("field", "DURATION_Y2M");
		fromOra = new OraIntervalConverter.Value<>();
		fromOra.configure(props);
		updated = (Struct) fromOra.apply(original).value();
		assertEquals("P3Y7M", updated.get("DURATION_Y2M"));
		fromOra.close();

		// oracle.sql.INTERVALDS -> ISO08601 String
		props.clear();
		fromOra = null;
		props.put("field", "DURATION_D2S");
		fromOra = new OraIntervalConverter.Value<>();
		fromOra.configure(props);
		updated = (Struct) fromOra.apply(original).value();
		assertEquals("P77DT14H33M15S", updated.get("DURATION_D2S"));
		fromOra.close();

	}
}
