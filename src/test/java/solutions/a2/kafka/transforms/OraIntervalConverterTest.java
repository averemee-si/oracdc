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
