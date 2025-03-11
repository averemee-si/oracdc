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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import oracle.sql.NUMBER;

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
public class OraNumberConverterTest {

	@Test
	public void test() {

		final long lineId = 0x0000FFFFFFFF0001l;
		final int orgId = 0x00FFFF01;
		final short invoicingRuleId = (short) 0xEFF1;
		final BigDecimal quantity = BigDecimal.valueOf(362, 2);
		final double acceptedQuantity = 412d/100d;

		final Map<String, String> props = new HashMap<>();
		OraNumberConverter<SinkRecord> fromOra;
		final Schema schema = SchemaBuilder.struct()
				.field("LINE_ID", Schema.BYTES_SCHEMA)
				.field("ORG_ID", Schema.BYTES_SCHEMA)
				.field("INVOICING_RULE_ID", Schema.BYTES_SCHEMA)
				.field("QUANTITY", Schema.OPTIONAL_BYTES_SCHEMA)
				.field("ACCEPTED_QUANTITY", Schema.OPTIONAL_BYTES_SCHEMA)
				.build();
		final Struct struct = new Struct(schema);
		struct.put("LINE_ID", new NUMBER(lineId).getBytes());
		struct.put("ORG_ID", new NUMBER(orgId).getBytes());
		struct.put("INVOICING_RULE_ID", new NUMBER(invoicingRuleId).getBytes());
		try {
			struct.put("QUANTITY", new NUMBER(quantity).getBytes());
			struct.put("ACCEPTED_QUANTITY", new NUMBER(acceptedQuantity).getBytes());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		final SinkRecord original = new SinkRecord("test_topic", 0, null, null, schema, struct, 0);
		Struct updated;

		// oracle.sql.NUMBER -> long
		props.clear();
		fromOra = null;
		props.put("field", "LINE_ID");
		props.put("target.type", "long");
		fromOra = new OraNumberConverter.Value<>();
		fromOra.configure(props);
		updated = (Struct) fromOra.apply(original).value();
		assertEquals(lineId, updated.get("LINE_ID"));
		fromOra.close();
		
		// oracle.sql.NUMBER -> int
		props.clear();
		fromOra = null;
		props.put("field", "ORG_ID");
		props.put("target.type", "int");
		fromOra = new OraNumberConverter.Value<>();
		fromOra.configure(props);
		updated = (Struct) fromOra.apply(original).value();
		assertEquals(orgId, updated.get("ORG_ID"));
		fromOra.close();

		// oracle.sql.NUMBER -> short
		props.put("field", "INVOICING_RULE_ID");
		props.put("target.type", "short");
		fromOra = new OraNumberConverter.Value<>();
		fromOra.configure(props);
		updated = (Struct) fromOra.apply(original).value();
		assertEquals(invoicingRuleId, updated.get("INVOICING_RULE_ID"));
		fromOra.close();

		// oracle.sql.NUMBER -> decimal
		props.put("field", "QUANTITY");
		props.put("target.type", "decimal");
		props.put("decimal.scale", "2");
		fromOra = new OraNumberConverter.Value<>();
		fromOra.configure(props);
		updated = (Struct) fromOra.apply(original).value();
		assertEquals(quantity, updated.get("QUANTITY"));
		fromOra.close();

		// oracle.sql.NUMBER -> double
		props.put("field", "ACCEPTED_QUANTITY");
		props.put("target.type", "double");
		fromOra = new OraNumberConverter.Value<>();
		fromOra.configure(props);
		updated = (Struct) fromOra.apply(original).value();
		assertEquals(acceptedQuantity, updated.get("ACCEPTED_QUANTITY"));
		fromOra.close();

	}
}
