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

package solutions.a2.cdc.oracle;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import java.math.BigDecimal;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import solutions.a2.cdc.oracle.data.OraNumber;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraNumberTest {

	@Test
	public void test() {
		/*
			create table NUMBER_TEST(ID NUMBER, BF BINARY_FLOAT, BD BINARY_DOUBLE, NN117 NUMBER(11,7));
			insert into NUMBER_TEST values(-.1828, SQRT(3),SQRT(3),SQRT(3));
			SQL> select dump(ID, 16) from NUMBER_TEST;
			DUMP(ID,16)
			--------------------------------------------------------------------------------
			Typ=2 Len=4: 3f,53,49,66

			SQL> select dump(BF, 16) from NUMBER_TEST;
			DUMP(BF,16)
			--------------------------------------------------------------------------------
			Typ=100 Len=4: bf,dd,b3,d7

			SQL> select dump(BD, 16) from NUMBER_TEST;
			DUMP(BD,16)
			--------------------------------------------------------------------------------
			Typ=101 Len=8: bf,fb,b6,7a,e8,58,4c,aa

			SQL> select dump(NN117, 16) from NUMBER_TEST;
			DUMP(NN117,16)
			--------------------------------------------------------------------------------
			Typ=2 Len=6: c1,2,4a,15,33,51
		 */
		final SchemaBuilder schemaBuilder = SchemaBuilder
				.struct()
				.required()
				.name("NUMBER.Test")
				.version(1);
		schemaBuilder.field("NUMBER01",
				OraNumber.builder().optional().build());
		schemaBuilder.field("NUMBER02",
				OraNumber.builder().build());
		Schema schema = schemaBuilder.build();
		Struct struct = new Struct(schema);
		struct.put("NUMBER01", hexToRaw("3f534966"));
		struct.put("NUMBER02", hexToRaw("c1024a153351"));

		System.out.println(struct.schema().field("NUMBER01").schema().name());
		assertTrue(struct.schema().field("NUMBER01").schema().name().equals(OraNumber.LOGICAL_NAME));
		assertTrue(struct.schema().field("NUMBER02").schema().name().equals(OraNumber.LOGICAL_NAME));

		final BigDecimal bdRef1 = new BigDecimal("-0.1828");
		final BigDecimal bdRef2 = new BigDecimal("1.7320508");
		
		final BigDecimal bd1 = OraNumber.toLogical(struct.getBytes("NUMBER01"));
		final BigDecimal bd2 = OraNumber.toLogical(struct.getBytes("NUMBER02"));

		assertTrue(bd1.equals(bdRef1));
		assertTrue(bd2.equals(bdRef2));
	}
}
