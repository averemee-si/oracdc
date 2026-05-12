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

package solutions.a2.cdc.oracle;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static solutions.a2.cdc.oracle.runtime.data.KafkaConnectSchema.oraNumberBuilder;
import static solutions.a2.cdc.oracle.runtime.data.KafkaConnectSchema.ORA_NUMBER_LOGICAL_NAME;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import java.math.BigDecimal;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import oracle.sql.NUMBER;

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
				oraNumberBuilder().optional().build());
		schemaBuilder.field("NUMBER02",
				oraNumberBuilder().build());
		Schema schema = schemaBuilder.build();
		Struct struct = new Struct(schema);
		struct.put("NUMBER01", hexToRaw("3f534966"));
		struct.put("NUMBER02", hexToRaw("c1024a153351"));

		System.out.println(struct.schema().field("NUMBER01").schema().name());
		assertTrue(struct.schema().field("NUMBER01").schema().name().equals(ORA_NUMBER_LOGICAL_NAME));
		assertTrue(struct.schema().field("NUMBER02").schema().name().equals(ORA_NUMBER_LOGICAL_NAME));

		final BigDecimal bdRef1 = new BigDecimal("-0.1828");
		final BigDecimal bdRef2 = new BigDecimal("1.7320508");
		
		final BigDecimal bd1 = toLogical(struct.getBytes("NUMBER01"));
		final BigDecimal bd2 = toLogical(struct.getBytes("NUMBER02"));

		assertTrue(bd1.equals(bdRef1));
		assertTrue(bd2.equals(bdRef2));
	}

	public static BigDecimal toLogical(final byte[] dumpValue) {
		final BigDecimal bd;
		try {
			bd = new NUMBER(dumpValue).bigDecimalValue();
		} catch (SQLException  sqle) {
			if (dumpValue.length == 1 && dumpValue[0] == 0x0) {
				return null;
			} else {
				throw new IllegalArgumentException(sqle);
			}
		}
		return bd;
	}

}
