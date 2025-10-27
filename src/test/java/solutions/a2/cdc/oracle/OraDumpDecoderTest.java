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

import static java.sql.Types.FLOAT;
import static java.sql.Types.DOUBLE;
import static oracle.jdbc.OracleTypes.BINARY_FLOAT;
import static oracle.jdbc.OracleTypes.BINARY_DOUBLE;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.NUMBER;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraDumpDecoderTest {

	@Test
	public void test() {
		//	select DUMP('thanks', 16) from DUAL;
		//	Typ=96 Len=6: 74,68,61,6e,6b,73
		String sUsAscii = "7468616e6b73";
		//	select DUMP('謝謝啦', 16) from DUAL;
		//	Typ=96 Len=9: e8,ac,9d,e8,ac,9d,e5,95,a6
		String sTrChinese = "e8ac9de8ac9de595a6";
		//	select DUMP('Σας ευχαριστώ', 16) from DUAL;
		//	Typ=96 Len=25: ce,a3,ce,b1,cf,82,20,ce,b5,cf,85,cf,87,ce,b1,cf,81,ce,b9,cf,83,cf,84,cf,8e
		String sGreek = "cea3ceb1cf8220ceb5cf85cf87ceb1cf81ceb9cf83cf84cf8e";
		//	select DUMP('Спасибо', 16) from DUAL;
		//	Typ=96 Len=14: d0,a1,d0,bf,d0,b0,d1,81,d0,b8,d0,b1,d0,be
		String sCyrillic = "d0a1d0bfd0b0d181d0b8d0b1d0be";

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
		String bdNegative = "3f534966";
		String binaryFloatSqrt3 = "bfddb3d7";
		String binaryDoubleSqrt3 = "bffbb67ae8584caa";
		String number_11_7_Sqrt3 = "c1024a153351";

		try {
			OraCdcDecoder decoder = OraCdcDecoderFactory.get("AL32UTF8");
			System.out.println(decoder.decode(sUsAscii));
			assertEquals(decoder.decode(sUsAscii), "thanks");
			System.out.println(decoder.decode(sTrChinese));
			assertEquals(decoder.decode(sTrChinese), "謝謝啦");
			System.out.println(decoder.decode(sGreek));
			assertEquals(decoder.decode(sGreek), "Σας ευχαριστώ");
			System.out.println(decoder.decode(sCyrillic));
			assertEquals(decoder.decode(sCyrillic), "Спасибо");

			// BigDecimal
			decoder = OraCdcDecoderFactory.getNUMBER(NUMBER.toBigDecimal(hexToRaw(bdNegative)).scale());
			System.out.println(decoder.decode(bdNegative));
			assertEquals(decoder.decode(bdNegative), NUMBER.toBigDecimal(hexToRaw(bdNegative)));

			// float
			decoder = OraCdcDecoderFactory.get(FLOAT);
			System.out.println(decoder.decode(hexToRaw(bdNegative)));
			assertEquals(decoder.decode(hexToRaw(bdNegative)), NUMBER.toFloat(hexToRaw(bdNegative)));
	
			// double
			decoder = OraCdcDecoderFactory.get(DOUBLE);
			System.out.println(decoder.decode(hexToRaw(bdNegative)));
			assertEquals(decoder.decode(hexToRaw(bdNegative)), NUMBER.toDouble(hexToRaw(bdNegative)));

			decoder = OraCdcDecoderFactory.get(BINARY_FLOAT);
			System.out.println(decoder.decode(binaryFloatSqrt3));
			assertEquals(decoder.decode(binaryFloatSqrt3), new BINARY_FLOAT(hexToRaw(binaryFloatSqrt3)).floatValue());

			decoder = OraCdcDecoderFactory.get(BINARY_DOUBLE);
			System.out.println(decoder.decode(binaryDoubleSqrt3));
			assertEquals(decoder.decode(binaryDoubleSqrt3), new BINARY_DOUBLE(hexToRaw(binaryDoubleSqrt3)).doubleValue());

			// BigDecimal
			decoder = OraCdcDecoderFactory.getNUMBER(NUMBER.toBigDecimal(hexToRaw(number_11_7_Sqrt3)).scale());
			System.out.println(decoder.decode(number_11_7_Sqrt3));
			assertEquals(decoder.decode(number_11_7_Sqrt3), NUMBER.toBigDecimal(hexToRaw(number_11_7_Sqrt3)));

			// float
			decoder = OraCdcDecoderFactory.get(FLOAT);
			System.out.println(decoder.decode(hexToRaw(number_11_7_Sqrt3)));
			assertEquals(decoder.decode(hexToRaw(number_11_7_Sqrt3)), NUMBER.toFloat(hexToRaw(number_11_7_Sqrt3)));

			// double
			decoder = OraCdcDecoderFactory.get(DOUBLE);
			System.out.println(decoder.decode(hexToRaw(number_11_7_Sqrt3)));
			assertEquals(decoder.decode(hexToRaw(number_11_7_Sqrt3)), NUMBER.toDouble(hexToRaw(number_11_7_Sqrt3)));

		} catch (SQLException e) {
			e.printStackTrace();
			fail("Exception " + e.getMessage());
		}
	}
}
