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

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.NUMBER;
import solutions.a2.oracle.jdbc.types.OracleNumber;

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

		OraDumpDecoder odd = null;
		odd = new OraDumpDecoder("AL32UTF8", "AL16UTF16");
		try {
			System.out.println(odd.fromVarchar2(sUsAscii));
			assertEquals(odd.fromVarchar2(sUsAscii), "thanks");
			System.out.println(odd.fromVarchar2(sTrChinese));
			assertEquals(odd.fromVarchar2(sTrChinese), "謝謝啦");
			System.out.println(odd.fromVarchar2(sGreek));
			assertEquals(odd.fromVarchar2(sGreek), "Σας ευχαριστώ");
			System.out.println(odd.fromVarchar2(sCyrillic));
			assertEquals(odd.fromVarchar2(sCyrillic), "Спасибо");

			// BigDecimal
			System.out.println(OraDumpDecoder.toBigDecimal(bdNegative));
			assertEquals(OraDumpDecoder.toBigDecimal(bdNegative), NUMBER.toBigDecimal(hexToRaw(bdNegative)));
			// float
			System.out.println(OracleNumber.toFloat(hexToRaw(bdNegative)));
			assertEquals(OracleNumber.toFloat(hexToRaw(bdNegative)), NUMBER.toFloat(hexToRaw(bdNegative)));
			// double
			System.out.println(OracleNumber.toDouble(hexToRaw(bdNegative)));
			assertEquals(OracleNumber.toDouble(hexToRaw(bdNegative)), NUMBER.toDouble(hexToRaw(bdNegative)));

			System.out.println(OraDumpDecoder.fromBinaryFloat(binaryFloatSqrt3));
			assertEquals(OraDumpDecoder.fromBinaryFloat(binaryFloatSqrt3), new BINARY_FLOAT(hexToRaw(binaryFloatSqrt3)).floatValue());
			System.out.println(OraDumpDecoder.fromBinaryDouble(binaryDoubleSqrt3));
			assertEquals(OraDumpDecoder.fromBinaryDouble(binaryDoubleSqrt3), new BINARY_DOUBLE(hexToRaw(binaryDoubleSqrt3)).doubleValue());

			// BigDecimal
			System.out.println(OraDumpDecoder.toBigDecimal(number_11_7_Sqrt3));
			assertEquals(OraDumpDecoder.toBigDecimal(number_11_7_Sqrt3), NUMBER.toBigDecimal(hexToRaw(number_11_7_Sqrt3)));
			// float
			System.out.println(OracleNumber.toFloat(hexToRaw(number_11_7_Sqrt3)));
			assertEquals(OracleNumber.toFloat(hexToRaw(number_11_7_Sqrt3)), NUMBER.toFloat(hexToRaw(number_11_7_Sqrt3)));
			// double
			System.out.println(OracleNumber.toDouble(hexToRaw(number_11_7_Sqrt3)));
			assertEquals(OracleNumber.toDouble(hexToRaw(number_11_7_Sqrt3)), NUMBER.toDouble(hexToRaw(number_11_7_Sqrt3)));

		} catch (SQLException e) {
			e.printStackTrace();
			fail("Exception " + e.getMessage());
		}
	}
}
