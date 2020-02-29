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

package eu.solutions.a2.cdc.oracle;

import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.junit.Test;

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

		// 2020-02-04T13:59:23,000000000
		// Typ=180 Len=7: Typ=180 Len=7: 78,78,02,04,0e,3c,18
		String sDatTsTyp180 = "787802040e3c18";

		OraDumpDecoder odd = null;
		odd = new OraDumpDecoder("AL32UTF8", "AL16UTF16");
		try {
			System.out.println(odd.fromVarchar2(sUsAscii));
			System.out.println(odd.fromVarchar2(sTrChinese));
			System.out.println(odd.fromVarchar2(sGreek));
			System.out.println(odd.fromVarchar2(sCyrillic));
			System.out.println(OraDumpDecoder.toTimestamp(sDatTsTyp180));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Exception " + e.getMessage());
		}
	}
}
