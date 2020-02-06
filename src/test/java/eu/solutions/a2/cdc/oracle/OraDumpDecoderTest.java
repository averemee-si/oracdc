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
		
		try {
			if (false) {
				// Must have Oracle JDBC drivers in CLASSPATH!!!
				OraDumpDecoder odd = new OraDumpDecoder("AL32UTF8", "AL16UTF16");
				System.out.println(odd.fromVarchar2(sUsAscii));
				System.out.println(odd.fromVarchar2(sTrChinese));
				System.out.println(odd.fromVarchar2(sGreek));
				System.out.println(odd.fromVarchar2(sCyrillic));
				System.out.println(odd.toTimestamp(sDatTsTyp180));
			}
		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | SQLException e) {
			e.printStackTrace();
			fail("Exception " + e.getMessage());
		}
	}
}
