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

package solutions.a2.cdc.oracle.internals;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_35_LCU;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import solutions.a2.cdc.oracle.OraCdcRedoMinerStatement;
import solutions.a2.cdc.oracle.OraCdcTransaction;
import solutions.a2.cdc.oracle.OraCdcTransactionArrayList;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class Op10_35_Test {

	@Test
	public void test() {

		OraCdcRedoLog orl = OraCdcRedoLog.getLinux19c();
		
		// update TEST_IOT_33 set descr1='descr1-02-U',descr2='descr22-02-U',descr3='descr333-02-U',descr4='descr4444-02-U' where id=2 and name1='name1-02';
		byte[] baUpdate1 = hexToRaw("9001000001000000608B7800010000009F99485100000000050114000E00FFFF950C0000608B7800000000000400FFFF03000000000000001C001400180010001D0016000600000000000B001C00020002000000B4004E192200000002000400060900008B020900433B0100433B010006000000000000000A16040800000609020DD98AFFFF0000950C00008B020800230295006209000063090000000000000000000002030307D8FF03000000000002C103086E616D65312D3032096E616D6532322D30320000000001000200000064657363723333332D303200014C010001000100040000000010000000020000000000000000000007000000000000000A2301000F00010063090000608B7800000000000400433B0300000000000000100010000D0008000B000C000D000E00020DA989FFFF0000950C00008B0209000210010053000204040028000000000000000100020003006465736372312D30322D5500646573637232322D30322D5564657363723333332D30322D550000006465736372343434342D30322D55F000");
		OraCdcRedoRecord rrUpdate1 = new OraCdcRedoRecord(orl, 0x788b60, "0x000042.00055ad8.00fc", baUpdate1);

		assertTrue(rrUpdate1.has5_1());
		assertTrue(rrUpdate1.has10_x());
		assertFalse(rrUpdate1.has11_x());
		assertEquals(rrUpdate1.change10_x().operation(), _10_35_LCU);

		// update TEST_IOT_33 set descr1='descr1-03-UU',descr2='descr22-03-UUU',descr3='descr333-03-UUUU' where id=3;
		byte[] baUpdate2 = hexToRaw("B00100000100000005D37800010000009F99485100000000050114000E00FFFF9C0C000005D37800000000000200FFFF03000000000000001C001400180010001D00160006000B000C000D001C00020002000E00E000480522000000020016000A0900008B022800433B0100433B010006000000000000000A16162700000A09020D0000000000009C0C00008B022700230291006209000063090000000000000000000002040307FAFF00000000000002C104086E616D65312D3033096E616D6532322D3033000000000100020000006465736372312D30332D5500646573637232322D30332D5564657363723333332D30332D55000000010C0100010001000400000000100000000200000000000000000000070000000E0000006465736372343434342D30332D5500000A2301000F0001006309000005D37800000000000300433B03000000000000000E0010000D0006000C000E0010000000020D00008B0227009C0C00008B0228000210020059000204030006000000000000000100020000006465736372312D30332D5555646573637232322D30332D555555000064657363723333332D30332D55555555");
		OraCdcRedoRecord rrUpdate2 = new OraCdcRedoRecord(orl, 0x78d305, "0x000043.0000226c.0110", baUpdate2);

		assertTrue(rrUpdate2.has5_1());
		assertTrue(rrUpdate2.has10_x());
		assertFalse(rrUpdate2.has11_x());
		assertEquals(rrUpdate2.change10_x().operation(), _10_35_LCU);

		OraCdcTransaction transaction = new OraCdcTransactionArrayList("0x0002.004.00000906", 0x788b60, 0x10, orl.cdb());
		OraCdcRedoMinerStatement stmt = new OraCdcRedoMinerStatement();
		try {
			transaction.processRowChange(rrUpdate1, false, System.currentTimeMillis());
			transaction.processRowChange(rrUpdate2, false, System.currentTimeMillis());
			
			assertTrue(transaction.getStatement(stmt));
			assertEquals(stmt.getOperation(), UPDATE);
			assertEquals(stmt.getRba(), rrUpdate1.rba());

			assertTrue(transaction.getStatement(stmt));
			assertEquals(stmt.getOperation(), UPDATE);
			assertEquals(stmt.getRba(), rrUpdate2.rba());

		} catch(IOException ioe) {}

	}
}
