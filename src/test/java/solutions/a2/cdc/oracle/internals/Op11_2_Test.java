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
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_2_IRP;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import java.io.IOException;

import org.apache.log4j.BasicConfigurator;
import org.junit.jupiter.api.Test;

import solutions.a2.cdc.oracle.OraCdcRedoMinerStatement;
import solutions.a2.cdc.oracle.OraCdcTransaction;
import solutions.a2.cdc.oracle.OraCdcTransactionArrayList;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class Op11_2_Test {

	@Test
	public void singleRedoRecord() {

		BasicConfigurator.configure();

		OraCdcRedoLog orl = OraCdcRedoLog.getLinux19c();
		
		byte[] baInsert1 = hexToRaw("980200000500000099E2DC03010000003A00ADD1000000000000010003000000030000000A00000098E2DC030000000097E2DC03000000009CE2DC0300000000FF6D9C4805021D000B00FFFFE000400247E0DC03000000000100FFFF0300000000000000060020000400000012000000EE0C0000340A400219040A005200880000FF000000000000000000003A00ADD105011E000B00FFFF340A400246E0DC03000000000300FFFF03000000000000000C0014004C000800140014008800F8191200000007001200EE0C000019040A00412701004127010005000000000000000B011200080C010000000000340A4002190407008B13DB03000000009613DB030000000001000000FFFFFFFFFFFFFFFF300A40020000000000000000030D000000000000DB000003DA000003FA1223010100000000000000021C0000010000000100000000000000000200000B0201000C000100DB00000360353C0100000000020041270300000000000000160018003100030004000800030007000300020002000000010D00000000000007001200EE0C0000340A400219040A00DB000003DA000003FA120201010000002C01080000000000000000000000000000000000000000002B0000000000000000000000C24C16005741524453414C45534D414EC24D630077B5021601010100C20D3300C2060000C11F000005130000000000000000000000000000000000000006000003000000000000002000080003000300000006000C0005000300200000000600040004000000000000006A62E800000053595300535953006F7261636C6500003932326264343936653439317074732F300000003732330073716C706C7573403932326264343936653439312028544E532056312D563329000001000000000000000013FFFFFFFF");
		OraCdcRedoRecord rrInsert1 = new OraCdcRedoRecord(orl, 0x0000000003dce299l, "0x000363.00001224.0010", baInsert1);
		assertTrue(rrInsert1.has5_1());
		assertTrue(rrInsert1.change5_1().supplementalLogData());
		assertTrue(rrInsert1.change5_1().supplementalFb() != 0);
		assertTrue(rrInsert1.change5_1().fb() == 0);
		assertFalse(rrInsert1.has10_x());
		assertTrue(rrInsert1.has11_x());
		assertTrue(rrInsert1.change11_x().fb() != 0);
		assertEquals(rrInsert1.change11_x().operation, _11_2_IRP);

		OraCdcTransaction transaction = new OraCdcTransactionArrayList("0x0007.012.00000cee", 0x0000000003dce299l, 0x10, orl.cdb());
		OraCdcRedoMinerStatement stmt = new OraCdcRedoMinerStatement();
		try {
			transaction.processRowChange(rrInsert1, false, System.currentTimeMillis());
			transaction.setCommitScn(0x0000000003da258al);

			assertTrue(transaction.completed());

			assertTrue(transaction.getStatement(stmt));
			assertEquals(stmt.getOperation(), INSERT);
			assertEquals(stmt.getRba(), rrInsert1.rba());
			assertEquals(stmt.getRowId().toString(), "AAASdBAAMAAAADbAAA");
			System.out.println(stmt);

		} catch(IOException ioe) {}
	}

}
