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
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_3_DRP;
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
public class Op11_3_Test {

	@Test
	public void singleRedoRecord() {

		BasicConfigurator.configure();

		OraCdcRedoLog orl = OraCdcRedoLog.getLinux19c();
		
		byte[] baDelete1 = hexToRaw("B0020000050000008925DA03010000003A00ADD1000000000000010003000000030000000A0000008825DA03000000008725DA03000000008C25DA0300000000369B9A4805021B000B00FFFFD0004002AC22DA03000000000100FFFF030000000000000006002000040000001B00000043100000E30B4002B6041A005200F8000000000000000000000000003A00ADD105011C000B00FFFFE30B4002A822DA03000000000300FFFF03000000000000001C0014004C0020003100030004000800030007000300020002001400F800B00F1200000006001B0043100000B6041A6D412701004127010005000000000000000B011B00080C010000000000E30B4002B6041700C794D80300000000D594D80300000000FFFF0000FFFFFFFFFFFFFFFFE10B40020000000000000000040D00000000000002001A00C20E0000540B40029B05180000800080AC19C403DF000003DA000003FA122201020000002C000800000000001800000000000000E0FC5B46010000002B0002000000000000000000C24C16005741524453414C45534D414EC24D630077B5021601010100C20D3300C2060000C11F0000041C0000010001000000000000000000000200000B0301000C000100DF000003281DC403000000000102412703000000000000000600180014000000010D00000000000006001B0043100000E30B4002B6041A00DF000003DA000003FA120301020000000200000005130000000000000000000000000000000000000006000003000000000000002000080003000300000006000C000500030020000000060004000400000000000000A699E900000053595300535953006F7261636C6500003932326264343936653439317074732F300000003633380073716C706C7573403932326264343936653439312028544E532056312D563329000001000000000000000013FFFFFFFF");
		OraCdcRedoRecord rrDelete1 = new OraCdcRedoRecord(orl, 0x0000000003da2589l, "0x000362.000011e8.0010", baDelete1);
		assertTrue(rrDelete1.has5_1());
		assertFalse(rrDelete1.has10_x());
		assertTrue(rrDelete1.has11_x());
		assertEquals(rrDelete1.change11_x().operation, _11_3_DRP);
		

		OraCdcTransaction transaction = new OraCdcTransactionArrayList("0x0006.01b.00001043", 0x00000d069638cd26l, 0x10, orl.cdb());
		OraCdcRedoMinerStatement stmt = new OraCdcRedoMinerStatement();
		try {
			transaction.processRowChange(rrDelete1, false, System.currentTimeMillis());
			transaction.setCommitScn(0x0000000003da258al);

			assertTrue(transaction.completed());

			assertTrue(transaction.getStatement(stmt));
			assertEquals(stmt.getOperation(), DELETE);
			assertEquals(stmt.getRba(), rrDelete1.rba());
			assertEquals(stmt.getRowId().toString(), "AAASdBAAMAAAADfAAC");
			System.out.println(stmt);

		} catch(IOException ioe) {}

	}
}
