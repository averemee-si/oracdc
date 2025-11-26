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
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_5_URP;
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
public class Op11_5_Test {

	@Test
	public void singleRedoRecord() {

		BasicConfigurator.configure();

		OraCdcRedoLog orl = OraCdcRedoLog.getLinux19c();
		
		byte[] baUpdate1 = hexToRaw("5C02000005000000EAFCDC03010000003A00ADD1000030000000010002000000020000000A002D00E9FCDC0300000000E8FCDC0300000000EDFCDC03000000009A759C48050215000B00FFFFA000400299F0DC03000000000100FFFF0300000000000000060020000400000010000000F70E0000B70E40023C0318005200A4000000000000000000000000003A00ADD1050116000B00FFFFB70E400298F0DC03000000000300FFFF0300000000000000120014004C0008001D0004000300020014000000A400040D1200000003001000F70E00003C031800412701004127010005000000000000000B011000080C010000000000B70E40023C03150042DFDC030000000058DFDC0300000000FFFF0000FFFFFFFFFFFFFFFFB50E40020000000000000000030D000000000000DB000003DA000003FA122501020000002C00000000000802010000000012470105000600C20D3346C2065345011C0000010006000600000000100000000200000B0501000C000100DB0000039AE2DC03000000000102412703000000000000000C0046001D00040002000200110D00000000000003001000F70E0000B70E40023C031800000000000000000000000000C0BFC57EFFFF000002010D00EAFCDC0300800000010600809AE2DC030000000000000546DB000003DA000003FA120501020000002C02000000000802FFFF0000000206C505000600C2105452C20904540514000000000000000000000000000000000000000600000300000000000000120008000000060004000400000000000300F37E00006A62E8000000000001000000802C00000013FFFFFFFF53595331");
		OraCdcRedoRecord rrUpdate1 = new OraCdcRedoRecord(orl, 0x0000000003dcfceal, "0x000363.000042db.0010", baUpdate1);
		assertTrue(rrUpdate1.has5_1());
		assertTrue(rrUpdate1.change5_1().supplementalLogData());
		assertTrue(rrUpdate1.change5_1().supplementalFb() != 0);
		assertTrue(rrUpdate1.change5_1().fb() != 0);
		assertFalse(rrUpdate1.has10_x());
		assertTrue(rrUpdate1.has11_x());
		assertTrue(rrUpdate1.change11_x().fb() != 0);
		assertEquals(rrUpdate1.change11_x().operation, _11_5_URP);

		OraCdcTransaction transaction = new OraCdcTransactionArrayList("0x0003.010.00000ef7", 0x0000000003dcfceal, 0x10, orl.cdb());
		OraCdcRedoMinerStatement stmt = new OraCdcRedoMinerStatement();
		try {
			transaction.processRowChange(rrUpdate1, false, System.currentTimeMillis());
			transaction.setCommitScn(0x0000000003dcfcebl);

			assertTrue(transaction.completed());

			assertTrue(transaction.getStatement(stmt));
			assertEquals(stmt.getOperation(), UPDATE);
			assertEquals(stmt.getRba(), rrUpdate1.rba());
			assertEquals(stmt.getRowId().toString(), "AAASdBAAMAAAADbAAA");
			System.out.println(stmt);

		} catch(IOException ioe) {}
	}

}
