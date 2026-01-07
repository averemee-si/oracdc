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
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_30_LNU;
import static solutions.a2.cdc.oracle.OraCdcTransaction.LobProcessingStatus.REDOMINER;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import solutions.a2.cdc.oracle.OraCdcLobExtras;
import solutions.a2.cdc.oracle.OraCdcRawTransaction;
import solutions.a2.cdc.oracle.OraCdcRedoMinerStatement;
import solutions.a2.cdc.oracle.OraCdcTransactionArrayList;
import solutions.a2.oracle.internals.Xid;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class Op10_30_Test extends TestWithOutput {

	@Test
	public void test() {

		var orl = OraCdcRedoLog.getLinux19c();

		// update TEST_IOT_33 set descr1='descr1-03-U',descr2='descr22-03-U',descr3='descr333-03-U',descr4='descr4444-03-U' where id=3 and name1='name1-03' and name2='name22-03';
		var baUpdate = hexToRaw("4C01000001000000608B7800010000009F9948510000FFFF050114000E00FFFF950C0000608B7800000000000500FFFF030000000000000010001400180010001400160003001C00840098182200000002000400060900008B020A00433B0100433B010006000000000000000A16040900000609020D14000E00FFFF950C00008B0209001E0295006209000063090000000000000000000002C104086E616D65312D3033096E616D6532322D303300002C010000010C00000100010004000000001000000002000000000000000000000A1E01000F00010063090000608B7800000000000500433B03000000000000000800100006003900020D0000608B7800950C00008B020A0002100200530000002C01040B6465736372312D30332D550C646573637232322D30332D550D64657363723333332D30332D550E6465736372343434342D30332D55000000");
		var rrUpdate = new OraCdcRedoRecord(orl, 0x788b60, "0x000042.00055ad9.009c", baUpdate);

		assertTrue(rrUpdate.has5_1());
		assertTrue(rrUpdate.has10_x());
		assertFalse(rrUpdate.has11_x());
		assertEquals(rrUpdate.change10_x().operation(), _10_30_LNU);

		var raw = new OraCdcRawTransaction(new Xid((short)2, (short)4, 0x906), ZoneId.systemDefault(), 0x10, new OraCdcLobExtras());
		try {
			raw.add(rrUpdate, (int)(System.currentTimeMillis() / 1000));
			raw.commitScn(0x788b61l);
			var transaction = new OraCdcTransactionArrayList(raw, orl.cdb(), REDOMINER, Path.of(System.getProperty("java.io.tmpdir")));
			var stmt = new OraCdcRedoMinerStatement();
			assertTrue(transaction.getStatement(stmt));
			assertEquals(stmt.getOperation(), UPDATE);
			assertEquals(stmt.getRba(), rrUpdate.rba());
			assertTrue(transaction.completed());

		} catch(IOException | SQLException e) {}

	}
}
