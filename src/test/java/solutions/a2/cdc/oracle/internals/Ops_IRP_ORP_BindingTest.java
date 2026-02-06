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
import static solutions.a2.cdc.oracle.OraCdcTransaction.LobProcessingStatus.REDOMINER;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_2_IRP;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_6_ORP;
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
public class Ops_IRP_ORP_BindingTest extends TestWithOutput {

	@Test
	public void advancedCompression() {

		var orl = OraCdcRedoLog.getLinux19c();

		/*
		 * SCN:59233255, RBA:0x00020a.00009a65.0148, fb:--------, supp fb:----F---, OP:11.2 fb:----FL--
		 */
		var baInsert1 = hexToRaw("7401000001000000E7D387037A00001F2984184B00000001050124000B00FFFF1A044002E7D38703000000001300FFFF0300000000000000100014001800200014001C00020001008000CA12220000000A002000471600005A0513008B2701006D33010006000000000000000B01201200004716040D1100D10F00000A00040046160000160440025A051A000080008045CF87039300400392004003FA122301020000009300000001080000020000000100000000100000002200009700400310000100070000007E0000000B0201000D0001009300400348CF87030000000001006D3303000000000000001400180031000200020002000700040007000700010D40025A0510000A002000471600001A0440025A0513009300400392004003FA120201021800000C02070097004003100000000000000000000000047F00002F00930000000000C0454C24C1122206C1121157C11203C2616161616161610362626161787E01140A38070D77A0041401010106");
		var rrInsert1 = new OraCdcRedoRecord(orl, 0x000000000387d3e7l, "0x00020a.00009a65.0148", baInsert1);

		assertTrue(rrInsert1.has5_1());
		assertTrue(rrInsert1.change5_1().supplementalLogData());
		assertTrue(rrInsert1.change5_1().supplementalFb() != 0);
		assertTrue(rrInsert1.change5_1().fb() == 0);
		assertFalse(rrInsert1.has10_x());
		assertTrue(rrInsert1.has11_x());
		assertTrue(rrInsert1.change11_x().fb() != 0);
		assertEquals(rrInsert1.change11_x().operation, _11_2_IRP);

		/*
		 * SCN:59233255, RBA:0x00020a.00009a66.00cc, OP:5.1 fb:--H-FL--, supp fb:-----L--, OP:11.6 fb:--H-----
		 */
		var baOverwrite1 = hexToRaw("B401000001330000E7D387037B0000342984184B00000303050124000B00FFFF1A044002E7D38703000000001400FFFF03000000000000002000140018001000310019001C000E000E00020002000200070004000B00070000014812220000000A002000471600005A05140C8B2701006D33010006000000000000000B01201300004716020D0000000000001A0440025A0512009700400392004003FA122601010000002C01050C047F000000000000047F000006000000047F00001900100001000000C006C5192C010500D3787E01140A38073661B120CAC112CAC112CAC112C6C2000104070002000100000000000018000000020000970040031000010001000200030004000500060007000001020002000200070004000B0007001111C1125D0DC1127434C1127335616161616161611962626161787E01140A38073661B120FF77A00414010101000B0601000D00010097004003E7D387030000000001006D3303000000000000000600100030000000020D40025A050E001A0440025A0514009700400392004003FA120601010000002001000C047F0000000000009300400393000000047F00000900100001000000");
		var rrOverwrite1 = new OraCdcRedoRecord(orl, 0x000000000387d3e7l, "0x00020a.00009a66.00cc", baOverwrite1);

		assertTrue(rrOverwrite1.has5_1());
		assertTrue(rrOverwrite1.change5_1().supplementalLogData());
		assertTrue(rrOverwrite1.change5_1().supplementalFb() != 0);
		assertTrue(rrOverwrite1.change5_1().fb() != 0);
		assertFalse(rrOverwrite1.has10_x());
		assertTrue(rrOverwrite1.has11_x());
		assertTrue(rrOverwrite1.change11_x().fb() != 0);
		assertEquals(rrOverwrite1.change11_x().operation, _11_6_ORP);

		assertEquals(rrInsert1.halfDoneKey(), rrOverwrite1.halfDoneKey());

		var raw = new OraCdcRawTransaction(new Xid((short)0xa, (short)0x20, 0x1647), ZoneId.systemDefault(), 0x10, new OraCdcLobExtras());
		try {
			raw.add(rrInsert1, (int)(System.currentTimeMillis() / 1000));
			raw.add(rrOverwrite1, (int)(System.currentTimeMillis() / 1000));
			raw.commitScn(0x000000000387d3e9l);
			var transaction = new OraCdcTransactionArrayList(raw, orl.cdb(), REDOMINER, Path.of(System.getProperty("java.io.tmpdir")));
			var stmt = new OraCdcRedoMinerStatement();

			assertTrue(transaction.completed());

			assertTrue(transaction.getStatement(stmt));
			assertEquals(stmt.getOperation(), UPDATE);
			assertEquals(stmt.getRba(), rrInsert1.rba());
			assertEquals(stmt.getRowId().toString(), "AAATNtAANAAAACXAAQ");
			System.out.println(stmt);
			final String lmUpd1 =
					"update \"UNKNOWN\".\"OBJ# 75659\" set \"COL 2\" = HEXTORAW('c112'), \"COL 3\" = HEXTORAW('c112'), \"COL 4\" = HEXTORAW('61616161616161'), \"COL 5\" = HEXTORAW('62626161'), \"COL 6\" = HEXTORAW('787e01140a3807'), \"COL 7\" = HEXTORAW('77a00414010101') where \"COL 1\" = HEXTORAW('c112') and \"COL 2\" = HEXTORAW('c112') and \"COL 3\" = HEXTORAW('c112') and \"COL 4\" = HEXTORAW('61616161616161') and \"COL 5\" = HEXTORAW('62626161') and \"COL 6\" = HEXTORAW('787e01140a38073661b120') and \"COL 7\" = HEXTORAW('77a00414010101')";
			//TODO
			//TODO
			//TODO Need to find out why "COL 1" column in the LogMiner output is only present in the WHERE clause:
			//TODO Looks like the supplemental data in OP:11.2 contains a hint to exclude this column from the SET clause?
			//TODO
			//TODO
			assertFalse(compareLogMinerText(lmUpd1, stmt.getSqlRedo()));

		} catch(IOException | SQLException e) {}
	}


}
