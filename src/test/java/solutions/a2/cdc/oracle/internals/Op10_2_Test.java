/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle.internals;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._10_2_LIN;
import static solutions.a2.cdc.oracle.OraCdcTransaction.LobProcessingStatus.REDOMINER;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
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
public class Op10_2_Test extends TestWithOutput {

	@Test
	public void testPlainIotArrayInsert() {

		var orl = OraCdcRedoLog.getLinux19c();

		var baInsert1 = hexToRaw("80020000050000003095390001000000e6716d87000000000000010002000000020000000a0000002f95390000000000000000000000000033953900000000009132814a050219000e00ffffc000000019953900000000000100ffff0300000000000000060020000400000002000000f6030000b50d0000340112005200cc0000ff00000000000000000000e6716d8705011a000e00ffffb50d00000f953900000000000200ffff0300000000000000100014004c0020001c0012001c000100cc0010171200000005000200f603000034011200862401008624010006000000000000000a160200080c010000000000b50d000034011000c08e3900000000007d8f390000000000ffff0000ffffffffffffffff9a0d00000000000000000000040d00000000000004001600ee020000af0c0000ca001d00008000802d9539000302a10012040000130400000000000000000000020000000900090002c10305544553543202c1040554455354330000020c0000010001000100000030000000000a00000000000000000000ff0000000a0201000f000100130400002d953900000000000102862403000000000000000a004600100012000400878d110d00000000000005000200f6030000b50d000034011200000000000000000000000000000000000000000002010d003095390000800000020600802d9539000000000000000000022001001a000000020000000100020002c10305544553543202c104055445535433000009000900051400000000000000000000000000000000000000064f470300000000000000120008000000060004000800000000000300414e00004484dd000000000001000000000000000017ffffffff0000000053595300");
		var rrInsert1 = new OraCdcRedoRecord(orl, 0x0000000000399530l, "0x00000d.0002a117.0010", baInsert1);

		assertTrue(rrInsert1.has5_1());
		assertTrue(rrInsert1.change5_1().supplementalLogData());
		assertTrue(rrInsert1.change5_1().supplementalFb() != 0);
		assertTrue(rrInsert1.change5_1().fb() == 0);
		assertTrue(rrInsert1.has10_x());
		assertFalse(rrInsert1.has11_x());
		assertTrue(rrInsert1.change10_x().fb() == 0);
		assertFalse(rrInsert1.change10_x().compressed());
		assertEquals(rrInsert1.change10_x().operation, _10_2_LIN);

		var raw = new OraCdcRawTransaction(new Xid((short)0x0005, (short)0x002, 0x000003f6), ZoneId.systemDefault(), 0x10, new OraCdcLobExtras());
		var stmt1 = new OraCdcRedoMinerStatement();
		var stmt2 = new OraCdcRedoMinerStatement();
		try {
			raw.add(rrInsert1, oraRedoNow());
			raw.commitScn(0x0000000000399531l);
			var transaction = new OraCdcTransactionArrayList(raw, orl.cdb(), REDOMINER, Path.of(System.getProperty("java.io.tmpdir")));
			assertTrue(transaction.completed());
			assertTrue(transaction.getStatement(stmt1));
			assertTrue(transaction.getStatement(stmt2));
			assertEquals(INSERT, stmt1.getOperation());
			assertEquals(INSERT, stmt2.getOperation());
			assertEquals(rrInsert1.rba(), stmt1.getRba());
			assertEquals(rrInsert1.rba(), stmt2.getRba());

		} catch(IOException | SQLException e) {}

	}
}
