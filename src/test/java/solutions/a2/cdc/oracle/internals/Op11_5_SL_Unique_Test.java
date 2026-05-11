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
import static solutions.a2.cdc.oracle.internals.OraCdcChange._11_5_URP;
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
public class Op11_5_SL_Unique_Test extends TestWithOutput {

	@Test
	public void poRequisitionLinesAll() {

		var orl = OraCdcRedoLog.getLinux19c();

		/*
		 * SCN:6110237249339, RBA:0x004786.00dde4c1.0154, OP:5.1 fb:--H-FL--, supp fb:---DFL--, OP:11.5 fb:--H-FL--
		 */
		var baUpdate1 = hexToRaw("2801000001008E053B4F85A62700000007D7CD5100000100050144005D00FFFF768B41173B4F85A68E0500001B00FFFF030000000000000010001400180010001D000C000600140080004A15220000001A0001005F28350015821B00CBAA0200CBAA02000E000000000000000B01011A00005F28020D000000000000728B411715824100B05FA81632F2C40829032501070000002C0700000600B9060C003E000000CAF5B800B700B600B500B400B300435245415445000A011C00001900B900B900000000100000000000000B0501005A000200B05FA816814E85A68E0500000300CBAA03000000000000000A0010001C0002000000E9F4020D000000000000768B411715821B00B05FA81632F2C40829030501070000002C0700000600B300F4FF0100B800D400");
		var rrUpdate1 = new OraCdcRedoRecord(orl, 0x0000058ea6854f3bl, "0x004786.00dde4c1.0154", baUpdate1);
		assertTrue(rrUpdate1.has5_1());
		assertTrue(rrUpdate1.change5_1().supplementalLogData());
		assertTrue(rrUpdate1.change5_1().supplementalFb() != 0);
		assertTrue(rrUpdate1.change5_1().fb() != 0);
		assertFalse(rrUpdate1.has10_x());
		assertTrue(rrUpdate1.has11_x());
		assertEquals(rrUpdate1.change11_x().operation, _11_5_URP);
		assertTrue(rrUpdate1.change11_x().fb() != 0);

		var raw = new OraCdcRawTransaction(new Xid((short)0x1a, (short)0x1, 0x35285f), ZoneId.systemDefault(), 0x10, new OraCdcLobExtras());
		try {
			raw.add(rrUpdate1, oraRedoNow());
			raw.commitScn(0x0000058ea6854f3cl);
			var transaction = new OraCdcTransactionArrayList(raw, orl.cdb(), REDOMINER, Path.of(System.getProperty("java.io.tmpdir")));
			var stmt = new OraCdcRedoMinerStatement();

			assertTrue(transaction.completed());

			assertTrue(transaction.getStatement(stmt));
			assertEquals(stmt.getOperation(), UPDATE);
			assertEquals(stmt.getRba(), rrUpdate1.rba());
			assertEquals(stmt.getRowId().toString(), "AAAqrLABaAAKF+wAAG");
			System.out.println(stmt);
			final String lmUpd1 =
					"update \"UNKNOWN\".\"OBJ# 174795\" set \"COL 180\" = NULL, \"COL 181\" = NULL, \"COL 182\" = NULL, \"COL 183\" = NULL, \"COL 184\" = NULL, \"COL 185\" = NULL where \"COL 180\" IS NULL and \"COL 181\" IS NULL and \"COL 182\" IS NULL and \"COL 183\" IS NULL and \"COL 184\" IS NULL and \"COL 185\" = HEXTORAW('435245415445')";
			/* There is an error in the database supplemental logging settings and therefore we are temporarily ignoring the comparison with LogMiner.
REDO RECORD - Thread:1 RBA: 0x004786.00dde4c1.0154 LEN: 0x0128 VLD: 0x01 CON_UID: 1372444423
SCN: 0x0000058ea6854f3b SUBSCN: 39
CHANGE #1 CON_ID:3 TYP:0 CLS:68 AFN:93 DBA:0x17418b76 OBJ:4294967295 SCN:0x0000058ea6854f3b SEQ:27 OP:5.1 ENC:0 RBL:0 FLG:0x0000
ktudb redo: siz: 128 spc: 5450 flg: 0x0022 seq: 0x8215 rec: 0x1b
            xid:  0x001a.001.0035285f
ktubu redo: slt: 1 wrp: 10335 flg: 0x0000 prev dba: 0x00000000 rci: 26 opc: 11.1 [objn: 174795 objd: 174795 tsn: 14]
[Undo type  ] Regular undo  [User undo done   ]  No [Last buffer split]  No
[Temp object]           No  [Tablespace Undo  ]  No [User only        ]  No
KDO undo record:
KTB Redo
op: 0x02  ver: 0x01
compat bit: 4 (post-11) padding: 1
op: C  uba: 0x17418b72.8215.41
KDO Op code: URP row dependencies Disabled
  xtype: XA flags: 0x00000000  bdba: 0x16a85fb0  hdba: 0x08c4f232
itli: 7  ispac: 0  maxfr: 809
tabn: 0 slot: 6(0x6) flag: 0x2c lock: 7 ckix: 0
ncol: 185 nnew: 6 size: 12
col 184: [ 6]  43 52 45 41 54 45
col 183: *NULL*
col 182: *NULL*
col 181: *NULL*
col 180: *NULL*
col 179: *NULL*
LOGMINER DATA:
 Number of columns supplementally logged: 0
opcode: UPDATE
 segcol# in Undo starting from 185
 segcol# in Redo starting from 185
 pos: 6 fb: ---DFL--
CHANGE #2 CON_ID:3 TYP:0 CLS:1 AFN:90 DBA:0x16a85fb0 OBJ:174795 SCN:0x0000058ea6854e81 SEQ:3 OP:11.5 ENC:0 RBL:0 FLG:0x0000
KTB Redo
op: 0x02  ver: 0x01
compat bit: 4 (post-11) padding: 1
op: C  uba: 0x17418b76.8215.1b
KDO Op code: URP row dependencies Disabled
  xtype: XA flags: 0x00000000  bdba: 0x16a85fb0  hdba: 0x08c4f232
itli: 7  ispac: 0  maxfr: 809
tabn: 0 slot: 6(0x6) flag: 0x2c lock: 7 ckix: 0
ncol: 179 nnew: 1 size: -12
			 */
			assertFalse(compareLogMinerText(lmUpd1, stmt.getSqlRedo()));


		} catch(IOException | SQLException e) {}
	}

}
