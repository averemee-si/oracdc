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

package solutions.a2.cdc.oracle;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcRollbackData {

	/*
	 * Test data contains following sequence of operations for ROWID's 
	 *   AAAqvfABcAAEXtqAAN & AAAqvfABcAAEXtqAAO with partial rollback
	 *   due to ROLLBACK TO SAVEPOINT... statement :
	 * OBJECT_ID  RBA                     ROWID               OPERATION  ROLLBACK
	 * 175071     0x0031f7.008fdb97.0010  AAAqvfABcAAEXtqAAN  INSERT     0
	 * 175071     0x0031f7.008fdc24.010c  AAAqvfABcAAEXtqAAO  INSERT     0
	 * 175071     0x0031f7.008fdc32.0048  AAAqvfABcAAEXtqAAO  UPDATE     0
	 * 175071     0x0031f7.008fdc43.0010  AAAqvfABcAAEXtqAAO  UPDATE     0
	 * 175071     0x0031f7.008fdc46.01b0  AAAqvfABcAAEXtqAAO  UPDATE     1
	 * 175071     0x0031f7.008fdc54.015c  AAAqvfABcAAEXtqAAO  UPDATE     1
	 * 175071     0x0031f7.008fdc5f.0198  AAAqvfABcAAEXtqAAO  DELETE     1
	 * 175071     0x0031f7.008fdc9b.0020  AAAqvfABcAAEXtqAAN  DELETE     1
	 * 175071     0x0031f7.008fe16a.0188  AAAqvfABcAAEXtqAAN  INSERT     0
	 * 175071     0x0031f7.008fe23d.0158  AAAqvfABcAAEXtqAAO  INSERT     0
	 * 
	 * Only data from RBA's 0x0031f7.008fe16a.0188/0x0031f7.008fe23d.0158 
	 * must be returned!
	 */
	OraCdcTransaction get(final boolean arrayList) throws IOException {
		final OraCdcTransaction transaction;
		final String xid = "31001100981F2000";
		final OraCdcLogMinerStatement firstStmt  = new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926012000L, 6084035199279L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fcf21.0010 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false); 
		if (arrayList) {
			transaction = new OraCdcTransactionArrayList(xid, firstStmt.getScn(), 0x20, false);
			transaction.addStatement(firstStmt);
		} else {
			final String tmpDir = System.getProperty("java.io.tmpdir");
			final Path queuesRoot = FileSystems.getDefault().getPath(tmpDir);
			transaction = new OraCdcTransactionChronicleQueue(queuesRoot, xid, firstStmt, false);
		}
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926012000L, 6084035199279L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fcf22.0088 "), 92,
				new RowId("AAAtoxABcAACcDrABc"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200317L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb48.0010 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200317L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb4a.0064 "), 93,
				new RowId("AAAtoxABcAACcDrABd"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200317L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb4a.01c8 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200317L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb4d.01d0 "), 94,
				new RowId("AAAtoxABcAACcDrABe"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200326L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb66.0010 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200326L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb66.015c "), 95,
				new RowId("AAAtoxABcAACcDrABf"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200326L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb67.00d0 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200326L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb68.0010 "), 96,
				new RowId("AAAtoxABcAACcDrABg"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175047, (short)1,
				"insert into \"WMS\".\"WMS_EXCEPTIONS\"(".getBytes(StandardCharsets.US_ASCII), 1726926015000L, 6084035200326L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb68.0174 "), 29,
				new RowId("AAAqvHABaAAImEnAAd"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200326L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb6c.00e0 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200326L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb6d.0010 "), 97,
				new RowId("AAAtoxABcAACcDrABh"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175043, (short)1,
				"insert into \"WMS\".\"WMS_DISPATCHED_TASKS_HISTORY\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200326L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb6d.0174 "), 9,
				new RowId("AAAqvDABcAAEsKnAAJ"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175038, (short)2,
				"delete from \"WMS\".\"WMS_DISPATCHED_TASKS\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200332L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb7f.0010 "), 0,
				new RowId("AAAqu+AArAAEc47AAj"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200332L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb83.0010 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200332L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb84.016c "), 98,
				new RowId("AAAtoxABcAACcDrABi"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200332L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb85.00e0 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200332L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb86.0044 "), 99,
				new RowId("AAAtoxABcAACcDrABj"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173314, (short)3,
				"update \"INV\".\"MTL_TRANSACTION_LOTS_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200332L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb86.01a8 "), 0,
				new RowId("AAAqUCAAeAANOaiAAe"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200332L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb87.017c "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200332L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb88.00dc "), 100,
				new RowId("AAAtoxABcAACcDrABk"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_DETAILS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200341L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdb97.0010 "), 13,
				new RowId("AAAqvfABcAAEXtqAAN"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200341L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdba3.0038 "), 55,
				new RowId("AAAqvmABXAAItR4AA3"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200341L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdba5.00f4 "), 114,
				new RowId("AABmHHABaAAICzGABy"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200341L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdba6.01c0 "), 0,
				new RowId("AAArDwABeAAAMG9AAD"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200341L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdbaa.0108 "), 0,
				new RowId("AAArDwABeAAAMG9AAD"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(178735, (short)2,
				"delete from \"INV\".\"MTL_ONHAND_QUANTITIES_DETAIL\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200347L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdbb1.0148 "), 0,
				new RowId("AAArovAAlAAHhjAAAv"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186928, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_ONHAND_QUANTITIE\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200347L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdbb6.003c "), 62,
				new RowId("AAAtowAAeAAHk1qAA+"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173337, (short)1,
				"insert into \"INV\".\"MTL_TRANSACTION_LOT_NUMBERS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200347L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdbc4.0140 "), 28,
				new RowId("AAAqUZABfAAAC5sAAc"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)1,
				"insert into \"INV\".\"MTL_MATERIAL_transaction\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200347L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdbc9.010c "), 14,
				new RowId("AAAqUAABfAAAS8sAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(178735, (short)1,
				"insert into \"INV\".\"MTL_ONHAND_QUANTITIES_DETAIL\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200348L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdbf2.0010 "), 4,
				new RowId("AAArovAAkAAHIu8AAE"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186928, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_ONHAND_QUANTITIE\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200348L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdbf6.00c4 "), 63,
				new RowId("AAAtowAAeAAHk1qAA/"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173337, (short)1,
				"insert into \"INV\".\"MTL_TRANSACTION_LOT_NUMBERS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200348L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdbf7.001c "), 29,
				new RowId("AAAqUZABfAAAC5sAAd"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)1,
				"insert into \"INV\".\"MTL_MATERIAL_transaction\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200348L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdbfb.00d8 "), 15,
				new RowId("AAAqUAABfAAAS8sAAP"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173347, (short)3,
				"update \"INV\".\"MTL_TXN_REQUEST_LINES\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200349L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc0c.0030 "), 0,
				new RowId("AAAqUjABaAAH8deAAV"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(189147, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_TXN_REQUEST_LINE\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200349L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc0e.0110 "), 101,
				new RowId("AAAuLbAAjAAFPuwABl"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200354L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc12.0068 "), 0,
				new RowId("AAAqUzAAmAAHc+9AAN"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_RESERVATIONS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200354L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc13.0048 "), 31,
				new RowId("AAAto2AAfAAAAtjAAf"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173294, (short)3,
				"update \"INV\".\"MTL_DEMAND\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200354L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc14.00f8 "), 0,
				new RowId("AAAqTuAAkAAHNdNAAB"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186905, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_DEMAND\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200354L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc17.0044 "), 91,
				new RowId("AAAtoZAAnAAHmXdABb"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200357L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc18.00a8 "), 0,
				new RowId("AAAqUzAAnAAHmWKAAJ"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_RESERVATIONS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200357L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc19.0090 "), 32,
				new RowId("AAAto2AAfAAAAtjAAg"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173294, (short)3,
				"update \"INV\".\"MTL_DEMAND\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200357L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc1a.0128 "), 0,
				new RowId("AAAqTuAApAAHoz2AAK"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186905, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_DEMAND\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200357L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc1d.0060 "), 92,
				new RowId("AAAtoZAAnAAHmXdABc"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200358L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc1e.0010 "), 0,
				new RowId("AAAqUzAAmAAHc+9AAN"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_RESERVATIONS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200358L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc1e.012c "), 33,
				new RowId("AAAto2AAfAAAAtjAAh"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200359L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc20.015c "), 0,
				new RowId("AAAqvfABcAAEW4/AAF"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_DETAILS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200359L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc24.010c "), 14,
				new RowId("AAAqvfABcAAEXtqAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200359L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc32.0048 "), 0,
				new RowId("AAAqvfABcAAEXtqAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200359L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc36.0030 "), 56,
				new RowId("AAAqvmABXAAItR4AA4"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200359L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc39.0028 "), 115,
				new RowId("AABmHHABaAAICzGABz"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200359L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc39.0188 "), 0,
				new RowId("AAAqvfABcAAEW4/AAF"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200359L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc3e.01c0 "), 0,
				new RowId("AAAqvfABcAAEW4/AAF"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc43.0010 "), 0,
				new RowId("AAAqvfABcAAEXtqAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc46.01b0 "), 0,
				new RowId("AAAqvfABcAAEXtqAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc4a.011c "), 0,
				new RowId("AAAqvfABcAAEW4/AAF"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc4e.01c0 "), 0,
				new RowId("AAAqvfABcAAEW4/AAF"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc52.0104 "), 0,
				new RowId("AABmHHABaAAICzGABz"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc54.0098 "), 0,
				new RowId("AAAqvmABXAAItR4AA4"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc54.015c "), 0,
				new RowId("AAAqvfABcAAEXtqAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_DETAILS\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc5f.0198 "), 0,
				new RowId("AAAqvfABcAAEXtqAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc60.006c "), 0,
				new RowId("AAAqvfABcAAEW4/AAF"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_RESERVATIONS\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc64.0174 "), 0,
				new RowId("AAAto2AAfAAAAtjAAh"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc65.0048 "), 0,
				new RowId("AAAqUzAAmAAHc+9AAN"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186905, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_DEMAND\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc65.0120 "), 0,
				new RowId("AAAtoZAAnAAHmXdABc"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173294, (short)3,
				"update \"INV\".\"MTL_DEMAND\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc65.01e4 "), 0,
				new RowId("AAAqTuAApAAHoz2AAK"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_RESERVATIONS\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc67.01c0 "), 0,
				new RowId("AAAto2AAfAAAAtjAAg"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc68.0094 "), 0,
				new RowId("AAAqUzAAnAAHmWKAAJ"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186905, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_DEMAND\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc68.01cc "), 0,
				new RowId("AAAtoZAAnAAHmXdABb"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173294, (short)3,
				"update \"INV\".\"MTL_DEMAND\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc69.00b0 "), 0,
				new RowId("AAAqTuAAkAAHNdNAAB"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_RESERVATIONS\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc6b.0074 "), 0,
				new RowId("AAAto2AAfAAAAtjAAf"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc6b.0148 "), 0,
				new RowId("AAAqUzAAmAAHc+9AAN"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(189147, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_TXN_REQUEST_LINE\" where".getBytes(StandardCharsets.US_ASCII), 1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc6e.005c "), 0,
				new RowId("AAAuLbAAjAAFPuwABl"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173347, (short)3,
				"update \"INV\".\"MTL_TXN_REQUEST_LINES\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc6e.0130 "), 0,
				new RowId("AAAqUjABaAAH8deAAV"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)2,
				"delete from \"INV\".\"MTL_MATERIAL_transaction\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc79.00e0 "), 0,
				new RowId("AAAqUAABfAAAS8sAAP"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173337, (short)2,
				"delete from \"INV\".\"MTL_TRANSACTION_LOT_NUMBERS\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc7b.019c "), 0,
				new RowId("AAAqUZABfAAAC5sAAd"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186928, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_ONHAND_QUANTITIE\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc7c.0070 "), 0,
				new RowId("AAAtowAAeAAHk1qAA/"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(178735, (short)2,
				"delete from \"INV\".\"MTL_ONHAND_QUANTITIES_DETAIL\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc7e.016c "), 0,
				new RowId("AAArovAAkAAHIu8AAE"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)2,
				"delete from \"INV\".\"MTL_MATERIAL_transaction\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc88.01a4 "), 0,
				new RowId("AAAqUAABfAAAS8sAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173337, (short)2,
				"delete from \"INV\".\"MTL_TRANSACTION_LOT_NUMBERS\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc8b.0114 "), 0,
				new RowId("AAAqUZABfAAAC5sAAc"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186928, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_ONHAND_QUANTITIE\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc8c.0044 "), 0,
				new RowId("AAAtowAAeAAHk1qAA+"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(178735, (short)1,
				"insert into \"INV\".\"MTL_ONHAND_QUANTITIES_DETAIL\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc8e.0178 "), 47,
				new RowId("AAArovAAlAAHhjAAAv"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc90.0024 "), 0,
				new RowId("AAArDwABeAAAMG9AAD"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc92.003c "), 0,
				new RowId("AAArDwABeAAAMG9AAD"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc93.00ac "), 0,
				new RowId("AABmHHABaAAICzGABy"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc94.01ac "), 0,
				new RowId("AAAqvmABXAAItR4AA3"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_DETAILS\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200361L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc9b.0020 "), 0,
				new RowId("AAAqvfABcAAEXtqAAN"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc9d.01b4 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc9e.015c "), 101,
				new RowId("AAAtoxABcAACcDrABl"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdc9f.00d0 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca0.00f0 "), 102,
				new RowId("AAAtoxABcAACcDrABm"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(192340, (short)2,
				"/* No".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca1.0064 "), 0,
				new RowId("ABWIOAAAFAAGIOBAAB"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca1.01c4 "), 0,
				new RowId("AAAtoxABcAACcDrABm"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca2.0098 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca2.01a0 "), 0,
				new RowId("AAAtoxABcAACcDrABl"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca3.0074 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca3.0158 "), 0,
				new RowId("AAAtoxABcAACcDrABk"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca4.002c "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173314, (short)3,
				"update \"INV\".\"MTL_TRANSACTION_LOTS_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca4.0120 "), 0,
				new RowId("AAAqUCAAeAANOaiAAe"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca5.0044 "), 0,
				new RowId("AAAtoxABcAACcDrABj"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca5.0108 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca6.0010 "), 0,
				new RowId("AAAtoxABcAACcDrABi"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca7.004c "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175038, (short)1,
				"insert into \"WMS\".\"WMS_DISPATCHED_TASKS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdca9.001c "), 35,
				new RowId("AAAqu+AArAAEc47AAj"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175043, (short)2,
				"delete from \"WMS\".\"WMS_DISPATCHED_TASKS_HISTORY\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdcaf.0104 "), 0,
				new RowId("AAAqvDABcAAEsKnAAJ"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdcaf.01c4 "), 0,
				new RowId("AAAtoxABcAACcDrABh"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdcb0.0098 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175047, (short)2,
				"delete from \"WMS\".\"WMS_EXCEPTIONS\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdcb2.0114 "), 0,
				new RowId("AAAqvHABaAAImEnAAd"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdcb2.01e8 "), 0,
				new RowId("AAAtoxABcAACcDrABg"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdcb3.00bc "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdcb3.0198 "), 0,
				new RowId("AAAtoxABcAACcDrABf"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdcb4.006c "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdcb4.0144 "), 0,
				new RowId("AAAtoxABcAACcDrABe"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdcb6.00e8 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdcb7.0010 "), 0,
				new RowId("AAAtoxABcAACcDrABd"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926015000L, 6084035200364L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fdcb8.004c "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200801L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe118.0010 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200801L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe119.01e8 "), 93,
				new RowId("AAAtoxABcAACcDrABd"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200801L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe11a.015c "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200801L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe11d.0104 "), 94,
				new RowId("AAAtoxABcAACcDrABe"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200811L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe134.0010 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200811L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe134.015c "), 95,
				new RowId("AAAtoxABcAACcDrABf"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200811L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe135.00d0 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200811L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe136.0010 "), 96,
				new RowId("AAAtoxABcAACcDrABg"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175047, (short)1,
				"insert into \"WMS\".\"WMS_EXCEPTIONS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200812L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe136.01c0 "), 29,
				new RowId("AAAqvHABaAAImEnAAd"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200813L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe13a.01bc "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200813L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe13b.00ec "), 97,
				new RowId("AAAtoxABcAACcDrABh"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175043, (short)1,
				"insert into \"WMS\".\"WMS_DISPATCHED_TASKS_HISTORY\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200813L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe146.0170 "), 9,
				new RowId("AAAqvDABcAAEsKnAAJ"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175038, (short)2,
				"delete from \"WMS\".\"WMS_DISPATCHED_TASKS\" where".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200819L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe158.0010 "), 0,
				new RowId("AAAqu+AArAAEc47AAj"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200819L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe15b.00b4 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200819L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe15d.0020 "), 98,
				new RowId("AAAtoxABcAACcDrABi"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200821L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe15e.0038 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200821L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe15e.018c "), 99,
				new RowId("AAAtoxABcAACcDrABj"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173314, (short)3,
				"update \"INV\".\"MTL_TRANSACTION_LOTS_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200821L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe15f.0100 "), 0,
				new RowId("AAAqUCAAeAANOaiAAe"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200821L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe160.009c "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200821L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe161.0010 "), 100,
				new RowId("AAAtoxABcAACcDrABk"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_DETAILS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200821L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe16a.0188 "), 13,
				new RowId("AAAqvfABcAAEXtqAAN"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200821L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe175.0050 "), 55,
				new RowId("AAAqvmABXAAItR4AA3"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200821L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe177.011c "), 114,
				new RowId("AABmHHABaAAICzGABy"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200821L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe178.01b0 "), 0,
				new RowId("AAArDwABeAAAMG9AAD"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200821L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe17c.0098 "), 0,
				new RowId("AAArDwABeAAAMG9AAD"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(178735, (short)2,
				"delete from \"INV\".\"MTL_ONHAND_QUANTITIES_DETAIL\" where".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200827L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe183.0148 "), 0,
				new RowId("AAArovAAlAAHhjAAAv"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186928, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_ONHAND_QUANTITIE\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200827L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe187.01c0 "), 62,
				new RowId("AAAtowAAeAAHk1qAA+"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173337, (short)1,
				"insert into \"INV\".\"MTL_TRANSACTION_LOT_NUMBERS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200827L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe1ba.0174 "), 28,
				new RowId("AAAqUZABfAAAC5sAAc"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)1,
				"insert into \"INV\".\"MTL_MATERIAL_transaction\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200827L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe1ec.0010 "), 14,
				new RowId("AAAqUAABfAAAS8sAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(178735, (short)1,
				"insert into \"INV\".\"MTL_ONHAND_QUANTITIES_DETAIL\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200831L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe208.0010 "), 4,
				new RowId("AAArovAAkAAHIu8AAE"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186928, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_ONHAND_QUANTITIE\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200831L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe20c.00f0 "), 63,
				new RowId("AAAtowAAeAAHk1qAA/"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173337, (short)1,
				"insert into \"INV\".\"MTL_TRANSACTION_LOT_NUMBERS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200834L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe210.0010 "), 29,
				new RowId("AAAqUZABfAAAC5sAAd"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)1,
				"insert into \"INV\".\"MTL_MATERIAL_transaction\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200834L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe214.00f8 "), 15,
				new RowId("AAAqUAABfAAAS8sAAP"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173347, (short)3,
				"update \"INV\".\"MTL_TXN_REQUEST_LINES\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe227.0148 "), 0,
				new RowId("AAAqUjABaAAH8deAAV"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(189147, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_TXN_REQUEST_LINE\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe22a.0038 "), 101,
				new RowId("AAAuLbAAjAAFPuwABl"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe22c.00fc "), 0,
				new RowId("AAAqUzAAmAAHc+9AAN"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_RESERVATIONS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe22d.00dc "), 31,
				new RowId("AAAto2AAfAAAAtjAAf"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173294, (short)3,
				"update \"INV\".\"MTL_DEMAND\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe22e.015c "), 0,
				new RowId("AAAqTuAAkAAHNdNAAB"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186905, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_DEMAND\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe231.0064 "), 91,
				new RowId("AAAtoZAAnAAHmXdABb"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe231.01c8 "), 0,
				new RowId("AAAqUzAAnAAHmWKAAJ"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_RESERVATIONS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe232.01b0 "), 32,
				new RowId("AAAto2AAfAAAAtjAAg"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173294, (short)3,
				"update \"INV\".\"MTL_DEMAND\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe234.0028 "), 0,
				new RowId("AAAqTuAApAAHoz2AAK"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186905, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_DEMAND\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe236.0150 "), 92,
				new RowId("AAAtoZAAnAAHmXdABc"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe237.00ac "), 0,
				new RowId("AAAqUzAAmAAHc+9AAN"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_RESERVATIONS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe237.01c8 "), 33,
				new RowId("AAAto2AAfAAAAtjAAh"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe239.01ac "), 0,
				new RowId("AAAqvfABcAAEW4/AAF"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_DETAILS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe23d.0158 "), 14,
				new RowId("AAAqvfABcAAEXtqAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe24b.00c4 "), 0,
				new RowId("AAAqvfABcAAEXtqAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe24f.00ac "), 56,
				new RowId("AAAqvmABXAAItR4AA4"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe252.004c "), 115,
				new RowId("AABmHHABaAAICzGABz"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe252.01ac "), 0,
				new RowId("AAAqvfABcAAEW4/AAF"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe257.01e8 "), 0,
				new RowId("AAAqvfABcAAEW4/AAF"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175081, (short)3,
				"update \"WSH\".\"WSH_NEW_DELIVERIES\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe25c.00d4 "), 0,
				new RowId("AAAqvpAA3AALk1RAAG"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe261.0194 "), 0,
				new RowId("AAAqvIABaAAItaLAAX"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186927, (short)1,
				"insert into \"WSH\".\"MLOG$_WSH_TRIP_STOPS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe263.0148 "), 26,
				new RowId("AAAtovAApAAHwFiAAa"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe264.00bc "), 0,
				new RowId("AAAqvmABXAAItR4AA4"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe265.01a4 "), 116,
				new RowId("AABmHHABaAAICzGAB0"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200837L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe266.011c "), 0,
				new RowId("AAAqvmABXAAItR4AA3"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe269.003c "), 117,
				new RowId("AABmHHABaAAICzGAB1"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe269.01a4 "), 0,
				new RowId("AAAqvmABXAAItR4AA4"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175081, (short)3,
				"update \"WSH\".\"WSH_NEW_DELIVERIES\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe26a.0188 "), 0,
				new RowId("AAAqvpAA3AALk1RAAG"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175081, (short)3,
				"update \"WSH\".\"WSH_NEW_DELIVERIES\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe26f.00d0 "), 0,
				new RowId("AAAqvpAA3AALk1RAAG"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe272.019c "), 0,
				new RowId("AAAqvIABaAAItaLAAX"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186927, (short)1,
				"insert into \"WSH\".\"MLOG$_WSH_TRIP_STOPS\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe274.014c "), 27,
				new RowId("AAAtovAApAAHwFiAAb"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe275.00a8 "), 0,
				new RowId("AAAqvfABcAAEXtqAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe279.009c "), 0,
				new RowId("AAAqvfABcAAEXtqAAN"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe27f.0180 "), 0,
				new RowId("AAAqvfABcAAEXtqAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe289.014c "), 0,
				new RowId("AAAqvfABcAAEXtqAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173314, (short)2,
				"delete from \"INV\".\"MTL_TRANSACTION_LOTS_TEMP\" where".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe28d.01a4 "), 0,
				new RowId("AAAqUCAAeAANOaiAAe"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)2,
				"delete from \"INV\".\"MTL_MATERIAL_transaction_TEMP\" where".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe28f.0144 "), 0,
				new RowId("AAAqT4AAmAAHL6bAAO"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe299.00a8 "), 101,
				new RowId("AAAtoxABcAACcDrABl"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(192340, (short)2,
				"/* No SQL_REDO for temporary tables */".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe29a.001c "), 0,
				new RowId("ABWIOAAAFAAGIOBAAA"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe29a.0118 "), 0,
				new RowId("AAArDwABeAAAMG9AAD"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)1,
				"/* No SQL_REDO for temporary tables */".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe29b.01b0 "), 18,
				new RowId("ABVkkAAAFAAFkkBAAS"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)3,
				"/* No SQL_REDO for temporary tables */".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe29c.0124 "), 0,
				new RowId("ABVkkAAAFAAFkkBAAS"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)3,
				"/* No SQL_REDO for temporary tables */".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe29c.01e0 "), 0,
				new RowId("ABVkkAAAFAAFkkBAAS"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)3,
				"/* No SQL_REDO for temporary tables */".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe29d.00b0 "), 0,
				new RowId("ABVkkAAAFAAFkkBAAS"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)3,
				"/* No SQL_REDO for temporary tables */".getBytes(StandardCharsets.US_ASCII),
				1726926017000L, 6084035200838L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe29d.017c "), 0,
				new RowId("ABVkkAAAFAAFkkBAAS"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)1,
				"/* No SQL_REDO for temporary tables */".getBytes(StandardCharsets.US_ASCII),
				1726926018000L, 6084035200867L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe2df.0010 "), 19,
				new RowId("ABVkkAAAFAAFkkBAAT"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)3,
				"/* No SQL_REDO for temporary tables */".getBytes(StandardCharsets.US_ASCII),
				1726926018000L, 6084035200867L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0031f7.008fe2df.0180 "), 0,
				new RowId("ABVkkAAAFAAFkkBAAT"), false));
		transaction.setCommitScn(6084035200867L);
		return transaction;
	}


	/*
	 * Test data contains following sequence of operations
SCN     TIMESTAMP       RBA     SSN     OBJECT_ID       ROWID   OPERATION_CODE  ROLLBACK
6106846712091   2025-12-01T10:57:08Z[Etc/UTC]   0x0042db.00f6825d.00fc  16      175017  AAAqupABaAAOiRjAAC      3       0
6106846712091   2025-12-01T10:57:08Z[Etc/UTC]   0x0042db.00f68260.0098  18      175017  AAAqupABaAAOiRjAAC      3       0
6106846712093   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f68263.0010  1       175017  AAAqupABaAAOiRjAAC      3       0
6106846712093   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f68265.0180  3       175009  AAAquhABcAAE8aIAAa      1       0
6106846712093   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f68265.0180  3       175009  AAAquhABcAAE8aIAAb      1       0
6106846712094   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f68271.0180  1       175017  AAAqupABaAAOiRjAAC      3       0
6106846712095   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f68275.0010  1       175002  AAAquaABXAAK44YAAg      3       0
6106846712095   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f6827a.0068  7       175002  AAAquaABXAAK44YAAh      3       0
6106846712095   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f6827f.0068  13      175002  AAAquaABXAAK44oAAK      3       0
6106846712095   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f68284.0078  19      175002  AAAquaABXAAK44oAAL      3       0
6106846712095   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f68289.0120  26      175002  AAAquaABXAAK45IAAA      3       0
6106846712095   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f6828e.0164  32      175002  AAAquaABXAAK45IAAK      3       0
6106846712095   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f68293.0178  38      175002  AAAquaABXAAK45IAAN      3       0
6106846712095   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f68298.01a4  44      175002  AAAquaABXAAK454AAJ      3       0
6106846712096   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f6829e.0040  1       194941  AAAvl9ABaAAJzeLAAM      3       0
6106846712098   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f682a5.00bc  1       194941  AAAvl9ABaAAJzeLAAM      3       0
6106846712106   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f682bc.00d0  1       173312  AAAqUAABbAAJXgnAAJ      1       0
6106846712106   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f682c4.01b4  16      173312  AAAqUAABbAAJXgnAAJ      2       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f682e0.0018  38      194941  AAAvl9ABaAAJzeLAAM      3       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f682e2.01a0  41      194941  AAAvl9ABaAAJzeLAAM      3       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f682e6.016c  47      175002  AAAquaABXAAK454AAJ      3       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f682ea.0090  53      175002  AAAquaABXAAK45IAAN      3       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f682ed.0190  59      175002  AAAquaABXAAK45IAAK      3       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f682f1.00a4  65      175002  AAAquaABXAAK45IAAA      3       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f682f4.01ac  71      175002  AAAquaABXAAK44oAAL      3       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f682f8.00c4  77      175002  AAAquaABXAAK44oAAK      3       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f682fb.01e4  83      175002  AAAquaABXAAK44YAAh      3       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f682ff.00f0  89      175002  AAAquaABXAAK44YAAg      3       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f68301.01d8  92      175017  AAAqupABaAAOiRjAAC      3       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f6830a.00d0  109     175009  AAAquhABcAAE8aIAAa      2       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f6830a.00d0  109     175009  AAAquhABcAAE8aIAAb      2       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f6830b.0080  111     175017  AAAqupABaAAOiRjAAC      3       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f6830d.009c  113     175017  AAAqupABaAAOiRjAAC      3       1
6106846712108   2025-12-01T10:57:09Z[Etc/UTC]   0x0042db.00f6830f.00c8  115     175017  AAAqupABaAAOiRjAAC      3       1
6106846715268   2025-12-01T10:57:25Z[Etc/UTC]   0x0042db.00f6a506.01c4  4       173312  AAAqUAABbAAJXgnAAJ      1       0
6106846715293   2025-12-01T10:57:25Z[Etc/UTC]   0x0042db.00f6a525.0184  2       173312  AAAqUAABbAAJXgnAAK      1       0
6106846715296   2025-12-01T10:57:25Z[Etc/UTC]   0x0042db.00f6a543.003c  10      178735  AAArovAAoAAHRPaAAs      3       0
6106846715323   2025-12-01T10:57:25Z[Etc/UTC]   0x0042db.00f6a569.016c  1       175002  AAAquaABXAAK44YAAg      3       0
6106846715328   2025-12-01T10:57:25Z[Etc/UTC]   0x0042db.00f6a56e.0064  1       173312  AAAqUAABbAAJXgnAAL      1       0
6106846715429   2025-12-01T10:57:25Z[Etc/UTC]   0x0042db.00f6a613.019c  1       173312  AAAqUAABbAAJXgnAAM      1       0
6106846715430   2025-12-01T10:57:25Z[Etc/UTC]   0x0042db.00f6a623.007c  26      173312  AAAqUAABbAAJXgnAAN      1       0
6106846715431   2025-12-01T10:57:25Z[Etc/UTC]   0x0042db.00f6a63b.00f4  8       178735  AAArovAAoAAHRPaAAs      3       0
6106846715434   2025-12-01T10:57:25Z[Etc/UTC]   0x0042db.00f6a650.0114  1       175002  AAAquaABXAAK44YAAg      3       0
6106846715434   2025-12-01T10:57:25Z[Etc/UTC]   0x0042db.00f6a654.0068  5       173312  AAAqUAABbAAJXgnAAO      1       0
6106846715663   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a720.00b8  1       173312  AAAqUAABbAAJXgnAAP      1       0
6106846715671   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a738.0094  4       173312  AAAqUAABbAAJXgnAAQ      1       0
6106846715680   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a758.0074  9       178735  AAArovAAlAAHKRDAAJ      3       0
6106846715699   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a766.0010  1       175002  AAAquaABXAAK45IAAK      3       0
6106846715704   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a76e.0090  1       173312  AAAqUAABbAAJXgnAAR      1       0
6106846715900   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a838.0020  6       173312  AAAqUAABbAAJXgnAAS      1       0
6106846715902   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a84f.0114  2       173312  AAAqUAABbAAJXhnAAA      1       0
6106846715902   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a867.01c0  43      178735  AAArovAAlAAHKRDAAJ      3       0
6106846715910   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a876.01b4  3       175002  AAAquaABXAAK45IAAK      3       0
6106846715912   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a87a.01ac  1       173312  AAAqUAABbAAJXhnAAB      1       0
6106846715937   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a89b.01bc  2       178735  AAArovAAnAAHP04AAM      3       0
6106846715959   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a8cc.00b8  1       175002  AAAquaABXAAK454AAJ      3       0
6106846715961   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a8d0.00bc  1       173312  AAAqUAABbAAJXhnAAC      1       0
6106846715992   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a8ef.0120  1       178735  AAArovAAnAAHP04AAM      3       0
6106846715999   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a8f8.0150  1       175002  AAAquaABXAAK454AAJ      3       0
6106846716002   2025-12-01T10:57:26Z[Etc/UTC]   0x0042db.00f6a8fc.0180  1       173312  AAAqUAABbAAJXhnAAD      1       0
	 *  
	 */
	OraCdcTransaction getQmdQmi(final boolean arrayList) throws IOException {
		final OraCdcTransaction transaction;
		final String xid = "11000200E1D23400";
		final OraCdcLogMinerStatement firstStmt  = new OraCdcLogMinerStatement(175017, (short)3,
				"update \"WIP\".\"WIP_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712091l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6825d.00fc "), 16,
				new RowId("AAAqupABaAAOiRjAAC"), false); 
		if (arrayList) {
			transaction = new OraCdcTransactionArrayList(xid, firstStmt.getScn(), 0x20, false);
			transaction.addStatement(firstStmt);
		} else {
			final String tmpDir = System.getProperty("java.io.tmpdir");
			final Path queuesRoot = FileSystems.getDefault().getPath(tmpDir);
			transaction = new OraCdcTransactionChronicleQueue(queuesRoot, xid, firstStmt, false);
		}
		transaction.addStatement(new OraCdcLogMinerStatement(175017, (short)3,
				"update \"WIP\".\"WIP_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712091l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f68260.0098 "), 18,
				new RowId("AAAqupABaAAOiRjAAC"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175017, (short)3,
				"update \"WIP\".\"WIP_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712093l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f68263.0010 "), 1,
				new RowId("AAAqupABaAAOiRjAAC"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175009, (short)1,
				"insert into \"WIP\".\"WIP_MOVE_TRANSACTIONS\"(".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712093l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f68265.0180 "), 3,
				new RowId("AAAquhABcAAE8aIAAa"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175009, (short)1,
				"insert into \"WIP\".\"WIP_MOVE_TRANSACTIONS\"(".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712093l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f68265.0180 "), 3,
				new RowId("AAAquhABcAAE8aIAAb"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175017, (short)3,
				"update \"WIP\".\"WIP_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712094l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f68271.0180 "), 1,
				new RowId("AAAqupABaAAOiRjAAC"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712095l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f68275.0010 "), 1,
				new RowId("AAAquaABXAAK44YAAg"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712095l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6827a.0068 "), 7,
				new RowId("AAAquaABXAAK44YAAh"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712095l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6827f.0068 "), 13,
				new RowId("AAAquaABXAAK44oAAK"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712095l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f68284.0078 "), 19,
				new RowId("AAAquaABXAAK44oAAL"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712095l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f68289.0120 "), 26,
				new RowId("AAAquaABXAAK45IAAA"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712095l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6828e.0164 "), 32,
				new RowId("AAAquaABXAAK45IAAK"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712095l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f68293.0178 "), 38,
				new RowId("AAAquaABXAAK45IAAN"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712095l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f68298.01a4 "), 44,
				new RowId("AAAquaABXAAK454AAJ"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(194941, (short)3,
				"update \"WIP\".\"WIP_DISCRETE_JOBS\" set ".getBytes(StandardCharsets.US_ASCII),
				6106846712096l, 6106846712095l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6829e.0040 "), 1,
				new RowId("AAAvl9ABaAAJzeLAAM"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(194941, (short)3,
				"update \"WIP\".\"WIP_DISCRETE_JOBS\" set ".getBytes(StandardCharsets.US_ASCII),
				6106846712098l, 6106846712095l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f682a5.00bc "), 1,
				new RowId("AAAvl9ABaAAJzeLAAM"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)1,
				"insert into \"INV\".\"MTL_MATERIAL_TRANSACTIONS\"(".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712106l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f682bc.00d0 "), 1,
				new RowId("AAAqUAABbAAJXgnAAJ"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)2,
				"delete from \"INV\".\"MTL_MATERIAL_TRANSACTIONS\" where ROWID = 'AAAqUAABbAAJXgnAAJ'".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712106l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f682c4.01b4 "), 16,
				new RowId("AAAqUAABbAAJXgnAAJ"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(194941, (short)3,
				"update \"WIP\".\"WIP_DISCRETE_JOBS\" set ".getBytes(StandardCharsets.US_ASCII),
				6106846712108l, 6106846712095l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f682e0.0018 "), 38,
				new RowId("AAAvl9ABaAAJzeLAAM"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(194941, (short)3,
				"update \"WIP\".\"WIP_DISCRETE_JOBS\" set ".getBytes(StandardCharsets.US_ASCII),
				6106846712108l, 6106846712095l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f682e2.01a0 "), 41,
				new RowId("AAAvl9ABaAAJzeLAAM"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f682e6.016c "), 47,
				new RowId("AAAquaABXAAK454AAJ"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f682ea.0090 "), 53,
				new RowId("AAAquaABXAAK45IAAN"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f682ed.0190 "), 59,
				new RowId("AAAquaABXAAK45IAAK"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f682f1.00a4 "), 65,
				new RowId("AAAquaABXAAK45IAAA"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f682f4.01ac "), 71,
				new RowId("AAAquaABXAAK44oAAL"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f682f8.00c4 "), 77,
				new RowId("AAAquaABXAAK44oAAK"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f682fb.01e4 "), 83,
				new RowId("AAAquaABXAAK44YAAh"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175002, (short)3,
				"update \"WIP\".\"WIP_REQUIREMENT_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f682ff.00f0 "), 89,
				new RowId("AAAquaABXAAK44YAAg"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175017, (short)3,
				"update \"WIP\".\"WIP_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f68301.01d8 "), 92,
				new RowId("AAAqupABaAAOiRjAAC"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175009, (short)2,
				"delete from \"WIP\".\"WIP_MOVE_TRANSACTIONS\" where ROWID = 'AAAquhABcAAE8aIAAa'".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6830a.00d0 "), 109,
				new RowId("AAAquhABcAAE8aIAAa"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175009, (short)2,
				"delete from \"WIP\".\"WIP_MOVE_TRANSACTIONS\" where ROWID = 'AAAquhABcAAE8aIAAb'".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6830a.00d0 "), 109,
				new RowId("AAAquhABcAAE8aIAAb"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175017, (short)3,
				"update \"WIP\".\"WIP_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6830b.0080 "), 111,
				new RowId("AAAqupABaAAOiRjAAC"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175017, (short)3,
				"update \"WIP\".\"WIP_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6830d.009c "), 113,
				new RowId("AAAqupABaAAOiRjAAC"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175017, (short)3,
				"update \"WIP\".\"WIP_OPERATIONS\" set ".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846712108l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6830f.00c8 "), 115,
				new RowId("AAAqupABaAAOiRjAAC"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)1,
				"insert into \"INV\".\"MTL_MATERIAL_TRANSACTIONS\"(".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846715268l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6a506.01c4 "), 4,
				new RowId("AAAqUAABbAAJXgnAAJ"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)1,
				"insert into \"INV\".\"MTL_MATERIAL_TRANSACTIONS\"(".getBytes(StandardCharsets.US_ASCII),
				1764437342000L, 6106846715293l, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6a525.0184 "), 2,
				new RowId("AAAqUAABbAAJXgnAAK"), false));

		transaction.setCommitScn(6106846718158L);
		return transaction;
	}
}
