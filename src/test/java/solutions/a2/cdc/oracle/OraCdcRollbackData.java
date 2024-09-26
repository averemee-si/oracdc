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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcRollbackData implements Closeable {

	private final OraCdcTransaction transaction;

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
	public OraCdcRollbackData(final boolean arrayList) throws IOException {
		final String xid = "31001100981F2000";
		final OraCdcLogMinerStatement firstStmt  = new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926012000L, 6084035199279L, 
				" 0x0031f7.008fcf21.0010 ", 0, "AAAqT4AAmAAHL6bAAO", false); 
		if (arrayList) {
			transaction = new OraCdcTransactionArrayList(xid, firstStmt);
		} else {
			final String tmpDir = System.getProperty("java.io.tmpdir");
			final Path queuesRoot = FileSystems.getDefault().getPath(tmpDir);
			transaction = new OraCdcTransactionChronicleQueue(queuesRoot, xid, firstStmt);
		}
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926012000L, 6084035199279L, 
				" 0x0031f7.008fcf22.0088 ", 92, "AAAtoxABcAACcDrABc", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200317L, 
				" 0x0031f7.008fdb48.0010 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926015000L, 6084035200317L, 
				" 0x0031f7.008fdb4a.0064 ", 93, "AAAtoxABcAACcDrABd", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200317L, 
				" 0x0031f7.008fdb4a.01c8 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926015000L, 6084035200317L, 
				" 0x0031f7.008fdb4d.01d0 ", 94, "AAAtoxABcAACcDrABe", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200326L, 
				" 0x0031f7.008fdb66.0010 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926015000L, 6084035200326L, 
				" 0x0031f7.008fdb66.015c ", 95, "AAAtoxABcAACcDrABf", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200326L, 
				" 0x0031f7.008fdb67.00d0 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926015000L, 6084035200326L, 
				" 0x0031f7.008fdb68.0010 ", 96, "AAAtoxABcAACcDrABg", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175047, (short)1,
				"insert into \"WMS\".\"WMS_EXCEPTIONS\"(", 1726926015000L, 6084035200326L, 
				" 0x0031f7.008fdb68.0174 ", 29, "AAAqvHABaAAImEnAAd", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200326L, 
				" 0x0031f7.008fdb6c.00e0 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926015000L, 6084035200326L, 
				" 0x0031f7.008fdb6d.0010 ", 97, "AAAtoxABcAACcDrABh", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175043, (short)1,
				"insert into \"WMS\".\"WMS_DISPATCHED_TASKS_HISTORY\"(", 1726926015000L, 6084035200326L, 
				" 0x0031f7.008fdb6d.0174 ", 9, "AAAqvDABcAAEsKnAAJ", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175038, (short)2,
				"delete from \"WMS\".\"WMS_DISPATCHED_TASKS\" where", 1726926015000L, 6084035200332L, 
				" 0x0031f7.008fdb7f.0010 ", 0, "AAAqu+AArAAEc47AAj", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200332L, 
				" 0x0031f7.008fdb83.0010 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926015000L, 6084035200332L, 
				" 0x0031f7.008fdb84.016c ", 98, "AAAtoxABcAACcDrABi", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200332L, 
				" 0x0031f7.008fdb85.00e0 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926015000L, 6084035200332L, 
				" 0x0031f7.008fdb86.0044 ", 99, "AAAtoxABcAACcDrABj", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173314, (short)3,
				"update \"INV\".\"MTL_TRANSACTION_LOTS_TEMP\" set", 1726926015000L, 6084035200332L, 
				" 0x0031f7.008fdb86.01a8 ", 0, "AAAqUCAAeAANOaiAAe", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200332L, 
				" 0x0031f7.008fdb87.017c ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926015000L, 6084035200332L, 
				" 0x0031f7.008fdb88.00dc ", 100, "AAAtoxABcAACcDrABk", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_DETAILS\"(", 1726926015000L, 6084035200341L, 
				" 0x0031f7.008fdb97.0010 ", 13, "AAAqvfABcAAEXtqAAN", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\"(", 1726926015000L, 6084035200341L, 
				" 0x0031f7.008fdba3.0038 ", 55, "AAAqvmABXAAItR4AA3", false));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\"(", 1726926015000L, 6084035200341L, 
				" 0x0031f7.008fdba5.00f4 ", 114, "AABmHHABaAAICzGABy", false));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set", 1726926015000L, 6084035200341L, 
				" 0x0031f7.008fdba6.01c0 ", 0, "AAArDwABeAAAMG9AAD", false));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set", 1726926015000L, 6084035200341L, 
				" 0x0031f7.008fdbaa.0108 ", 0, "AAArDwABeAAAMG9AAD", false));
		transaction.addStatement(new OraCdcLogMinerStatement(178735, (short)2,
				"delete from \"INV\".\"MTL_ONHAND_QUANTITIES_DETAIL\" where", 1726926015000L, 6084035200347L, 
				" 0x0031f7.008fdbb1.0148 ", 0, "AAArovAAlAAHhjAAAv", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186928, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_ONHAND_QUANTITIE\"(", 1726926015000L, 6084035200347L, 
				" 0x0031f7.008fdbb6.003c ", 62, "AAAtowAAeAAHk1qAA+", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173337, (short)1,
				"insert into \"INV\".\"MTL_TRANSACTION_LOT_NUMBERS\"(", 1726926015000L, 6084035200347L, 
				" 0x0031f7.008fdbc4.0140 ", 28, "AAAqUZABfAAAC5sAAc", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)1,
				"insert into \"INV\".\"MTL_MATERIAL_transaction\"(", 1726926015000L, 6084035200347L, 
				" 0x0031f7.008fdbc9.010c ", 14, "AAAqUAABfAAAS8sAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(178735, (short)1,
				"insert into \"INV\".\"MTL_ONHAND_QUANTITIES_DETAIL\"(", 1726926015000L, 6084035200348L, 
				" 0x0031f7.008fdbf2.0010 ", 4, "AAArovAAkAAHIu8AAE", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186928, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_ONHAND_QUANTITIE\"(", 1726926015000L, 6084035200348L, 
				" 0x0031f7.008fdbf6.00c4 ", 63, "AAAtowAAeAAHk1qAA/", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173337, (short)1,
				"insert into \"INV\".\"MTL_TRANSACTION_LOT_NUMBERS\"(", 1726926015000L, 6084035200348L, 
				" 0x0031f7.008fdbf7.001c ", 29, "AAAqUZABfAAAC5sAAd", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)1,
				"insert into \"INV\".\"MTL_MATERIAL_transaction\"(", 1726926015000L, 6084035200348L, 
				" 0x0031f7.008fdbfb.00d8 ", 15, "AAAqUAABfAAAS8sAAP", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173347, (short)3,
				"update \"INV\".\"MTL_TXN_REQUEST_LINES\" set", 1726926015000L, 6084035200349L, 
				" 0x0031f7.008fdc0c.0030 ", 0, "AAAqUjABaAAH8deAAV", false));
		transaction.addStatement(new OraCdcLogMinerStatement(189147, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_TXN_REQUEST_LINE\"(", 1726926015000L, 6084035200349L, 
				" 0x0031f7.008fdc0e.0110 ", 101, "AAAuLbAAjAAFPuwABl", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set", 1726926015000L, 6084035200354L, 
				" 0x0031f7.008fdc12.0068 ", 0, "AAAqUzAAmAAHc+9AAN", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_RESERVATIONS\"(", 1726926015000L, 6084035200354L, 
				" 0x0031f7.008fdc13.0048 ", 31, "AAAto2AAfAAAAtjAAf", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173294, (short)3,
				"update \"INV\".\"MTL_DEMAND\" set", 1726926015000L, 6084035200354L, 
				" 0x0031f7.008fdc14.00f8 ", 0, "AAAqTuAAkAAHNdNAAB", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186905, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_DEMAND\"(", 1726926015000L, 6084035200354L, 
				" 0x0031f7.008fdc17.0044 ", 91, "AAAtoZAAnAAHmXdABb", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set", 1726926015000L, 6084035200357L, 
				" 0x0031f7.008fdc18.00a8 ", 0, "AAAqUzAAnAAHmWKAAJ", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_RESERVATIONS\"(", 1726926015000L, 6084035200357L, 
				" 0x0031f7.008fdc19.0090 ", 32, "AAAto2AAfAAAAtjAAg", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173294, (short)3,
				"update \"INV\".\"MTL_DEMAND\" set", 1726926015000L, 6084035200357L, 
				" 0x0031f7.008fdc1a.0128 ", 0, "AAAqTuAApAAHoz2AAK", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186905, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_DEMAND\"(", 1726926015000L, 6084035200357L, 
				" 0x0031f7.008fdc1d.0060 ", 92, "AAAtoZAAnAAHmXdABc", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set", 1726926015000L, 6084035200358L, 
				" 0x0031f7.008fdc1e.0010 ", 0, "AAAqUzAAmAAHc+9AAN", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_RESERVATIONS\"(", 1726926015000L, 6084035200358L, 
				" 0x0031f7.008fdc1e.012c ", 33, "AAAto2AAfAAAAtjAAh", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926015000L, 6084035200359L, 
				" 0x0031f7.008fdc20.015c ", 0, "AAAqvfABcAAEW4/AAF", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_DETAILS\"(", 1726926015000L, 6084035200359L, 
				" 0x0031f7.008fdc24.010c ", 14, "AAAqvfABcAAEXtqAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926015000L, 6084035200359L, 
				" 0x0031f7.008fdc32.0048 ", 0, "AAAqvfABcAAEXtqAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\"(", 1726926015000L, 6084035200359L, 
				" 0x0031f7.008fdc36.0030 ", 56, "AAAqvmABXAAItR4AA4", false));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\"(", 1726926015000L, 6084035200359L, 
				" 0x0031f7.008fdc39.0028 ", 115, "AABmHHABaAAICzGABz", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926015000L, 6084035200359L, 
				" 0x0031f7.008fdc39.0188 ", 0, "AAAqvfABcAAEW4/AAF", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926015000L, 6084035200359L, 
				" 0x0031f7.008fdc3e.01c0 ", 0, "AAAqvfABcAAEW4/AAF", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc43.0010 ", 0, "AAAqvfABcAAEXtqAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc46.01b0 ", 0, "AAAqvfABcAAEXtqAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc4a.011c ", 0, "AAAqvfABcAAEW4/AAF", true));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc4e.01c0 ", 0, "AAAqvfABcAAEW4/AAF", true));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc52.0104 ", 0, "AABmHHABaAAICzGABz", true));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc54.0098 ", 0, "AAAqvmABXAAItR4AA4", true));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc54.015c ", 0, "AAAqvfABcAAEXtqAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_DETAILS\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc5f.0198 ", 0, "AAAqvfABcAAEXtqAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc60.006c ", 0, "AAAqvfABcAAEW4/AAF", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_RESERVATIONS\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc64.0174 ", 0, "AAAto2AAfAAAAtjAAh", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc65.0048 ", 0, "AAAqUzAAmAAHc+9AAN", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186905, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_DEMAND\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc65.0120 ", 0, "AAAtoZAAnAAHmXdABc", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173294, (short)3,
				"update \"INV\".\"MTL_DEMAND\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc65.01e4 ", 0, "AAAqTuAApAAHoz2AAK", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_RESERVATIONS\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc67.01c0 ", 0, "AAAto2AAfAAAAtjAAg", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc68.0094 ", 0, "AAAqUzAAnAAHmWKAAJ", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186905, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_DEMAND\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc68.01cc ", 0, "AAAtoZAAnAAHmXdABb", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173294, (short)3,
				"update \"INV\".\"MTL_DEMAND\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc69.00b0 ", 0, "AAAqTuAAkAAHNdNAAB", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_RESERVATIONS\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc6b.0074 ", 0, "AAAto2AAfAAAAtjAAf", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc6b.0148 ", 0, "AAAqUzAAmAAHc+9AAN", true));
		transaction.addStatement(new OraCdcLogMinerStatement(189147, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_TXN_REQUEST_LINE\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc6e.005c ", 0, "AAAuLbAAjAAFPuwABl", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173347, (short)3,
				"update \"INV\".\"MTL_TXN_REQUEST_LINES\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc6e.0130 ", 0, "AAAqUjABaAAH8deAAV", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)2,
				"delete from \"INV\".\"MTL_MATERIAL_transaction\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc79.00e0 ", 0, "AAAqUAABfAAAS8sAAP", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173337, (short)2,
				"delete from \"INV\".\"MTL_TRANSACTION_LOT_NUMBERS\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc7b.019c ", 0, "AAAqUZABfAAAC5sAAd", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186928, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_ONHAND_QUANTITIE\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc7c.0070 ", 0, "AAAtowAAeAAHk1qAA/", true));
		transaction.addStatement(new OraCdcLogMinerStatement(178735, (short)2,
				"delete from \"INV\".\"MTL_ONHAND_QUANTITIES_DETAIL\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc7e.016c ", 0, "AAArovAAkAAHIu8AAE", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)2,
				"delete from \"INV\".\"MTL_MATERIAL_transaction\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc88.01a4 ", 0, "AAAqUAABfAAAS8sAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173337, (short)2,
				"delete from \"INV\".\"MTL_TRANSACTION_LOT_NUMBERS\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc8b.0114 ", 0, "AAAqUZABfAAAC5sAAc", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186928, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_ONHAND_QUANTITIE\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc8c.0044 ", 0, "AAAtowAAeAAHk1qAA+", true));
		transaction.addStatement(new OraCdcLogMinerStatement(178735, (short)1,
				"insert into \"INV\".\"MTL_ONHAND_QUANTITIES_DETAIL\"(", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc8e.0178 ", 47, "AAArovAAlAAHhjAAAv", true));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc90.0024 ", 0, "AAArDwABeAAAMG9AAD", true));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc92.003c ", 0, "AAArDwABeAAAMG9AAD", true));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc93.00ac ", 0, "AABmHHABaAAICzGABy", true));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc94.01ac ", 0, "AAAqvmABXAAItR4AA3", true));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_DETAILS\" where", 1726926015000L, 6084035200361L, 
				" 0x0031f7.008fdc9b.0020 ", 0, "AAAqvfABcAAEXtqAAN", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdc9d.01b4 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdc9e.015c ", 101, "AAAtoxABcAACcDrABl", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdc9f.00d0 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca0.00f0 ", 102, "AAAtoxABcAACcDrABm", false));
		transaction.addStatement(new OraCdcLogMinerStatement(192340, (short)2,
				"/* No", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca1.0064 ", 0, "ABWIOAAAFAAGIOBAAB", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca1.01c4 ", 0, "AAAtoxABcAACcDrABm", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca2.0098 ", 0, "AAAqT4AAmAAHL6bAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca2.01a0 ", 0, "AAAtoxABcAACcDrABl", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca3.0074 ", 0, "AAAqT4AAmAAHL6bAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca3.0158 ", 0, "AAAtoxABcAACcDrABk", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca4.002c ", 0, "AAAqT4AAmAAHL6bAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173314, (short)3,
				"update \"INV\".\"MTL_TRANSACTION_LOTS_TEMP\" set", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca4.0120 ", 0, "AAAqUCAAeAANOaiAAe", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca5.0044 ", 0, "AAAtoxABcAACcDrABj", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca5.0108 ", 0, "AAAqT4AAmAAHL6bAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca6.0010 ", 0, "AAAtoxABcAACcDrABi", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca7.004c ", 0, "AAAqT4AAmAAHL6bAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(175038, (short)1,
				"insert into \"WMS\".\"WMS_DISPATCHED_TASKS\"(", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdca9.001c ", 35, "AAAqu+AArAAEc47AAj", true));
		transaction.addStatement(new OraCdcLogMinerStatement(175043, (short)2,
				"delete from \"WMS\".\"WMS_DISPATCHED_TASKS_HISTORY\" where", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdcaf.0104 ", 0, "AAAqvDABcAAEsKnAAJ", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdcaf.01c4 ", 0, "AAAtoxABcAACcDrABh", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdcb0.0098 ", 0, "AAAqT4AAmAAHL6bAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(175047, (short)2,
				"delete from \"WMS\".\"WMS_EXCEPTIONS\" where", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdcb2.0114 ", 0, "AAAqvHABaAAImEnAAd", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdcb2.01e8 ", 0, "AAAtoxABcAACcDrABg", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdcb3.00bc ", 0, "AAAqT4AAmAAHL6bAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdcb3.0198 ", 0, "AAAtoxABcAACcDrABf", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdcb4.006c ", 0, "AAAqT4AAmAAHL6bAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdcb4.0144 ", 0, "AAAtoxABcAACcDrABe", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdcb6.00e8 ", 0, "AAAqT4AAmAAHL6bAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)2,
				"delete from \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\" where", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdcb7.0010 ", 0, "AAAtoxABcAACcDrABd", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926015000L, 6084035200364L, 
				" 0x0031f7.008fdcb8.004c ", 0, "AAAqT4AAmAAHL6bAAO", true));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926017000L, 6084035200801L, 
				" 0x0031f7.008fe118.0010 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926017000L, 6084035200801L, 
				" 0x0031f7.008fe119.01e8 ", 93, "AAAtoxABcAACcDrABd", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926017000L, 6084035200801L, 
				" 0x0031f7.008fe11a.015c ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926017000L, 6084035200801L, 
				" 0x0031f7.008fe11d.0104 ", 94, "AAAtoxABcAACcDrABe", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926017000L, 6084035200811L, 
				" 0x0031f7.008fe134.0010 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926017000L, 6084035200811L, 
				" 0x0031f7.008fe134.015c ", 95, "AAAtoxABcAACcDrABf", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926017000L, 6084035200811L, 
				" 0x0031f7.008fe135.00d0 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926017000L, 6084035200811L, 
				" 0x0031f7.008fe136.0010 ", 96, "AAAtoxABcAACcDrABg", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175047, (short)1,
				"insert into \"WMS\".\"WMS_EXCEPTIONS\"(", 1726926017000L, 6084035200812L, 
				" 0x0031f7.008fe136.01c0 ", 29, "AAAqvHABaAAImEnAAd", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926017000L, 6084035200813L, 
				" 0x0031f7.008fe13a.01bc ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926017000L, 6084035200813L, 
				" 0x0031f7.008fe13b.00ec ", 97, "AAAtoxABcAACcDrABh", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175043, (short)1,
				"insert into \"WMS\".\"WMS_DISPATCHED_TASKS_HISTORY\"(", 1726926017000L, 6084035200813L, 
				" 0x0031f7.008fe146.0170 ", 9, "AAAqvDABcAAEsKnAAJ", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175038, (short)2,
				"delete from \"WMS\".\"WMS_DISPATCHED_TASKS\" where", 1726926017000L, 6084035200819L, 
				" 0x0031f7.008fe158.0010 ", 0, "AAAqu+AArAAEc47AAj", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926017000L, 6084035200819L, 
				" 0x0031f7.008fe15b.00b4 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926017000L, 6084035200819L, 
				" 0x0031f7.008fe15d.0020 ", 98, "AAAtoxABcAACcDrABi", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926017000L, 6084035200821L, 
				" 0x0031f7.008fe15e.0038 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926017000L, 6084035200821L, 
				" 0x0031f7.008fe15e.018c ", 99, "AAAtoxABcAACcDrABj", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173314, (short)3,
				"update \"INV\".\"MTL_TRANSACTION_LOTS_TEMP\" set", 1726926017000L, 6084035200821L, 
				" 0x0031f7.008fe15f.0100 ", 0, "AAAqUCAAeAANOaiAAe", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)3,
				"update \"INV\".\"MTL_MATERIAL_transaction_TEMP\" set", 1726926017000L, 6084035200821L, 
				" 0x0031f7.008fe160.009c ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926017000L, 6084035200821L, 
				" 0x0031f7.008fe161.0010 ", 100, "AAAtoxABcAACcDrABk", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_DETAILS\"(", 1726926017000L, 6084035200821L, 
				" 0x0031f7.008fe16a.0188 ", 13, "AAAqvfABcAAEXtqAAN", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\"(", 1726926017000L, 6084035200821L, 
				" 0x0031f7.008fe175.0050 ", 55, "AAAqvmABXAAItR4AA3", false));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\"(", 1726926017000L, 6084035200821L, 
				" 0x0031f7.008fe177.011c ", 114, "AABmHHABaAAICzGABy", false));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set", 1726926017000L, 6084035200821L, 
				" 0x0031f7.008fe178.01b0 ", 0, "AAArDwABeAAAMG9AAD", false));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set", 1726926017000L, 6084035200821L, 
				" 0x0031f7.008fe17c.0098 ", 0, "AAArDwABeAAAMG9AAD", false));
		transaction.addStatement(new OraCdcLogMinerStatement(178735, (short)2,
				"delete from \"INV\".\"MTL_ONHAND_QUANTITIES_DETAIL\" where", 1726926017000L, 6084035200827L, 
				" 0x0031f7.008fe183.0148 ", 0, "AAArovAAlAAHhjAAAv", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186928, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_ONHAND_QUANTITIE\"(", 1726926017000L, 6084035200827L, 
				" 0x0031f7.008fe187.01c0 ", 62, "AAAtowAAeAAHk1qAA+", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173337, (short)1,
				"insert into \"INV\".\"MTL_TRANSACTION_LOT_NUMBERS\"(", 1726926017000L, 6084035200827L, 
				" 0x0031f7.008fe1ba.0174 ", 28, "AAAqUZABfAAAC5sAAc", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)1,
				"insert into \"INV\".\"MTL_MATERIAL_transaction\"(", 1726926017000L, 6084035200827L, 
				" 0x0031f7.008fe1ec.0010 ", 14, "AAAqUAABfAAAS8sAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(178735, (short)1,
				"insert into \"INV\".\"MTL_ONHAND_QUANTITIES_DETAIL\"(", 1726926017000L, 6084035200831L, 
				" 0x0031f7.008fe208.0010 ", 4, "AAArovAAkAAHIu8AAE", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186928, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_ONHAND_QUANTITIE\"(", 1726926017000L, 6084035200831L, 
				" 0x0031f7.008fe20c.00f0 ", 63, "AAAtowAAeAAHk1qAA/", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173337, (short)1,
				"insert into \"INV\".\"MTL_TRANSACTION_LOT_NUMBERS\"(", 1726926017000L, 6084035200834L, 
				" 0x0031f7.008fe210.0010 ", 29, "AAAqUZABfAAAC5sAAd", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173312, (short)1,
				"insert into \"INV\".\"MTL_MATERIAL_transaction\"(", 1726926017000L, 6084035200834L, 
				" 0x0031f7.008fe214.00f8 ", 15, "AAAqUAABfAAAS8sAAP", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173347, (short)3,
				"update \"INV\".\"MTL_TXN_REQUEST_LINES\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe227.0148 ", 0, "AAAqUjABaAAH8deAAV", false));
		transaction.addStatement(new OraCdcLogMinerStatement(189147, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_TXN_REQUEST_LINE\"(", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe22a.0038 ", 101, "AAAuLbAAjAAFPuwABl", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe22c.00fc ", 0, "AAAqUzAAmAAHc+9AAN", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_RESERVATIONS\"(", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe22d.00dc ", 31, "AAAto2AAfAAAAtjAAf", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173294, (short)3,
				"update \"INV\".\"MTL_DEMAND\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe22e.015c ", 0, "AAAqTuAAkAAHNdNAAB", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186905, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_DEMAND\"(", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe231.0064 ", 91, "AAAtoZAAnAAHmXdABb", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe231.01c8 ", 0, "AAAqUzAAnAAHmWKAAJ", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_RESERVATIONS\"(", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe232.01b0 ", 32, "AAAto2AAfAAAAtjAAg", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173294, (short)3,
				"update \"INV\".\"MTL_DEMAND\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe234.0028 ", 0, "AAAqTuAApAAHoz2AAK", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186905, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_DEMAND\"(", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe236.0150 ", 92, "AAAtoZAAnAAHmXdABc", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173363, (short)3,
				"update \"INV\".\"MTL_RESERVATIONS\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe237.00ac ", 0, "AAAqUzAAmAAHc+9AAN", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186934, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_RESERVATIONS\"(", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe237.01c8 ", 33, "AAAto2AAfAAAAtjAAh", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe239.01ac ", 0, "AAAqvfABcAAEW4/AAF", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_DETAILS\"(", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe23d.0158 ", 14, "AAAqvfABcAAEXtqAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe24b.00c4 ", 0, "AAAqvfABcAAEXtqAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\"(", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe24f.00ac ", 56, "AAAqvmABXAAItR4AA4", false));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\"(", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe252.004c ", 115, "AABmHHABaAAICzGABz", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe252.01ac ", 0, "AAAqvfABcAAEW4/AAF", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe257.01e8 ", 0, "AAAqvfABcAAEW4/AAF", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175081, (short)3,
				"update \"WSH\".\"WSH_NEW_DELIVERIES\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe25c.00d4 ", 0, "AAAqvpAA3AALk1RAAG", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe261.0194 ", 0, "AAAqvIABaAAItaLAAX", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186927, (short)1,
				"insert into \"WSH\".\"MLOG$_WSH_TRIP_STOPS\"(", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe263.0148 ", 26, "AAAtovAApAAHwFiAAa", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe264.00bc ", 0, "AAAqvmABXAAItR4AA4", false));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\"(", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe265.01a4 ", 116, "AABmHHABaAAICzGAB0", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\" set", 1726926017000L, 6084035200837L, 
				" 0x0031f7.008fe266.011c ", 0, "AAAqvmABXAAItR4AA3", false));
		transaction.addStatement(new OraCdcLogMinerStatement(418247, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS_A\"(", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe269.003c ", 117, "AABmHHABaAAICzGAB1", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175078, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_ASSIGNMENTS\" set", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe269.01a4 ", 0, "AAAqvmABXAAItR4AA4", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175081, (short)3,
				"update \"WSH\".\"WSH_NEW_DELIVERIES\" set", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe26a.0188 ", 0, "AAAqvpAA3AALk1RAAG", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175081, (short)3,
				"update \"WSH\".\"WSH_NEW_DELIVERIES\" set", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe26f.00d0 ", 0, "AAAqvpAA3AALk1RAAG", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe272.019c ", 0, "AAAqvIABaAAItaLAAX", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186927, (short)1,
				"insert into \"WSH\".\"MLOG$_WSH_TRIP_STOPS\"(", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe274.014c ", 27, "AAAtovAApAAHwFiAAb", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe275.00a8 ", 0, "AAAqvfABcAAEXtqAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe279.009c ", 0, "AAAqvfABcAAEXtqAAN", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe27f.0180 ", 0, "AAAqvfABcAAEXtqAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(175071, (short)3,
				"update \"WSH\".\"WSH_DELIVERY_DETAILS\" set", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe289.014c ", 0, "AAAqvfABcAAEXtqAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173314, (short)2,
				"delete from \"INV\".\"MTL_TRANSACTION_LOTS_TEMP\" where", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe28d.01a4 ", 0, "AAAqUCAAeAANOaiAAe", false));
		transaction.addStatement(new OraCdcLogMinerStatement(173304, (short)2,
				"delete from \"INV\".\"MTL_MATERIAL_transaction_TEMP\" where", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe28f.0144 ", 0, "AAAqT4AAmAAHL6bAAO", false));
		transaction.addStatement(new OraCdcLogMinerStatement(186929, (short)1,
				"insert into \"INV\".\"MLOG$_MTL_MATERIAL_TRANSAC\"(", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe299.00a8 ", 101, "AAAtoxABcAACcDrABl", false));
		transaction.addStatement(new OraCdcLogMinerStatement(192340, (short)2,
				"/* No SQL_REDO for temporary tables */", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe29a.001c ", 0, "ABWIOAAAFAAGIOBAAA", false));
		transaction.addStatement(new OraCdcLogMinerStatement(176368, (short)3,
				"update \"WMS\".\"WMS_LICENSE_PLATE_NUMBERS\" set", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe29a.0118 ", 0, "AAArDwABeAAAMG9AAD", false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)1,
				"/* No SQL_REDO for temporary tables */", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe29b.01b0 ", 18, "ABVkkAAAFAAFkkBAAS", false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)3,
				"/* No SQL_REDO for temporary tables */", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe29c.0124 ", 0, "ABVkkAAAFAAFkkBAAS", false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)3,
				"/* No SQL_REDO for temporary tables */", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe29c.01e0 ", 0, "ABVkkAAAFAAFkkBAAS", false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)3,
				"/* No SQL_REDO for temporary tables */", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe29d.00b0 ", 0, "ABVkkAAAFAAFkkBAAS", false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)3,
				"/* No SQL_REDO for temporary tables */", 1726926017000L, 6084035200838L, 
				" 0x0031f7.008fe29d.017c ", 0, "ABVkkAAAFAAFkkBAAS", false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)1,
				"/* No SQL_REDO for temporary tables */", 1726926018000L, 6084035200867L, 
				" 0x0031f7.008fe2df.0010 ", 19, "ABVkkAAAFAAFkkBAAT", false));
		transaction.addStatement(new OraCdcLogMinerStatement(191357, (short)3,
				"/* No SQL_REDO for temporary tables */", 1726926018000L, 6084035200867L, 
				" 0x0031f7.008fe2df.0180 ", 0, "ABVkkAAAFAAFkkBAAT", false));
		transaction.setCommitScn(6084035200867L);
	}

	public OraCdcTransaction get() {
		return transaction;
	}

	@Override
	public void close() throws IOException {
		transaction.close();
	}

}
