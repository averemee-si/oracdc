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
public class OraCdcRollbackZeroRows implements Closeable {

	private final OraCdcTransaction transaction;

	/*
	 * Test data with a transaction that, despite being committed, should not return changed rows
	 */
	public OraCdcRollbackZeroRows(final boolean arrayList) throws IOException {
		final String xid = "1F000D00FB0D2600";
		final OraCdcLogMinerStatement firstStmt  = new OraCdcLogMinerStatement(175041, (short)1,
				"insert into \"WSH\".\"WSH_TRIPS\"(".getBytes(StandardCharsets.US_ASCII),
				1727689803000L, 6084777349030L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00715538.00e0 "), 12,
				new RowId("AAAqvBABcAACu52AAM"), false); 
		if (arrayList) {
			transaction = new OraCdcTransactionArrayList(xid, firstStmt);
		} else {
			final String tmpDir = System.getProperty("java.io.tmpdir");
			final Path queuesRoot = FileSystems.getDefault().getPath(tmpDir);
			transaction = new OraCdcTransactionChronicleQueue(queuesRoot, xid, firstStmt);
		}
		transaction.addStatement(new OraCdcLogMinerStatement(175041, (short)3,
				"update \"WSH\".\"WSH_TRIPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689803000L, 6084777349087L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00715629.0030 "), 0,
				new RowId("AAAqvBABcAACu52AAM"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)1,
				"insert into \"WSH\".\"WSH_TRIP_STOPS\"(".getBytes(StandardCharsets.US_ASCII),
				1727689803000L, 6084777349166L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.0071567f.00a0 "), 23,
				new RowId("AAAqvIABaAAItMFAAX"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777350237L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00715fa4.0128 "), 0,
				new RowId("AAAqvIABaAAItMFAAX"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)1,
				"insert into \"WSH\".\"WSH_TRIP_STOPS\"(".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777350257L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.0071600e.00a8 "), 24,
				new RowId("AAAqvIABaAAItMFAAY"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351501L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716cea.0080 "), 0,
				new RowId("AAAqvIABaAAItMFAAX"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351503L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716cee.0124 "), 0,
				new RowId("AAAqvIABaAAItMFAAY"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175066, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_LEGS\"(".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351515L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716cfe.0010 "), 12,
				new RowId("AAAqvaAAuAANM13AAM"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351520L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716d01.0078 "), 0,
				new RowId("AAAqvIABaAAItMFAAX"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351520L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716d03.015c "), 0,
				new RowId("AAAqvIABaAAItMFAAY"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351520L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716d06.0038 "), 0,
				new RowId("AAAqvIABaAAItMFAAX"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351520L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716d08.013c "), 0,
				new RowId("AAAqvIABaAAItMFAAY"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175081, (short)3,
				"update \"WSH\".\"WSH_NEW_DELIVERIES\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351532L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716d26.01a4 "), 0,
				new RowId("AAAqvpAA3AALk2aAAL"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175066, (short)1,
				"insert into \"WSH\".\"WSH_DELIVERY_LEGS\"(".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351555L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716d59.0010 "), 13,
				new RowId("AAAqvaAAuAANM13AAN"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351563L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716d6a.0010 "), 0,
				new RowId("AAAqvIABaAAItMFAAX"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351566L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716d77.0010 "), 0,
				new RowId("AAAqvIABaAAItMFAAY"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351566L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716d79.018c "), 0,
				new RowId("AAAqvIABaAAItMFAAX"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351569L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716d7d.0180 "), 0,
				new RowId("AAAqvIABaAAItMFAAY"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175081, (short)3,
				"update \"WSH\".\"WSH_NEW_DELIVERIES\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351576L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716d92.005c "), 0,
				new RowId("AAAqvpAA3AALk2aAAK"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175127, (short)1,
				"insert into \"WSH\".\"WSH_FREIGHT_COSTS\"(".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351600L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716dc8.0170 "), 62,
				new RowId("AAAqwXABZAAIlgVAA+"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175127, (short)1,
				"insert into \"WSH\".\"WSH_FREIGHT_COSTS\"(".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351607L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716ddc.0010 "), 63,
				new RowId("AAAqwXABZAAIlgVAA/"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175127, (short)1,
				"insert into \"WSH\".\"WSH_FREIGHT_COSTS\"(".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351613L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716de5.0010 "), 64,
				new RowId("AAAqwXABZAAIlgVABA"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175127, (short)1,
				"insert into \"WSH\".\"WSH_FREIGHT_COSTS\"(".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351613L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716de7.00fc "), 65,
				new RowId("AAAqwXABZAAIlgVABB"), false));
		transaction.addStatement(new OraCdcLogMinerStatement(175127, (short)2,
				"delete from \"WSH\".\"WSH_FREIGHT_COSTS\" where".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716df5.0184 "), 0,
				new RowId("AAAqwXABZAAIlgVABB"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175127, (short)2,
				"delete from \"WSH\".\"WSH_FREIGHT_COSTS\" where".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716df7.0054 "), 0,
				new RowId("AAAqwXABZAAIlgVABA"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175127, (short)2,
				"delete from \"WSH\".\"WSH_FREIGHT_COSTS\" where".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716df8.0114 "), 0,
				new RowId("AAAqwXABZAAIlgVAA/"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175127, (short)2,
				"delete from \"WSH\".\"WSH_FREIGHT_COSTS\" where".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716dfa.0010 "), 0,
				new RowId("AAAqwXABZAAIlgVAA+"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175081, (short)3,
				"update \"WSH\".\"WSH_NEW_DELIVERIES\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716dfb.003c "), 0,
				new RowId("AAAqvpAA3AALk2aAAK"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716dfe.0144 "), 0,
				new RowId("AAAqvIABaAAItMFAAY"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e00.016c "), 0,
				new RowId("AAAqvIABaAAItMFAAX"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e02.0194 "), 0,
				new RowId("AAAqvIABaAAItMFAAY"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e04.01a0 "), 0,
				new RowId("AAAqvIABaAAItMFAAX"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175066, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_LEGS\" where".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e07.017c "), 0,
				new RowId("AAAqvaAAuAANM13AAN"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175081, (short)3,
				"update \"WSH\".\"WSH_NEW_DELIVERIES\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e0e.00f0 "), 0,
				new RowId("AAAqvpAA3AALk2aAAL"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e11.01e8 "), 0,
				new RowId("AAAqvIABaAAItMFAAY"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e14.0010 "), 0,
				new RowId("AAAqvIABaAAItMFAAX"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e16.0014 "), 0,
				new RowId("AAAqvIABaAAItMFAAY"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e17.01e4 "), 0,
				new RowId("AAAqvIABaAAItMFAAX"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175066, (short)2,
				"delete from \"WSH\".\"WSH_DELIVERY_LEGS\" where".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e1a.01d8 "), 0,
				new RowId("AAAqvaAAuAANM13AAM"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e1b.0180 "), 0,
				new RowId("AAAqvIABaAAItMFAAY"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351617L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e1d.015c "), 0,
				new RowId("AAAqvIABaAAItMFAAX"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)2,
				"delete from \"WSH\".\"WSH_TRIP_STOPS\" where".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351619L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e21.016c "), 0,
				new RowId("AAAqvIABaAAItMFAAY"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)3,
				"update \"WSH\".\"WSH_TRIP_STOPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351619L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e22.0104 "), 0,
				new RowId("AAAqvIABaAAItMFAAX"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175048, (short)2,
				"delete from \"WSH\".\"WSH_TRIP_STOPS\" where".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351620L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e2c.01d0 "), 0,
				new RowId("AAAqvIABaAAItMFAAX"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175041, (short)3,
				"update \"WSH\".\"WSH_TRIPS\" set".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351620L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e2d.0178 "), 0,
				new RowId("AAAqvBABcAACu52AAM"), true));
		transaction.addStatement(new OraCdcLogMinerStatement(175041, (short)2,
				"delete from \"WSH\".\"WSH_TRIPS\" where".getBytes(StandardCharsets.US_ASCII),
				1727689804000L, 6084777351622L, 
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x00327b.00716e46.00ac "), 0,
				new RowId("AAAqvBABcAACu52AAM"), true));
		transaction.setCommitScn(6084777351622L);
	}

	public OraCdcTransaction get() {
		return transaction;
	}

	@Override
	public void close() throws IOException {
		transaction.close();
	}

}
