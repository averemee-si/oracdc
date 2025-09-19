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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcChronicleQueueTest {

	@Test
	public void test() throws IOException {
		final String tmpDir = System.getProperty("java.io.tmpdir");
		final Path queuesRoot = FileSystems.getDefault().getPath(tmpDir);
		final OraCdcLogMinerStatement updIn =  new  OraCdcLogMinerStatement(
				74590, (short)3,
				"update DEPT set DNAME='SALES' where DEPTNO=10".getBytes(StandardCharsets.US_ASCII),
				System.currentTimeMillis(),275168436063l,
				RedoByteAddress.fromLogmnrContentsRs_Id(" 0x000098.000001b5.0010 "),
				0, 
				new RowId("AAAWbzAAEAAAB6FAAA"), false);
		String xid = "0000270016000000";
		OraCdcTransaction transaction = new OraCdcTransactionChronicleQueue(queuesRoot, xid, updIn, false);
		transaction.setCommitScn(updIn.getScn());
		OraCdcLogMinerStatement updOut = new OraCdcLogMinerStatement();
		transaction.getStatement(updOut);
		transaction.close();

		assertEquals(updIn.getRba(), updOut.getRba(), "Not same RBA!");
		assertEquals(updIn.getSqlRedo(), updOut.getSqlRedo(), "Not same strings!");
		assertEquals(updIn.getScn(), updOut.getScn(), "Not same longs!");
		assertEquals(updIn.getTs(), updOut.getTs(), "Not same longs!");
		assertEquals(updIn.getRowId(), updOut.getRowId(), "Not same ROWID!");
	}
}
