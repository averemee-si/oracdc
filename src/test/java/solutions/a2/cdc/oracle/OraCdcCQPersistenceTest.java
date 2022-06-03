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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import solutions.a2.cdc.oracle.OraCdcLogMinerStatement;
import solutions.a2.cdc.oracle.OraCdcTransaction;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcCQPersistenceTest {

	private final static OraCdcLogMinerStatement updIn1 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='SALES' where DEPTNO=10",
			System.currentTimeMillis(),275168436063l," 0x000098.000001b5.0010 ",
			0, "AAAWbzAAEAAAB6FAAA");

	private final static OraCdcLogMinerStatement updIn2 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='OPERATIONS' where DEPTNO=20",
			System.currentTimeMillis(),275168436122l," 0x000098.000001b5.0020 ",
			0, "AAAWbzAAEAAAB6FABB");

	private final static OraCdcLogMinerStatement updIn3 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='ACCOUNTING' where DEPTNO=30",
			System.currentTimeMillis(),275168436125l," 0x000098.000001b5.0030 ",
			0, "AAAWbzAAEAAAB6FACC");

	private static Path persistenceQueuePath;
	private static String persistenceXid;
	private static long persistenceFirstChange;
	private static long persistenceNextChange;
	private static Long persistenceCommitScn;
	private static int persistenceQueueSize;
	private static int persistenceQueueOffset;

	private void processQueueBeforeRestart() throws IOException {

		final String tmpDir = System.getProperty("java.io.tmpdir");
		final Path queuesRoot = FileSystems.getDefault().getPath(tmpDir);
		final String xid = "0000270016000000";

		// Add some statements.....
		final OraCdcTransaction transaction = new OraCdcTransaction(queuesRoot, xid, updIn1);
		transaction.addStatement(updIn2);
		transaction.addStatement(updIn3);

		// Read just one.....
		OraCdcLogMinerStatement updOut = new OraCdcLogMinerStatement();
		transaction.getStatement(updOut);

		// Stop of processing and store value to static variables.....
		persistenceQueuePath = transaction.getPath();
		persistenceXid = transaction.getXid();
		persistenceFirstChange = transaction.getFirstChange();
		persistenceNextChange = transaction.getNextChange();
		persistenceCommitScn = transaction.getCommitScn();
		persistenceQueueSize = transaction.length();
		persistenceQueueOffset = transaction.offset();

	}


	@Test
	public void test() throws IOException {
		OraCdcCQPersistenceTest qt = new OraCdcCQPersistenceTest();

		// Create transaction object and save state to static variables
		qt.processQueueBeforeRestart();

		// Restore transaction object from file...
		final OraCdcTransaction transaction = new OraCdcTransaction(
				persistenceQueuePath,
				persistenceXid,
				persistenceFirstChange,
				persistenceNextChange,
				persistenceCommitScn,
				persistenceQueueSize,
				persistenceQueueOffset);

		OraCdcLogMinerStatement updOut = new OraCdcLogMinerStatement();
		// We expect updIn2
		assertTrue(transaction.getStatement(updOut));
		assertEquals(updIn2.getSqlRedo(), updOut.getSqlRedo(), "Not same strings!");
		assertEquals(2, transaction.offset(), "transaction.offset() should return 2!");
		
		// We expect updIn3
		assertTrue(transaction.getStatement(updOut));
		assertEquals(updIn3.getSqlRedo(), updOut.getSqlRedo(), "Not same strings!");
		assertEquals(3, transaction.offset(), "transaction.offset() should return 3!");

		// No more records in queue
		assertFalse(transaction.getStatement(updOut));

		transaction.close();
	}
}
