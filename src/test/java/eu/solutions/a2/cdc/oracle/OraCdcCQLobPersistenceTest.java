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

package eu.solutions.a2.cdc.oracle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class OraCdcCQLobPersistenceTest {

	private final static OraCdcLogMinerStatement updIn1 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='SALES' where DEPTNO=10",
			System.currentTimeMillis(),275168436063l," 0x000098.000001b5.0010 ",
			0, "AAAWbzAAEAAAB6FAAA");
	private final static List<OraCdcLargeObjectHolder> updInLobs1 = new ArrayList<>(Arrays.asList(
			new OraCdcLargeObjectHolder(11, "Lob011".getBytes())));

	private final static OraCdcLogMinerStatement updIn2 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='OPERATIONS' where DEPTNO=20",
			System.currentTimeMillis(),275168436122l," 0x000098.000001b5.0020 ",
			0, "AAAWbzAAEAAAB6FABB");
	private final static List<OraCdcLargeObjectHolder> updInLobs2 = new ArrayList<>(Arrays.asList(
			new OraCdcLargeObjectHolder(21, "Lob021".getBytes()),
			new OraCdcLargeObjectHolder(22, "Lob022".getBytes())));

	private final static OraCdcLogMinerStatement updIn3 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='ACCOUNTING' where DEPTNO=30",
			System.currentTimeMillis(),275168436125l," 0x000098.000001b5.0030 ",
			0, "AAAWbzAAEAAAB6FACC");
	private final static List<OraCdcLargeObjectHolder> updInLobs3 = new ArrayList<>(Arrays.asList(
			new OraCdcLargeObjectHolder(31, "Lob031".getBytes()),
			new OraCdcLargeObjectHolder(32, "Lob032".getBytes()),
			new OraCdcLargeObjectHolder(33, "Lob033".getBytes())));

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
		final OraCdcTransaction transaction = new OraCdcTransaction(true, queuesRoot, xid);
		transaction.addStatement(updIn1, updInLobs1);
		transaction.addStatement(updIn2, updInLobs2);
		transaction.addStatement(updIn3, updInLobs3);

		// Read just one.....
		OraCdcLogMinerStatement updOut = new OraCdcLogMinerStatement();
		List<OraCdcLargeObjectHolder> updOutLobs = new ArrayList<>();
		transaction.getStatement(updOut, updOutLobs);

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
		OraCdcCQLobPersistenceTest qt = new OraCdcCQLobPersistenceTest();

		// Create transaction object and save state to static variables
		qt.processQueueBeforeRestart();

		// Restore transaction object from file...
		final OraCdcTransaction transaction = new OraCdcTransaction(
				true,
				persistenceQueuePath,
				persistenceXid,
				persistenceFirstChange,
				persistenceNextChange,
				persistenceCommitScn,
				persistenceQueueSize,
				persistenceQueueOffset);

		OraCdcLogMinerStatement updOut = new OraCdcLogMinerStatement();
		List<OraCdcLargeObjectHolder> updOutLobs2 = new ArrayList<>();
		// We expect updIn2
		assertTrue(transaction.getStatement(updOut, updOutLobs2));
		assertEquals("Not same strings!", updIn2.getSqlRedo(), updOut.getSqlRedo());
		assertEquals("transaction.offset() should return 2!", 2, transaction.offset());
		assertEquals("updOutLobs2.get(0).getLobId() should return 21!", 21, updOutLobs2.get(0).getLobId());
		assertEquals("updOutLobs2.get(1).getLobId() should return 22!", 22, updOutLobs2.get(1).getLobId());
		
		// We expect updIn3
		List<OraCdcLargeObjectHolder> updOutLobs3 = new ArrayList<>();
		assertTrue(transaction.getStatement(updOut, updOutLobs3));
		assertEquals("Not same strings!", updIn3.getSqlRedo(), updOut.getSqlRedo());
		assertEquals("transaction.offset() should return 3!", 3, transaction.offset());
		assertEquals("updOutLobs3.get(2).getLobId() should return 33!", 33, updOutLobs3.get(2).getLobId());

		// No more records in queue
		assertFalse(transaction.getStatement(updOut));

		transaction.close();
	}
}
