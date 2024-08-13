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

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcRollbackCQTest extends OraCdcRollbackData {

	@Test
	public void test() throws IOException {
		
		final String tmpDir = System.getProperty("java.io.tmpdir");
		final Path queuesRoot = FileSystems.getDefault().getPath(tmpDir);
		final String xid = "0000270016000000";

		
		final OraCdcTransaction transaction = new OraCdcTransactionChronicleQueue(queuesRoot, xid, updIn1);
		transaction.addStatement(updIn2);
		transaction.addStatement(rb1);
		transaction.addStatement(updIn3);
		transaction.addStatement(updIn4);
		transaction.addStatement(updIn5);
		transaction.addStatement(rb2);
		transaction.addStatement(updIn6);
		transaction.addStatement(rb3);
		transaction.setCommitScn(updIn6.getScn());

		OraCdcLogMinerStatement updOut = new OraCdcLogMinerStatement();
		
		// We expect updIn1
		assertTrue(transaction.getStatement(updOut));
		assertEquals(updIn1.getSqlRedo(), updOut.getSqlRedo(), "Not same strings!");

		// We expect updIn3 (updIn2 - rolled back!)
		assertTrue(transaction.getStatement(updOut));
		assertEquals(updIn3.getSqlRedo(), updOut.getSqlRedo(), "Not same strings!");

		// We expect updIn4
		assertTrue(transaction.getStatement(updOut));
		assertEquals(updIn4.getSqlRedo(), updOut.getSqlRedo(), "Not same strings!");

		// No more records in queue - only rollback records in this transaction
		assertFalse(transaction.getStatement(updOut));

		transaction.close();
	}

}
