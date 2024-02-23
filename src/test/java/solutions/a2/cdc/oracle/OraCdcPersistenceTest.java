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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcPersistenceTest {

	private final static String xid1 = "0000270016000000";
	private final static OraCdcLogMinerStatement updIn1 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='SALES' where DEPTNO=10",
			System.currentTimeMillis(),275168436063l," 0x000098.000001b5.0010 ",
			0, "AAAWbzAAEAAAB6FAAA", false);

	private final static String xid2 = "00002700160000AA";
	private final static OraCdcLogMinerStatement updIn2 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='OPERATIONS' where DEPTNO=20",
			System.currentTimeMillis(),275168436122l," 0x000098.000001b5.0020 ",
			0, "AAAWbzAAEAAAB6FABB", false);

	private final static String xid3 = "00002700160000BB";
	private final static OraCdcLogMinerStatement updIn3 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='ACCOUNTING' where DEPTNO=30",
			System.currentTimeMillis(),275168436125l," 0x000098.000001b5.0030 ",
			0, "AAAWbzAAEAAAB6FACC", false);



	@Test
	public void test() throws IOException {

		final String tmpDir = System.getProperty("java.io.tmpdir");
		final Path queuesRoot = FileSystems.getDefault().getPath(tmpDir);
		final OraCdcTransaction trans1 = new OraCdcTransactionChronicleQueue(queuesRoot, xid1, updIn1);
		trans1.setCommitScn(275168436063l);
		final OraCdcTransaction trans2 = new OraCdcTransactionChronicleQueue(queuesRoot, xid2, updIn2);
		List<Map<String, Object>> inProgress = new ArrayList<>();
		inProgress.add(((OraCdcTransactionChronicleQueue)trans2).attrsAsMap());
		final OraCdcTransaction trans3 = new OraCdcTransactionChronicleQueue(queuesRoot, xid3, updIn3);
		List<Map<String, Object>> committed = new ArrayList<>();
		committed.add(((OraCdcTransactionChronicleQueue)trans3).attrsAsMap());

		OraCdcPersistentState ops = new OraCdcPersistentState();
		ops.setDbId(710804450l);
		ops.setInstanceName("TESTAPPS");
		ops.setHostName("ebstst061");
		ops.setLastOpTsMillis(System.currentTimeMillis());
		ops.setLastScn(275168436063l);
		ops.setLastRsId(" 0x000098.000001b5.0030 ");
		ops.setLastSsn(0l);
		ops.setCurrentTransaction(((OraCdcTransactionChronicleQueue)trans1).attrsAsMap());
		ops.setInProgressTransactions(inProgress);
		ops.setCommittedTransactions(committed);

		final String stateFileName = tmpDir +  
				(StringUtils.endsWith(tmpDir, File.separator) ? "" : File.separator) +
				"oracdc.state." + System.currentTimeMillis();
		ops.toFile(stateFileName);

		OraCdcPersistentState restored = OraCdcPersistentState.fromFile(stateFileName);
		Files.deleteIfExists(Paths.get(stateFileName)); 

		assertEquals(ops.getDbId(), restored.getDbId());
		assertEquals(ops.getHostName(), restored.getHostName());
		assertEquals(ops.getInstanceName(), restored.getInstanceName());
		assertEquals(ops.getLastOpTsMillis(), restored.getLastOpTsMillis());
		assertEquals(ops.getLastScn(), restored.getLastScn());
		assertEquals(ops.getLastRsId(), restored.getLastRsId());
		assertEquals(ops.getLastSsn(), restored.getLastSsn());

		trans1.close();
		trans2.close();
		trans3.close();

	}
}
