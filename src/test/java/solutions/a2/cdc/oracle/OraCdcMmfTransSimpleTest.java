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

package solutions.a2.cdc.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.apache.log4j.BasicConfigurator;
import org.junit.jupiter.api.Test;

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcMmfTransSimpleTest {

	@Test
	public void test() throws IOException {
		BasicConfigurator.configure();
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
		OraCdcTransaction transaction = new OraCdcTransactionMmf(queuesRoot, xid, updIn, false);
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
