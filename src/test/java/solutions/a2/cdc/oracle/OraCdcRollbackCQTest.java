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

import org.apache.log4j.BasicConfigurator;
import org.junit.jupiter.api.Test;

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcRollbackCQTest {
	
	OraCdcRollbackCQTest() {
		BasicConfigurator.configure();
	}

	@Test
	public void test() throws IOException {

		final OraCdcRollbackData testData = new OraCdcRollbackData();
		OraCdcTransaction transaction = testData.get(false);
		assertFalse(transaction.suspicious());

		boolean AAAqvfABcAAEXtqAAN_At_008fdb97_0010 = false;
		boolean AAAqvfABcAAEXtqAAO_At_008fdc24_010c = false;
		boolean AAAqvfABcAAEXtqAAO_At_008fdc32_0048 = false;
		boolean AAAqvfABcAAEXtqAAO_At_008fdc43_0010 = false;

		boolean AAAqvfABcAAEXtqAAN_At_008fe16a_0188 = false;
		boolean AAAqvfABcAAEXtqAAO_At_008fe23d_0158 = false;

		int count = 0;
		boolean processTransaction = false;
		final OraCdcLogMinerStatement stmt = new OraCdcLogMinerStatement();
		do {
			processTransaction = transaction.getStatement(stmt);
			if (processTransaction) {
				count++;

				assertFalse(stmt.isRollback());

				if (stmt.getRba().toString().equals("0x0031f7.008fdb97.0010") &&
						stmt.getRowId().toString().equals("AAAqvfABcAAEXtqAAN")) {
					AAAqvfABcAAEXtqAAN_At_008fdb97_0010 = true;
				}
				if (stmt.getRba().toString().equals("0x0031f7.008fdc24.010c") &&
						stmt.getRowId().toString().equals("AAAqvfABcAAEXtqAAO")) {
					AAAqvfABcAAEXtqAAO_At_008fdc24_010c = true;
				}
				if (stmt.getRba().toString().equals("0x0031f7.008fdc32.0048") &&
						stmt.getRowId().toString().equals("AAAqvfABcAAEXtqAAO")) {
					AAAqvfABcAAEXtqAAO_At_008fdc32_0048 = true;
				}
				if (stmt.getRba().toString().equals("0x0031f7.008fdc43.0010") &&
						stmt.getRowId().toString().equals("AAAqvfABcAAEXtqAAO")) {
					AAAqvfABcAAEXtqAAO_At_008fdc43_0010 = true;
				}

				if (stmt.getRba().toString().equals("0x0031f7.008fe16a.0188") &&
						stmt.getRowId().toString().equals("AAAqvfABcAAEXtqAAN")) {
					AAAqvfABcAAEXtqAAN_At_008fe16a_0188 = true;
				}
				if (stmt.getRba().toString().equals("0x0031f7.008fe23d.0158") &&
						stmt.getRowId().toString().equals("AAAqvfABcAAEXtqAAO")) {
					AAAqvfABcAAEXtqAAO_At_008fe23d_0158 = true;
				}
			}
		} while (processTransaction);

		assertFalse(AAAqvfABcAAEXtqAAN_At_008fdb97_0010);
		assertFalse(AAAqvfABcAAEXtqAAO_At_008fdc24_010c);
		assertFalse(AAAqvfABcAAEXtqAAO_At_008fdc32_0048);
		assertFalse(AAAqvfABcAAEXtqAAO_At_008fdc43_0010);

		assertTrue(AAAqvfABcAAEXtqAAN_At_008fe16a_0188);
		assertTrue(AAAqvfABcAAEXtqAAO_At_008fe23d_0158);

		assertEquals(count, 83);

		transaction.close();

		final OraCdcRollbackZeroRows zeroRows = new  OraCdcRollbackZeroRows(false);
		transaction = zeroRows.get();
		assertFalse(transaction.suspicious());
		
		assertEquals(transaction.length(), 46);
		count = 0;
		do {
			processTransaction = transaction.getStatement(stmt);
			if (processTransaction) {
				count++;
			}
		} while (processTransaction);
		assertEquals(count, 0);

		zeroRows.close();

		// QMI/QMD test
		OraCdcTransaction transQmiQmd = testData.getQmdQmi(false);
		assertFalse(transQmiQmd.suspicious());
		
		count = 0;
		do {
			processTransaction = transQmiQmd.getStatement(stmt);
			if (processTransaction) {
				count++;
			}
			if (count == 1) {
				assertEquals(stmt.rowId, new RowId("AAAqUAABbAAJXgnAAJ"));
				assertEquals(stmt.getRba(), RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6a506.01c4 "));
			}
			if (count == 2) {
				assertEquals(stmt.rowId, new RowId("AAAqUAABbAAJXgnAAK"));
				assertEquals(stmt.getRba(), RedoByteAddress.fromLogmnrContentsRs_Id(" 0x0042db.00f6a525.0184 "));
			}
		} while (processTransaction);
		assertEquals(count, 2);

		transQmiQmd.close();
	}

}
