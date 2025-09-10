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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;
import static solutions.a2.oracle.utils.BinaryUtils.putU16;
import static solutions.a2.oracle.utils.BinaryUtils.putU24;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcUpdateWithoutChangesTest {

	private static int REAL_UPDATE = 0;
	private static int FAKE_UPDATE = 1;
	
	private List<OraCdcRedoMinerStatement> testData() {
		final List<OraCdcRedoMinerStatement> testData = new ArrayList<>();
		ByteArrayOutputStream baos;

		baos = null;
		baos = new ByteArrayOutputStream(0x100);
		try {
			putU16(baos, 1);	// Column count in set clause
			putU16(baos, 2);	// Column #
			baos.write(5);		// Column length
			baos.writeBytes(hexToRaw("4142434445"));
			putU24(baos, 7);
			putU16(baos, 2);	// Column #
			baos.write(4);		// Column length
			baos.writeBytes(hexToRaw("41424344"));
			putU16(baos, 1);	// Column count in where clause
			putU16(baos, 1);	// Column #
			baos.write(2);		// Column length
			baos.writeBytes(hexToRaw("c102"));
		} catch (IOException ioe) {}
		System.out.println("real update:" + rawToHex(baos.toByteArray()));
		testData.add(REAL_UPDATE,
				new OraCdcRedoMinerStatement(
						77845l,
						UPDATE,
						baos.toByteArray(),
						System.currentTimeMillis(),
						System.nanoTime(),
						RedoByteAddress.MIN_VALUE,
						1l,
						RowId.ZERO,
						false));

		baos = null;
		baos = new ByteArrayOutputStream(0x100);
		try {
			putU16(baos, 1);	// Column count in set clause
			putU16(baos, 2);	// Column #
			baos.write(5);		// Column length
			baos.writeBytes(hexToRaw("4142434445"));
			putU24(baos, 8);
			putU16(baos, 2);	// Column #
			baos.write(5);		// Column length
			baos.writeBytes(hexToRaw("4142434445"));
			putU16(baos, 1);	// Column count in where clause
			putU16(baos, 1);	// Column #
			baos.write(2);		// Column length
			baos.writeBytes(hexToRaw("c102"));
		} catch (IOException ioe) {}
		System.out.println("fake update:" + rawToHex(baos.toByteArray()));
		testData.add(FAKE_UPDATE,
				new OraCdcRedoMinerStatement(
						77845l,
						UPDATE,
						baos.toByteArray(),
						System.currentTimeMillis(),
						System.nanoTime(),
						RedoByteAddress.MIN_VALUE,
						1l,
						RowId.ZERO,
						false));
		return testData;
	}

	@Test
	public void test() {
		final List<OraCdcRedoMinerStatement> testData = testData();
		System.out.println(testData.get(REAL_UPDATE));
		System.out.println(testData.get(FAKE_UPDATE));
		assertFalse(testData.get(REAL_UPDATE).updateWithoutChanges());
		assertTrue(testData.get(FAKE_UPDATE).updateWithoutChanges());
	}
}
