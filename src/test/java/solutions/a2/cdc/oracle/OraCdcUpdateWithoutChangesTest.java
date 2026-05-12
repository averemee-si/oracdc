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
