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

import static oracle.jdbc.OracleTypes.VECTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static solutions.a2.oracle.internals.LobLocator.BLOB;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import solutions.a2.oracle.internals.LobId;
import solutions.a2.oracle.internals.LobLocator;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class VectorTest {

	@SuppressWarnings("unchecked")
	@Test
	public void test() {

		OraCdcDecoder decoder = OraCdcDecoderFactory.get(VECTOR);
		final Set<LobId> lobIds = new HashSet<>();
		final OraCdcTransaction transaction = null;
		
		LobLocator ll;
		byte[] raw;
		Struct vector;
		try {

			// FLOAT32, 7 elements
			raw = hexToRaw(
				"00 70 00 01 01 0c 00 80 00 01 00 00 00 01 00 00 00 21 24 ad 00 37 48 90 00 31 00 00 2d 01 db 00 00 12 02 00 00 00 07 c0 1d 8d 0d 89 58 b5 f2 c0 06 66 66 c0 33 33 33 ba 83 12 6f bb 03 12 6f c0 60 00 00 c0 83 d7 0a c0 67 ae 14"
				.replace(" ", ""));
			ll = new LobLocator(raw);
			assertEquals(BLOB, ll.type());
			assertTrue(ll.secureFile());
			assertEquals(0x2d, ll.dataLength());
			assertTrue(ll.dataInRow());

			vector = (Struct) decoder.decode(raw, 0, raw.length, transaction, lobIds);
			assertEquals(0x7, ((ArrayList<Float>) vector.get("F")).size());
			assertNull(vector.get("D"));
			assertNull(vector.get("I"));
			assertNull(vector.get("B"));
			vector.validate();

			// FLOAT64, 8 elements
			raw = hexToRaw(
				"00 70 00 01 01 0c 00 80 00 01 00 00 00 01 00 00 00 21 24 b0 00 5b 48 90 00 55 00 00 51 01 db 00 00 12 03 00 00 00 08 c0 1d 8f d3 14 33 d1 4e c0 00 cc cc cc cc cc cd c0 06 66 66 66 66 66 66 bf 50 62 4d d2 f1 a9 fc bf 60 62 4d d2 f1 a9 fc c0 0c 00 00 00 00 00 00 c0 10 7a e1 47 ae 14 7a c0 0c f5 c2 8f 5c 28 f6 bf c9 99 99 99 99 99 9a"
				.replace(" ", ""));
			ll = new LobLocator(raw);
			assertEquals(BLOB, ll.type());
			assertTrue(ll.secureFile());
			assertEquals(0x51, ll.dataLength());
			assertTrue(ll.dataInRow());

			vector = (Struct) decoder.decode(raw, 0, raw.length, transaction, lobIds);
			assertNull(vector.get("F"));
			assertEquals(0x8, ((ArrayList<Double>) vector.get("D")).size());
			assertNull(vector.get("I"));
			assertNull(vector.get("B"));
			vector.validate();

			// INT8, 5 elements
			raw = hexToRaw(
				"00 70 00 01 01 0c 00 80 00 01 00 00 00 01 00 00 00 21 24 95 00 20 48 90 00 1a 00 00 16 01 db 00 00 12 04 00 00 00 05 c0 52 8a 5d f5 ca cd 27 0a 14 1e 28 32"
				.replace(" ", ""));
			ll = new LobLocator(raw);
			assertEquals(BLOB, ll.type());
			assertTrue(ll.secureFile());
			assertEquals(0x16, ll.dataLength());
			assertTrue(ll.dataInRow());

			vector = (Struct) decoder.decode(raw, 0, raw.length, transaction, lobIds);
			assertNull(vector.get("F"));
			assertNull(vector.get("D"));
			assertEquals(0x5, ((ArrayList<Byte>) vector.get("I")).size());
			assertNull(vector.get("B"));
			vector.validate();

			// BINARY, 56 elements
			raw = hexToRaw(
				"00 70 00 01 01 0c 00 80 00 01 00 00 00 01 00 00 00 21 24 a7 00 22 48 90 00 1c 00 00 18 01 db 01 00 10 05 00 00 00 38 80 00 00 00 00 00 00 00 07 03 0b 0d 02 01 13"
				.replace(" ", ""));
			ll = new LobLocator(raw);
			assertEquals(BLOB, ll.type());
			assertTrue(ll.secureFile());
			assertEquals(0x18, ll.dataLength());
			assertTrue(ll.dataInRow());

			vector = (Struct) decoder.decode(raw, 0, raw.length, transaction, lobIds);
			assertNull(vector.get("F"));
			assertNull(vector.get("D"));
			assertNull(vector.get("I"));
			assertEquals(0x38, ((ArrayList<Boolean>) vector.get("B")).size());
			vector.validate();

		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}

	}
}
