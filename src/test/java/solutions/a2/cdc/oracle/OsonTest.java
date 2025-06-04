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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static solutions.a2.oracle.internals.LobLocator.BLOB;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import oracle.sql.json.OracleJsonException;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonParser;
import oracle.sql.json.OracleJsonValue;
import solutions.a2.oracle.internals.LobLocator;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OsonTest {

	@Test
	public void test() {

		LobLocator ll;
		byte[] oson;
		
		oson = hexToRaw(
				"00 70 00 01 01 0c 00 80 00 01 00 00 00 01 00 00 00 08 a4 87 00 41 48 90 00 3b 00 00 37 01 ff 4a 5a 01 21 06 02 00 0f 00 15 00 00 91 d8 00 00 00 06 05 66 72 75 69 74 08 71 75 61 6e 74 69 74 79 84 02 01 02 00 08 00 12 09 70 69 6e 65 61 70 70 6c 65 21 c1 15"
				.replace(" ", ""));
		ll = new LobLocator(oson);
		assertEquals(BLOB, ll.type());
		assertTrue(ll.secureFile());
		assertEquals(0x37, ll.dataLength());
		assertTrue(ll.dataInRow());

		ByteBuffer bb = ByteBuffer.wrap(oson, oson.length - ll.dataLength(), ll.dataLength());
		OracleJsonFactory factory = new OracleJsonFactory();
		try (OracleJsonParser parser = factory.createJsonBinaryParser(bb)) {
			parser.next();
			OracleJsonValue value = parser.getValue();
			assertEquals(value.toString(), "{\"fruit\":\"pineapple\",\"quantity\":20}");
		} catch(OracleJsonException oje) {
			oje.printStackTrace();
		}
		
	}
}
