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

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcRedoFileNameConvertTest {

	@Test
	public void test() {
		OraCdcSourceConnectorConfig config = null;
		final Map<String, String> props = new HashMap<>();
		props.put(
				"a2.redo.filename.convert",
				"/opt/oracle/oradata/archive=/Users/averemee/polyxena/oracle/oradata/KAFKA19/archive,/opt/oracle/oradata/KAFKA19=/Users/averemee/polyxena/oracle/oradata/KAFKA19/KAFKA19");
		config = new OraCdcSourceConnectorConfig(props);

		assertEquals(
				config.convertRedoFileName("/opt/oracle/oradata/archive/1_700_1155880919.dbf"),
				"/Users/averemee/polyxena/oracle/oradata/KAFKA19/archive/1_700_1155880919.dbf");
		assertEquals(
				config.convertRedoFileName("/opt/oracle/oradata/KAFKA19/log01.redo"),
				"/Users/averemee/polyxena/oracle/oradata/KAFKA19/KAFKA19/log01.redo");

		props.clear();
		props.put(
				"a2.redo.filename.convert",
				"C:\\ORACLE\\ORADATA\\WINTEST=/C:/ORACLE/ORADATA/WINTEST");
		config = new OraCdcSourceConnectorConfig(props);
		config.msWindows(true);

		assertEquals(
				config.convertRedoFileName("C:\\ORACLE\\ORADATA\\WINTEST\\REDO01.LOG"),
				"/C:/ORACLE/ORADATA/WINTEST/REDO01.LOG");
	}
}
