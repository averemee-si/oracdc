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

import solutions.a2.cdc.oracle.runtime.config.GenericSourceConnectorConfig;
import solutions.a2.cdc.oracle.runtime.config.KafkaSourceConnectorConfig;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcRedoFileNameConvertTest {

	@Test
	public void test() {
		OraCdcSourceConnectorConfig configKafka = null;
		OraCdcSourceConnectorConfig configGeneric = null;
		String sourceFile = null;

		final Map<String, String> props = new HashMap<>();
		props.put(
				"a2.redo.filename.convert",
				"/opt/oracle/oradata/archive=/Users/averemee/polyxena/oracle/oradata/KAFKA19/archive,/opt/oracle/oradata/KAFKA19=/Users/averemee/polyxena/oracle/oradata/KAFKA19/KAFKA19");
		configKafka = new KafkaSourceConnectorConfig(props);
		configGeneric = new GenericSourceConnectorConfig(props);

		sourceFile = "/opt/oracle/oradata/archive/1_700_1155880919.dbf";
		assertEquals(
				configKafka.convertRedoFileName(sourceFile, false),
				"/Users/averemee/polyxena/oracle/oradata/KAFKA19/archive/1_700_1155880919.dbf");
		assertEquals(
				configKafka.convertRedoFileName(sourceFile, false),
				configGeneric.convertRedoFileName(sourceFile, false));
		sourceFile = "/opt/oracle/oradata/KAFKA19/log01.redo";
		assertEquals(
				configKafka.convertRedoFileName(sourceFile, false),
				"/Users/averemee/polyxena/oracle/oradata/KAFKA19/KAFKA19/log01.redo");
		assertEquals(
				configKafka.convertRedoFileName(sourceFile, false),
				configGeneric.convertRedoFileName(sourceFile, false));

		props.clear();
		props.put(
				"a2.redo.filename.convert",
				"C:\\ORACLE\\ORADATA\\WINTEST=/C:/ORACLE/ORADATA/WINTEST");
		configKafka = new KafkaSourceConnectorConfig(props);
		configKafka.msWindows(true);
		configGeneric = new GenericSourceConnectorConfig(props);
		configGeneric.msWindows(true);
		sourceFile = "C:\\ORACLE\\ORADATA\\WINTEST\\REDO01.LOG";
		assertEquals(
				configKafka.convertRedoFileName(sourceFile, false),
				"/C:/ORACLE/ORADATA/WINTEST/REDO01.LOG");
		assertEquals(
				configKafka.convertRedoFileName(sourceFile, false),
				configGeneric.convertRedoFileName(sourceFile, false));

		props.clear();
		props.put(
				"a2.redo.filename.convert",
				"C:\\APP\\ORACLE=/C:/APP/ORACLE");
		configKafka = new KafkaSourceConnectorConfig(props);
		configKafka.msWindows(true);
		configGeneric = new GenericSourceConnectorConfig(props);
		configGeneric.msWindows(true);
		sourceFile = "C:\\APP\\ORACLE\\FAST_RECOVERY_AREA\\MATADOR\\ARCHIVELOG\\2025_03_07\\O1_MF_1_601466_MWO3L9SX_.ARC";
		assertEquals(
				configKafka.convertRedoFileName(sourceFile, false),
				"/C:/APP/ORACLE/FAST_RECOVERY_AREA/MATADOR/ARCHIVELOG/2025_03_07/O1_MF_1_601466_MWO3L9SX_.ARC");
		assertEquals(
				configKafka.convertRedoFileName(sourceFile, false),
				configGeneric.convertRedoFileName(sourceFile, false));

		props.clear();
		configKafka = new KafkaSourceConnectorConfig(props);
		configGeneric = new GenericSourceConnectorConfig(props);
		sourceFile = "/data/archive/1_2636_1185479976.dbf";
		assertEquals(
				configKafka.convertRedoFileName(sourceFile, true),
				"1_2636_1185479976.dbf");
		assertEquals(
				configKafka.convertRedoFileName(sourceFile, true),
				configGeneric.convertRedoFileName(sourceFile, true));

	}
}
