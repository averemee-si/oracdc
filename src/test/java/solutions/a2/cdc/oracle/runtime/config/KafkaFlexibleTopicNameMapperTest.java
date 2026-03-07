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

package solutions.a2.cdc.oracle.runtime.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;

import org.junit.jupiter.api.Test;
import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;


/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class KafkaFlexibleTopicNameMapperTest {

	@Test
	public void test() {
		var props = new HashMap<String, String>();
		var smallTables1 = "SCOTTSmallTables";
		var smallTables2 = "HRSmallTables";
		var tableInSeparateTopic1 = "SALGRADE";
		var tableInSeparateTopic2 = "ASSIGNMENTS";
		props.put("a2.topic.mapper", "solutions.a2.cdc.oracle.runtime.config.KafkaFlexibleTopicNameMapper");
		props.put("a2.map.topic." + smallTables1, "SCOTT.EMP,SCOTT.DEPT");
		props.put("a2.map.topic." + smallTables2, "HRPDB.HR.PEOPLE,HRPDB.HR.JOBS");
		// a2.map.topic.<TOPIC_NAME>=PDB1.OWNER1.TABLE1,PDB2.OWNER2.TABLE2....
		// a2.map.topic.<TOPIC_NAME>=OWNER1.TABLE1,OWNER2.TABLE2....

		final OraCdcSourceConnectorConfig config = new KafkaSourceConnectorConfig(props);
		var tnm = ((KafkaSourceConnectorConfig)config).getTopicNameMapper();
		tnm.configure(config);

		assertEquals(smallTables1, tnm.getTopicName(null, "SCOTT", "DEPT"));
		assertEquals(smallTables1, tnm.getTopicName(null, "SCOTT", "EMP"));
		assertEquals(tableInSeparateTopic1, tnm.getTopicName(null, "SCOTT", tableInSeparateTopic1));
		assertEquals(smallTables2, tnm.getTopicName("HRPDB", "HR", "PEOPLE"));
		assertEquals(smallTables2, tnm.getTopicName("HRPDB", "HR", "JOBS"));
		assertEquals(tableInSeparateTopic2, tnm.getTopicName("HRPDB", "HR", tableInSeparateTopic2));

	}
}
