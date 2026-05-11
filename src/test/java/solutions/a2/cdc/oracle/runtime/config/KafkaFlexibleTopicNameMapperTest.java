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

 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
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
