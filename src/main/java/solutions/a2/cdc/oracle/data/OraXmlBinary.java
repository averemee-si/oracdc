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

package solutions.a2.cdc.oracle.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * 
 * Representation of Oracle Binary XML (default storage for XMLType) for Kafka Connect
 * https://docs.oracle.com/en/database/oracle/oracle-database/21/adxdb/intro-to-XML-DB.html#GUID-B8507F44-B010-4384-94E6-101131DD7D88
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraXmlBinary {

	public static final String LOGICAL_NAME = "solutions.a2.cdc.oracle.data.OraXmlBinary";

	public static SchemaBuilder builder() {
		return SchemaBuilder.string()
				.optional()
				.name(LOGICAL_NAME)
				.version(1)
				.doc("Oracle binary XML (BLOB)");
	}

	public static Schema schema() {
		return builder().build();
	}

}
