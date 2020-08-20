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

package eu.solutions.a2.cdc.oracle.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import eu.solutions.a2.cdc.oracle.utils.GzipUtil;

/**
 * 
 * Representation of Oracle BLOB/CLOB for Kafka Connect
 * 
 * 
 * @author averemee
 *
 */
public class OraClob {

	public static final String LOGICAL_NAME = "eu.solutions.a2.cdc.oracle.data.OraClob";

	public static SchemaBuilder builder() {
		return SchemaBuilder.bytes()
				.optional()
				.name(LOGICAL_NAME)
				.version(1)
				.doc("Oracle CLOB");
	}

	public static Schema schema() {
		return builder().build();
	}

	/**
	 * 
	 * @param schema
	 * @param blobData 
	 * @return byte[] with compressed CLOB data
	 */
	public static byte[] fromLogical(final Schema schema, final String clobData) {
		if (clobData != null && clobData.length() > 0) {
			return GzipUtil.compress(clobData);
		} else {
			return null;
		}
	}

}
