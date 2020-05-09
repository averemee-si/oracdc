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

import java.math.BigDecimal;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.sql.NUMBER;

/**
 * 
 * Representation of Oracle NUMBER for Kafka Connect
 * 
 * For more information about Oracle NUMBER format:
 *	   https://gotodba.com/2015/03/24/how-are-numbers-saved-in-oracle/
 *	   https://www.orafaq.com/wiki/Number
 *     https://support.oracle.com/rs?type=doc&id=1031902.6
 *       How Does Oracle Store Internal Numeric Data? (Doc ID 1031902.6)
 *     https://docs.oracle.com/en/database/oracle/oracle-database/12.2/jajdb/index.html?oracle/sql/NUMBER.html
 * 
 * @author averemee
 *
 */
public class OraNumber {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraNumber.class);

	public static final String LOGICAL_NAME = "eu.solutions.a2.cdc.oracle.data.OraNumber";

	public static SchemaBuilder builder() {
		return SchemaBuilder.bytes()
				.name(LOGICAL_NAME)
				.version(1);
	}

	public static Schema schema() {
		return builder().build();
	}

	public static byte[] fromLogical(final Schema schema, final byte[] dumpValue) {
		if (dumpValue == null) {
			throw new DataException("oracle.sql.NUMBER representation is null!");
		}
		return dumpValue;
	}

	public static BigDecimal toLogical(final byte[] dumpValue) {
		final BigDecimal bd;
		try {
			bd = new NUMBER(dumpValue).bigDecimalValue();
		} catch (SQLException  sqle) {
			if (dumpValue.length == 1 && dumpValue[0] == 0x0) {
				LOGGER.error("Wrong dump value for Oracle NUMBER: 'Typ=2 Len=1: 0', assuming null!!!");
				return null;
			} else {
				throw new DataException(sqle);
			}
		}
		return bd;
	}

}
