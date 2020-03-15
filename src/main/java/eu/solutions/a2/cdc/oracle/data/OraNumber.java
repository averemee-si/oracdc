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
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

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

	public static final String LOGICAL_NAME = "eu.solutions.a2.cdc.oracle.data.OraNumber";
	public static final String VALUE_FIELD = "DUMP";

	public static SchemaBuilder builder() {
		return SchemaBuilder.struct()
				.name(LOGICAL_NAME)
				.version(1)
				.field(VALUE_FIELD, Schema.BYTES_SCHEMA);
	}

	public static Schema schema() {
		return builder().build();
	}

	public static Struct fromLogical(final Schema schema, final byte[] dumpValue) {
		if (dumpValue == null) {
			throw new DataException("oracle.sql.NUMBER representation is null!");
		}
		final Struct result = new Struct(schema);
		result.put(VALUE_FIELD, dumpValue);
		return result;
	}

	public static Struct fromLogical(final byte[] dumpValue) {
		if (dumpValue == null) {
			throw new DataException("oracle.sql.NUMBER representation is null!");
		}
		final Struct result = new Struct(builder().build());
		result.put(VALUE_FIELD, dumpValue);
		return result;
	}

	public static BigDecimal toLogical(final Struct struct) {
		final BigDecimal bd;
		try {
			bd = new NUMBER(struct.getBytes(VALUE_FIELD)).bigDecimalValue();
		} catch (SQLException  sqle) {
			throw new DataException(sqle);
		}
		return bd;
	}

}
