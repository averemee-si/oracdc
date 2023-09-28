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

import java.sql.SQLException;
import java.time.Period;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.oracle.jdbc.types.IntervalYearToMonth;

/**
 * 
 * Binary representation of Oracle INTERVALYM for Kafka Connect
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraIntervalYM {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraIntervalYM.class);

	public static final String LOGICAL_NAME = "solutions.a2.cdc.oracle.data.OraIntervalYM";

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
			throw new DataException("oracle.sql.INTERVALYM representation is null!");
		}
		return dumpValue;
	}

	public static Period toLogical(final byte[] dumpValue) {
		final Period period;
		try {
			period = IntervalYearToMonth.toPeriod(dumpValue);
		} catch (SQLException  sqle) {
			if (dumpValue.length == 1 && dumpValue[0] == 0x0) {
				LOGGER.error("Wrong dump value for oracle.sql.INTERVALYM: 'Typ=182 Len=1: 0', assuming null!!!");
				return null;
			} else {
				throw new DataException(sqle);
			}
		}
		return period;
	}

}
