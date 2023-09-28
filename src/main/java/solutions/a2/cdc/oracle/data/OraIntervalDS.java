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
import java.time.Duration;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.oracle.jdbc.types.IntervalDayToSecond;

/**
 * 
 * Binary representation of Oracle INTERVALDS for Kafka Connect
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraIntervalDS {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraIntervalDS.class);

	public static final String LOGICAL_NAME = "solutions.a2.cdc.oracle.data.OraIntervalDS";

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
			throw new DataException("oracle.sql.INTERVALDS representation is null!");
		}
		return dumpValue;
	}

	public static Duration toLogical(final byte[] dumpValue) {
		final Duration duration;
		try {
			duration = IntervalDayToSecond.toDuration(dumpValue);
		} catch (SQLException  sqle) {
			if (dumpValue.length == 1 && dumpValue[0] == 0x0) {
				LOGGER.error("Wrong dump value for oracle.sql.INTERVALDS: 'Typ=183 Len=1: 0', assuming null!!!");
				return null;
			} else {
				throw new DataException(sqle);
			}
		}
		return duration;
	}

}
