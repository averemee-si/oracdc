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
import java.time.Period;
import java.time.temporal.TemporalAmount;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import solutions.a2.cdc.oracle.OraDumpDecoder;
import solutions.a2.oracle.jdbc.types.IntervalDayToSecond;
import solutions.a2.oracle.jdbc.types.IntervalYearToMonth;

/**
 * 
 * String representation of Oracle INTERVAL% for Kafka Connect
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraInterval {

	public static final String LOGICAL_NAME = "solutions.a2.cdc.oracle.data.OraInterval";

	public static SchemaBuilder builder() {
		return SchemaBuilder.string()
				.name(LOGICAL_NAME)
				.version(1);
	}

	public static Schema schema() {
		return builder().build();
	}

	public static String fromLogical(final byte[] dumpValue) {
		if (dumpValue == null) {
			throw new DataException("oracle.sql.INTERVALYM/oracle.sql.INTERVALDS representation is null!");
		}
		if (dumpValue.length == IntervalYearToMonth.DATA_LENGTH) {
			// oracle.sql.INTERVALYM
			try {
				return IntervalYearToMonth.toString(dumpValue);
			} catch (SQLException e) {
				throw new DataException("Unable to convert " +
						OraDumpDecoder.toHexString(dumpValue) + " to oracle.sql.INTERVALYM");
			}
		} else if (dumpValue.length == IntervalDayToSecond.DATA_LENGTH) {
			// oracle.sql.INTERVALDS
			try {
				return IntervalDayToSecond.toString(dumpValue);
			} catch (SQLException e) {
				throw new DataException("Unable to convert " +
						OraDumpDecoder.toHexString(dumpValue) + " to oracle.sql.INTERVALDS");
			}
		} else {
			throw new DataException("Unable to convert " +
					OraDumpDecoder.toHexString(dumpValue) + " to " +  LOGICAL_NAME);
		}
	}

	public static TemporalAmount toLogical(final String serialized) {
		if (StringUtils.startsWith(serialized, "P")) {
			if (StringUtils.contains(serialized, "T")) {
				return Duration.parse(serialized);
			} else {
				return Period.parse(serialized);
			}
		} else {
			throw new DataException("'" + serialized + "' is not a valid IS0-8601 time interval!");
		}
	}

}
