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
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle.data;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;

import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import solutions.a2.oracle.jdbc.types.IntervalDayToSecond;
import solutions.a2.oracle.jdbc.types.IntervalYearToMonth;

import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

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
				throw new DataException("Unable to convert '" +
						rawToHex(dumpValue) + "' to oracle.sql.INTERVALYM");
			}
		} else if (dumpValue.length == IntervalDayToSecond.DATA_LENGTH) {
			// oracle.sql.INTERVALDS
			try {
				return IntervalDayToSecond.toString(dumpValue);
			} catch (SQLException e) {
				throw new DataException("Unable to convert '" +
						rawToHex(dumpValue) + "' to oracle.sql.INTERVALDS");
			}
		} else {
			throw new DataException("Unable to convert '" +
					rawToHex(dumpValue) + "' to " +  LOGICAL_NAME);
		}
	}

	public static TemporalAmount toLogical(final String serialized) {
		if (Strings.CS.startsWith(serialized, "P")) {
			if (Strings.CS.contains(serialized, "T")) {
				return Duration.parse(serialized);
			} else {
				return Period.parse(serialized);
			}
		} else {
			throw new DataException("'" + serialized + "' is not a valid IS0-8601 time interval!");
		}
	}

}
