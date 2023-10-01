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
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import solutions.a2.cdc.oracle.OraDumpDecoder;
import solutions.a2.oracle.jdbc.types.OracleTimestamp;
import solutions.a2.oracle.jdbc.types.TimestampWithTimeZone;

/**
 * 
 * Representation of Oracle TIMESTAMP% for Kafka Connect
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraTimestamp {

	public static final String LOGICAL_NAME = "solutions.a2.cdc.oracle.data.OraTimestamp";
	public static final DateTimeFormatter ISO_8601_FMT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

	public static SchemaBuilder builder() {
		return SchemaBuilder.string()
				.name(LOGICAL_NAME)
				.version(1);
	}

	public static Schema schema() {
		return builder().build();
	}

	public static String fromLogical(final byte[] dumpValue, final boolean isLocal, ZoneId dbTimeZone) {
		if (dumpValue == null) {
			throw new DataException("oracle.sql.TIMESTAMPTZ/oracle.sql.TIMESTAMPTZ representation is null!");
		}
		final ZonedDateTime zdt;
		if (isLocal) {
			try {
				zdt = OracleTimestamp.toZonedDateTime(dumpValue, dbTimeZone);
			} catch (SQLException sqle) {
				throw new DataException("Unable to convert " +
							OraDumpDecoder.toHexString(dumpValue) +
							" to oracle.sql.TIMESTAMPLTZ !", sqle);
			}
		} else {
			try {				
				zdt = TimestampWithTimeZone.toZonedDateTime(dumpValue);
			} catch (SQLException sqle) {
				throw new DataException("Unable to convert " +
							OraDumpDecoder.toHexString(dumpValue) +
							" to oracle.sql.TIMESTAMPTZ !", sqle);
			}
		}
		return ISO_8601_FMT.format(zdt);
	}

	public static OffsetDateTime toLogical(final String serialized) {
		final OffsetDateTime odt;
		try {
			odt = ZonedDateTime.parse(serialized, ISO_8601_FMT).toOffsetDateTime();
		} catch (DateTimeParseException  dtpe) {
			throw new DataException(dtpe);
		}
		return odt;
	}

}
