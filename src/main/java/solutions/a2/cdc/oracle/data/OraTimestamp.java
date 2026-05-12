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
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import solutions.a2.oracle.jdbc.types.OracleTimestamp;
import solutions.a2.oracle.jdbc.types.TimestampWithTimeZone;

import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;


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
		return fromLogical(dumpValue, 0, dumpValue.length, isLocal, dbTimeZone);
	}

	public static String fromLogical(final byte[] dumpValue, final int offset, final int length, final boolean isLocal, ZoneId dbTimeZone) {
		if (dumpValue == null) {
			throw new DataException("oracle.sql.TIMESTAMPTZ/oracle.sql.TIMESTAMPTZ representation is null!");
		}
		final ZonedDateTime zdt;
		if (isLocal) {
			try {
				zdt = OracleTimestamp.toZonedDateTime(dumpValue, offset, length, dbTimeZone);
			} catch (SQLException sqle) {
				throw new DataException("Unable to convert '" +
							rawToHex(dumpValue) +
							"' to oracle.sql.TIMESTAMPLTZ !", sqle);
			}
		} else {
			try {				
				zdt = TimestampWithTimeZone.toZonedDateTime(dumpValue, offset);
			} catch (SQLException sqle) {
				throw new DataException("Unable to convert '" +
							rawToHex(dumpValue) +
							"' to oracle.sql.TIMESTAMPTZ !", sqle);
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
