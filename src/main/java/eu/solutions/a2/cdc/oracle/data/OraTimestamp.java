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

import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import eu.solutions.a2.cdc.oracle.OraDumpDecoder;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;

/**
 * 
 * Representation of Oracle TIMESTAMP% for Kafka Connect
 * 
 * @author averemee
 *
 */
public class OraTimestamp {

	public static final String LOGICAL_NAME = "eu.solutions.a2.cdc.oracle.data.OraTimestamp";
	public static final DateTimeFormatter ISO_8601_FMT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

	public static SchemaBuilder builder() {
		return SchemaBuilder.string()
				.name(LOGICAL_NAME)
				.version(1);
	}

	public static Schema schema() {
		return builder().build();
	}

	public static String fromLogical(final byte[] dumpValue, final boolean isLocal, Connection connection) {
		if (dumpValue == null) {
			throw new DataException("oracle.sql.TIMESTAMPTZ/oracle.sql.TIMESTAMPTZ representation is null!");
		}
		final OffsetDateTime odt;
		if (isLocal) {
			try {
				odt = TIMESTAMPLTZ.toOffsetDateTime(connection, dumpValue);
			} catch (SQLException sqle) {
				throw new DataException("Unable to convert " +
							OraDumpDecoder.toHexString(dumpValue) +
							" to oracle.sql.TIMESTAMPLTZ !", sqle);
			}
		} else {
			try {
				odt = TIMESTAMPTZ.toOffsetDateTime(connection, dumpValue);
			} catch (SQLException sqle) {
				throw new DataException("Unable to convert " +
							OraDumpDecoder.toHexString(dumpValue) +
							" to oracle.sql.TIMESTAMPTZ !", sqle);
			}
		}
		return ISO_8601_FMT.format(odt);
	}

	public static OffsetDateTime toLogical(final String serialized) {
		final OffsetDateTime odt;
		try {
			odt = OffsetDateTime.parse(serialized, ISO_8601_FMT);
		} catch (DateTimeParseException  dtpe) {
			throw new DataException(dtpe);
		}
		return odt;
	}

}
