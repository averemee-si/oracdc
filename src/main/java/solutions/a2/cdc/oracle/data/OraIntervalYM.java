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
