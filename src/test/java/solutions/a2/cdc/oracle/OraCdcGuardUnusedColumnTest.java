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

package solutions.a2.cdc.oracle;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static solutions.a2.cdc.oracle.OraCdcColumn.GUARD_COLUMN;
import static solutions.a2.cdc.oracle.OraCdcColumn.UNUSED_COLUMN;

import java.util.regex.Matcher;


/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcGuardUnusedColumnTest {

	@Test
	public void test() {
		final String[] guardColNames = {
				"SYS_NC00007$",
				"SYS_NC00021$",
				"SYS_NC00100$"
		};
		final String[] unusedColNames = {
				"SYS_C00003_25052819:22:53$",
				"SYS_C00201_22021118:15:45$",
				"SYS_C00005_25082809:15:01$"
		};
		final String[] invalidNames = {
				"SYS_C00003_25052819:22:53",
				"SYS_C1234$",
				"SYS_C123456$",
				"SYS_NC1234$",
				"SYS_NC123456$",
				"SYS_D00003_25052819:22:53$",
				"SYS_C00003_25052819:22$",
				"SYS_C00003_$",       
				"SYS_NC00003_$"       
		};

		for (final String columnName : guardColNames) {
			final Matcher guardMatcher = GUARD_COLUMN.matcher(columnName);
			final Matcher unusedMatcher = UNUSED_COLUMN.matcher(columnName);
			assertTrue(guardMatcher.matches());
			assertFalse(unusedMatcher.matches());
		}

		for (final String columnName : unusedColNames) {
			final Matcher guardMatcher = GUARD_COLUMN.matcher(columnName);
			final Matcher unusedMatcher = UNUSED_COLUMN.matcher(columnName);
			assertFalse(guardMatcher.matches());
			assertTrue(unusedMatcher.matches());
		}

		for (final String columnName : invalidNames) {
			final Matcher guardMatcher = GUARD_COLUMN.matcher(columnName);
			final Matcher unusedMatcher = UNUSED_COLUMN.matcher(columnName);
			assertFalse(guardMatcher.matches());
			assertFalse(unusedMatcher.matches());
		}

	}
}
