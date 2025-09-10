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

package solutions.a2.cdc.oracle;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static solutions.a2.cdc.oracle.OraColumn.GUARD_COLUMN;
import static solutions.a2.cdc.oracle.OraColumn.UNUSED_COLUMN;

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
