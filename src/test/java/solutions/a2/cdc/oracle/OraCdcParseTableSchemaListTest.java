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

 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import solutions.a2.cdc.oracle.utils.OraSqlUtils;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcParseTableSchemaListTest {

	@Test
	public void test() {
		String case1 = "BEWWFR. EXAMPLE_TEST_ORA_CDC,";
		assertEquals(" and ((O.OWNER='BEWWFR' and O.OBJECT_NAME='EXAMPLE_TEST_ORA_CDC'))",
				OraSqlUtils.parseTableSchemaList(
						false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, Arrays.asList(case1.split("\\s*,\\s*"))));

		String case2 = "AP.% , SCOTT.TEST_IOT%";
		assertEquals(" and ((O.OWNER='AP') or (O.OWNER='SCOTT' and O.OBJECT_NAME LIKE 'TEST_IOT%'))",
				OraSqlUtils.parseTableSchemaList(
						false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, Arrays.asList(case2.split("\\s*,\\s*"))));

		String case3 = "AP.% , SCOTT.TEST_IOT%,BEWWFR. EXAMPLE_TEST_ORA_CDC,";
		assertEquals(" and ((O.OWNER='AP') or (O.OWNER='SCOTT' and O.OBJECT_NAME LIKE 'TEST_IOT%') or (O.OWNER='BEWWFR' and O.OBJECT_NAME='EXAMPLE_TEST_ORA_CDC'))",
				OraSqlUtils.parseTableSchemaList(
						false, OraSqlUtils.MODE_WHERE_ALL_OBJECTS, Arrays.asList(case3.split("\\s*,\\s*"))));
	}
}
