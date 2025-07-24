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
