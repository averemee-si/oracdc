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

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Test;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcNumberMapTest {

	@Test
	public void test() {
		final Map<String, String> props = new HashMap<>();
		props.put("a2.map.number.SCOTT.DEPT.DEPTNO", "SHORT");
		props.put("a2.map.number.SCOTT.EMP.EMPNO", "LONG");
		props.put("a2.map.number.SCOTT.EMP.%NO", "INT");
		props.put("a2.map.number.KAFKA19.SCOTT.EMP.%ID", "SHORT");
		props.put("a2.map.number.KAFKA19.SCOTT.EMP.SAL%", "DECIMAL(38,2)");
		props.put("a2.map.number.AP.AP_INVOICES_ALL.%ID", "INTEGER");
		props.put("a2.map.number.EBS122.AP.AP_INVOICES_ALL.INVOICE_ID", "LONG");

		// a2.number.map.[PDB_NAME.]SCHEMA_NAME.TABLE_NAME.COL_NAME_OR_PATTERN
		// BOOL | BOOLEAN | BYTE | TINYINT | SHORT | SMALLINT | INT | INTEGER | LONG | BIGINT | FLOAT | DOUBLE | DECIMAL([P],S) | NUMERIC([P],S) 

		final OraCdcSourceConnectorConfig config = new OraCdcSourceConnectorConfig(props);

		final List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>>
			redefScottDept = config.tableNumberMapping("SCOTT", "DEPT");
		final List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>>
			redefApInvAll = config.tableNumberMapping("EBS122", "AP", "AP_INVOICES_ALL");


		assertEquals(config.columnNumberMapping(redefScottDept, "DEPTNO").getJdbcType(), Types.SMALLINT);
		assertEquals(config.columnNumberMapping(redefApInvAll, "INVOICE_ID").getJdbcType(), Types.BIGINT);

		assertEquals(config.columnNumberMapping(redefApInvAll, "LEGAL_ENTITY_ID").getJdbcType(), Types.INTEGER);

	}
}
