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

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Test;

import solutions.a2.cdc.oracle.runtime.config.GenericSourceConnectorConfig;
import solutions.a2.cdc.oracle.runtime.config.KafkaSourceConnectorConfig;

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

		final OraCdcSourceConnectorConfig configKafka = new KafkaSourceConnectorConfig(props);
		final OraCdcSourceConnectorConfig configGeneric = new GenericSourceConnectorConfig(props);

		final List<Triple<List<Pair<String, OraCdcColumn>>, Map<String, OraCdcColumn>, List<Pair<String, OraCdcColumn>>>>
			redefScottDept = configKafka.tableNumberMapping("SCOTT", "DEPT");
		final List<Triple<List<Pair<String, OraCdcColumn>>, Map<String, OraCdcColumn>, List<Pair<String, OraCdcColumn>>>>
			redefApInvAll = configKafka.tableNumberMapping("EBS122", "AP", "AP_INVOICES_ALL");

		assertEquals(redefScottDept, configGeneric.tableNumberMapping("SCOTT", "DEPT"));
		assertEquals(redefApInvAll, configGeneric.tableNumberMapping("EBS122", "AP", "AP_INVOICES_ALL"));

		assertEquals(configKafka.columnNumberMapping(redefScottDept, "DEPTNO").jdbcType(), Types.SMALLINT);
		assertEquals(configKafka.columnNumberMapping(redefApInvAll, "INVOICE_ID").jdbcType(), Types.BIGINT);

		assertEquals(configKafka.columnNumberMapping(redefApInvAll, "LEGAL_ENTITY_ID").jdbcType(), Types.INTEGER);

	}
}
