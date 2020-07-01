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

package eu.solutions.a2.cdc.oracle;

import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import eu.solutions.a2.cdc.oracle.utils.TargetDbSqlUtils;

public class OraCdcSinkCreateTableTest {

	@Test
	public void test() {

		final Field deptNo = new Field("DEPTNO", 0, Schema.INT8_SCHEMA);
		final List<Field> keyFields = new ArrayList<>();
		keyFields.add(deptNo);

		final Field dName = new Field("DNAME", 0, Schema.OPTIONAL_STRING_SCHEMA);
		final Field loc = new Field("LOC", 1, Schema.OPTIONAL_STRING_SCHEMA);
		final List<Field> valueFields = new ArrayList<>();
		valueFields.add(dName);
		valueFields.add(loc);

		final List<OraColumn> allColumns = new ArrayList<>();
		final Map<String, OraColumn> pkColumns = new HashMap<>();

		for (Field field : keyFields) {
			try {
				final OraColumn column = new OraColumn(field, true);
				pkColumns.put(column.getColumnName(), column);
			} catch (SQLException sqle) {
				sqle.printStackTrace();
			}
		}
		// Only non PK columns!!!
		for (Field field : valueFields) {
			if (!pkColumns.containsKey(field.name())) {
				try {
					final OraColumn column = new OraColumn(field, false);
					allColumns.add(column);
				} catch (SQLException sqle) {
					sqle.printStackTrace();
				}
			}
		}

		String createScottDeptOra = TargetDbSqlUtils.createTableSql(
				"DEPT", OraCdcJdbcSinkConnectionPool.DB_TYPE_ORACLE, pkColumns, allColumns); 
		String createScottDeptPg = TargetDbSqlUtils.createTableSql(
				"DEPT", OraCdcJdbcSinkConnectionPool.DB_TYPE_POSTGRESQL, pkColumns, allColumns); 
		String createScottDeptMySql = TargetDbSqlUtils.createTableSql(
				"DEPT", OraCdcJdbcSinkConnectionPool.DB_TYPE_MYSQL, pkColumns, allColumns);

		assertTrue(createScottDeptOra.contains("DEPTNO NUMBER(3)"));
		assertTrue(createScottDeptPg.contains("DEPTNO smallint"));
		assertTrue(createScottDeptMySql.contains("DEPTNO tinyint"));

	}

}
