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

package solutions.a2.kafka.sink;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_INT8_SCHEMA;
import static solutions.a2.cdc.oracle.data.WrappedSchemas.WRAPPED_OPT_STRING_SCHEMA;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class WrappedDataJdbcSinkCreateTableTest {

	private static final int PK_STRING_LENGTH_DEFAULT = 25;

	@Test
	public void test() {

		final var keySchema = SchemaBuilder
				.struct()
				.name("FREEPDB1_SCOTT_DEPT_Key")
				.field("ORA_ROW_ID", STRING_SCHEMA)
				.build();
		final var valueSchema = SchemaBuilder
				.struct()
				.optional()
				.name("FREEPDB1_SCOTT_DEPT_Value")
				.field("DEPTNO", WRAPPED_INT8_SCHEMA)
				.field("DNAME", WRAPPED_OPT_STRING_SCHEMA)
				.field("LOC", WRAPPED_OPT_STRING_SCHEMA)
				.field("LAST_UPDATE_DATE", Timestamp.builder().required().build())
				.build();

		final var keyStruct = new Struct(keySchema);
		keyStruct.put("ORA_ROW_ID", "AAATCcAAxAACAAyAAB");
		
		final var valueStruct = new Struct(valueSchema);
		valueStruct.put("DNAME", new Struct(WRAPPED_OPT_STRING_SCHEMA).put("V", "ACCOUNTING"));
		valueStruct.put("LOC", new Struct(WRAPPED_OPT_STRING_SCHEMA).put("V", "NEW YORK"));
		valueStruct.put("LAST_UPDATE_DATE", new java.util.Date(System.currentTimeMillis()));
		assertDoesNotThrow(() -> {
			valueStruct.validate();
		});

		final var sinkRecord = new SinkRecord("SCOTT_DEPT", 0, keySchema, keyStruct, valueSchema, valueStruct, 0);

		final List<JdbcSinkColumn> allColumns = new ArrayList<>();
		final Map<String, JdbcSinkColumn> pkColumns = new HashMap<>();
		final Map<String, Object> lobColumns = new HashMap<>();

		for (var field : sinkRecord.keySchema().fields())
			assertDoesNotThrow(() -> {
				pkColumns.put(field.name(), new JdbcSinkColumn(field, true));
			});

		for (var field : sinkRecord.valueSchema().fields())
			if (!pkColumns.containsKey(field.name()))
				assertDoesNotThrow(() -> {
					allColumns.add(new JdbcSinkColumn(field, false));
				});

		List<String> createScottDeptOra = TargetDbSqlUtils.createTableSql(
				"DEPT", JdbcSinkConnectionPool.DB_TYPE_ORACLE,
				PK_STRING_LENGTH_DEFAULT,
				pkColumns, allColumns, lobColumns); 
		List<String> createScottDeptPg = TargetDbSqlUtils.createTableSql(
				"DEPT", JdbcSinkConnectionPool.DB_TYPE_POSTGRESQL,
				PK_STRING_LENGTH_DEFAULT,
				pkColumns, allColumns, lobColumns); 
		List<String> createScottDeptMySql = TargetDbSqlUtils.createTableSql(
				"DEPT", JdbcSinkConnectionPool.DB_TYPE_MYSQL,
				PK_STRING_LENGTH_DEFAULT,
				pkColumns, allColumns, lobColumns);
		List<String> createScottDeptMsSql = TargetDbSqlUtils.createTableSql(
				"DEPT", JdbcSinkConnectionPool.DB_TYPE_MSSQL,
				PK_STRING_LENGTH_DEFAULT,
				pkColumns, allColumns, lobColumns);

		System.out.println("++++++++++ Oracle ++++++++++++++++++++++++");
		System.out.println(createScottDeptOra.get(0));
		System.out.println("++++++++++ PostgreSQL +++++++++++++++++++++");
		System.out.println(createScottDeptPg.get(0));
		if (createScottDeptPg.size() > 1) {
			for (int i = 1; i < createScottDeptPg.size(); i++) {
				System.out.println("\t" + createScottDeptPg.get(i));
			}
		}
		System.out.println("++++++++++ MySQL ++++++++++++++++++++++++++");
		System.out.println(createScottDeptMySql.get(0));
		System.out.println("++++++++++ MsSQL ++++++++++++++++++++++++++");
		System.out.println(createScottDeptMsSql.get(0));

		assertTrue(createScottDeptOra.get(0).contains("DEPTNO NUMBER(3)"));
		assertTrue(createScottDeptPg.get(0).contains("DEPTNO smallint"));
		assertTrue(createScottDeptMySql.get(0).contains("DEPTNO tinyint"));
		assertTrue(createScottDeptMsSql.get(0).contains("DEPTNO tinyint"));

	}

}
