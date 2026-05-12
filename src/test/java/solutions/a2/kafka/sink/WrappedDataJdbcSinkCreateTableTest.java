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

package solutions.a2.kafka.sink;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_INT8_SCHEMA;
import static solutions.a2.cdc.oracle.runtime.data.KafkaWrappedSchemas.WRAPPED_OPT_STRING_SCHEMA;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import solutions.a2.cdc.TargetDbSqlUtils;

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
