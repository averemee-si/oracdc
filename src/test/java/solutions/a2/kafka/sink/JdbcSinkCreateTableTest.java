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

package solutions.a2.kafka.sink;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import solutions.a2.cdc.TargetDbSqlUtils;
import solutions.a2.cdc.oracle.data.OraBlob;
import solutions.a2.cdc.oracle.data.OraXml;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class JdbcSinkCreateTableTest {
	
	private static final int PK_STRING_LENGTH_DEFAULT = 25;

	@Test
	public void test() {

		final Field deptNo = new Field("DEPTNO", 0, Schema.INT8_SCHEMA);
		final Field deptKey = new Field("DEPT_KEY", 0, Schema.STRING_SCHEMA);
		final List<Field> keyFields = new ArrayList<>();
		keyFields.add(deptNo);
		keyFields.add(deptKey);

		final Field dName = new Field("DNAME", 0, Schema.OPTIONAL_STRING_SCHEMA);
		final Field loc = new Field("LOC", 1, Schema.OPTIONAL_STRING_SCHEMA);
		final List<Field> valueFields = new ArrayList<>();
		valueFields.add(dName);
		valueFields.add(loc);

		final Field deptCodePdf = new Field("DEPT_CODE_PDF", 2, OraBlob.builder().build());
		final Field deptCodeDocx = new Field("DEPT_CODE_DOCX", 2, OraBlob.builder().build());
		final Field deptCodeXml = new Field("DEPT_CODE_XML", 2, OraXml.builder().build());
		final List<Field> lobFields = new ArrayList<>();
		lobFields.add(deptCodePdf);
		lobFields.add(deptCodeDocx);
		lobFields.add(deptCodeXml);

		final List<JdbcSinkColumn> allColumns = new ArrayList<>();
		final Map<String, JdbcSinkColumn> pkColumns = new HashMap<>();
		final Map<String, Object> lobColumns = new HashMap<>();

		for (Field field : keyFields) {
			try {
				final var column = new JdbcSinkColumn(field, true);
				pkColumns.put(column.name(), column);
			} catch (SQLException sqle) {
				sqle.printStackTrace();
			}
		}
		// Only non PK columns!!!
		for (Field field : valueFields) {
			if (!pkColumns.containsKey(field.name())) {
				try {
					final var column = new JdbcSinkColumn(field, false);
					allColumns.add(column);
				} catch (SQLException sqle) {
					sqle.printStackTrace();
				}
			}
		}
		for (Field field : lobFields) {
			try {
				final var column = new JdbcSinkColumn(field, false);
				lobColumns.put(column.name(), column);
			} catch (SQLException sqle) {
				sqle.printStackTrace();
			}
		}

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

		List<String> createScottDeptClickhouse = TargetDbSqlUtils.createTableSql(
				"DEPT", JdbcSinkConnectionPool.DB_TYPE_CLICKHOUSE,
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
		System.out.println("++++++++++ Clickhouse +++++++++++++++++++++");
		System.out.println(createScottDeptClickhouse.get(0));

		assertTrue(createScottDeptOra.get(0).contains("DEPTNO NUMBER(3)"));
		assertTrue(createScottDeptPg.get(0).contains("DEPTNO smallint"));
		assertTrue(createScottDeptMySql.get(0).contains("DEPTNO tinyint"));
		assertTrue(createScottDeptMsSql.get(0).contains("varbinary(max)"));
		assertTrue(createScottDeptClickhouse.get(0).contains("ENGINE = ReplacingMergeTree"));

	}

}
