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
public class JdbcSinkSqlInsertTest {

	@Test
	public void test() {

		final Field deptNo = new Field("DEPTNO", 0, Schema.INT8_SCHEMA);
		final Field deptId = new Field("DEPTID", 0, Schema.INT8_SCHEMA);
		final List<Field> keyFields = new ArrayList<>();
		keyFields.add(deptNo);
		keyFields.add(deptId);

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

		final Map<String, String> sqlTextsOra = TargetDbSqlUtils.generateSinkSql(
				"DEPT", JdbcSinkConnectionPool.DB_TYPE_ORACLE, pkColumns, allColumns, lobColumns, false);
		final Map<String, String> sqlTextsPg = TargetDbSqlUtils.generateSinkSql(
				"DEPT", JdbcSinkConnectionPool.DB_TYPE_POSTGRESQL, pkColumns, allColumns, lobColumns, false);
		final Map<String, String> sqlTextsMySql = TargetDbSqlUtils.generateSinkSql(
				"DEPT", JdbcSinkConnectionPool.DB_TYPE_MYSQL, pkColumns, allColumns, lobColumns, false);
		final Map<String, String> sqlTextsMsSql = TargetDbSqlUtils.generateSinkSql(
				"DEPT", JdbcSinkConnectionPool.DB_TYPE_MSSQL, pkColumns, allColumns, lobColumns, false);

		final String sinkUpsertSqlOra = sqlTextsOra.get(TargetDbSqlUtils.UPSERT);
		final String sinkUpsertSqlPg = sqlTextsPg.get(TargetDbSqlUtils.UPSERT);
		final String sinkUpsertSqlMySql = sqlTextsMySql.get(TargetDbSqlUtils.UPSERT);
		final String sinkUpsertSqlMsSql = sqlTextsMsSql.get(TargetDbSqlUtils.UPSERT);

		System.out.println("========== Oracle ========================");
		System.out.println(sinkUpsertSqlOra);
		System.out.println();
		System.out.println(sqlTextsOra.get("DEPT_CODE_PDF"));
		System.out.println("========== PostgreSQL ====================");
		System.out.println(sinkUpsertSqlPg);
		System.out.println();
		System.out.println(sqlTextsPg.get("DEPT_CODE_PDF"));
		System.out.println("========== MySQL ==========================");
		System.out.println(sinkUpsertSqlMySql);
		System.out.println();
		System.out.println(sqlTextsMySql.get("DEPT_CODE_PDF"));
		System.out.println("========== MsSQL ==========================");
		System.out.println(sinkUpsertSqlMsSql);
		System.out.println();
		System.out.println(sqlTextsMsSql.get("DEPT_CODE_PDF"));

		assertTrue(sinkUpsertSqlOra.contains("when matched then update"));
		assertTrue(sinkUpsertSqlPg.contains("on conflict"));
		assertTrue(sinkUpsertSqlMySql.contains("on duplicate key update"));
		assertTrue(sinkUpsertSqlMsSql.contains("when not matched then"));

	}

}
