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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import solutions.a2.cdc.oracle.OraColumn;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class JdbcSinkSqlDeleteTest {

	@Test
	public void test() {

		final Field deptNo = new Field("DEPTNO", 0, Schema.INT8_SCHEMA);
		final Field deptId = new Field("DEPTID", 0, Schema.INT8_SCHEMA);
		final List<Field> keyFields = new ArrayList<>();
		keyFields.add(deptNo);
		keyFields.add(deptId);

		final List<OraColumn> allColumns = new ArrayList<>();
		final Map<String, OraColumn> pkColumns = new HashMap<>();
		final Map<String, Object> lobColumns = new HashMap<>();

		for (Field field : keyFields) {
			try {
				final OraColumn column = new OraColumn(field, true, true);
				pkColumns.put(column.getColumnName(), column);
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

		final String sinkDeleteSqlOra = sqlTextsOra.get(TargetDbSqlUtils.DELETE);
		final String sinkDeleteSqlPg = sqlTextsPg.get(TargetDbSqlUtils.DELETE);
		final String sinkDeleteSqlMySql = sqlTextsMySql.get(TargetDbSqlUtils.DELETE);
		final String sinkDeleteSqlMsSql = sqlTextsMsSql.get(TargetDbSqlUtils.DELETE);

		System.out.println("========== Oracle ========================");
		System.out.println(sinkDeleteSqlOra);
		System.out.println("========== PostgreSQL ====================");
		System.out.println(sinkDeleteSqlPg);
		System.out.println("========== MySQL ==========================");
		System.out.println(sinkDeleteSqlMySql);
		System.out.println("========== MsSQL ==========================");
		System.out.println(sinkDeleteSqlMsSql);

		assertTrue(sinkDeleteSqlOra.equals("delete from DEPT where DEPTID=? and DEPTNO=?"));
		assertTrue(sinkDeleteSqlPg.equals("delete from DEPT where DEPTID=? and DEPTNO=?"));
		assertTrue(sinkDeleteSqlMySql.equals("delete from DEPT where DEPTID=? and DEPTNO=?"));
		assertTrue(sinkDeleteSqlMsSql.equals("delete from DEPT where DEPTID=? and DEPTNO=?"));

	}

}
