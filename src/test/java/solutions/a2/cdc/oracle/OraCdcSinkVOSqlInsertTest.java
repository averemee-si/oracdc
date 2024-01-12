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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import solutions.a2.cdc.oracle.utils.TargetDbSqlUtils;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcSinkVOSqlInsertTest {

	@Test
	public void test() {

		final Field grade = new Field("GRADE", 0, Schema.OPTIONAL_FLOAT64_SCHEMA);
		final Field losal = new Field("LOSAL", 1, Schema.OPTIONAL_FLOAT64_SCHEMA);
		final Field hisal = new Field("HISAL", 1, Schema.OPTIONAL_FLOAT64_SCHEMA);
		final List<Field> valueFields = new ArrayList<>();
		valueFields.add(grade);
		valueFields.add(losal);
		valueFields.add(hisal);


		final List<OraColumn> allColumns = new ArrayList<>();
		final Map<String, OraColumn> pkColumns = new HashMap<>();
		final Map<String, Object> lobColumns = new HashMap<>();

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

		final Map<String, String> sqlTextsOra = TargetDbSqlUtils.generateSinkSql(
				"SALGRADE", OraCdcJdbcSinkConnectionPool.DB_TYPE_ORACLE, pkColumns, allColumns, lobColumns);
		final Map<String, String> sqlTextsPg = TargetDbSqlUtils.generateSinkSql(
				"SALGRADE", OraCdcJdbcSinkConnectionPool.DB_TYPE_POSTGRESQL, pkColumns, allColumns, lobColumns);
		final Map<String, String> sqlTextsMySql = TargetDbSqlUtils.generateSinkSql(
				"SALGRADE", OraCdcJdbcSinkConnectionPool.DB_TYPE_MYSQL, pkColumns, allColumns, lobColumns);
		final Map<String, String> sqlTextsMsSql = TargetDbSqlUtils.generateSinkSql(
				"SALGRADE", OraCdcJdbcSinkConnectionPool.DB_TYPE_MSSQL, pkColumns, allColumns, lobColumns);

		final String sinkInsertSqlOra = sqlTextsOra.get(TargetDbSqlUtils.INSERT);
		final String sinkInsertSqlPg = sqlTextsPg.get(TargetDbSqlUtils.INSERT);
		final String sinkInsertSqlMySql = sqlTextsMySql.get(TargetDbSqlUtils.INSERT);
		final String sinkInsertSqlMsSql = sqlTextsMsSql.get(TargetDbSqlUtils.INSERT);

		System.out.println("========== Oracle ========================");
		System.out.println(sinkInsertSqlOra);
		System.out.println();
		System.out.println("========== PostgreSQL ====================");
		System.out.println(sinkInsertSqlPg);
		System.out.println();
		System.out.println("========== MySQL ==========================");
		System.out.println(sinkInsertSqlMySql);
		System.out.println();
		System.out.println("========== MsSQL ==========================");
		System.out.println(sinkInsertSqlMsSql);
		System.out.println();

		assertTrue(sinkInsertSqlOra.contains("insert into SALGRADE(GRADE, LOSAL, HISAL)"));
		assertTrue(sinkInsertSqlPg.contains("insert into SALGRADE(GRADE, LOSAL, HISAL)"));
		assertTrue(sinkInsertSqlMySql.contains("insert into SALGRADE(GRADE, LOSAL, HISAL)"));
		assertTrue(sinkInsertSqlMsSql.contains("insert into SALGRADE(GRADE, LOSAL, HISAL)"));

	}

}
