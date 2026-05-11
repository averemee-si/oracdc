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

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class JdbcSinkVOCreateTableTest {

	private static final int PK_STRING_LENGTH_DEFAULT = 25;

	@Test
	public void test() {

		final Field grade = new Field("GRADE", 0, Schema.OPTIONAL_FLOAT64_SCHEMA);
		final Field losal = new Field("LOSAL", 1, Schema.OPTIONAL_FLOAT64_SCHEMA);
		final Field hisal = new Field("HISAL", 1, Schema.OPTIONAL_FLOAT64_SCHEMA);
		final List<Field> valueFields = new ArrayList<>();
		valueFields.add(grade);
		valueFields.add(losal);
		valueFields.add(hisal);


		final List<JdbcSinkColumn> allColumns = new ArrayList<>();
		final Map<String, JdbcSinkColumn> pkColumns = new HashMap<>();
		final Map<String, Object> lobColumns = new HashMap<>();

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

		List<String> createScottSalgradeOra = TargetDbSqlUtils.createTableSql(
				"SALGRADE", JdbcSinkConnectionPool.DB_TYPE_ORACLE,
				PK_STRING_LENGTH_DEFAULT,
				pkColumns, allColumns, lobColumns); 
		List<String> createScottSalgradePg = TargetDbSqlUtils.createTableSql(
				"SALGRADE", JdbcSinkConnectionPool.DB_TYPE_POSTGRESQL,
				PK_STRING_LENGTH_DEFAULT,
				pkColumns, allColumns, lobColumns); 
		List<String> createScottSalgradeMySql = TargetDbSqlUtils.createTableSql(
				"SALGRADE", JdbcSinkConnectionPool.DB_TYPE_MYSQL,
				PK_STRING_LENGTH_DEFAULT,
				pkColumns, allColumns, lobColumns);
		List<String> createScottSalgradeMsSql = TargetDbSqlUtils.createTableSql(
				"SALGRADE", JdbcSinkConnectionPool.DB_TYPE_MSSQL,
				PK_STRING_LENGTH_DEFAULT,
				pkColumns, allColumns, lobColumns);

		System.out.println("++++++++++ Oracle ++++++++++++++++++++++++");
		System.out.println(createScottSalgradeOra.get(0));
		System.out.println("++++++++++ PostgreSQL +++++++++++++++++++++");
		System.out.println(createScottSalgradePg.get(0));
		if (createScottSalgradePg.size() > 1) {
			for (int i = 1; i < createScottSalgradePg.size(); i++) {
				System.out.println("\t" + createScottSalgradePg.get(i));
			}
		}
		System.out.println("++++++++++ MySQL ++++++++++++++++++++++++++");
		System.out.println(createScottSalgradeMySql.get(0));
		System.out.println("++++++++++ MsSQL ++++++++++++++++++++++++++");
		System.out.println(createScottSalgradeMsSql.get(0));

		assertTrue(createScottSalgradeOra.get(0).contains("GRADE BINARY_DOUBLE"));
		assertTrue(createScottSalgradePg.get(0).contains("GRADE double precision"));
		assertTrue(createScottSalgradeMySql.get(0).contains("GRADE double"));
		assertTrue(createScottSalgradeMsSql.get(0).contains("GRADE float"));
	}

}
