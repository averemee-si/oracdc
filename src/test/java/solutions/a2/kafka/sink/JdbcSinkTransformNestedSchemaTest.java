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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import solutions.a2.cdc.TargetDbSqlUtils;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class JdbcSinkTransformNestedSchemaTest {

	private static final int PK_STRING_LENGTH_DEFAULT = 25;

	@Test
	public void test() {

		final SchemaBuilder keySchemaBuilder = SchemaBuilder
				.struct()
				.required()
				.name("FND_LOBS.Key")
				.version(1);
		keySchemaBuilder.field("FILE_ID", Schema.INT64_SCHEMA);
		final Schema keySchema = keySchemaBuilder.build();

		final SchemaBuilder columnFileDataSchemaBuilder = SchemaBuilder
				.struct()
				.optional()
				.name("FILE_DATA.Transform")
				.version(1);
		columnFileDataSchemaBuilder.field("S3_URL", Schema.OPTIONAL_STRING_SCHEMA);
		final Schema columnFileDataSchema = columnFileDataSchemaBuilder.build();

		final SchemaBuilder valueSchemaBuilder = SchemaBuilder
				.struct()
				.optional()
				.name("FND_LOBS.Value")
				.version(1);
		valueSchemaBuilder.field("FILE_NAME", Schema.OPTIONAL_STRING_SCHEMA);
		valueSchemaBuilder.field("FILE_CONTENT_TYPE", Schema.STRING_SCHEMA);
		//... ... ...
		valueSchemaBuilder.field("FILE_DATA", columnFileDataSchema);
		final Schema valueSchema = valueSchemaBuilder.build();

		final List<JdbcSinkColumn> allColumns = new ArrayList<>();
		final Map<String, JdbcSinkColumn> pkColumns = new HashMap<>();
		final Map<String, Object> lobColumns = new HashMap<>();

		for (Field field : keySchema.fields()) {
			try {
				final var column = new JdbcSinkColumn(field, true);
				pkColumns.put(column.name(), column);
			} catch (SQLException sqle) {
				sqle.printStackTrace();
			}
		}

		for (Field field : valueSchema.fields()) {
			if (Strings.CS.equals("struct", field.schema().type().getName())) {
				final List<JdbcSinkColumn> transformation = new ArrayList<>();
				for (Field unnestField : field.schema().fields()) {
					try {
						final var column = new JdbcSinkColumn(unnestField, false);
						transformation.add(column);
					} catch (SQLException sqle) {
						sqle.printStackTrace();
					}
				}
				lobColumns.put(field.name(), transformation);
			} else {
				if (!pkColumns.containsKey(field.name())) {
					try {
						final var column = new JdbcSinkColumn(field, false);
						if (column.jdbcType() == Types.BLOB ||
								column.jdbcType() == Types.CLOB ||
								column.jdbcType() == Types.NCLOB ||
								column.jdbcType() == Types.SQLXML) {
							lobColumns.put(column.name(), column);
						} else {
							allColumns.add(column);
						}
					} catch (SQLException sqle) {
						sqle.printStackTrace();
					}
				}
			}
		}

		List<String> createScottDeptOra = TargetDbSqlUtils.createTableSql(
				"FND_LOBS", JdbcSinkConnectionPool.DB_TYPE_ORACLE,
				PK_STRING_LENGTH_DEFAULT,
				pkColumns, allColumns, lobColumns); 
		List<String> createScottDeptPg = TargetDbSqlUtils.createTableSql(
				"FND_LOBS", JdbcSinkConnectionPool.DB_TYPE_POSTGRESQL,
				PK_STRING_LENGTH_DEFAULT,
				pkColumns, allColumns, lobColumns); 
		List<String> createScottDeptMySql = TargetDbSqlUtils.createTableSql(
				"FND_LOBS", JdbcSinkConnectionPool.DB_TYPE_MYSQL,
				PK_STRING_LENGTH_DEFAULT,
				pkColumns, allColumns, lobColumns);
		List<String> createScottDeptMsSql = TargetDbSqlUtils.createTableSql(
				"FND_LOBS", JdbcSinkConnectionPool.DB_TYPE_MSSQL,
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

		assertTrue(createScottDeptOra.get(0).contains("S3_URL VARCHAR2(4000)"));
		assertTrue(createScottDeptPg.get(0).contains("S3_URL text"));
		assertTrue(createScottDeptMySql.get(0).contains("S3_URL varchar(255)"));
		assertTrue(createScottDeptMsSql.get(0).contains("S3_URL nvarchar(4000)"));

		final Map<String, String> sqlTextsOra = TargetDbSqlUtils.generateSinkSql(
				"FND_LOBS", JdbcSinkConnectionPool.DB_TYPE_ORACLE, pkColumns, allColumns, lobColumns, false);
		final Map<String, String> sqlTextsPg = TargetDbSqlUtils.generateSinkSql(
				"FND_LOBS", JdbcSinkConnectionPool.DB_TYPE_POSTGRESQL, pkColumns, allColumns, lobColumns, false);
		final Map<String, String> sqlTextsMySql = TargetDbSqlUtils.generateSinkSql(
				"FND_LOBS", JdbcSinkConnectionPool.DB_TYPE_MYSQL, pkColumns, allColumns, lobColumns, false);
		final Map<String, String> sqlTextsMsSql = TargetDbSqlUtils.generateSinkSql(
				"FND_LOBS", JdbcSinkConnectionPool.DB_TYPE_MSSQL, pkColumns, allColumns, lobColumns, false);

		final String sinkUpsertSqlOra = sqlTextsOra.get(TargetDbSqlUtils.UPSERT);
		final String sinkUpsertSqlPg = sqlTextsPg.get(TargetDbSqlUtils.UPSERT);
		final String sinkUpsertSqlMySql = sqlTextsMySql.get(TargetDbSqlUtils.UPSERT);
		final String sinkUpsertSqlMsSql = sqlTextsMsSql.get(TargetDbSqlUtils.UPSERT);

		System.out.println("========== Oracle ========================");
		System.out.println(sinkUpsertSqlOra);
		System.out.println();
		System.out.println(sqlTextsOra.get("FILE_DATA"));
		System.out.println("========== PostgreSQL ====================");
		System.out.println(sinkUpsertSqlPg);
		System.out.println();
		System.out.println(sqlTextsPg.get("FILE_DATA"));
		System.out.println("========== MySQL ==========================");
		System.out.println(sinkUpsertSqlMySql);
		System.out.println();
		System.out.println(sqlTextsMySql.get("FILE_DATA"));
		System.out.println("========== MsSQL ==========================");
		System.out.println(sinkUpsertSqlMsSql);
		System.out.println();
		System.out.println(sqlTextsMsSql.get("FILE_DATA"));

		assertTrue(sinkUpsertSqlOra.contains("when matched then update"));
		assertTrue(sinkUpsertSqlPg.contains("on conflict"));
		assertTrue(sinkUpsertSqlMySql.contains("on duplicate key update"));
		assertTrue(sinkUpsertSqlMsSql.contains("when not matched then"));

	}

}
