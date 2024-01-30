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

package solutions.a2.cdc.postgres;

/**
 * 
 * PgDictSqlTexts: Just container for SQL Statements
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class PgDictSqlTexts {

	/*
select TABLE_NAME, COLUMN_NAME,INDEX_NAME
from (
      select n.nspname AS TABLE_SCHEMA, ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,
             (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS INDEX_NAME,
             information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM
      from   pg_catalog.pg_class ct,
             pg_catalog.pg_attribute a,
             pg_catalog.pg_namespace n,
             pg_catalog.pg_index i,
             pg_catalog.pg_class ci
      where  ct.oid = a.attrelid
        and  ct.relnamespace = n.oid
        and  a.attrelid = i.indrelid
        and  ci.oid = i.indexrelid
        and  i.indisprimary
     ) PK  
where PK.A_ATTNUM = (PK.KEYS).x
  and PK.TABLE_SCHEMA = 'public'
  and PK.TABLE_NAME = 'dept'
order by PK.KEY_SEQ;
	 */
	public static final String WELL_DEFINED_PK_COLUMNS =
			"select TABLE_NAME, COLUMN_NAME, INDEX_NAME\n" +
			"from (\n" +
			"      select n.nspname AS TABLE_SCHEMA, ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,\n" +
			"             (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS INDEX_NAME,\n" +
			"             information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM\n" +
			"      from   pg_catalog.pg_class ct,\n" +
			"             pg_catalog.pg_attribute a,\n" +
			"             pg_catalog.pg_namespace n,\n" +
			"             pg_catalog.pg_index i,\n" +
			"             pg_catalog.pg_class ci\n" +
			"      where  ct.oid = a.attrelid\n" +
			"        and  ct.relnamespace = n.oid\n" +
			"        and  a.attrelid = i.indrelid\n" +
			"        and  ci.oid = i.indexrelid\n" +
			"        and  i.indisprimary\n" +
			"     ) PK  \n" +
			"where PK.A_ATTNUM = (PK.KEYS).x\n" +
			"  and PK.TABLE_SCHEMA = ?\n" +
			"  and PK.TABLE_NAME = ?\n" +
			"order by PK.KEY_SEQ\n";


	/*
select TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, a.attname as COLUMN_NAME
from   (select TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, count(*) as TOTAL, sum(attnotnull) as NON_NULL
        from   (select n.nspname AS TABLE_SCHEMA, ct.relname AS TABLE_NAME,
                       case when (a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) then 1 else 0 end AS attnotnull,
                       (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS INDEX_NAME,
                       information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM
                from   pg_catalog.pg_class ct,
                       pg_catalog.pg_attribute a,
                       pg_catalog.pg_type t,
                       pg_catalog.pg_namespace n,
                       pg_catalog.pg_index i,
                       pg_catalog.pg_class ci
                where  ct.oid = a.attrelid
                  and  ct.relnamespace = n.oid
                  and  a.attrelid = i.indrelid
                  and  a.atttypid = t.oid
                  and  ci.oid = i.indexrelid
                  and  (i.indisunique AND i.indisvalid AND i.indpred IS null AND i.indexprs IS null)
     ) PK  
where PK.A_ATTNUM = (PK.KEYS).x
  and PK.TABLE_SCHEMA = 'public'
  and PK.TABLE_NAME = 'mtl_item_locations'
group by TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
       ) IMPLICIT_KEY,
       pg_catalog.pg_class ct,
       pg_catalog.pg_attribute a,
       pg_catalog.pg_namespace n,
       pg_catalog.pg_index i,
       pg_catalog.pg_class ci
where  IMPLICIT_KEY.TOTAL = IMPLICIT_KEY.NON_NULL
  and  ct.oid = i.indrelid
  and  ct.relnamespace = n.oid
  and  ci.oid = i.indexrelid
  and  a.attrelid = ct.oid
  and  a.attnum = ANY(i.indkey)
  and  ct.relkind = 'r'
  and  n.nspname = IMPLICIT_KEY.TABLE_SCHEMA
  and  ct.relname = IMPLICIT_KEY.TABLE_NAME
  and  ci.relname = IMPLICIT_KEY.INDEX_NAME;
	 */
	public static final String LEGACY_DEFINED_PK_COLUMNS =
			"select TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, a.attname as COLUMN_NAME\n" +
			"from   (select TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, count(*) as TOTAL, sum(attnotnull) as NON_NULL\n" +
			"        from   (select n.nspname AS TABLE_SCHEMA, ct.relname AS TABLE_NAME,\n" +
			"                       case when (a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) then 1 else 0 end AS attnotnull,\n" +
			"                       (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS INDEX_NAME,\n" +
			"                       information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM\n" +
			"                from   pg_catalog.pg_class ct,\n" +
			"                       pg_catalog.pg_attribute a,\n" +
			"                       pg_catalog.pg_type t,\n" +
			"                       pg_catalog.pg_namespace n,\n" +
			"                       pg_catalog.pg_index i,\n" +
			"                       pg_catalog.pg_class ci\n" +
			"                where  ct.oid = a.attrelid\n" +
			"                  and  ct.relnamespace = n.oid\n" +
			"                  and  a.attrelid = i.indrelid\n" +
			"                  and  a.atttypid = t.oid\n" +
			"                  and  ci.oid = i.indexrelid\n" +
			"                  and  (i.indisunique AND i.indisvalid AND i.indpred IS null AND i.indexprs IS null)\n" +
			"     ) PK  \n" +
			"where PK.A_ATTNUM = (PK.KEYS).x\n" +
			"  and PK.TABLE_SCHEMA = ?\n" +
			"  and PK.TABLE_NAME = ?\n" +
			"group by TABLE_SCHEMA, TABLE_NAME, INDEX_NAME\n" +
			"       ) IMPLICIT_KEY,\n" +
			"       pg_catalog.pg_class ct,\n" +
			"       pg_catalog.pg_attribute a,\n" +
			"       pg_catalog.pg_namespace n,\n" +
			"       pg_catalog.pg_index i,\n" +
			"       pg_catalog.pg_class ci\n" +
			"where  IMPLICIT_KEY.TOTAL = IMPLICIT_KEY.NON_NULL\n" +
			"  and  ct.oid = i.indrelid\n" +
			"  and  ct.relnamespace = n.oid\n" +
			"  and  ci.oid = i.indexrelid\n" +
			"  and  a.attrelid = ct.oid\n" +
			"  and  a.attnum = ANY(i.indkey)\n" +
			"  and  ct.relkind = 'r'\n" +
			"  and  n.nspname = IMPLICIT_KEY.TABLE_SCHEMA\n" +
			"  and  ct.relname = IMPLICIT_KEY.TABLE_NAME\n" +
			"  and  ci.relname = IMPLICIT_KEY.INDEX_NAME\n";


	/*
select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, INDEX_NAME
from (
      select n.nspname AS TABLE_SCHEMA, ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,
             case when (a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) then 1 else 0 end AS attnotnull,
             (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS INDEX_NAME,
             information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM
      from   pg_catalog.pg_class ct,
             pg_catalog.pg_attribute a,
             pg_catalog.pg_type t,
             pg_catalog.pg_namespace n,
             pg_catalog.pg_index i,
             pg_catalog.pg_class ci
      where  ct.oid = a.attrelid
        and  ct.relnamespace = n.oid
        and  a.attrelid = i.indrelid
        and  a.atttypid = t.oid
        and  ci.oid = i.indexrelid
        and  (i.indisunique AND i.indisvalid AND i.indpred IS null AND i.indexprs IS null)
     ) UQ  
where UQ.A_ATTNUM = (UQ.KEYS).x
  and UQ.TABLE_SCHEMA = 'public'
  and UQ.TABLE_NAME = 'mtl_item_locations'
order by UQ.INDEX_NAME, UQ.KEY_SEQ;
	 */
	public static final String WELL_DEFINED_UNIQUE_COLUMNS =
			"select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, INDEX_NAME\n" +
			"from (\n" +
			"      select n.nspname AS TABLE_SCHEMA, ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,\n" +
			"             case when (a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) then 1 else 0 end AS attnotnull,\n" +
			"             (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS INDEX_NAME,\n" +
			"             information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM\n" +
			"      from   pg_catalog.pg_class ct,\n" +
			"             pg_catalog.pg_attribute a,\n" +
			"             pg_catalog.pg_type t,\n" +
			"             pg_catalog.pg_namespace n,\n" +
			"             pg_catalog.pg_index i,\n" +
			"             pg_catalog.pg_class ci\n" +
			"      where  ct.oid = a.attrelid\n" +
			"        and  ct.relnamespace = n.oid\n" +
			"        and  a.attrelid = i.indrelid\n" +
			"        and  a.atttypid = t.oid\n" +
			"        and  ci.oid = i.indexrelid\n" +
			"        and  (i.indisunique AND i.indisvalid AND i.indpred IS null AND i.indexprs IS null)\n" +
			"     ) UQ  \n" +
			"where UQ.A_ATTNUM = (UQ.KEYS).x\n" +
			"  and UQ.TABLE_SCHEMA = ?\n" +
			"  and UQ.TABLE_NAME = ?\n" +
			"order by UQ.INDEX_NAME, UQ.KEY_SEQ\n";


}


