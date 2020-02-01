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

public class OraDictSqlTexts {

	/*
select count(*)
from   ALL_MVIEW_LOGS L
where  (L.ROWIDS='YES' or L.PRIMARY_KEY='YES') and L.OBJECT_ID='NO'
  and  L.COMMIT_SCN_BASED='NO' and L.INCLUDE_NEW_VALUES='NO';
	 */
	public static final String MVIEW_COUNT_PK_SEQ_NOSCN_NONV_NOOI =
			"select count(*)\n" +
			"from   ALL_MVIEW_LOGS L\n" +
			"where  (L.ROWIDS='YES' or L.PRIMARY_KEY='YES') and L.OBJECT_ID='NO'\n" + 
			"  and  L.COMMIT_SCN_BASED='NO' and L.INCLUDE_NEW_VALUES='NO'\n";
	/*
select count(*)
from   ALL_MVIEW_LOGS L
where  (L.ROWIDS='YES' or L.PRIMARY_KEY='YES') and L.OBJECT_ID='NO'
  and  L.COMMIT_SCN_BASED='NO' and L.INCLUDE_NEW_VALUES='NO';
	 */
	public static final String MVIEW_COUNT_PK_SEQ_NOSCN_NONV_NOOI_PRE11G =
			"select count(*)\n" +
			"from   ALL_MVIEW_LOGS L\n" +
			"where  (L.ROWIDS='YES' or L.PRIMARY_KEY='YES') and L.OBJECT_ID='NO'\n" + 
			"  and L.INCLUDE_NEW_VALUES='NO'\n";
	/*
select count(*)
from   ALL_TABLES T
where  T.TEMPORARY='N' and T.STATUS='VALID'
  and  T.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER');
	 */
	public static final String TABLE_COUNT_PLAIN =
			"select count(*)\n" + 
			"from   ALL_TABLES T\n" +
			"where  T.TEMPORARY='N' and T.STATUS='VALID'\n" +
			"  and  T.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER')";
	/*
select count(*)
from   CDB_TABLES T, DBA_PDBS P
where  T.TEMPORARY='N' and T.STATUS='VALID'
  and  T.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER')
  and  P.PDB_ID = T.CON_ID and P.PDB_ID > 2;
	 */
	public static final String TABLE_COUNT_CDB =
			"select count(*)\n" + 
			"from   CDB_TABLES T, DBA_PDBS P\n" +
			"where  T.TEMPORARY='N' and T.STATUS='VALID'\n" +
			"  and  T.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER')\n" +
			"  and  P.PDB_ID = T.CON_ID and P.PDB_ID > 2";

	/*
select L.LOG_OWNER, L.MASTER, L.LOG_TABLE, L.ROWIDS, L.PRIMARY_KEY
from   ALL_MVIEW_LOGS L
where  (L.ROWIDS='YES' or L.PRIMARY_KEY='YES') and L.OBJECT_ID='NO'
  and  L.COMMIT_SCN_BASED='NO' and L.INCLUDE_NEW_VALUES='NO';
	 */
	public static final String MVIEW_LIST_PK_SEQ_NOSCN_NONV_NOOI =
			"select L.LOG_OWNER, L.MASTER, L.LOG_TABLE, L.ROWIDS, L.PRIMARY_KEY, L.SEQUENCE\n" +
			"from   ALL_MVIEW_LOGS L\n" +
			"where  (L.ROWIDS='YES' or L.PRIMARY_KEY='YES') and L.OBJECT_ID='NO'\n" + 
			"  and  L.COMMIT_SCN_BASED='NO' and L.INCLUDE_NEW_VALUES='NO'\n";
	/*
select L.LOG_OWNER, L.MASTER, L.LOG_TABLE, L.ROWIDS, L.PRIMARY_KEY
from   ALL_MVIEW_LOGS L
where  (L.ROWIDS='YES' or L.PRIMARY_KEY='YES') and L.OBJECT_ID='NO'
  and  L.COMMIT_SCN_BASED='NO' and L.INCLUDE_NEW_VALUES='NO';
	 */
	public static final String MVIEW_LIST_PK_SEQ_NOSCN_NONV_NOOI_PRE11G =
			"select L.LOG_OWNER, L.MASTER, L.LOG_TABLE, L.ROWIDS, L.PRIMARY_KEY, L.SEQUENCE\n" +
			"from   ALL_MVIEW_LOGS L\n" +
			"where  (L.ROWIDS='YES' or L.PRIMARY_KEY='YES') and L.OBJECT_ID='NO'\n" + 
			"  and L.INCLUDE_NEW_VALUES='NO'\n";
	/*
select T.OWNER, T.TABLE_NAME
from   ALL_TABLES T
where  T.TEMPORARY='N' and T.STATUS='VALID'
  and  T.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER');
	 */
	public static final String TABLE_LIST_PLAIN =
			"select T.OWNER, T.TABLE_NAME\n" + 
			"from   ALL_TABLES T\n" +
			"where  T.TEMPORARY='N' and T.STATUS='VALID'\n" +
			"  and  T.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER')";
	/*
select P.PDB_NAME, T.OWNER, T.TABLE_NAME
from   CDB_TABLES T, DBA_PDBS P
where  T.TEMPORARY='N' and T.STATUS='VALID'
  and  T.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER')
  and  P.PDB_ID = T.CON_ID and P.PDB_ID > 2;
	 */
	public static final String TABLE_LIST_CDB =
			"select P.PDB_NAME, T.OWNER, T.TABLE_NAME\n" + 
			"from   CDB_TABLES T, DBA_PDBS P\n" +
			"where  T.TEMPORARY='N' and T.STATUS='VALID'\n" +
			"  and  T.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER')\n" +
			"  and  P.PDB_ID = T.CON_ID and P.PDB_ID > 2";

	/*
	select C.COLUMN_NAME, C.DATA_TYPE, C.DATA_LENGTH, C.DATA_PRECISION, C.DATA_SCALE, C.NULLABLE,
		(select 'Y'
		 from   ALL_TAB_COLUMNS TC
		 where  TC.TABLE_NAME='MLOG$_DEPT' and TC.OWNER=C.OWNER
		   and  TC.COLUMN_NAME=C.COLUMN_NAME and TC.COLUMN_NAME not like '%$$') PK
	from   ALL_TAB_COLUMNS C
	where  C.OWNER='SCOTT' and C.TABLE_NAME='DEPT'
	and    (C.DATA_TYPE in ('DATE', 'FLOAT', 'NUMBER', 'RAW', 'CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2', 'BLOB', 'CLOB') or C.DATA_TYPE like 'TIMESTAMP%');
	 */
	public static final String COLUMN_LIST =
			"select C.COLUMN_NAME, C.DATA_TYPE, C.DATA_LENGTH, C.DATA_PRECISION, C.DATA_SCALE, C.NULLABLE,\n" +
			"	 (select 'Y'\n" + 
			"	  from   ALL_TAB_COLUMNS TC\n" +
			"	  where  TC.TABLE_NAME=? and TC.OWNER=C.OWNER\n" +
			"	    and  TC.COLUMN_NAME=C.COLUMN_NAME and TC.COLUMN_NAME not like '%$$') PK\n" +
			"from   ALL_TAB_COLUMNS C\n" +
			"where  C.OWNER=? and C.TABLE_NAME=?\n" +
			"  and  (C.DATA_TYPE in ('DATE', 'FLOAT', 'NUMBER', 'RAW', 'CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2', 'BLOB', 'CLOB') or C.DATA_TYPE like 'TIMESTAMP%')";

	/*
select IC.COLUMN_NAME
from   ALL_CONSTRAINTS C, ALL_IND_COLUMNS IC
where  C.INDEX_OWNER=IC.INDEX_OWNER and C.INDEX_NAME=IC.INDEX_NAME and C.CONSTRAINT_TYPE='P'
  and  C.OWNER='SCOTT' and C.TABLE_NAME='DEPT';
	 */
	public static final String WELL_DEFINED_PK_COLUMNS =
			"select IC.COLUMN_NAME\n" + 
			"from   ALL_CONSTRAINTS C, ALL_IND_COLUMNS IC\n" + 
			"where  C.INDEX_OWNER=IC.INDEX_OWNER and C.INDEX_NAME=IC.INDEX_NAME and C.CONSTRAINT_TYPE='P'\n" + 
			"  and  C.OWNER=? and C.TABLE_NAME=?\n";

	/*
select TC.COLUMN_NAME
from   ALL_IND_COLUMNS IC, ALL_TAB_COLUMNS TC, (
	select RI.OWNER, RI.INDEX_NAME
	from (
    	select I.OWNER, I.INDEX_NAME, count(*) TOTAL, sum(case when TC.NULLABLE='N' then 1 else 0 end) NON_NULL
	    from   ALL_INDEXES I, ALL_IND_COLUMNS IC, ALL_TAB_COLUMNS TC
    	where  I.INDEX_TYPE='NORMAL' and I.UNIQUENESS='UNIQUE' and I.STATUS='VALID'
	      and  I.OWNER=IC.INDEX_OWNER and I.INDEX_NAME=IC.INDEX_NAME
    	  and  TC.OWNER=I.TABLE_OWNER and TC.TABLE_NAME=I.TABLE_NAME and IC.COLUMN_NAME=TC.COLUMN_NAME
	      and  I.TABLE_OWNER='SCOTT' and I.TABLE_NAME='DEPT'
    	group by I.OWNER, I.INDEX_NAME
		order by TOTAL asc) RI
	where RI.TOTAL=RI.NON_NULL
	and rownum=1) FL
where TC.OWNER=IC.TABLE_OWNER and TC.TABLE_NAME=IC.TABLE_NAME and IC.COLUMN_NAME=TC.COLUMN_NAME
  and IC.INDEX_OWNER=FL.OWNER and IC.INDEX_NAME=FL.INDEX_NAME;
	 */
	public static final String LEGACY_DEFINED_PK_COLUMNS =
			"select TC.COLUMN_NAME\n" + 
			"from   ALL_IND_COLUMNS IC, ALL_TAB_COLUMNS TC, (\n" + 
			"	select RI.OWNER, RI.INDEX_NAME\n" + 
			"	from (\n" + 
			"    	select I.OWNER, I.INDEX_NAME, count(*) TOTAL, sum(case when TC.NULLABLE='N' then 1 else 0 end) NON_NULL\n" + 
			"	    from   ALL_INDEXES I, ALL_IND_COLUMNS IC, ALL_TAB_COLUMNS TC\n" + 
			"    	where  I.INDEX_TYPE='NORMAL' and I.UNIQUENESS='UNIQUE' and I.STATUS='VALID'\n" + 
			"	      and  I.OWNER=IC.INDEX_OWNER and I.INDEX_NAME=IC.INDEX_NAME\n" + 
			"    	  and  TC.OWNER=I.TABLE_OWNER and TC.TABLE_NAME=I.TABLE_NAME and IC.COLUMN_NAME=TC.COLUMN_NAME\n" + 
			"	      and  I.TABLE_OWNER=? and I.TABLE_NAME=?\n" + 
			"    	group by I.OWNER, I.INDEX_NAME\n" + 
			"		order by TOTAL asc) RI\n" + 
			"	where RI.TOTAL=RI.NON_NULL\n" + 
			"	and rownum=1) FL\n" + 
			"where TC.OWNER=IC.TABLE_OWNER and TC.TABLE_NAME=IC.TABLE_NAME and IC.COLUMN_NAME=TC.COLUMN_NAME\n" + 
			"  and IC.INDEX_OWNER=FL.OWNER and IC.INDEX_NAME=FL.INDEX_NAME";

	/*
select DBID, NAME, PLATFORM_NAME from V$DATABASE;
	 */
	public static final String DB_INFO_PRE12C =
			"select DBID, NAME, PLATFORM_NAME from V$DATABASE";

	/*
select DBID, NAME, PLATFORM_NAME, CDB, SYS_CONTEXT('USERENV','CON_NAME') CON_NAME from V$DATABASE;
	 */
	public static final String DB_CDB_PDB_INFO =
			"select DBID, NAME, PLATFORM_NAME, CDB, SYS_CONTEXT('USERENV','CON_NAME') CON_NAME from V$DATABASE";

	/*
select VERSION, INSTANCE_NUMBER, INSTANCE_NAME, HOST_NAME from V$INSTANCE;
	 */
	public static final String RDBMS_VERSION_AND_MORE =
			"select VERSION, INSTANCE_NUMBER, INSTANCE_NAME, HOST_NAME from V$INSTANCE";

	/*
select DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER from DUAL;
	 */
	public static final String CURRENT_SCN =
			"select DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER from DUAL";

}
