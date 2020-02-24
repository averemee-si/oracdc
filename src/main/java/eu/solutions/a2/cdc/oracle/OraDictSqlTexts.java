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

/**
 * 
 * @author averemee
 *
 */
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
	select C.COLUMN_NAME, C.DATA_TYPE, C.DATA_LENGTH, C.DATA_PRECISION, C.DATA_SCALE, C.NULLABLE, C.COLUMN_ID,
		(select 'Y'
		 from   ALL_TAB_COLUMNS TC
		 where  TC.TABLE_NAME='MLOG$_DEPT' and TC.OWNER=C.OWNER
		   and  TC.COLUMN_NAME=C.COLUMN_NAME and TC.COLUMN_NAME not like '%$$') PK
	from   ALL_TAB_COLUMNS C
	where  C.OWNER='SCOTT' and C.TABLE_NAME='DEPT'
	and    (C.DATA_TYPE in ('DATE', 'FLOAT', 'NUMBER', 'RAW', 'CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2', 'BLOB', 'CLOB') or C.DATA_TYPE like 'TIMESTAMP%');
	 */
	public static final String COLUMN_LIST_MVIEW =
			"select C.COLUMN_NAME, C.DATA_TYPE, C.DATA_LENGTH, C.DATA_PRECISION, C.DATA_SCALE, C.NULLABLE, C.COLUMN_ID,\n" +
			"	 (select 'Y'\n" + 
			"	  from   ALL_TAB_COLUMNS TC\n" +
			"	  where  TC.TABLE_NAME=? and TC.OWNER=C.OWNER\n" +
			"	    and  TC.COLUMN_NAME=C.COLUMN_NAME and TC.COLUMN_NAME not like '%$$') PK\n" +
			"from   ALL_TAB_COLUMNS C\n" +
			"where  C.OWNER=? and C.TABLE_NAME=?\n" +
			"  and  (C.DATA_TYPE in ('DATE', 'FLOAT', 'NUMBER', 'RAW', 'CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2', 'BLOB', 'CLOB') or C.DATA_TYPE like 'TIMESTAMP%')";
	/*
select C.COLUMN_NAME, C.DATA_TYPE, C.DATA_LENGTH, C.DATA_PRECISION, C.DATA_SCALE, C.NULLABLE, C.COLUMN_ID
from   ALL_TAB_COLUMNS C
where  C.OWNER='SCOTT' and C.TABLE_NAME='DEPT'
  and  (C.DATA_TYPE in ('DATE', 'FLOAT', 'NUMBER', 'RAW', 'CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2') or C.DATA_TYPE like 'TIMESTAMP%');
	 */
	public static final String COLUMN_LIST_PLAIN =
			"select C.COLUMN_NAME, C.DATA_TYPE, C.DATA_LENGTH, C.DATA_PRECISION, C.DATA_SCALE, C.NULLABLE, C.COLUMN_ID\n" +
			"from   ALL_TAB_COLUMNS C\n" +
			"where  C.OWNER=? and C.TABLE_NAME=?\n" +
			"  and  (C.DATA_TYPE in ('DATE', 'FLOAT', 'NUMBER', 'RAW', 'CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2') or C.DATA_TYPE like 'TIMESTAMP%')";
	/*
select C.COLUMN_NAME, C.DATA_TYPE, C.DATA_LENGTH, C.DATA_PRECISION, C.DATA_SCALE, C.NULLABLE, C.COLUMN_ID
from   CDB_TAB_COLUMNS C
where  C.OWNER='SCOTT' and C.TABLE_NAME='DEPT' and CON_ID=0
  and  (C.DATA_TYPE in ('DATE', 'FLOAT', 'NUMBER', 'RAW', 'CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2') or C.DATA_TYPE like 'TIMESTAMP%');
	 */
	public static final String COLUMN_LIST_CDB =
			"select C.COLUMN_NAME, C.DATA_TYPE, C.DATA_LENGTH, C.DATA_PRECISION, C.DATA_SCALE, C.NULLABLE, C.COLUMN_ID\n" +
			"from   CDB_TAB_COLUMNS C\n" +
			"where  C.OWNER=? and C.TABLE_NAME=? and CON_ID=?\n" +
			"  and  (C.DATA_TYPE in ('DATE', 'FLOAT', 'NUMBER', 'RAW', 'CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2') or C.DATA_TYPE like 'TIMESTAMP%')";

	/*
select IC.COLUMN_NAME
from   ALL_CONSTRAINTS C, ALL_IND_COLUMNS IC
where  C.INDEX_OWNER=IC.INDEX_OWNER and C.INDEX_NAME=IC.INDEX_NAME and C.CONSTRAINT_TYPE='P'
  and  C.OWNER='SCOTT' and C.TABLE_NAME='DEPT';
	 */
	public static final String WELL_DEFINED_PK_COLUMNS_NON_CDB =
			"select IC.COLUMN_NAME\n" + 
			"from   ALL_CONSTRAINTS C, ALL_IND_COLUMNS IC\n" + 
			"where  C.INDEX_OWNER=IC.INDEX_OWNER and C.INDEX_NAME=IC.INDEX_NAME and C.CONSTRAINT_TYPE='P'\n" + 
			"  and  C.OWNER=? and C.TABLE_NAME=?\n";
	/*
select IC.COLUMN_NAME
from   CDB_CONSTRAINTS C, CDB_IND_COLUMNS IC
where  C.INDEX_OWNER=IC.INDEX_OWNER and C.INDEX_NAME=IC.INDEX_NAME and C.CONSTRAINT_TYPE='P'
  and  C.CON_ID=IC.CON_ID and C.OWNER='SCOTT' and C.TABLE_NAME='DEPT' and C.CON_ID=0;
	 */
	public static final String WELL_DEFINED_PK_COLUMNS_CDB =
			"select IC.COLUMN_NAME\n" + 
			"from   CDB_CONSTRAINTS C, CDB_IND_COLUMNS IC\n" + 
			"where  C.INDEX_OWNER=IC.INDEX_OWNER and C.INDEX_NAME=IC.INDEX_NAME and C.CONSTRAINT_TYPE='P'\n" + 
			"  and  C.CON_ID=IC.CON_ID and C.OWNER=? and C.TABLE_NAME=? and C.CON_ID=?\n";

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
	      and  I.TABLE_OWNER='INV' and I.TABLE_NAME='MTL_MATERIAL_TRANSACTIONS'
    	group by I.OWNER, I.INDEX_NAME
		order by TOTAL asc) RI
	where RI.TOTAL=RI.NON_NULL
	and rownum=1) FL
where TC.OWNER=IC.TABLE_OWNER and TC.TABLE_NAME=IC.TABLE_NAME and IC.COLUMN_NAME=TC.COLUMN_NAME
  and IC.INDEX_OWNER=FL.OWNER and IC.INDEX_NAME=FL.INDEX_NAME;
	 */
	public static final String LEGACY_DEFINED_PK_COLUMNS_NON_CDB =
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
select TC.COLUMN_NAME
from   CDB_IND_COLUMNS IC, CDB_TAB_COLUMNS TC, (
	select RI.OWNER, RI.INDEX_NAME, RI.CON_ID
	from (
    	select I.OWNER, I.INDEX_NAME, I.CON_ID, count(*) TOTAL, sum(case when TC.NULLABLE='N' then 1 else 0 end) NON_NULL
	    from   CDB_INDEXES I, CDB_IND_COLUMNS IC, CDB_TAB_COLUMNS TC
    	where  I.INDEX_TYPE='NORMAL' and I.UNIQUENESS='UNIQUE' and I.STATUS='VALID'
	      and  I.OWNER=IC.INDEX_OWNER and I.INDEX_NAME=IC.INDEX_NAME
	      and  I.CON_ID=IC.CON_ID and IC.CON_ID=TC.CON_ID
    	  and  TC.OWNER=I.TABLE_OWNER and TC.TABLE_NAME=I.TABLE_NAME and IC.COLUMN_NAME=TC.COLUMN_NAME
	      and  I.TABLE_OWNER='INV' and I.TABLE_NAME='MTL_MATERIAL_TRANSACTIONS' and I.CON_ID=0
    	group by I.OWNER, I.INDEX_NAME, I.CON_ID
		order by TOTAL asc) RI
	where RI.TOTAL=RI.NON_NULL
	and rownum=1) FL
where TC.OWNER=IC.TABLE_OWNER and TC.TABLE_NAME=IC.TABLE_NAME and IC.COLUMN_NAME=TC.COLUMN_NAME
  and IC.INDEX_OWNER=FL.OWNER and IC.INDEX_NAME=FL.INDEX_NAME and IC.CON_ID=TC.CON_ID and IC.CON_ID=FL.CON_ID;
	 */
	public static final String LEGACY_DEFINED_PK_COLUMNS_CDB =
			"select TC.COLUMN_NAME\n" + 
			"from   CDB_IND_COLUMNS IC, CDB_TAB_COLUMNS TC, (\n" + 
			"	select RI.OWNER, RI.INDEX_NAME, RI.CON_ID\n" + 
			"	from (\n" + 
			"    	select I.OWNER, I.INDEX_NAME, I.CON_ID, count(*) TOTAL, sum(case when TC.NULLABLE='N' then 1 else 0 end) NON_NULL\n" + 
			"	    from   CDB_INDEXES I, CDB_IND_COLUMNS IC, CDB_TAB_COLUMNS TC\n" + 
			"    	where  I.INDEX_TYPE='NORMAL' and I.UNIQUENESS='UNIQUE' and I.STATUS='VALID'\n" + 
			"	      and  I.OWNER=IC.INDEX_OWNER and I.INDEX_NAME=IC.INDEX_NAME\n" + 
			"	      and  I.CON_ID=IC.CON_ID and IC.CON_ID=TC.CON_ID\n" + 
			"    	  and  TC.OWNER=I.TABLE_OWNER and TC.TABLE_NAME=I.TABLE_NAME and IC.COLUMN_NAME=TC.COLUMN_NAME\n" + 
			"	      and  I.TABLE_OWNER=? and I.TABLE_NAME=? and I.CON_ID=?\n" + 
			"    	group by I.OWNER, I.INDEX_NAME, I.CON_ID\n" + 
			"		order by TOTAL asc) RI\n" + 
			"	where RI.TOTAL=RI.NON_NULL\n" + 
			"	and rownum=1) FL\n" + 
			"where TC.OWNER=IC.TABLE_OWNER and TC.TABLE_NAME=IC.TABLE_NAME and IC.COLUMN_NAME=TC.COLUMN_NAME\n" + 
			"  and IC.INDEX_OWNER=FL.OWNER and IC.INDEX_NAME=FL.INDEX_NAME and IC.CON_ID=TC.CON_ID and IC.CON_ID=FL.CON_ID";

	/*
select DBID, NAME, PLATFORM_NAME,
       (select VALUE from V$NLS_PARAMETERS where PARAMETER = 'NLS_CHARACTERSET') NLS_CHARACTERSET,
       (select VALUE from V$NLS_PARAMETERS where PARAMETER = 'NLS_NCHAR_CHARACTERSET') NLS_NCHAR_CHARACTERSET
from   V$DATABASE;
	 */
	public static final String DB_INFO_PRE12C =
			"select DBID, NAME, DB_UNIQUE_NAME, PLATFORM_NAME,\n" + 
			"       (select VALUE from V$NLS_PARAMETERS where PARAMETER = 'NLS_CHARACTERSET') NLS_CHARACTERSET,\n" + 
			"       (select VALUE from V$NLS_PARAMETERS where PARAMETER = 'NLS_NCHAR_CHARACTERSET') NLS_NCHAR_CHARACTERSET\n" + 
			"from   V$DATABASE\n";

	/*
select DBID, NAME, PLATFORM_NAME, CDB, SYS_CONTEXT('USERENV','CON_NAME') CON_NAME,
       (select VALUE from V$NLS_PARAMETERS where PARAMETER = 'NLS_CHARACTERSET') NLS_CHARACTERSET,
       (select VALUE from V$NLS_PARAMETERS where PARAMETER = 'NLS_NCHAR_CHARACTERSET') NLS_NCHAR_CHARACTERSET
from   V$DATABASE;
	 */
	public static final String DB_CDB_PDB_INFO =
			"select DBID, NAME, DB_UNIQUE_NAME, PLATFORM_NAME, CDB, SYS_CONTEXT('USERENV','CON_NAME') CON_NAME,\n" + 
			"       (select VALUE from V$NLS_PARAMETERS where PARAMETER = 'NLS_CHARACTERSET') NLS_CHARACTERSET,\n" + 
			"       (select VALUE from V$NLS_PARAMETERS where PARAMETER = 'NLS_NCHAR_CHARACTERSET') NLS_NCHAR_CHARACTERSET\n" + 
			"from   V$DATABASE";

	/*
select VERSION, INSTANCE_NUMBER, INSTANCE_NAME, HOST_NAME from V$INSTANCE;
	 */
	public static final String RDBMS_VERSION_AND_MORE =
			"select VERSION, INSTANCE_NUMBER, INSTANCE_NAME, HOST_NAME from V$INSTANCE";

/*
select OPEN_MODE, DBID from V$DATABASE;
 */
	public static final String RDBMS_OPEN_MODE =
			"select OPEN_MODE, DBID, DB_UNIQUE_NAME from V$DATABASE";

	/*
select min(FIRST_CHANGE#) from V$ARCHIVED_LOG;
	 */
	public static final String FIRST_AVAILABLE_SCN_IN_ARCHIVE =
			"select min(FIRST_CHANGE#) from V$ARCHIVED_LOG";
	
	/*
select   NAME, THREAD#, SEQUENCE#, FIRST_CHANGE#, NEXT_CHANGE#, BLOCKS*BLOCK_SIZE BYTES
from     V$ARCHIVED_LOG
where    ARCHIVED='YES' and STANDBY_DEST='NO' and (? >= FIRST_CHANGE# or ? <= NEXT_CHANGE#)
  and    SEQUENCE# >= 
          (select min(SEQUENCE#)
           from   V$ARCHIVED_LOG
           where  ARCHIVED='YES' and STANDBY_DEST='NO' and ? between FIRST_CHANGE# and NEXT_CHANGE#)
order by SEQUENCE#;
	 */
	public static final String ARCHIVED_LOGS =
			"select   NAME, THREAD#, SEQUENCE#, FIRST_CHANGE#, NEXT_CHANGE#, BLOCKS*BLOCK_SIZE BYTES\n" + 
			"from     V$ARCHIVED_LOG\n" + 
			"where    ARCHIVED='YES' and STANDBY_DEST='NO' and (? >= FIRST_CHANGE# or ? <= NEXT_CHANGE#)\n" + 
			"  and    SEQUENCE# >= \n" + 
			"          (select min(SEQUENCE#)\n" + 
			"           from   V$ARCHIVED_LOG\n" + 
			"           where  ARCHIVED='YES' and STANDBY_DEST='NO' and ? between FIRST_CHANGE# and NEXT_CHANGE#)\n" + 
			"order by SEQUENCE#";

	/*
declare
  l_OPTION binary_integer; 
begin
  if (? = 0) then
    l_OPTION := DBMS_LOGMNR.NEW;
  else
    l_OPTION := DBMS_LOGMNR.ADDFILE;
  end if;
  DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => ?, OPTIONS =>  l_OPTION);
end;
	 */
	public static final String ADD_ARCHIVED_LOG =
			"declare\n" + 
			"  l_OPTION binary_integer; \n" + 
			"begin\n" + 
			"  if (? = 0) then\n" + 
			"    l_OPTION := DBMS_LOGMNR.NEW;\n" + 
			"  else\n" + 
			"    l_OPTION := DBMS_LOGMNR.ADDFILE;\n" + 
			"  end if;\n" + 
			"  DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => ?, OPTIONS =>  l_OPTION);\n" + 
			"end;\n";
	/*
begin
  DBMS_LOGMNR.START_LOGMNR(
    STARTSCN => ?,
	ENDSCN => ?,
	OPTIONS =>  
      DBMS_LOGMNR.SKIP_CORRUPTION +
      DBMS_LOGMNR.NO_SQL_DELIMITER +
      DBMS_LOGMNR.NO_ROWID_IN_STMT);
end;
	 */
	public static final String START_LOGMINER =
			"begin\n" + 
			"  DBMS_LOGMNR.START_LOGMNR(\n" + 
			"    STARTSCN => ?,\n" +
			"	 ENDSCN => ?,\n" +
			"	 OPTIONS =>  \n" +
			"      DBMS_LOGMNR.SKIP_CORRUPTION +\n" +
			"      DBMS_LOGMNR.NO_SQL_DELIMITER +\n" + 
			"      DBMS_LOGMNR.NO_ROWID_IN_STMT);\n" + 
			"end;\n";

	/*
begin
  DBMS_LOGMNR.END_LOGMNR;
end;
	 */
	public static final String STOP_LOGMINER =
			"begin\n" + 
			"  DBMS_LOGMNR.END_LOGMNR;\n" + 
			"end;\n";

/*
select SCN, CSCN, TIMESTAMP, OPERATION_CODE, RS_ID, SSN, CSF, ROW_ID, DATA_OBJ#, SQL_REDO
from   V$LOGMNR_CONTENTS
where  OPERATION_CODE in (1,2,3)
 */
	public static final String MINE_DATA_NON_CDB =
			"select SCN, CSCN COMMIT_SCN, TIMESTAMP, OPERATION_CODE, XID, RS_ID, SSN, CSF, ROW_ID, DATA_OBJ#, SQL_REDO\n" + 
			"from   V$LOGMNR_CONTENTS\n";
/*
select SCN, CSCN, TIMESTAMP, OPERATION_CODE, RS_ID, SSN, CSF, ROW_ID, DATA_OBJ#, SQL_REDO
from   V$LOGMNR_CONTENTS
where  OPERATION_CODE in (1,2,3)
 */
	public static final String MINE_DATA_CDB =
			"select SCN, COMMIT_SCN, TIMESTAMP, OPERATION_CODE, XID, RS_ID, SSN, CSF, ROW_ID, DATA_OBJ#, SQL_REDO, CON_ID\n" + 
			"from   V$LOGMNR_CONTENTS\n";

/*
select O.OWNER, O.OBJECT_NAME TABLE_NAME
from   DBA_OBJECTS O
where  O.OBJECT_TYPE='TABLE'
  and  O.TEMPORARY='N'
  and  O.OBJECT_ID=?
  and  O.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER');
 */
	public static final String CHECK_TABLE_NON_CDB =
			"select O.OWNER, O.OBJECT_NAME TABLE_NAME\n" + 
			"from   DBA_OBJECTS O\n" + 
			"where  O.OBJECT_TYPE='TABLE'\n" + 
			"  and  O.TEMPORARY='N'\n" + 
			"  and  O.OBJECT_ID=?\n" + 
			"  and  O.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER')\n";
/*
select O.OWNER, O.OBJECT_NAME TABLE_NAME, P.PDB_NAME
from   CDB_OBJECTS O, CDB_PDBS P
where  O.OBJECT_TYPE='TABLE'
  and  O.TEMPORARY='N'
  and  O.OBJECT_ID=?
  and  O.CON_ID=?
  and  O.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER')
  and  O.CON_ID=P.CON_ID (+);
 */
	public static final String CHECK_TABLE_CDB =
		"select O.OWNER, O.OBJECT_NAME TABLE_NAME, P.PDB_NAME\n" + 
		"from   CDB_OBJECTS O, CDB_PDBS P\n" + 
		"where  O.OBJECT_TYPE='TABLE'\n" + 
		"  and  O.TEMPORARY='N'\n" + 
		"  and  O.OBJECT_ID=?\n" + 
		"  and  O.CON_ID=?\n" + 
		"  and  O.OWNER not in ('SYS','SYSTEM','MGDSYS','OJVMSYS','AUDSYS','OUTLN','APPQOSSYS','DBSNMP','CTXSYS','ORDSYS','ORDPLUGINS','ORDDATA','MDSYS','OLAPSYS','GGSYS','XDB','GSMADMIN_INTERNAL','DBSFWUSER')\n" + 
		"  and  O.CON_ID=P.CON_ID (+)\n";

}
