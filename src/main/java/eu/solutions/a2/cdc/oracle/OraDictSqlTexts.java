package eu.solutions.a2.cdc.oracle;

public class OraDictSqlTexts {

	/*
	select count(*)
	from   ALL_MVIEW_LOGS L
	where  L.ROWIDS='NO' and L.PRIMARY_KEY='YES' and L.OBJECT_ID='NO'
	  and  L.SEQUENCE='YES' and L.COMMIT_SCN_BASED='NO' and L.INCLUDE_NEW_VALUES='NO';
	 */
	public static final String MVIEW_COUNT_PK_SEQ_NOSCN_NONV_NOOI =
			"select count(*)\n" +
			"from   ALL_MVIEW_LOGS L\n" +
			"where  L.ROWIDS='NO' and L.PRIMARY_KEY='YES' and L.OBJECT_ID='NO'\n" + 
			"  and  L.SEQUENCE='YES' and L.COMMIT_SCN_BASED='NO' and L.INCLUDE_NEW_VALUES='NO'\n";

	/*
	select L.LOG_OWNER, L.MASTER, L.LOG_TABLE
	from   ALL_MVIEW_LOGS L
	where  L.ROWIDS='NO' and L.PRIMARY_KEY='YES' and L.OBJECT_ID='NO'
	  and  L.SEQUENCE='YES' and L.COMMIT_SCN_BASED='NO' and L.INCLUDE_NEW_VALUES='NO';
	 */
	public static final String MVIEW_LIST_PK_SEQ_NOSCN_NONV_NOOI =
			"select L.LOG_OWNER, L.MASTER, L.LOG_TABLE\n" +
			"from   ALL_MVIEW_LOGS L\n" +
			"where  L.ROWIDS='NO' and L.PRIMARY_KEY='YES' and L.OBJECT_ID='NO'\n" + 
			"  and  L.SEQUENCE='YES' and L.COMMIT_SCN_BASED='NO' and L.INCLUDE_NEW_VALUES='NO'\n";

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
	public static final String  COLUMN_LIST =
			"select C.COLUMN_NAME, C.DATA_TYPE, C.DATA_LENGTH, C.DATA_PRECISION, C.DATA_SCALE, C.NULLABLE,\n" +
			"	 (select 'Y'\n" + 
			"	  from   ALL_TAB_COLUMNS TC\n" +
			"	  where  TC.TABLE_NAME=? and TC.OWNER=C.OWNER\n" +
			"	    and  TC.COLUMN_NAME=C.COLUMN_NAME and TC.COLUMN_NAME not like '%$$') PK\n" +
			"from   ALL_TAB_COLUMNS C\n" +
			"where  C.OWNER=? and C.TABLE_NAME=?\n" +
			"  and  (C.DATA_TYPE in ('DATE', 'FLOAT', 'NUMBER', 'RAW', 'CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2', 'BLOB', 'CLOB') or C.DATA_TYPE like 'TIMESTAMP%')";

}
