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
public class ParamConstants {

	public static final int SCHEMA_TYPE_INT_DEBEZIUM = 1;
	public static final int SCHEMA_TYPE_INT_KAFKA_STD = 2;

	public static final int TOPIC_NAME_STYLE_INT_TABLE = 1;
	public static final int TOPIC_NAME_STYLE_INT_SCHEMA_TABLE = 2;
	public static final int TOPIC_NAME_STYLE_INT_PDB_SCHEMA_TABLE = 3;

	public static final String CONNECTION_URL_PARAM = "a2.jdbc.url";
	public static final String CONNECTION_URL_DOC = "JDBC connection URL";

	public static final String CONNECTION_USER_PARAM = "a2.jdbc.username";
	public static final String CONNECTION_USER_DOC = "JDBC connection user";

	public static final String CONNECTION_PASSWORD_PARAM = "a2.jdbc.password";
	public static final String CONNECTION_PASSWORD_DOC = "JDBC connection password";

	public static final String CONNECTION_WALLET_PARAM = "a2.wallet.location";
	public static final String CONNECTION_WALLET_DOC = "Location of Oracle Wallet. Not required when a2.jdbc.url & a2.jdbc.username & a2.jdbc.password are set";

	public static final String CONNECTION_TNS_ADMIN_PARAM = "a2.tns.admin";
	public static final String CONNECTION_TNS_ADMIN_DOC = "Location of tnsnames.ora file. Not required when a2.jdbc.url & a2.jdbc.username & a2.jdbc.password are set";

	public static final String CONNECTION_TNS_ALIAS_PARAM = "a2.tns.alias";
	public static final String CONNECTION_TNS_ALIAS_DOC = "Connection TNS alias. Not required when a2.jdbc.url & a2.jdbc.username & a2.jdbc.password are set";

	public static final String POLL_INTERVAL_MS_PARAM = "a2.poll.interval";
	public static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in each table";
	public static final int POLL_INTERVAL_MS_DEFAULT = 1000;

	public static final String BATCH_SIZE_PARAM = "a2.batch.size";
	public static final String BATCH_SIZE_DOC = "Maximum number of statements to include in a single batch when inserting/updating/deleting data";
	public static final int BATCH_SIZE_DEFAULT = 1000;

	public static final String SCHEMA_TYPE_PARAM = "a2.schema.type";
	public static final String SCHEMA_TYPE_DOC = "Type of schema used by oracdc: Kafka Connect JDBC compatible (default) or Debezium";
	public static final String SCHEMA_TYPE_KAFKA = "kafka";
	public static final String SCHEMA_TYPE_DEBEZIUM = "debezium";

	public static final String TABLE_EXCLUDE_PARAM = "a2.exclude";
	public static final String TABLE_EXCLUDE_DOC = "List of tables to exclude from processing";

	public static final String TABLE_INCLUDE_PARAM = "a2.include";
	public static final String TABLE_INCLUDE_DOC = "List of table names to include in processing";

	public static final String REDO_FILES_COUNT_PARAM = "a2.redo.count";
	public static final String REDO_FILES_COUNT_DOC = "Quantity of archived logs to process during each DBMS_LOGMNR.START_LOGMNR call";

	public static final String REDO_FILES_SIZE_PARAM = "a2.redo.size";
	public static final String REDO_FILES_SIZE_DOC = "Minimal size of archived logs to process during each DBMS_LOGMNR.START_LOGMNR call";

	public static final String LGMNR_START_SCN_PARAM = "a2.first.change";
	public static final String LGMNR_START_SCN_DOC = "When set DBMS_LOGMNR.START_LOGMNR will start mining from this SCN. When not set min(FIRST_CHANGE#) from V$ARCHIVED_LOG will used. Overrides SCN value  stored in offset file";

	public static final String TEMP_DIR_PARAM = "a2.tmpdir";
	public static final String TEMP_DIR_DOC = "Temporary directory for non-heap storage. When not set, OS temp directory used"; 

	public static final String MAKE_STANDBY_ACTIVE_PARAM = "a2.standby.activate";
	public static final String MAKE_STANDBY_ACTIVE_DOC = "Use standby database with V$DATABASE.OPEN_MODE = MOUNTED for LogMiner calls. Default - false"; 

	public static final String STANDBY_WALLET_PARAM = "a2.standby.wallet.location";
	public static final String STANDBY_WALLET_DOC = "Location of Oracle Wallet for connecting to standby database with V$DATABASE.OPEN_MODE = MOUNTED";

	public static final String STANDBY_TNS_ADMIN_PARAM = "a2.standby.tns.admin";
	public static final String STANDBY_TNS_ADMIN_DOC = "Location of tnsnames.ora file for connecting to standby database with V$DATABASE.OPEN_MODE = MOUNTED";

	public static final String STANDBY_TNS_ALIAS_PARAM = "a2.standby.tns.alias";
	public static final String STANDBY_TNS_ALIAS_DOC = "Connection TNS alias for connecting to standby database with V$DATABASE.OPEN_MODE = MOUNTED";

	public static final String PERSISTENT_STATE_FILE_PARAM = "a2.persistent.state.file";
	public static final String PERSISTENT_STATE_FILE_DOC = "Name of file to store oracdc state between restart. Default $TMPDIR/oracdc.state";

	public static final String ORACDC_SCHEMAS_PARAM = "a2.oracdc.schemas";
	public static final String ORACDC_SCHEMAS_DOC = "Use oracdc extensions for Oracle datatypes. Default false";

	public static final String DICTIONARY_FILE_PARAM = "a2.dictionary.file";
	public static final String DICTIONARY_FILE_DOC = "File with stored columns data type mapping. For more details contact us at oracle@a2-solutions.eu";

	public static final String INITIAL_LOAD_PARAM = "a2.initial.load";
	public static final String INITIAL_LOAD_DOC = "A mode for performing initial load of data from tables when set to EXECUTE. Default - IGNORE";
	public static final String INITIAL_LOAD_IGNORE = "IGNORE";
	public static final String INITIAL_LOAD_EXECUTE = "EXECUTE";
	public static final String INITIAL_LOAD_COMPLETED = "COMPLETED";

	public static final String TOPIC_NAME_STYLE_PARAM = "a2.topic.name.style";
	public static final String TOPIC_NAME_STYLE_DOC = "Kafka topic naming convention when a2.schema.type=kafka. Valid values - TABLE (default), SCHEMA_TABLE, PDB_SCHEMA_TABLE";
	public static final String TOPIC_NAME_STYLE_TABLE = "TABLE";
	public static final String TOPIC_NAME_STYLE_SCHEMA_TABLE = "SCHEMA_TABLE";
	public static final String TOPIC_NAME_STYLE_PDB_SCHEMA_TABLE = "PDB_SCHEMA_TABLE";

	public static final String TOPIC_NAME_DELIMITER_PARAM = "a2.topic.name.delimiter";
	public static final String TOPIC_NAME_DELIMITER_DOC = "Kafka topic name delimiter when a2.schema.type=kafka and a2.topic.name.style set to SCHEMA_TABLE or PDB_SCHEMA_TABLE. Valid values - '_' (Default), '-', '.'";
	public static final String TOPIC_NAME_DELIMITER_UNDERSCORE = "_";
	public static final String TOPIC_NAME_DELIMITER_DASH = "-";
	public static final String TOPIC_NAME_DELIMITER_DOT = ".";

	public static final String TABLE_LIST_STYLE_PARAM = "a2.table.list.style";
	public static final String TABLE_LIST_STYLE_DOC = "When set to 'static' (default) oracdc reads tables and partition list to process only at startup according to values of a2.include and a2.exclude parameters. When set to 'dynamic' oracdc builds list of objects to process on the fly";
	public static final String TABLE_LIST_STYLE_STATIC = "static";
	public static final String TABLE_LIST_STYLE_DYNAMIC = "dynamic";

	public static final String PROCESS_LOBS_PARAM = "a2.process.lobs";
	public static final String PROCESS_LOBS_DOC = "process Oracle LOB columns? Default - false";

	public static final String CONNECTION_BACKOFF_PARAM = "a2.connection.backoff";
	public static final String CONNECTION_BACKOFF_DOC = "backoff time in milliseconds between reconnectoion attempts. Default - 30000ms";
	public static final int CONNECTION_BACKOFF_DEFAULT = 30000;

	public static final String ARCHIVED_LOG_CAT_PARAM = "a2.archived.log.catalog";
	public static final String ARCHIVED_LOG_CAT_DOC = "name of class which implements eu.solutions.a2.cdc.oracle.OraLogMiner interface. Default - eu.solutions.a2.cdc.oracle.OraCdcV$ArchivedLogImpl which reads archived log information from V$ARCHIVED_LOG fixed view";
	public static final String ARCHIVED_LOG_CAT_DEFAULT = "eu.solutions.a2.cdc.oracle.OraCdcV$ArchivedLogImpl";

	public static final String FETCH_SIZE_PARAM = "a2.fetch.size";
	public static final String FETCH_SIZE_DOC = "number of rows fetched with each RDBMS round trip for access V$LOGMNR_CONTENTS. Default 32";
	public static final int FETCH_SIZE_DEFAULT = 32;

	public static final String TRACE_LOGMINER_PARAM = "a2.logminer.trace";
	public static final String TRACE_LOGMINER_DOC = "trace with 'event 10046 level 8' LogMiner calls? Default - false";

}
