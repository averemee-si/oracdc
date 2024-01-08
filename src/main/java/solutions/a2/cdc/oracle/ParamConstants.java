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

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class ParamConstants {

	public static final int SCHEMA_TYPE_INT_DEBEZIUM = 1;
	public static final int SCHEMA_TYPE_INT_KAFKA_STD = 2;
	public static final int SCHEMA_TYPE_INT_SINGLE = 3;

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
	public static final String SCHEMA_TYPE_DOC = "Type of schema used by oracdc: kafka (default) with separate schemas for key and value, single - single schema for all fields or Debezium";
	public static final String SCHEMA_TYPE_KAFKA = "kafka";
	public static final String SCHEMA_TYPE_DEBEZIUM = "debezium";
	public static final String SCHEMA_TYPE_SINGLE = "single";

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

	public static final String STANDBY_URL_PARAM = "a2.standby.jdbc.url";
	public static final String STANDBY_URL_DOC = "JDBC connection URL for connecting to standby database with V$DATABASE.OPEN_MODE = MOUNTED";

	public static final String STANDBY_TNS_ADMIN_PARAM = "a2.standby.tns.admin";
	public static final String STANDBY_TNS_ADMIN_DOC = "Location of tnsnames.ora file for connecting to standby database with V$DATABASE.OPEN_MODE = MOUNTED";

	public static final String STANDBY_TNS_ALIAS_PARAM = "a2.standby.tns.alias";
	public static final String STANDBY_TNS_ALIAS_DOC = "Connection TNS alias for connecting to standby database with V$DATABASE.OPEN_MODE = MOUNTED";

	public static final String PERSISTENT_STATE_FILE_PARAM = "a2.persistent.state.file";
	public static final String PERSISTENT_STATE_FILE_DOC = "Name of file to store oracdc state between restart. Default $TMPDIR/oracdc.state";

	public static final String DICTIONARY_FILE_PARAM = "a2.dictionary.file";
	public static final String DICTIONARY_FILE_DOC = "File with stored columns data type mapping. For more details contact us at oracle@a2-solutions.eu";

	public static final String INITIAL_LOAD_PARAM = "a2.initial.load";
	public static final String INITIAL_LOAD_DOC = "A mode for performing initial load of data from tables when set to EXECUTE. Default - IGNORE";
	public static final String INITIAL_LOAD_IGNORE = "IGNORE";
	public static final String INITIAL_LOAD_EXECUTE = "EXECUTE";
	public static final String INITIAL_LOAD_COMPLETED = "COMPLETED";

	public static final String KAFKA_TOPIC_PARAM = "a2.kafka.topic";
	public static final String KAFKA_TOPIC_PARAM_DOC = "Target topic to send data";
	public static final String KAFKA_TOPIC_PARAM_DEFAULT = "oracdc-topic";

	public static final String TOPIC_PARTITION_PARAM = "a2.topic.partition";
	public static final String TOPIC_PARTITION_DOC = "Kafka topic partition to write data. Default - 0";

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
	public static final String ARCHIVED_LOG_CAT_DOC = "name of class which implements solutions.a2.cdc.oracle.OraLogMiner interface. Default - solutions.a2.cdc.oracle.OraCdcV$ArchivedLogImpl which reads archived log information from V$ARCHIVED_LOG fixed view";
	public static final String ARCHIVED_LOG_CAT_DEFAULT = "solutions.a2.cdc.oracle.OraCdcV$ArchivedLogImpl";

	public static final String FETCH_SIZE_PARAM = "a2.fetch.size";
	public static final String FETCH_SIZE_DOC = "number of rows fetched with each RDBMS round trip for access V$LOGMNR_CONTENTS. Default 32";
	public static final int FETCH_SIZE_DEFAULT = 32;

	public static final String TRACE_LOGMINER_PARAM = "a2.logminer.trace";
	public static final String TRACE_LOGMINER_DOC = "trace with 'event 10046 level 8' LogMiner calls? Default - false";

	public static final String MAKE_DISTRIBUTED_ACTIVE_PARAM = "a2.distributed.activate";
	public static final String MAKE_DISTRIBUTED_ACTIVE_DOC = "Use oracdc in distributed configuration (redo logs are generated at source RDBMS server and then transferred to compatible target RDBMS server for processing with LogMiner. Default - false"; 

	public static final String DISTRIBUTED_WALLET_PARAM = "a2.distributed.wallet.location";
	public static final String DISTRIBUTED_WALLET_DOC = "Location of Oracle Wallet for connecting to target database in distributed mode";

	public static final String DISTRIBUTED_URL_PARAM = "a2.distributed.jdbc.url";
	public static final String DISTRIBUTED_URL_DOC = "JDBC connection URL for connecting to target database in distributed mode";

	public static final String DISTRIBUTED_TNS_ADMIN_PARAM = "a2.distributed.tns.admin";
	public static final String DISTRIBUTED_TNS_ADMIN_DOC = "Location of tnsnames.ora file for connecting to target database in distributed mode";

	public static final String DISTRIBUTED_TNS_ALIAS_PARAM = "a2.distributed.tns.alias";
	public static final String DISTRIBUTED_TNS_ALIAS_DOC = "Connection TNS alias for connecting to target database in distributed mode";

	public static final String DISTRIBUTED_TARGET_HOST = "a2.distributed.target.host";
	public static final String DISTRIBUTED_TARGET_HOST_DOC = "hostname of the target (where dbms_logmnr runs) database on which the shipment agent is running";

	public static final String DISTRIBUTED_TARGET_PORT = "a2.distributed.target.port";
	public static final String DISTRIBUTED_TARGET_PORT_DOC = "port number on which shipping agent listens for requests";
	public static final int DISTRIBUTED_TARGET_PORT_DEFAULT = 21521;

	public static final String LOB_TRANSFORM_CLASS_PARAM = "a2.lob.transformation.class";
	public static final String LOB_TRANSFORM_CLASS_DOC = "name of class which implements solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf interface. Default - solutions.a2.cdc.oracle.data.OraCdcDefaultLobTransformationsImpl which just passes information about and values of BLOB/CLOB/NCLOB/XMLTYPE columns to Kafka Connect without performing any additional transformation";
	public static final String LOB_TRANSFORM_CLASS_DEFAULT = "solutions.a2.cdc.oracle.data.OraCdcDefaultLobTransformationsImpl";

	public static final String RESILIENCY_TYPE_PARAM = "a2.resiliency.type";
	public static final String RESILIENCY_TYPE_DOC = "How restarts and crashes are handled: " + 
			"In ``legacy`` mode (the default), all information is stored in the file system, " + 
			"delivery of all changes is guaranteed with exactly-once semantics, " + 
			"but this mode does not protect against file system failures. " + 
			"When set to ``fault-tolerant``, all restart data stored on Kafka topics, " + 
			"the connector depends only on Kafka cluster, " + 
			"but if an error occurs in the middle of sending a Oracle transaction to the Kafka broker, " +
			"that transaction will be re-read from archived redo and sending to Kafka will continue " +
			"after last successfully processed record to maintain exactly-once semantics";
	public static final String RESILIENCY_TYPE_LEGACY = "legacy";
	public static final String RESILIENCY_TYPE_FAULT_TOLERANT = "fault-tolerant";

	public static final String USE_RAC_PARAM = "a2.use.rac";
	public static final String USE_RAC_DOC = 
			"Default - false.\n" +
			"When set to true oracdc first tried to detect is this connection to Oracle RAC.\n" + 
			"If database is not RAC, only the warning message is printed.\n" + 
			"If oracdc is connected to Oracle RAC additional checks are performed and oracdc starts a separate task for each redo thread/RAC instance. " +
			"Changes for the same table from different redo threads (RAC instances) are delivered to the same topic but to different partition where <PARTITION_NUMBER> = <THREAD#> - 1";
	
	public static final String ORA_TRANSACTION_IMPL_PARAM = "a2.transaction.implementation";
	public static final String ORA_TRANSACTION_IMPL_DOC = 
			"Queue implementation for processing SQL statements within transactions.\n" +
			"Allowed values: ChronicleQueue and ArrayList.\n" + 
			"Default - ChronicleQueue.\n" + 
			"LOB processing is only possible if a2.transaction.implementation is set to ChronicleQueue.\n";
	public static final String ORA_TRANSACTION_IMPL_CHRONICLE = "ChronicleQueue";
	public static final String ORA_TRANSACTION_IMPL_JVM = "ArrayList";
	
	public static final String PROCESS_ONLINE_REDO_LOGS_PARAM = "a2.process.online.redo.logs";
	public static final String PROCESS_ONLINE_REDO_LOGS_DOC = 
			"Default - false.\n" +
			"When set to true oracdc process online redo logs.";
	
	public static final String CURRENT_SCN_QUERY_INTERVAL_PARAM = "a2.scn.query.interval.ms";
	public static final String CURRENT_SCN_QUERY_INTERVAL_DOC = 
			"Default - 60_000.\n" +
			"Minimum time in milliseconds to determine the current SCN during online redo log processing.";
	public static final int CURRENT_SCN_QUERY_INTERVAL_DEFAULT = 60_000;

	public static final String PRINT_ALL_ONLINE_REDO_RANGES_PARAM = "a2.print.all.online.scn.ranges";
	public static final String PRINT_ALL_ONLINE_REDO_RANGES_DOC =
			"Default - true.\n" +
			"If set to true oracdc prints detailed information about SCN ranges when working with the online log every time interval specified by the a2.scn.query.interval.ms parameter.\n" +
			"If set to false oracdc prints information about current online redo only when SEQUENCE# is changed.";

	public static final String LM_RECONNECT_INTERVAL_MS_PARAM = "a2.log.miner.reconnect.ms";
	public static final String LM_RECONNECT_INTERVAL_MS_DOC =
			"The time interval in milleseconds after which a reconnection to LogMiner occurs, including the re-creation of the Oracle connection.\n" +
			"Unix/Linux only, on Windows oracdc creates new LogMiner session and re-creation of database connection every time DBMS_LOGMNR.START_LOGMNR is called.\n" +
			"Default - Long.MAX_VALUE";

	public static final String INTERNAL_PARAMETER_DOC = "Internal. Do not set!"; 
	public static final String INTERNAL_RAC_URLS_PARAM = "__a2.internal.rac.urls"; 
	public static final String INTERNAL_DG4RAC_THREAD_PARAM = "__a2.internal.dg4rac.thread";

}
