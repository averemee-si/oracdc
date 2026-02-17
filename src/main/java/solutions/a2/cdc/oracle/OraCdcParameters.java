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
public class OraCdcParameters {

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

	public static final String TABLE_EXCLUDE_PARAM = "a2.exclude";
	public static final String TABLE_EXCLUDE_DOC = "List of tables to exclude from processing";

	public static final String TABLE_INCLUDE_PARAM = "a2.include";
	public static final String TABLE_INCLUDE_DOC = "List of table names to include in processing";

	public static final String KAFKA_TOPIC_PARAM = "a2.kafka.topic";
	public static final String KAFKA_TOPIC_DOC = "Target topic to send data";
	public static final String KAFKA_TOPIC_DEFAULT = "oracdc-topic";

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

	public static final String USE_ALL_COLUMNS_ON_DELETE_PARAM = "a2.use.all.columns.on.delete";
	public static final String USE_ALL_COLUMNS_ON_DELETE_DOC =
			"""
			Default - false.
			When set to false (default) oracdc reads and processes only the PK columns from the redo record and sends only the key fields to the Kafka topic.
			When set to true oracdc reads and processes all table columns from the redo record.
			""";
	public static final boolean USE_ALL_COLUMNS_ON_DELETE_DEFAULT = false; 

	public static final String TOPIC_PREFIX_PARAM = "a2.topic.prefix";
	public static final String TOPIC_PREFIX_DOC = "Prefix to prepend table names to generate name of Kafka topic.";

	public static final int INCOMPLETE_REDO_INT_ERROR = 1;
	public static final int INCOMPLETE_REDO_INT_SKIP = 2;
	public static final int INCOMPLETE_REDO_INT_RESTORE = 3;

	public static final int TOPIC_NAME_STYLE_INT_TABLE = 1;
	public static final int TOPIC_NAME_STYLE_INT_SCHEMA_TABLE = 2;
	public static final int TOPIC_NAME_STYLE_INT_PDB_SCHEMA_TABLE = 3;

	public static final int PK_TYPE_INT_WELL_DEFINED = 1;
	public static final int PK_TYPE_INT_ANY_UNIQUE = 2;

	public static final String ORACDC_SCHEMAS_PARAM = "a2.oracdc.schemas";
	public static final String ORACDC_SCHEMAS_DOC =
			"""
			Use oracdc extensions for Oracle NUMBER, TIMESTAMPTZ, and TIMESTAMPLTZ data types.
			Default - false
			""";

	public static final String INCOMPLETE_REDO_TOLERANCE_PARAM = "a2.incomplete.redo.tolerance";
	public static final String INCOMPLETE_REDO_TOLERANCE_DOC =
			"""
			Connector behavior when processing an incomplete redo record.
			Allowed values: error, skip, and restore.
			When set to:
			- 'error' oracdc prints information about incomplete redo record and stops connector.
			- 'skip' oracdc prints information about incomplete redo record and continue processing
			- 'restore' oracdc tries to restore missed information from actual row incarnation from the table using ROWID from redo the record.
			
			Default - 'error'.
			""";
	public static final String INCOMPLETE_REDO_TOLERANCE_ERROR = "error";
	public static final String INCOMPLETE_REDO_TOLERANCE_SKIP = "skip";
	public static final String INCOMPLETE_REDO_TOLERANCE_RESTORE = "restore";

	public static final String PRINT_INVALID_HEX_WARNING_PARAM = "a2.print.invalid.hex.value.warning";
	public static final String PRINT_INVALID_HEX_WARNING_DOC =
			"""
			When set to true oracdc prints information about invalid hex values (like single byte value for DATE/TIMESTAMP/TIMESTAMPTZ) in log.
			
			Default - false.
			""";

	public static final String PROCESS_ONLINE_REDO_LOGS_PARAM = "a2.process.online.redo.logs";
	public static final String PROCESS_ONLINE_REDO_LOGS_DOC =
			"""
			When set to true oracdc process online redo logs.
			
			Default - false.
			""";
	
	public static final String CURRENT_SCN_QUERY_INTERVAL_PARAM = "a2.scn.query.interval.ms";
	public static final String CURRENT_SCN_QUERY_INTERVAL_DOC =
			"""
			Minimum time in milliseconds to determine the current SCN during online redo log processing.
			
			Default - 60000.
			""";
	public static final int CURRENT_SCN_QUERY_INTERVAL_DEFAULT = 60_000;

	public static final String PRINT_ALL_ONLINE_REDO_RANGES_PARAM = "a2.print.all.online.scn.ranges";
	public static final String PRINT_ALL_ONLINE_REDO_RANGES_DOC =
			"""
			If set to true oracdc prints detailed information about SCN ranges when working with the online log every time interval specified by the a2.scn.query.interval.ms parameter.
			If set to false oracdc prints information about current online redo only when SEQUENCE# is changed.
			
			Default - true.
			""";

	public static final String PROTOBUF_SCHEMA_NAMING_PARAM = "a2.protobuf.schema.naming";
	public static final String PROTOBUF_SCHEMA_NAMING_DOC =
			"""
			When set to true oracdc generates schema names as valid Protocol Buffers identifiers using underscore as separator.
			When set to false (default) oracdc generates schema names using dot as separator.
			
			Default - false.
			""";
	
	public static final String INITIAL_LOAD_PARAM = "a2.initial.load";
	public static final String INITIAL_LOAD_DOC =
			"""
			A mode for performing initial load of data from tables when set to EXECUTE.
			
			Default - IGNORE.
			""";
	public static final String INITIAL_LOAD_IGNORE = "IGNORE";
	public static final String INITIAL_LOAD_EXECUTE = "EXECUTE";
	public static final String INITIAL_LOAD_COMPLETED = "COMPLETED";

	public static final String TOPIC_NAME_DELIMITER_PARAM = "a2.topic.name.delimiter";
	public static final String TOPIC_NAME_DELIMITER_DOC =
			"""
			Kafka topic name delimiter when a2.schema.type=kafka and a2.topic.name.style set to SCHEMA_TABLE or PDB_SCHEMA_TABLE.
			Valid values - '_' | '-' | '.'
			
			Default - '_'
			""";
	public static final String TOPIC_NAME_DELIMITER_UNDERSCORE = "_";
	public static final String TOPIC_NAME_DELIMITER_DASH = "-";
	public static final String TOPIC_NAME_DELIMITER_DOT = ".";

	public static final String TOPIC_NAME_STYLE_PARAM = "a2.topic.name.style";
	public static final String TOPIC_NAME_STYLE_DOC =
			"""
			Kafka topic naming convention when a2.schema.type=kafka.
			Valid values - TABLE | SCHEMA_TABLE | PDB_SCHEMA_TABLE
			
			Default - TABLE
			""";
	public static final String TOPIC_NAME_STYLE_TABLE = "TABLE";
	public static final String TOPIC_NAME_STYLE_SCHEMA_TABLE = "SCHEMA_TABLE";
	public static final String TOPIC_NAME_STYLE_PDB_SCHEMA_TABLE = "PDB_SCHEMA_TABLE";

	public static final String PK_TYPE_PARAM = "a2.pk.type";
	public static final String PK_TYPE_DOC =
			"""
			When set to 'well_defined' the key fields are the table's primary key columns or, if the table does not have a primary key, the table's unique key columns in which all columns are NOT NULL.
			If there are no appropriate keys in the table, oracdc uses the a2.use.rowid.as.key parameter and generates a pseudo key based on the row's ROWID, or generates a schema without any key fields.
			When set to 'any_unique' and the table does not have a primary key or a unique key with all NOT NULL columns, then the key fields will be the unique key columns which may have NULL columns.
			If there are no appropriate keys in the table, oracdc uses the a2.use.rowid.as.key parameter and generates a pseudo key based on the row's ROWID, or generates a schema without any key fields.
			
			Default - 'well_defined'.
			""";
	public static final String PK_TYPE_WELL_DEFINED = "well_defined";
	public static final String PK_TYPE_ANY_UNIQUE = "any_unique";

	public static final String USE_ROWID_AS_KEY_PARAM = "a2.use.rowid.as.key";
	public static final String USE_ROWID_AS_KEY_DOC =
			"""
			When set to true and the table does not have a appropriate primary or unique key, oracdc adds surrogate key using the ROWID.
			When set to false and the table does not have a appropriate primary or unique key, oracdc generates schema for the table without any key fields.
			
			Default - true.
			""";

	public static final String TOPIC_MAPPER_DEFAULT = "solutions.a2.cdc.oracle.OraCdcDefaultTopicNameMapper";
	public static final String TOPIC_MAPPER_PARAM = "a2.topic.mapper";
	public static final String TOPIC_MAPPER_DOC =
			"""
			The fully qualified name of the class that defines the Kafka topic to which data from the tables should be sent.
			The class must implement the `solutions.a2.cdc.oracle.TopicNameMapper` interface.
			The connector comes with two predefined classes:
			1) `solutions.a2.cdc.oracle.OraCdcDefaultTopicNameMapper`, which generates a unique name for each table based on the PDB name, schema name, and table name using the values ​​of the `a2.topic.prefix`, `a2.topic.name.style`, and `a2.topic.name.delimiter` parameters.
			2) Another predefined class, `solutions.a2.cdc.oracle.OraCdcSingleTopicNameMapper`, uses the value of the `a2.kafka.topic` parameter as the Kafka topic name, and all change events for all tables are written to a single Kafka topic.
			
			Default - """ + TOPIC_MAPPER_DEFAULT;

	public static final boolean STOP_ON_ORA_1284_DEFAULT = true;
	public static final String STOP_ON_ORA_1284_PARAM = "a2.stop.on.ora.1284";
	public static final String STOP_ON_ORA_1284_DOC =
			"""
			If set to true, the connector stops on an Oracle database error 'ORA-01284: file <Absolute-Path-To-Log-File> cannot be opened'.
			If set to false, the connector prints an error message and continues processing.
			
			Default - """ + STOP_ON_ORA_1284_DEFAULT;

	public static final boolean PRINT_UNABLE_TO_DELETE_WARNING_DEFAULT = false;
	public static final String PRINT_UNABLE_TO_DELETE_WARNING_PARAM = "a2.print.unable.to.delete.warning";
	public static final String PRINT_UNABLE_TO_DELETE_WARNING_DOC =
			"""
			If set to true, the connector prints a warning message including all redo record details about ignoring the DELETE operation for tables without a primary key or it surrogate or a schema that does not contain key information.
			If set to false, the connector does not print a warning message about ignoring the DELETE operation.
			
			Default - """ + PRINT_UNABLE_TO_DELETE_WARNING_DEFAULT;

	public static final String SCHEMANAME_MAPPER_DEFAULT = "solutions.a2.cdc.oracle.OraCdcDefaultSchemaNameMapper";
	public static final String SCHEMANAME_MAPPER_PARAM = "a2.schema.name.mapper";
	public static final String SCHEMANAME_MAPPER_DOC =
			"""
			The fully-qualified class name of the class that constructs schema name from the Oracle PDB name (if present), the table owner, and the table name.
			
			Default - """ + SCHEMANAME_MAPPER_DEFAULT;

	public static final String ORA_ROWSCN_PARAM = "a2.pseudocolumn.ora_rowscn";
	public static final String ORA_ROWSCN_DOC =
			"""
			The name of the field in the Kafka Connect record that contains the SCN where the row change was made. If the value is empty, the SCN field is not included in the Kafka Connect records.
			
			Default - \"\", i.e. do not include SCN field in Kafka Connect record"
			""";

	public static final String ORA_COMMITSCN_PARAM = "a2.pseudocolumn.ora_commitscn";
	public static final String ORA_COMMITSCN_DOC =
			"""
			The name of the field in the Kafka Connect record that contains the commit SCN of the transaction in which the row change was made. If the value is empty, the commit SCN field is not included in the Kafka Connect records.
			
			Default - \"\", i.e. do not include commit SCN field in Kafka Connect record
			""";

	public static final String ORA_ROWTS_PARAM = "a2.pseudocolumn.ora_rowts";
	public static final String ORA_ROWTS_DOC =
			"""
			The name of the field in the Kafka Connect record that contains the database server timestamp where the row change was made. If the value is empty, the timestamp field is not included in the Kafka Connect records.
			
			Default - \"\", i.e. do not include row change timestamp field in Kafka Connect record
			""";

	public static final String ORA_OPERATION_PARAM = "a2.pseudocolumn.ora_operation";
	public static final String ORA_OPERATION_DOC =
			"""
			The name of the field in the Kafka Connect record that contains the name of the operation (UPDATE/INSERT/DELETE) that changed the database row. If the value is empty, the operation field is not included in the Kafka Connect records.
			
			Default - \"\", i.e. do not include operation field in Kafka Connect record
			""";

	public static final String ORA_XID_PARAM = "a2.pseudocolumn.ora_xid";
	public static final String ORA_XID_DOC =
			"""
			The name of the field in the Kafka Connect record that contains the XID (transaction Id) of the transaction that changed the database row. If the value is empty, the XID field is not included in the Kafka Connect records.
			
			Default - \"\", i.e. do not include XID field in Kafka Connect record
			""";

	public static final String ORA_USERNAME_PARAM = "a2.pseudocolumn.ora_username";
	public static final String ORA_USERNAME_DOC =
			"""
			The name of the field in the Kafka Connect record that contains the name of the the user who executed the transaction. If the value is empty, the username is not included in the Kafka Connect records.
			
			Default - \"\", i.e. do not include username field in Kafka Connect record
			""";

	public static final String ORA_OSUSERNAME_PARAM = "a2.pseudocolumn.ora_osusername";
	public static final String ORA_OSUSERNAME_DOC =
			"""
			The name of the field in the Kafka Connect record that contains the name of the the OS user who executed the transaction. If the value is empty, the OS username is not included in the Kafka Connect records.
			
			Default - \"\", i.e. do not include OS username field in Kafka Connect record
			""";

	public static final String ORA_HOSTNAME_PARAM = "a2.pseudocolumn.ora_hostname";
	public static final String ORA_HOSTNAME_DOC =
			"""
			The name of the field in the Kafka Connect record that contains the hostname of the machine from which the user connected to the database. If the value is empty, the hostname is not included in the Kafka Connect records.
			
			Default - \"\", i.e. do not include hostname field in Kafka Connect record
			""";

	public static final String ORA_AUDIT_SESSIONID_PARAM = "a2.pseudocolumn.ora_audit_session_id";
	public static final String ORA_AUDIT_SESSIONID_DOC =
			"""
			The name of the field in the Kafka Connect record that contains the audit session ID associated with the user session making the change. If the value is empty, the audit session id field is not included in the Kafka Connect records.
			
			Default - \"\", i.e. do not include audit session id field in Kafka Connect record
			""";

	public static final String ORA_SESSION_INFO_PARAM = "a2.pseudocolumn.ora_session_info";
	public static final String ORA_SESSION_INFO_DOC =
			"""
			The name of the field in the Kafka Connect record that contains the information about the database session that executed the transaction. If the value is empty, the session info field is not included in the Kafka Connect records.
			
			Default - \"\", i.e. do not include session info field in Kafka Connect record
			""";

	public static final String ORA_CLIENT_ID_PARAM = "a2.pseudocolumn.ora_client_id";
	public static final String ORA_CLIENT_ID_DOC =
			"""
			The name of the field in the Kafka Connect record that contains the client identifier in the session that executed the transaction (if available). If the value is empty, the client identifier field is not included in the Kafka Connect records.
			
			Default - \"\", i.e. do not include client identifier field in Kafka Connect record
			""";

	public static final String LAST_SEQ_NOTIFIER_PARAM = "a2.last.sequence.notifier";
	public static final String LAST_SEQ_NOTIFIER_DOC =
			"""
			The fully-qualified class name of the class that implements LastProcessedSeqNotifier interface to send notifications about the last processed log sequence.
			Currently there is only a notifier that writes a last processed sequence number to a file. To configure it, you need to set the value of the parameter 'a2.last.sequence.notifier' to 'solutions.a2.cdc.oracle.OraCdcLastProcessedSeqFileNotifier' and the value of the parameter 'a2.last.sequence.notifier.file' to the name of the file in which the last processed number will be written.
			
			Default - \"\", i.e. no notification
			""";

	public static final String LAST_SEQ_NOTIFIER_FILE_PARAM = "a2.last.sequence.notifier.file";
	public static final String LAST_SEQ_NOTIFIER_FILE_DOC =
			"""
			The name of the file in which the last processed number will be written.
			
			Default - ${connectorName}.seq
			""";

	public static final String KEY_OVERRIDE_PARAM = "a2.key.override";
	public static final String KEY_OVERRIDE_DOC =
			"""
			A comma separated list of elements in the format TABLE_OWNER.TABLE_NAME=NOKEY|ROWID|INDEX(INDEX_NAME).
			If there is a table in this list, then the values ​​of the `a2.pk.type` and `a2.use.rowid.as.key` parameters for it are ignored and the values ​​of the key columns are set in accordance with this parameter:
			NONE - do not create key fields in the Kafka topic for this table,
			ROWID - use ROWID as a key field in the Kafka topic with the name ORA_ROW_ID and type STRING, 
			INDEX(INDEX_NAME) use the index columns of index named INDEX_NAME as key fields of the Kafka topic
			
			Default - empty value.
			""";

	public static final String ORA_TRANSACTION_IMPL_CHRONICLE = "ChronicleQueue";
	public static final String ORA_TRANSACTION_IMPL_JVM = "ArrayList";
	public static final String ORA_TRANSACTION_IMPL_DEFAULT = ORA_TRANSACTION_IMPL_CHRONICLE;
	public static final String ORA_TRANSACTION_IMPL_PARAM = "a2.transaction.implementation";
	public static final String ORA_TRANSACTION_IMPL_DOC = 
			"""
			Queue implementation for processing SQL statements within transactions.
			Allowed values: 'ChronicleQueue' and 'ArrayList'.
			LOB processing is only possible if 'a2.transaction.implementation' is set to 'ChronicleQueue'.
			
			Default - """ + ORA_TRANSACTION_IMPL_DEFAULT;

	public static final String PROCESS_LOBS_PARAM = "a2.process.lobs";
	public static final String PROCESS_LOBS_DOC =
			"""
			Process Oracle LOB columns?
			
			Default - false.
			""";

	public static final String LOB_TRANSFORM_CLASS_DEFAULT = "solutions.a2.cdc.oracle.data.OraCdcDefaultLobTransformationsImpl";
	public static final String LOB_TRANSFORM_CLASS_PARAM = "a2.lob.transformation.class";
	public static final String LOB_TRANSFORM_CLASS_DOC =
			"""
			The fully-qualified class name which implements solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf interface.
			
			Default - solutions.a2.cdc.oracle.data.OraCdcDefaultLobTransformationsImpl which just passes information about and values of BLOB/CLOB/NCLOB/XMLTYPE columns to Kafka Connect without performing any additional transformation"
			""";

	public static final String CONNECTION_BACKOFF_PARAM = "a2.connection.backoff";
	public static final int CONNECTION_BACKOFF_DEFAULT = 30_000;
	public static final String CONNECTION_BACKOFF_DOC =
			"""
			The delay time in milliseconds between reconnection attempts.
			
			Default - """ + CONNECTION_BACKOFF_DEFAULT;

	public static final String USE_RAC_PARAM = "a2.use.rac";
	public static final String USE_RAC_DOC =
			"""
			When set to true oracdc first tried to detect is this connection to Oracle RAC.
			If database is not RAC, only the warning message is printed.
			If oracdc is connected to Oracle RAC additional checks are performed and oracdc starts a separate task for each redo thread/RAC instance.
			Changes for the same table from different redo threads (RAC instances) are delivered to the same topic but to different partition where <PARTITION_NUMBER> = <THREAD#> - 1
			
			Default - false.
			""";
	
	public static final String MAKE_STANDBY_ACTIVE_PARAM = "a2.standby.activate";
	public static final String MAKE_STANDBY_ACTIVE_DOC =
			"""
			Enable redo log files processing on the physical standby database.
			
			Default - false.
			"""; 

	public static final String STANDBY_URL_PARAM = "a2.standby.jdbc.url";
	public static final String STANDBY_URL_DOC = "JDBC connection URL for connecting to the physical standby database with V$DATABASE.OPEN_MODE = MOUNTED";

	public static final String STANDBY_WALLET_PARAM = "a2.standby.wallet.location";
	public static final String STANDBY_WALLET_DOC = "Location of Oracle Wallet for connecting to the physical standby database with V$DATABASE.OPEN_MODE = MOUNTED";

	public static final String STANDBY_PRIVILEGE_SYSDG = "sysdg";
	public static final String STANDBY_PRIVILEGE_SYSBACKUP = "sysbackup";
	public static final String STANDBY_PRIVILEGE_SYSDBA = "sysdba";
	public static final String STANDBY_PRIVILEGE_DEFAULT = STANDBY_PRIVILEGE_SYSDG;
	public static final String STANDBY_PRIVILEGE_PARAM = "a2.standby.privilege";
	public static final String STANDBY_PRIVILEGE_DOC = 
			"""
			The privilege used to connect to the physical standby database with V$DATABASE.OPEN_MODE = MOUNTED. Can be 'sysbackup' or 'sysdg' or 'sysdba'.
			
			Defaults to - """ + STANDBY_PRIVILEGE_SYSDG;

	public static final String TOPIC_PARTITION_PARAM = "a2.topic.partition";
	public static final String TOPIC_PARTITION_DOC =
			"""
			Kafka topic partition to write data.
			
			Default - 0
			""";

	public static final String TEMP_DIR_PARAM = "a2.tmpdir";
	public static final String TEMP_DIR_DOC = "Temporary directory for off-heap storage. When not set, OS temp directory used"; 

	public static final String FIRST_CHANGE_PARAM = "a2.first.change";
	public static final String FIRST_CHANGE_DOC =
			"""
			If this value is specified, mining will begin from this SCN.
			If not specified, min(FIRST_CHANGE#) from V$ARCHIVED_LOG will be used.
			This overrides the SCN value stored in the offset file.
			""";

	public static final String INTERNAL_PARAMETER_DOC = "Internal. Do not set!"; 
	public static final String INTERNAL_RAC_URLS_PARAM = "__a2.internal.rac.urls"; 
	public static final String INTERNAL_DG4RAC_THREAD_PARAM = "__a2.internal.dg4rac.thread";

	public static final String TABLE_LIST_STYLE_PARAM = "a2.table.list.style";
	public static final String TABLE_LIST_STYLE_DOC =
			"""
			If set to 'static', oracdc reads the tables and partition list for processing only at startup, according to the values ​​of the 'a2.include' and 'a2.exclude' parameters.
			If set to 'dynamic', oracdc generates the list of objects for processing on the fly.
			
			Default - 'static'
			""";
	public static final String TABLE_LIST_STYLE_STATIC = "static";
	public static final String TABLE_LIST_STYLE_DYNAMIC = "dynamic";

	public static final String CONC_TRANSACTIONS_THRESHOLD_PARAM = "a2.transactions.threshold";
	public static final String CONC_TRANSACTIONS_THRESHOLD_DOC = 
			"""
			Maximum threshold of simultaneously processed (both in the process of reading from the database and in the process of sending) transactions in the connector on Linux systems.
			When not specified (0, default) value is calculated as (vm.max_map_count/16) *7"
			""";

	public static final int REDUCE_LOAD_MS_DEFAULT = 60_000;
	public static final String REDUCE_LOAD_MS_PARAM = "a2.reduce.load.ms";
	public static final String REDUCE_LOAD_MS_DOC =
			"""
			Wait time in ms to reduce the number of simultaneously processed transactions 
			Sending of processed messages continues, pause occurs only for the process of reading from the database.
			
			Default - """ + REDUCE_LOAD_MS_DEFAULT;

	public static final int AL_CAPACITY_DEFAULT = 0x20;
	public static final String AL_CAPACITY_PARAM = "a2.array.list.default.capacity";
	public static final String AL_CAPACITY_DOC =
			"""
			Initial capacity of ArrayList storing Oracle Database transaction data
			
			Default - """ + AL_CAPACITY_DEFAULT;

	public static final boolean IGNORE_STORED_OFFSET_DEFAULT = false;
	public static final String IGNORE_STORED_OFFSET_PARAM = "a2.ignore.stored.offset";
	public static final String IGNORE_STORED_OFFSET_DOC =
			"""
			When this parameter is set to true, the connector does not read the values ​​stored in the Kafka Connect offset for the last processed SCN/SUBSCN/RBA and instead, if the 'a2.first.change' parameter is set, it uses its value, otherwise it determines the minimum available SCN in the Oracle database.
			
			Default - """ + IGNORE_STORED_OFFSET_DEFAULT;

	public static final String NUMBER_MAP_PREFIX = "a2.map.number.";

	/*
	 * BEGIN LogMiner only parameters
	 */

	public static final String ARCHIVED_LOG_CAT_DEFAULT = "solutions.a2.cdc.oracle.OraCdcV$ArchivedLogImpl";
	public static final String ARCHIVED_LOG_CAT_PARAM = "a2.archived.log.catalog";
	public static final String ARCHIVED_LOG_CAT_DOC =
			"""
			The fully-qualified class name which implements solutions.a2.cdc.oracle.OraLogMiner interface.
			
			Default - solutions.a2.cdc.oracle.OraCdcV$ArchivedLogImpl which reads archived log information from V$ARCHIVED_LOG fixed view"
			""";

	public static final String MAKE_DISTRIBUTED_ACTIVE_PARAM = "a2.distributed.activate";
	public static final String MAKE_DISTRIBUTED_ACTIVE_DOC =
			"""
			Use oracdc in distributed configuration (redo logs are generated at source RDBMS server and then transferred to compatible target RDBMS server for processing with LogMiner.
			
			 Default - false.
			"""; 

	public static final String DISTRIBUTED_WALLET_PARAM = "a2.distributed.wallet.location";
	public static final String DISTRIBUTED_WALLET_DOC = "Location of Oracle Wallet for connecting to target database in distributed mode";

	public static final String DISTRIBUTED_URL_PARAM = "a2.distributed.jdbc.url";
	public static final String DISTRIBUTED_URL_DOC = "JDBC connection URL for connecting to target database in distributed mode";

	public static final String DISTRIBUTED_TARGET_HOST = "a2.distributed.target.host";
	public static final String DISTRIBUTED_TARGET_HOST_DOC = "hostname of the target (where dbms_logmnr runs) database on which the shipment agent is running";

	public static final String DISTRIBUTED_TARGET_PORT = "a2.distributed.target.port";
	public static final String DISTRIBUTED_TARGET_PORT_DOC = "port number on which shipping agent listens for requests";
	public static final int DISTRIBUTED_TARGET_PORT_DEFAULT = 21521;

	public static final int FETCH_SIZE_DEFAULT = 32;
	public static final String FETCH_SIZE_PARAM = "a2.fetch.size";
	public static final String FETCH_SIZE_DOC =
			"""
			The number of rows fetched with each RDBMS round trip for access V$LOGMNR_CONTENTS.
			
			Default -  """ + FETCH_SIZE_DEFAULT;

	public static final String TRACE_LOGMINER_PARAM = "a2.logminer.trace";
	public static final String TRACE_LOGMINER_DOC =
			"""
			Run a LogMiner trace using 'event 10046 level 8'?
			
			Default - false.
			""";

	public static final String LM_RECONNECT_INTERVAL_MS_PARAM = "a2.log.miner.reconnect.ms";
	public static final String LM_RECONNECT_INTERVAL_MS_DOC =
			"""
			The time interval in milleseconds after which a reconnection to LogMiner occurs, including the re-creation of the Oracle connection.
			Unix/Linux only, on Windows oracdc creates new LogMiner session and re-creation of database connection every time DBMS_LOGMNR.START_LOGMNR is called.
			
			Default - Long.MAX_VALUE
			""";

	/*
	 * END LogMiner only parameters
	 */

	/*
	 * BEGIN RedoMiner only parameters
	 */

	public static final String REDO_FILE_NAME_CONVERT_PARAM = "a2.redo.filename.convert";
	public static final String REDO_FILE_NAME_CONVERT_DOC =
			"""
			It converts the filename of a redo log to another path.
			It is specified as a comma separated list of a strins in the <ORIGINAL_PATH>=<NEW_PATH> format. If not specified (default), no conversion occurs.
			
			""";
	public static final String REDO_FILE_MEDIUM_FS = "FS";
	public static final String REDO_FILE_MEDIUM_ASM = "ASM";
	public static final String REDO_FILE_MEDIUM_SSH = "SSH";
	public static final String REDO_FILE_MEDIUM_SMB = "SMB";
	public static final String REDO_FILE_MEDIUM_BFILE = "BFILE";
	public static final String REDO_FILE_MEDIUM_TRANSFER = "TRANSFER";
	public static final String REDO_FILE_MEDIUM_PARAM = "a2.storage.media";
	public static final String REDO_FILE_MEDIUM_DOC =
			"""
			Parameter defining the storage medium for redo log files:
			FS - redo files will be read from the local file system
			ASM - redo files will be read from the Oracle ASM
			SSH - redo files will be read from the remote file system using ssh
			SMB  - redo files will be read from the remote file system using smb
			BFILE - access remode files via Oracle Net as Oracle BFILE's
			TRANSFER - use DBMS_TRANSFER package
			
			Default - FS
			"""; 

	public static final String ASM_JDBC_URL_PARAM = "a2.asm.jdbc.url";
	public static final String ASM_JDBC_URL_DOC =
			"""
			JDBC URL pointing to the Oracle ASM instance.
			For information about syntax please see description of parameter 'a2.jdbc.url' above
			""";
	public static final String ASM_USER_PARAM = "a2.asm.username";
	public static final String ASM_USER_DOC = "Username for connecting to Oracle ASM instance, must have SYSASM role";
	public static final String ASM_PASSWORD_PARAM = "a2.asm.password";
	public static final String ASM_PASSWORD_DOC = "User password for connecting to Oracle ASM instance";
	public static final String ASM_READ_AHEAD_PARAM = "a2.asm.read.ahead";
	public static final String ASM_READ_AHEAD_DOC = "When set to true (the default), the connector reads data from the redo logs in advance, with chunks larger than the redo log file block size.";
	public static final boolean ASM_READ_AHEAD_DEFAULT = true;
	public static final String ASM_RECONNECT_INTERVAL_MS_PARAM = "a2.asm.reconnect.ms";
	public static final long ASM_RECONNECT_INTERVAL_MS_DEFAULT = 604_800_000;
	public static final String ASM_RECONNECT_INTERVAL_MS_DOC =
			"""
			The time interval in milliseconds after which a reconnection to Oracle ASM occurs, including the re-creation of the Oracle connection.
			
			Default - """ + ASM_RECONNECT_INTERVAL_MS_DEFAULT + " (one week)";
	public static final String ASM_PRIVILEGE_SYSASM = "sysasm";
	public static final String ASM_PRIVILEGE_SYSDBA = "sysdba";
	public static final String ASM_PRIVILEGE_DEFAULT = ASM_PRIVILEGE_SYSASM;
	public static final String ASM_PRIVILEGE_PARAM = "a2.asm.privilege";
	public static final String ASM_PRIVILEGE_DOC = 
			"""
			The privilege used to connect to the ASM instance. Can be 'sysasm' or 'sysdba'.
			
			Default - """ + ASM_PRIVILEGE_SYSASM;

	public static final String SSH_HOST_PARAM = "a2.ssh.hostname";
	public static final String SSH_HOST_DOC = "FQDN or IP address of the remote server with redo log files";
	public static final String SSH_PORT_PARAM = "a2.ssh.port";
	public static final String SSH_PORT_DOC = "SSH port of the remote server with redo log files";
	public static final int SSH_PORT_DEFAULT = 22;
	public static final String SSH_USER_PARAM = "a2.ssh.user";
	public static final String SSH_USER_DOC = "Username for the authentication to the remote server with redo log files";
	public static final String SSH_KEY_PARAM = "a2.ssh.private.key";
	public static final String SSH_KEY_DOC = "Private key for the authentication to the remote server with redo log files";
	public static final String SSH_PASSWORD_PARAM = "a2.ssh.password";
	public static final String SSH_PASSWORD_DOC = "Password for the authentication to the remote server with redo log files";
	public static final String SSH_RECONNECT_INTERVAL_MS_PARAM = "a2.ssh.reconnect.ms";
	public static final long SSH_RECONNECT_INTERVAL_MS_DEFAULT = 86_400_000;
	public static final String SSH_RECONNECT_INTERVAL_MS_DOC =
			"""
			The time interval in milliseconds after which a reconnection to remote server with redo files, including the re-creation of the SSH connection.
			
			Default - """ + SSH_RECONNECT_INTERVAL_MS_DEFAULT + " (24 hours)";
	public static final String SSH_STRICT_HOST_KEY_CHECKING_PARAM = "a2.ssh.strict.host.key.checking";
	public static final String SSH_STRICT_HOST_KEY_CHECKING_DOC = "SSH strict host key checking. Default - false.";
	public static final String SSH_PROVIDER_MAVERICK = "maverick";
	public static final String SSH_PROVIDER_SSHJ = "sshj";
	public static final String SSH_PROVIDER_MINA = "mina";
	public static final String SSH_PROVIDER_PARAM = "a2.ssh.provider";
	public static final String SSH_PROVIDER_DEFAULT = SSH_PROVIDER_SSHJ;
	public static final String SSH_PROVIDER_DOC =
			"""
			Library that provides SSH connection:
			maverick for Maverick Synergy (https://jadaptive.com/)
			sshj for Hierynomus sshj (https://github.com/hierynomus/sshj)
			
			"Default - """ + SSH_PROVIDER_DEFAULT;
	public static final int SSH_UNCONFIRMED_READS_DEFAULT = 0x100;
	public static final String SSH_UNCONFIRMED_READS_PARAM = "a2.ssh.max.unconfirmed.reads";
	public static final String SSH_UNCONFIRMED_READS_DOC =
			"""
			Maximum number of unconfirmed reads from SFTP server when using Hierynomus sshj.
			
			Default - """ + SSH_UNCONFIRMED_READS_DEFAULT;
	public static final int SSH_BUFFER_SIZE_DEFAULT = 0x8000;
	public static final String SSH_BUFFER_SIZE_PARAM = "a2.ssh.buffer.size";
	public static final String SSH_BUFFER_SIZE_DOC =
			"""
			Read-ahead buffer size in bytes for fata from SFTP server when using Hierynomus sshj.
			
			Default - """ + SSH_BUFFER_SIZE_DEFAULT;

	public static final String SMB_SERVER_PARAM = "a2.smb.server";
	public static final String SMB_SERVER_DOC = "FQDN or IP address of the remote SMB (Windows) server with redo log files";
	public static final String SMB_SHARE_ONLINE_PARAM = "a2.smb.share.online";
	public static final String SMB_SHARE_ONLINE_DOC = "Name of the SMB (Windows) share with online redo logs";
	public static final String SMB_SHARE_ARCHIVE_PARAM = "a2.smb.share.archive";
	public static final String SMB_SHARE_ARCHIVE_DOC = "Name of the SMB (Windows) share with archived redo logs";
	public static final String SMB_USER_PARAM = "a2.smb.user";
	public static final String SMB_USER_DOC = "Username for the authentication to the remote SMB (Windows) server with redo log files";
	public static final String SMB_PASSWORD_PARAM = "a2.smb.password";
	public static final String SMB_PASSWORD_DOC = "Password for the authentication to the remote SMB (Windows) server with redo log files";
	public static final String SMB_DOMAIN_PARAM = "a2.smb.domain";
	public static final String SMB_DOMAIN_DOC = "SMB (Windows) authentication domain name";
	public static final int SMB_TIMEOUT_MS_DEFAULT = 180_000;
	public static final String SMB_TIMEOUT_MS_PARAM = "a2.smb.timeout";
	public static final String SMB_TIMEOUT_MS_DOC =
			"""
			SMB read timeout in ms.
			
			Default - """ + SMB_TIMEOUT_MS_DEFAULT;
	public static final int SMB_SOCKET_TIMEOUT_MS_DEFAULT = 180_000;
	public static final String SMB_SOCKET_TIMEOUT_MS_PARAM = "a2.smb.socket.timeout";
	public static final String SMB_SOCKET_TIMEOUT_MS_DOC =
			"""
			SMB socket timeout in ms.
			Default - """ + SMB_SOCKET_TIMEOUT_MS_DEFAULT;
	public static final long SMB_RECONNECT_INTERVAL_MS_DEFAULT = 86_400_000;
	public static final String SMB_RECONNECT_INTERVAL_MS_PARAM = "a2.smb.reconnect.ms";
	public static final String SMB_RECONNECT_INTERVAL_MS_DOC =
			"""
			The time interval in milliseconds after which a reconnection to remote server with redo files, including the re-creation of the SMB (Windows) connection.
			Default - """ + SMB_RECONNECT_INTERVAL_MS_DEFAULT + " (24 hours)";
	public static final int SMB_BUFFER_SIZE_DEFAULT = 0x100000;
	public static final String SMB_BUFFER_SIZE_PARAM = "a2.smb.buffer.size";
	public static final String SMB_BUFFER_SIZE_DOC = "Read-ahead buffer size in bytes for data from SMB (Windows) server. Default - " + SMB_BUFFER_SIZE_DEFAULT;
	public static final String BFILE_DIR_ONLINE_PARAM = "a2.bfile.directory.online";
	public static final String BFILE_DIR_ONLINE_DOC = "The name of the Oracle database directory that contains the online redo logs";
	public static final String BFILE_DIR_ARCHIVE_PARAM = "a2.bfile.directory.archive";
	public static final String BFILE_DIR_ARCHIVE_DOC = "The name of the Oracle database directory that contains the archived redo logs";
	public static final long BFILE_RECONNECT_INTERVAL_MS_DEFAULT = 3_600_000;
	public static final String BFILE_RECONNECT_INTERVAL_MS_PARAM = "a2.bfile.reconnect.ms";
	public static final String BFILE_RECONNECT_INTERVAL_MS_DOC =
			"""
			The time interval in milliseconds after which a reconnection to remote server with redo files, including the re-creation of the Oracle Net connection.
			Default - """ + BFILE_RECONNECT_INTERVAL_MS_DEFAULT + " (1 hour)";
	public static final int BFILE_BUFFER_SIZE_DEFAULT = 0x400000;
	public static final String BFILE_BUFFER_SIZE_PARAM = "a2.bfile.buffer.size";
	public static final String BFILE_BUFFER_SIZE_DOC =
			"""
			Oracle BFILE read-ahead buffer size in bytes.
			Default - """ + BFILE_BUFFER_SIZE_DEFAULT;

	public static final String TDE_WALLET_PATH_PARAM = "a2.tde.wallet.path";
	public static final String TDE_WALLET_PATH_DOC = "Full absolute path to Oracle Wallet file (ewallet.p12)";
	public static final String TDE_WALLET_PASSWORD_PARAM = "a2.tde.wallet.password";
	public static final String TDE_WALLET_PASSWORD_DOC = "Password Oracle Wallet";

	public static final String ALL_UPDATES_PARAM = "a2.process.all.update.statements";
	public static final boolean ALL_UPDATES_DEFAULT = true;
	public static final String ALL_UPDATES_DOC =
			"""
			When set to TRUE connector processes all UPDATE statements.
			When set to FALSE connector ignores UPDATE statements that do not actually change the data, i.e. 'update DEPT set DNAME=DNAME where DEPTNO=10'.
			Default - """ + ALL_UPDATES_DEFAULT;

	public static final String PRINT_UNABLE2MAP_COL_ID_WARNING_PARAM = "a2.unable.to.map.col.id.warning";
	public static final boolean PRINT_UNABLE2MAP_COL_ID_WARNING_DEFAULT = true;
	public static final String PRINT_UNABLE2MAP_COL_ID_WARNING_DOC =
			"""
			"When the value is set to 'true' and a redo record contains a column identifier that is not in the data dictionary, a message about that column and information about the redo record is printed.
			Default """ + PRINT_UNABLE2MAP_COL_ID_WARNING_DEFAULT;

	public static final String SUPPLEMENTAL_LOGGING_ALL = "ALL";
	public static final String SUPPLEMENTAL_LOGGING_NONE = "NONE";
	public static final String SUPPLEMENTAL_LOGGING_DEFAULT = SUPPLEMENTAL_LOGGING_ALL;
	public static final String SUPPLEMENTAL_LOGGING_PARAM = "a2.supplemental.logging";
	public static final String SUPPLEMENTAL_LOGGING_DOC =
			"""
			The supplemental logging level required for the oracdc to function.
			The parameter can take the values ​​ALL or NONE.
			The default is ALL, and you must set SUPPLEMENTAL LOG DATA(ALL) COLUMND for all tables participating in replication, as well as SUPPLEMENTAL LOG DATA at the database level.
			If this parameter is set to NONE, there are no or minimal requirements for supplemental logging.
			Use this parameter only after consulting with us via email at oracle@a2.solutions or by scheduling a meeting on our website at https://a2.solutions.
			
			Default - """ + SUPPLEMENTAL_LOGGING_DEFAULT;

	public static final boolean STOP_ON_MISSED_LOG_FILE_DEFAULT = true; 
	public static final String STOP_ON_MISSED_LOG_FILE_PARAM = "a2.stop.on.missed_log.file";
	public static final String STOP_ON_MISSED_LOG_FILE_DOC = 
			"""
			When the parameter value is set to true, the connector stops if it cannot open a redo log file whose description is present in the data dictionary.
			When the parameter value is set to false, the connector attempts to continue using the next redo log file (with a SEQUENCE# value greater than that of the missing redo log file).
			
			Default - """ + STOP_ON_MISSED_LOG_FILE_DEFAULT;
	
	public static final int TABLES_IN_PROCESS_SIZE_DEFAULT = 0x100;
	public static final String TABLES_IN_PROCESS_SIZE_PARAM = "a2.tables.in.process.size";
	public static final String TABLES_IN_PROCESS_SIZE_DOC =
			"""
			Specifies the initial size of the memory structure storing information about the tables being processed.
			
			Default - """ + TABLES_IN_PROCESS_SIZE_DEFAULT;

	public static final int TABLES_OUT_OF_SCOPE_SIZE_DEFAULT = 0x400;
	public static final String TABLES_OUT_OF_SCOPE_SIZE_PARAM = "a2.tables.out.of.scope.size";
	public static final String TABLES_OUT_OF_SCOPE_SIZE_DOC =
			"""
			Specifies the initial size of the memory structure that stores information about ID of tables that do not need to be processed.
			
			Default - """ + TABLES_OUT_OF_SCOPE_SIZE_DEFAULT;

	public static final int TRANS_IN_PROCESS_SIZE_DEFAULT = 0x400;
	public static final String TRANS_IN_PROCESS_SIZE_PARAM = "a2.transactions.in.process.size";
	public static final String TRANS_IN_PROCESS_SIZE_DOC =
			"""
			Specifies the initial size of the memory structure that stores information about currently processed transactions.
			
			Default - """ + TRANS_IN_PROCESS_SIZE_DEFAULT;

	public static final int EMITTER_TIMEOUT_MS_DEFAULT = 0x15;
	public static final String EMITTER_TIMEOUT_MS_PARAM = "a2.emitter.timeout.ms";
	public static final String EMITTER_TIMEOUT_MS_DOC =
			"""
			Sets the time interval in milliseconds after which the emitter thread checks for a new, unprocessed set of redo records.
			
			Default - """ + EMITTER_TIMEOUT_MS_DEFAULT;

	public static final int[] OFFHEAP_SIZE_FULL_INT = {0x4000000, 0x1000000};
	public static final String OFFHEAP_SIZE_FULL = "FULL";
	public static final int[] OFFHEAP_SIZE_HALF_INT = {0x2000000, 0x800000};
	public static final String OFFHEAP_SIZE_HALF = "HALF";
	public static final int[] OFFHEAP_SIZE_QUARTER_INT = {0x1000000, 0x400000};
	public static final String OFFHEAP_SIZE_QUARTER = "QUARTER";
	public static final int[] OFFHEAP_SIZE_HALFQUARTER_INT = {0x800000, 0x200000};
	public static final String OFFHEAP_SIZE_HALFQUARTER = "HALF-QUARTER";
	public static final String OFFHEAP_SIZE_DEFAULT = OFFHEAP_SIZE_HALFQUARTER;
	public static final String OFFHEAP_SIZE_PARAM = "a2.offheap.size";
	public static final String OFFHEAP_SIZE_DOC =
			"""
			Defines the initial size of the off-heap memory structure.
			
			Default - """ + OFFHEAP_SIZE_DEFAULT;

	public static final String TRANSFER_DIR_STAGE_PARAM = "a2.transfer.directory.stage";
	public static final String TRANSFER_DIR_STAGE_DOC = "The name of the Oracle database directory used as stage storage, which must be located on the file system.";

	/*
	 * END RedoMiner only parameters
	 */

}
