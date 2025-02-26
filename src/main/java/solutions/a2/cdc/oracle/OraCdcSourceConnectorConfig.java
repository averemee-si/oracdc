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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import solutions.a2.cdc.oracle.utils.KafkaUtils;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcSourceConnectorConfig extends OraCdcSourceBaseConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcSourceConnectorConfig.class);

	public static final int INCOMPLETE_REDO_INT_ERROR = 1;
	public static final int INCOMPLETE_REDO_INT_SKIP = 2;
	public static final int INCOMPLETE_REDO_INT_RESTORE = 3;

	public static final int TOPIC_NAME_STYLE_INT_TABLE = 1;
	public static final int TOPIC_NAME_STYLE_INT_SCHEMA_TABLE = 2;
	public static final int TOPIC_NAME_STYLE_INT_PDB_SCHEMA_TABLE = 3;

	public static final int PK_TYPE_INT_WELL_DEFINED = 1;
	public static final int PK_TYPE_INT_ANY_UNIQUE = 2;

	private static final String ORACDC_SCHEMAS_PARAM = "a2.oracdc.schemas";
	private static final String ORACDC_SCHEMAS_DOC = "Use oracdc extensions for Oracle datatypes. Default false";

	private static final String INCOMPLETE_REDO_TOLERANCE_PARAM = "a2.incomplete.redo.tolerance";
	private static final String INCOMPLETE_REDO_TOLERANCE_DOC =
			"Connector behavior when processing an incomplete redo record.\n" +
			"Allowed values: error, skip, and restore.\n" +
			"Default - error.\nWhen set to:\n" +
			"- 'error' oracdc prints information about incomplete redo record and stops connector.\n" +
			"- 'skip' oracdc prints information about incomplete redo record and continue processing\n" + 
			"- 'restore' oracdc tries to restore missed information from actual row incarnation from the table using ROWID from redo the record.";
	private static final String INCOMPLETE_REDO_TOLERANCE_ERROR = "error";
	private static final String INCOMPLETE_REDO_TOLERANCE_SKIP = "skip";
	private static final String INCOMPLETE_REDO_TOLERANCE_RESTORE = "restore";

	private static final String PRINT_INVALID_HEX_WARNING_PARAM = "a2.print.invalid.hex.value.warning";
	private static final String PRINT_INVALID_HEX_WARNING_DOC = 
			"Default - false.\n" +
			"When set to true oracdc prints information about invalid hex values (like single byte value for DATE/TIMESTAMP/TIMESTAMPTZ) in log.";
	
	private static final String PROTOBUF_SCHEMA_NAMING_PARAM = "a2.protobuf.schema.naming";
	private static final String PROTOBUF_SCHEMA_NAMING_DOC = 
			"Default - false.\n" +
			"When set to true oracdc generates schema names as valid Protocol Buffers identifiers using underscore as separator.\n" + 
			"When set to false (default) oracdc generates schema names using dot as separator.\n";
	
	private static final String TOPIC_NAME_DELIMITER_PARAM = "a2.topic.name.delimiter";
	private static final String TOPIC_NAME_DELIMITER_DOC = "Kafka topic name delimiter when a2.schema.type=kafka and a2.topic.name.style set to SCHEMA_TABLE or PDB_SCHEMA_TABLE. Valid values - '_' (Default), '-', '.'";
	private static final String TOPIC_NAME_DELIMITER_UNDERSCORE = "_";
	private static final String TOPIC_NAME_DELIMITER_DASH = "-";
	private static final String TOPIC_NAME_DELIMITER_DOT = ".";

	private static final String TOPIC_NAME_STYLE_PARAM = "a2.topic.name.style";
	private static final String TOPIC_NAME_STYLE_DOC = "Kafka topic naming convention when a2.schema.type=kafka. Valid values - TABLE (default), SCHEMA_TABLE, PDB_SCHEMA_TABLE";
	private static final String TOPIC_NAME_STYLE_TABLE = "TABLE";
	private static final String TOPIC_NAME_STYLE_SCHEMA_TABLE = "SCHEMA_TABLE";
	private static final String TOPIC_NAME_STYLE_PDB_SCHEMA_TABLE = "PDB_SCHEMA_TABLE";

	private static final String PK_TYPE_PARAM = "a2.pk.type";
	private static final String PK_TYPE_DOC =
			"Default - well_defined.\n" +
			"When set to well_defined the key fields are the table's primary key columns or, if the table does not have a primary key, the table's unique key columns in which all columns are NOT NULL. " +
			"If there are no appropriate keys in the table, oracdc uses the a2.use.rowid.as.key parameter and generates a pseudo key based on the row's ROWID, or generates a schema without any key fields.\n" +
			"When set to any_unique and the table does not have a primary key or a unique key with all NOT NULL columns, then the key fields will be the unique key columns which may have NULL columns. " +
			"If there are no appropriate keys in the table, oracdc uses the a2.use.rowid.as.key parameter and generates a pseudo key based on the row's ROWID, or generates a schema without any key fields.";
	private static final String PK_TYPE_WELL_DEFINED = "well_defined";
	private static final String PK_TYPE_ANY_UNIQUE = "any_unique";

	private static final String USE_ROWID_AS_KEY_PARAM = "a2.use.rowid.as.key";
	private static final String USE_ROWID_AS_KEY_DOC =
			"Default - true.\n" +
			"When set to true and the table does not have a appropriate primary or unique key, oracdc adds surrogate key using the ROWID.\n" +
			"When set to false and the table does not have a appropriate primary or unique key, oracdc generates schema for the table without any key fields.\n";

	private static final String TOPIC_MAPPER_DEFAULT = "solutions.a2.cdc.oracle.OraCdcDefaultTopicNameMapper";
	private static final String TOPIC_MAPPER_PARAM = "a2.topic.mapper";
	private static final String TOPIC_MAPPER_DOC =
			"The fully-qualified class name of the class that specifies which Kafka topic the data from the tables should be sent to.\n" +
			"If value of thee parameter 'a2.shema.type' is set to 'debezium', the default OraCdcDefaultTopicNameMapper uses the parameter 'a2.kafka.topic' value as the Kafka topic name,\n" +
			"otherwise it constructs the topic name according to the values of the parameters 'a2.topic.prefix', 'a2.topic.name.style', and 'a2.topic.name.delimiter', as well as the table name, table owner and PDB name.\n" +
			"Default - " + TOPIC_MAPPER_DEFAULT;

	private static final boolean STOP_ON_ORA_1284_DEFAULT = true;
	private static final String STOP_ON_ORA_1284_PARAM = "a2.stop.on.ora.1284";
	private static final String STOP_ON_ORA_1284_DOC =
			"If set to true, the connector stops on an Oracle database error 'ORA-01284: file <Absolute-Path-To-Log-File> cannot be opened'.\n" +
			"If set to false, the connector prints an error message and continues processing.\n" +
			"Default - " + STOP_ON_ORA_1284_DEFAULT;

	private static final boolean PRINT_UNABLE_TO_DELETE_WARNING_DEFAULT = false;
	private static final String PRINT_UNABLE_TO_DELETE_WARNING_PARAM = "a2.print.unable.to.delete.warning";
	private static final String PRINT_UNABLE_TO_DELETE_WARNING_DOC =
			"If set to true, the connector prints a warning message including all redo record details about ignoring the DELETE operation for tables without a primary key or it surrogate or a schema that does not contain key information.\n" +
			"If set to false, the connector does not print a warning message about ignoring the DELETE operation.\n" +
			"Default - " + PRINT_UNABLE_TO_DELETE_WARNING_DEFAULT;

	private static final String SCHEMANAME_MAPPER_DEFAULT = "solutions.a2.cdc.oracle.OraCdcDefaultSchemaNameMapper";
	private static final String SCHEMANAME_MAPPER_PARAM = "a2.schema.name.mapper";
	private static final String SCHEMANAME_MAPPER_DOC =
			"The fully-qualified class name of the class that constructs schema name from the Oracle PDB name (if present), the table owner, and the table name.\n" +
			"Default - " + SCHEMANAME_MAPPER_DEFAULT;

	private static final String ORA_ROWSCN_PARAM = "a2.pseudocolumn.ora_rowscn";
	private static final String ORA_ROWSCN_DOC =
			"The name of the field in the Kafka Connect record that contains the SCN where the row change was made. If the value is empty, the SCN field is not included in the Kafka Connect records.\n" +
			"Default - \"\", i.e. do not include SCN field in Kafka Connect record";

	private static final String ORA_COMMITSCN_PARAM = "a2.pseudocolumn.ora_commitscn";
	private static final String ORA_COMMITSCN_DOC =
			"The name of the field in the Kafka Connect record that contains the commit SCN of the transaction in which the row change was made. If the value is empty, the commit SCN field is not included in the Kafka Connect records.\n" +
			"Default - \"\", i.e. do not include commit SCN field in Kafka Connect record";

	private static final String ORA_ROWTS_PARAM = "a2.pseudocolumn.ora_rowts";
	private static final String ORA_ROWTS_DOC =
			"The name of the field in the Kafka Connect record that contains the database server timestamp where the row change was made. If the value is empty, the timestamp field is not included in the Kafka Connect records.\n" +
			"Default - \"\", i.e. do not include row change timestamp field in Kafka Connect record";

	private static final String ORA_OPERATION_PARAM = "a2.pseudocolumn.ora_operation";
	private static final String ORA_OPERATION_DOC =
			"The name of the field in the Kafka Connect record that contains the name of the operation (UPDATE/INSERT/DELETE) that changed the database row. If the value is empty, the operation field is not included in the Kafka Connect records.\n" +
			"Default - \"\", i.e. do not include operation field in Kafka Connect record";

	private static final String ORA_XID_PARAM = "a2.pseudocolumn.ora_xid";
	private static final String ORA_XID_DOC =
			"The name of the field in the Kafka Connect record that contains the XID (transaction Id) of the transaction that changed the database row. If the value is empty, the XID field is not included in the Kafka Connect records.\n" +
			"Default - \"\", i.e. do not include XID field in Kafka Connect record";

	private static final String ORA_USERNAME_PARAM = "a2.pseudocolumn.ora_username";
	private static final String ORA_USERNAME_DOC =
			"The name of the field in the Kafka Connect record that contains the name of the the user who executed the transaction. If the value is empty, the username is not included in the Kafka Connect records.\n" +
			"Default - \"\", i.e. do not include username field in Kafka Connect record";

	private static final String ORA_OSUSERNAME_PARAM = "a2.pseudocolumn.ora_osusername";
	private static final String ORA_OSUSERNAME_DOC =
			"The name of the field in the Kafka Connect record that contains the name of the the OS user who executed the transaction. If the value is empty, the OS username is not included in the Kafka Connect records.\n" +
			"Default - \"\", i.e. do not include OS username field in Kafka Connect record";

	private static final String ORA_HOSTNAME_PARAM = "a2.pseudocolumn.ora_hostname";
	private static final String ORA_HOSTNAME_DOC =
			"The name of the field in the Kafka Connect record that contains the hostname of the machine from which the user connected to the database. If the value is empty, the hostname is not included in the Kafka Connect records.\n" +
			"Default - \"\", i.e. do not include hostname field in Kafka Connect record";

	private static final String ORA_AUDIT_SESSIONID_PARAM = "a2.pseudocolumn.ora_audit_session_id";
	private static final String ORA_AUDIT_SESSIONID_DOC =
			"The name of the field in the Kafka Connect record that contains the audit session ID associated with the user session making the change. If the value is empty, the audit session id field is not included in the Kafka Connect records.\n" +
			"Default - \"\", i.e. do not include audit session id field in Kafka Connect record";

	private static final String ORA_SESSION_INFO_PARAM = "a2.pseudocolumn.ora_session_info";
	private static final String ORA_SESSION_INFO_DOC =
			"The name of the field in the Kafka Connect record that contains the information about the database session that executed the transaction. If the value is empty, the session info field is not included in the Kafka Connect records.\n" +
			"Default - \"\", i.e. do not include session info field in Kafka Connect record";

	private static final String ORA_CLIENT_ID_PARAM = "a2.pseudocolumn.ora_client_id";
	private static final String ORA_CLIENT_ID_DOC =
			"The name of the field in the Kafka Connect record that contains the client identifier in the session that executed the transaction (if available). If the value is empty, the client identifier field is not included in the Kafka Connect records.\n" +
			"Default - \"\", i.e. do not include client identifier field in Kafka Connect record";

	private static final String LAST_SEQ_NOTIFIER_PARAM = "a2.last.sequence.notifier";
	private static final String LAST_SEQ_NOTIFIER_DOC =
			"The fully-qualified class name of the class that implements LastProcessedSeqNotifier interface to send notifications about the last processed log sequence.\n" +
			"Currently there is only a notifier that writes a last processed sequence number to a file. To configure it, you need to set the value of the parameter 'a2.last.sequence.notifier' to 'solutions.a2.cdc.oracle.OraCdcLastProcessedSeqFileNotifier' and the value of the parameter 'a2.last.sequence.notifier.file' to the name of the file in which the last processed number will be written.\n" +
			"Default - \"\", i.e. no notification";

	private static final String LAST_SEQ_NOTIFIER_FILE_PARAM = "a2.last.sequence.notifier.file";
	private static final String LAST_SEQ_NOTIFIER_FILE_DOC = "The name of the file in which the last processed number will be written. Default - ${connectorName}.seq";

	private static final String KEY_OVERRIDE_PARAM = "a2.key.override";
	private static final String KEY_OVERRIDE_DOC =
			"A comma separated list of elements in the format TABLE_OWNER.TABLE_NAME=NOKEY|ROWID|INDEX(INDEX_NAME).\n" + 
			"If there is a table in this list, then the values ​​of the `a2.pk.type` and `a2.use.rowid.as.key` parameters for it are ignored and the values ​​of the key columns are set in accordance with this parameter:\n" +
			"NONE - do not create key fields in the Kafka topic for this table,\n" +
			"ROWID - use ROWID as a key field in the Kafka topic with the name ORA_ROW_ID and type STRING,\n" + 
			"INDEX(INDEX_NAME) use the index columns of index named INDEX_NAME as key fields of the Kafka topic\n" +
			"Default - empty value.";
	private Map<String, OraCdcKeyOverrideTypes> keyOverrideMap = null;
	private Map<String, String> keyOverrideIndexMap = null;

	private static final String PROCESS_LOBS_PARAM = "a2.process.lobs";
	private static final String PROCESS_LOBS_DOC = "process Oracle LOB columns? Default - false";

	private static final String LOB_TRANSFORM_CLASS_PARAM = "a2.lob.transformation.class";
	private static final String LOB_TRANSFORM_CLASS_DOC = "name of class which implements solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf interface. Default - solutions.a2.cdc.oracle.data.OraCdcDefaultLobTransformationsImpl which just passes information about and values of BLOB/CLOB/NCLOB/XMLTYPE columns to Kafka Connect without performing any additional transformation";
	private static final String LOB_TRANSFORM_CLASS_DEFAULT = "solutions.a2.cdc.oracle.data.OraCdcDefaultLobTransformationsImpl";

	private static final String CONNECTION_BACKOFF_PARAM = "a2.connection.backoff";
	private static final int CONNECTION_BACKOFF_DEFAULT = 30000;
	private static final String CONNECTION_BACKOFF_DOC = "backoff time in milliseconds between reconnectoion attempts. Default - " +
														CONNECTION_BACKOFF_DEFAULT;

	private static final String USE_RAC_PARAM = "a2.use.rac";
	private static final String USE_RAC_DOC = 
			"Default - false.\n" +
			"When set to true oracdc first tried to detect is this connection to Oracle RAC.\n" + 
			"If database is not RAC, only the warning message is printed.\n" + 
			"If oracdc is connected to Oracle RAC additional checks are performed and oracdc starts a separate task for each redo thread/RAC instance. " +
			"Changes for the same table from different redo threads (RAC instances) are delivered to the same topic but to different partition where <PARTITION_NUMBER> = <THREAD#> - 1";
	
	private static final String MAKE_STANDBY_ACTIVE_PARAM = "a2.standby.activate";
	private static final String MAKE_STANDBY_ACTIVE_DOC = "Use standby database with V$DATABASE.OPEN_MODE = MOUNTED for LogMiner calls. Default - false"; 

	private static final String TOPIC_PARTITION_PARAM = "a2.topic.partition";
	private static final String TOPIC_PARTITION_DOC = "Kafka topic partition to write data. Default - 0";

	private static final String TEMP_DIR_PARAM = "a2.tmpdir";
	private static final String TEMP_DIR_DOC = "Temporary directory for non-heap storage. When not set, OS temp directory used"; 

	private static final String LGMNR_START_SCN_PARAM = "a2.first.change";
	private static final String LGMNR_START_SCN_DOC = "When set DBMS_LOGMNR.START_LOGMNR will start mining from this SCN. When not set min(FIRST_CHANGE#) from V$ARCHIVED_LOG will used. Overrides SCN value  stored in offset file";

	private static final String INTERNAL_PARAMETER_DOC = "Internal. Do not set!"; 
	static final String INTERNAL_RAC_URLS_PARAM = "__a2.internal.rac.urls"; 
	static final String INTERNAL_DG4RAC_THREAD_PARAM = "__a2.internal.dg4rac.thread";

	static final String TABLE_LIST_STYLE_PARAM = "a2.table.list.style";
	private static final String TABLE_LIST_STYLE_DOC = "When set to 'static' (default) oracdc reads tables and partition list to process only at startup according to values of a2.include and a2.exclude parameters. When set to 'dynamic' oracdc builds list of objects to process on the fly";
	static final String TABLE_LIST_STYLE_STATIC = "static";
	static final String TABLE_LIST_STYLE_DYNAMIC = "dynamic";

	private static final String LM_RECONNECT_INTERVAL_MS_PARAM = "a2.log.miner.reconnect.ms";
	private static final String LM_RECONNECT_INTERVAL_MS_DOC =
			"The time interval in milleseconds after which a reconnection to LogMiner occurs, including the re-creation of the Oracle connection.\n" +
			"Unix/Linux only, on Windows oracdc creates new LogMiner session and re-creation of database connection every time DBMS_LOGMNR.START_LOGMNR is called.\n" +
			"Default - Long.MAX_VALUE";
	private static final String CONC_TRANSACTIONS_THRESHOLD_PARAM = "a2.transactions.threshold";
	private static final String CONC_TRANSACTIONS_THRESHOLD_DOC = "Maximum threshold of simultaneously processed (both in the process of reading from the database and in the process of sending) transactions in the connector on Linux systems. When not specified (0, default) value is calculated as (vm.max_map_count/16) *7";

	private static final int REDUCE_LOAD_MS_DEFAULT = 60_000;
	private static final String REDUCE_LOAD_MS_PARAM = "a2.reduce.load.ms";
	private static final String REDUCE_LOAD_MS_DOC = 
			"Wait time in ms to reduce the number of simultaneously processed transactions.\n" + 
			"Sending of processed messages continues, pause occurs only for the process of reading from the database.\n" +
			"Default - " + REDUCE_LOAD_MS_DEFAULT;

	private static final int AL_CAPACITY_DEFAULT = 0x20;
	private static final String AL_CAPACITY_PARAM = "a2.array.list.default.capacity";
	private static final String AL_CAPACITY_DOC = 
			"Initial capacity of ArrayList storing Oracle Database transaction data.\n" + 
			"Default - " + AL_CAPACITY_DEFAULT;
	
	private static final String NUMBER_MAP_PREFIX = "a2.map.number.";

	private int incompleteDataTolerance = -1;
	private int topicNameStyle = -1;
	private int pkType = -1;
	private String connectorName;
	private OraCdcLobTransformationsIntf transformLobsImpl = null;
	private OraCdcPseudoColumnsProcessor pseudoColumns = null;
	private int topicPartition = 0;
	private Path queuesRoot = null;
	private Map<String, Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>> numberColumnsMap = new LinkedHashMap<>();

	// Redo Miner only!
	private static final String REDO_FILE_NAME_CONVERT_PARAM = "a2.redo.filename.convert";
	private static final String REDO_FILE_NAME_CONVERT_DOC =
			"It converts the filename of a redo log to another path.\n" +
			"It is specified as a comma separated list of a strins in the <ORIGINAL_PATH>:<NEW_PATH> format. If not specified (default), no conversion occurs.";
	private static final String REDO_FILE_MEDIUM_FS = "FS";
	private static final String REDO_FILE_MEDIUM_ASM = "ASM";
	private static final String REDO_FILE_MEDIUM_SSH = "SSH";
	private static final String REDO_FILE_MEDIUM_PARAM = "a2.storage.media";
	private static final String REDO_FILE_MEDIUM_DOC = "Parameter defining the storage medium for redo log files: FS - redo files will be read from the local file system, ASM - redo files will be read from the Oracle ASM, SSH - redo files will be read from the remote file syystem using ssh. Default - FS"; 
	private static final String ASM_JDBC_URL_PARAM = "a2.asm.jdbc.url";
	private static final String ASM_JDBC_URL_DOC = "JDBC URL pointing to the Oracle ASM instance. For information about syntax please see description of parameter 'a2.jdbc.url' above";
	private static final String ASM_USER_PARAM = "a2.asm.username";
	private static final String ASM_USER_DOC = "Username for connecting to Oracle ASM instance, must have SYSASM role";
	private static final String ASM_PASSWORD_PARAM = "a2.asm.password";
	private static final String ASM_PASSWORD_DOC = "User password for connecting to Oracle ASM instance";
	private static final String ASM_READ_AHEAD_PARAM = "a2.asm.read.ahead";
	private static final String ASM_READ_AHEAD_DOC = "When set to true (the default), the connector reads data from the redo logs in advance, with chunks larger than the redo log file block size.";
	private static final boolean ASM_READ_AHEAD_DEFAULT = true;
	private static final String ASM_RECONNECT_INTERVAL_MS_PARAM = "a2.asm.reconnect.ms";
	private static final long ASM_RECONNECT_INTERVAL_MS_DEFAULT = 604_800_000;
	private static final String ASM_RECONNECT_INTERVAL_MS_DOC =
			"The time interval in milliseconds after which a reconnection to Oracle ASM occurs, including the re-creation of the Oracle connection.\n" +
			"Default - " + ASM_RECONNECT_INTERVAL_MS_DEFAULT + " (one week)";
	private static final String SSH_HOST_PARAM = "a2.ssh.hostname";
	private static final String SSH_HOST_DOC = "FQDN or IP address of the remote server with redo log files";
	private static final String SSH_PORT_PARAM = "a2.ssh.port";
	private static final String SSH_PORT_DOC = "SSH port of the remote server with redo log files";
	private static final int SSH_PORT_DEFAULT = 22;
	private static final String SSH_USER_PARAM = "a2.ssh.user";
	private static final String SSH_USER_DOC = "Username for the authentication to the remote server with redo log files";
	private static final String SSH_KEY_PARAM = "a2.ssh.private.key";
	private static final String SSH_KEY_DOC = "Private key for the authentication to the remote server with redo log files";
	private static final String SSH_PASSWORD_PARAM = "a2.ssh.password";
	private static final String SSH_PASSWORD_DOC = "Password for the authentication to the remote server with redo log files";
	private static final String SSH_RECONNECT_INTERVAL_MS_PARAM = "a2.ssh.reconnect.ms";
	private static final long SSH_RECONNECT_INTERVAL_MS_DEFAULT = 3_600_000;
	private static final String SSH_RECONNECT_INTERVAL_MS_DOC =
			"The time interval in milliseconds after which a reconnection to remote server with redo files, including the re-creation of the SSH connection.\n" +
			"Default - " + SSH_RECONNECT_INTERVAL_MS_DEFAULT + " (one hour)";
	private static final String SSH_BUFFER_SMALL = "small";
	private static final String SSH_BUFFER_MEDIUM = "medium";
	private static final String SSH_BUFFER_LARGE = "large";
	private static final String SSH_BUFFER_PARAM = "a2.ssh.buffer";
	private static final String SSH_BUFFER_DOC = "SSH read buffer size. Can be 'small' (65536 bytes), 'medium' (262144 bytes), or 'large' (1048576 bytes). Default - 'large'";

	private boolean fileNameConversionInited = false;
	private boolean fileNameConversion = false;
	private Map<String, String> fileNameConversionMap;
	private boolean logMiner = true;

	public static ConfigDef config() {
		return OraCdcSourceBaseConfig.config()
				.define(TOPIC_PARTITION_PARAM, Type.INT, 0,
						Importance.MEDIUM, TOPIC_PARTITION_DOC)
				.define(LGMNR_START_SCN_PARAM, Type.STRING, "0",
						Importance.MEDIUM, LGMNR_START_SCN_DOC)
				.define(TEMP_DIR_PARAM, Type.STRING, System.getProperty("java.io.tmpdir"),
						Importance.HIGH, TEMP_DIR_DOC)
				.define(MAKE_STANDBY_ACTIVE_PARAM, Type.BOOLEAN, false,
						Importance.LOW, MAKE_STANDBY_ACTIVE_DOC)
				.define(ParamConstants.STANDBY_WALLET_PARAM, Type.STRING, "",
						Importance.LOW, ParamConstants.STANDBY_WALLET_DOC)
				.define(ParamConstants.STANDBY_URL_PARAM, Type.STRING, "",
						Importance.LOW, ParamConstants.STANDBY_URL_DOC)
				.define(ORACDC_SCHEMAS_PARAM, Type.BOOLEAN, false,
						Importance.LOW, ORACDC_SCHEMAS_DOC)
				.define(ParamConstants.DICTIONARY_FILE_PARAM, Type.STRING, "",
						Importance.LOW, ParamConstants.DICTIONARY_FILE_DOC)
				.define(ParamConstants.INITIAL_LOAD_PARAM, Type.STRING,
						ParamConstants.INITIAL_LOAD_IGNORE,
						ConfigDef.ValidString.in(ParamConstants.INITIAL_LOAD_IGNORE,
								ParamConstants.INITIAL_LOAD_EXECUTE),
						Importance.LOW, ParamConstants.INITIAL_LOAD_DOC)
				.define(TOPIC_NAME_STYLE_PARAM, Type.STRING,
						TOPIC_NAME_STYLE_TABLE,
						ConfigDef.ValidString.in(TOPIC_NAME_STYLE_TABLE,
								TOPIC_NAME_STYLE_SCHEMA_TABLE,
								TOPIC_NAME_STYLE_PDB_SCHEMA_TABLE),
						Importance.LOW, TOPIC_NAME_STYLE_DOC)
				.define(TOPIC_NAME_DELIMITER_PARAM, Type.STRING,
						TOPIC_NAME_DELIMITER_UNDERSCORE,
						ConfigDef.ValidString.in(TOPIC_NAME_DELIMITER_UNDERSCORE,
								TOPIC_NAME_DELIMITER_DASH,
								TOPIC_NAME_DELIMITER_DOT),
						Importance.LOW, TOPIC_NAME_DELIMITER_DOC)
				.define(TABLE_LIST_STYLE_PARAM, Type.STRING,
						TABLE_LIST_STYLE_STATIC,
						ConfigDef.ValidString.in(TABLE_LIST_STYLE_STATIC,
								TABLE_LIST_STYLE_DYNAMIC),
						Importance.LOW, TABLE_LIST_STYLE_DOC)
				.define(PROCESS_LOBS_PARAM, Type.BOOLEAN, false,
						Importance.LOW, PROCESS_LOBS_DOC)
				.define(CONNECTION_BACKOFF_PARAM, Type.INT, CONNECTION_BACKOFF_DEFAULT,
						Importance.LOW, CONNECTION_BACKOFF_DOC)
				.define(ParamConstants.ARCHIVED_LOG_CAT_PARAM, Type.STRING, ParamConstants.ARCHIVED_LOG_CAT_DEFAULT,
						Importance.LOW, ParamConstants.ARCHIVED_LOG_CAT_DOC)
				.define(ParamConstants.FETCH_SIZE_PARAM, Type.INT, ParamConstants.FETCH_SIZE_DEFAULT,
						Importance.LOW, ParamConstants.FETCH_SIZE_DOC)
				.define(ParamConstants.TRACE_LOGMINER_PARAM, Type.BOOLEAN, false,
						Importance.LOW, ParamConstants.TRACE_LOGMINER_DOC)
				.define(ParamConstants.MAKE_DISTRIBUTED_ACTIVE_PARAM, Type.BOOLEAN, false,
						Importance.LOW, ParamConstants.MAKE_DISTRIBUTED_ACTIVE_DOC)
				.define(ParamConstants.DISTRIBUTED_WALLET_PARAM, Type.STRING, "",
						Importance.LOW, ParamConstants.DISTRIBUTED_WALLET_DOC)
				.define(ParamConstants.DISTRIBUTED_URL_PARAM, Type.STRING, "",
						Importance.LOW, ParamConstants.DISTRIBUTED_URL_DOC)
				.define(ParamConstants.DISTRIBUTED_TARGET_HOST, Type.STRING, "",
						Importance.LOW, ParamConstants.DISTRIBUTED_TARGET_HOST_DOC)
				.define(ParamConstants.DISTRIBUTED_TARGET_PORT, Type.INT, ParamConstants.DISTRIBUTED_TARGET_PORT_DEFAULT,
						Importance.LOW, ParamConstants.DISTRIBUTED_TARGET_PORT_DOC)
				.define(LOB_TRANSFORM_CLASS_PARAM, Type.STRING, LOB_TRANSFORM_CLASS_DEFAULT,
						Importance.LOW, LOB_TRANSFORM_CLASS_DOC)
				.define(USE_RAC_PARAM, Type.BOOLEAN, false,
						Importance.LOW, USE_RAC_DOC)
				.define(PROTOBUF_SCHEMA_NAMING_PARAM, Type.BOOLEAN, false,
						Importance.LOW, PROTOBUF_SCHEMA_NAMING_DOC)
				.define(ParamConstants.ORA_TRANSACTION_IMPL_PARAM, Type.STRING,
						ParamConstants.ORA_TRANSACTION_IMPL_CHRONICLE,
						ConfigDef.ValidString.in(ParamConstants.ORA_TRANSACTION_IMPL_CHRONICLE,
								ParamConstants.ORA_TRANSACTION_IMPL_JVM),
						Importance.LOW, ParamConstants.ORA_TRANSACTION_IMPL_DOC)
				.define(PRINT_INVALID_HEX_WARNING_PARAM, Type.BOOLEAN, false,
						Importance.LOW, PRINT_INVALID_HEX_WARNING_DOC)
				.define(ParamConstants.PROCESS_ONLINE_REDO_LOGS_PARAM, Type.BOOLEAN, false,
						Importance.LOW, ParamConstants.PROCESS_ONLINE_REDO_LOGS_DOC)
				.define(ParamConstants.CURRENT_SCN_QUERY_INTERVAL_PARAM, Type.INT, ParamConstants.CURRENT_SCN_QUERY_INTERVAL_DEFAULT,
						Importance.LOW, ParamConstants.CURRENT_SCN_QUERY_INTERVAL_DOC)
				.define(INCOMPLETE_REDO_TOLERANCE_PARAM, Type.STRING,
						INCOMPLETE_REDO_TOLERANCE_ERROR,
						ConfigDef.ValidString.in(
								INCOMPLETE_REDO_TOLERANCE_ERROR,
								INCOMPLETE_REDO_TOLERANCE_SKIP,
								INCOMPLETE_REDO_TOLERANCE_RESTORE),
						Importance.LOW, INCOMPLETE_REDO_TOLERANCE_DOC)
				.define(ParamConstants.PRINT_ALL_ONLINE_REDO_RANGES_PARAM, Type.BOOLEAN, true,
						Importance.LOW, ParamConstants.PRINT_ALL_ONLINE_REDO_RANGES_DOC)
				.define(LM_RECONNECT_INTERVAL_MS_PARAM, Type.LONG, Long.MAX_VALUE,
						Importance.LOW, LM_RECONNECT_INTERVAL_MS_DOC)
				.define(PK_TYPE_PARAM, Type.STRING,
						PK_TYPE_WELL_DEFINED,
						ConfigDef.ValidString.in(
								PK_TYPE_WELL_DEFINED,
								PK_TYPE_ANY_UNIQUE
						),
						Importance.MEDIUM, PK_TYPE_DOC)
				.define(USE_ROWID_AS_KEY_PARAM, Type.BOOLEAN, true,
						Importance.MEDIUM, USE_ROWID_AS_KEY_DOC)
				.define(ConnectorParams.USE_ALL_COLUMNS_ON_DELETE_PARAM,
						Type.BOOLEAN, ConnectorParams.USE_ALL_COLUMNS_ON_DELETE_DEFAULT,
						Importance.MEDIUM, ConnectorParams.USE_ALL_COLUMNS_ON_DELETE_DOC)
				.define(INTERNAL_RAC_URLS_PARAM, Type.LIST, "",
						Importance.LOW, INTERNAL_PARAMETER_DOC)
				.define(INTERNAL_DG4RAC_THREAD_PARAM, Type.LIST, "",
						Importance.LOW, INTERNAL_PARAMETER_DOC)
				.define(TOPIC_MAPPER_PARAM, Type.STRING,
						TOPIC_MAPPER_DEFAULT,
						Importance.LOW, TOPIC_MAPPER_DOC)
				.define(STOP_ON_ORA_1284_PARAM, Type.BOOLEAN, STOP_ON_ORA_1284_DEFAULT,
						Importance.LOW, STOP_ON_ORA_1284_DOC)
				.define(PRINT_UNABLE_TO_DELETE_WARNING_PARAM, Type.BOOLEAN, PRINT_UNABLE_TO_DELETE_WARNING_DEFAULT,
						Importance.LOW, PRINT_UNABLE_TO_DELETE_WARNING_DOC)
				.define(SCHEMANAME_MAPPER_PARAM, Type.STRING,
						SCHEMANAME_MAPPER_DEFAULT,
						Importance.LOW, SCHEMANAME_MAPPER_DOC)
				.define(ORA_ROWSCN_PARAM, Type.STRING, "",
						Importance.LOW, ORA_ROWSCN_DOC)
				.define(ORA_COMMITSCN_PARAM, Type.STRING, "",
						Importance.LOW, ORA_COMMITSCN_DOC)
				.define(ORA_ROWTS_PARAM, Type.STRING, "",
						Importance.LOW, ORA_ROWTS_DOC)
				.define(ORA_OPERATION_PARAM, Type.STRING, "",
						Importance.LOW, ORA_OPERATION_DOC)
				.define(ORA_XID_PARAM, Type.STRING, "",
						Importance.LOW, ORA_XID_DOC)
				.define(ORA_USERNAME_PARAM, Type.STRING, "",
						Importance.LOW, ORA_USERNAME_DOC)
				.define(ORA_OSUSERNAME_PARAM, Type.STRING, "",
						Importance.LOW, ORA_OSUSERNAME_DOC)
				.define(ORA_HOSTNAME_PARAM, Type.STRING, "",
						Importance.LOW, ORA_HOSTNAME_DOC)
				.define(ORA_AUDIT_SESSIONID_PARAM, Type.STRING, "",
						Importance.LOW, ORA_AUDIT_SESSIONID_DOC)
				.define(ORA_SESSION_INFO_PARAM, Type.STRING, "",
						Importance.LOW, ORA_SESSION_INFO_DOC)
				.define(ORA_CLIENT_ID_PARAM, Type.STRING, "",
						Importance.LOW, ORA_CLIENT_ID_DOC)
				.define(LAST_SEQ_NOTIFIER_PARAM, Type.STRING, "",
						Importance.LOW, LAST_SEQ_NOTIFIER_DOC)
				.define(LAST_SEQ_NOTIFIER_FILE_PARAM, Type.STRING, "",
						Importance.LOW, LAST_SEQ_NOTIFIER_FILE_DOC)
				.define(KEY_OVERRIDE_PARAM, Type.LIST, "",
						Importance.MEDIUM, KEY_OVERRIDE_DOC)
				.define(CONC_TRANSACTIONS_THRESHOLD_PARAM, Type.INT, 0,
						Importance.LOW, CONC_TRANSACTIONS_THRESHOLD_DOC)
				.define(REDUCE_LOAD_MS_PARAM, Type.INT, REDUCE_LOAD_MS_DEFAULT,
						Importance.LOW, REDUCE_LOAD_MS_DOC)
				.define(AL_CAPACITY_PARAM, Type.INT, AL_CAPACITY_DEFAULT,
						Importance.LOW, AL_CAPACITY_DOC)
				// Redo Miner only!
				.define(REDO_FILE_NAME_CONVERT_PARAM, Type.STRING, "",
						Importance.HIGH, REDO_FILE_NAME_CONVERT_DOC)
				.define(REDO_FILE_MEDIUM_PARAM, Type.STRING,
						REDO_FILE_MEDIUM_FS,
						ConfigDef.ValidString.in(
								REDO_FILE_MEDIUM_FS,
								REDO_FILE_MEDIUM_ASM,
								REDO_FILE_MEDIUM_SSH),
						Importance.HIGH, REDO_FILE_MEDIUM_DOC)
				.define(ASM_JDBC_URL_PARAM, Type.STRING, "",
						Importance.LOW, ASM_JDBC_URL_DOC)
				.define(ASM_USER_PARAM, Type.STRING, "",
						Importance.LOW, ASM_USER_DOC)
				.define(ASM_PASSWORD_PARAM, Type.PASSWORD, "",
						Importance.LOW, ASM_PASSWORD_DOC)
				.define(ASM_READ_AHEAD_PARAM, Type.BOOLEAN, ASM_READ_AHEAD_DEFAULT,
						Importance.LOW, ASM_READ_AHEAD_DOC)
				.define(ASM_RECONNECT_INTERVAL_MS_PARAM, Type.LONG, ASM_RECONNECT_INTERVAL_MS_DEFAULT,
						Importance.LOW, ASM_RECONNECT_INTERVAL_MS_DOC)
				.define(SSH_HOST_PARAM, Type.STRING, "",
						Importance.LOW, SSH_HOST_DOC)
				.define(SSH_PORT_PARAM, Type.INT, SSH_PORT_DEFAULT,
						Importance.LOW, SSH_PORT_DOC)
				.define(SSH_USER_PARAM, Type.STRING, "",
						Importance.LOW, SSH_USER_DOC)
				.define(SSH_KEY_PARAM, Type.PASSWORD, "",
						Importance.LOW, SSH_KEY_DOC)
				.define(SSH_PASSWORD_PARAM, Type.PASSWORD, "",
						Importance.LOW, SSH_PASSWORD_DOC)
				.define(SSH_RECONNECT_INTERVAL_MS_PARAM, Type.LONG, SSH_RECONNECT_INTERVAL_MS_DEFAULT,
						Importance.LOW, SSH_RECONNECT_INTERVAL_MS_DOC)
				.define(SSH_BUFFER_PARAM, Type.STRING,
						SSH_BUFFER_LARGE,
						ConfigDef.ValidString.in(
								SSH_BUFFER_SMALL,
								SSH_BUFFER_MEDIUM,
								SSH_BUFFER_LARGE),
						Importance.HIGH, SSH_BUFFER_DOC)
				;
	}

	public OraCdcSourceConnectorConfig(Map<String, String> originals) {
		super(config(), originals);
		// parse numberColumnsMap
		Map<String, String> numberMapParams = originals.entrySet().stream()
				.filter(prop -> StringUtils.startsWith(prop.getKey(), NUMBER_MAP_PREFIX))
				.collect(Collectors.toMap(
						prop -> StringUtils.replace(prop.getKey(), NUMBER_MAP_PREFIX, ""),
						Map.Entry::getValue));
				
		numberMapParams.forEach((param, value) -> {
			final int lastDotPos = StringUtils.lastIndexOf(param, '.');
			final String column = StringUtils.substring(param, lastDotPos + 1);
			final String fqn = StringUtils.substring(param, 0, lastDotPos);
			if (!numberColumnsMap.containsKey(fqn)) {
				numberColumnsMap.put(fqn, Triple.of(
						new ArrayList<Pair<String, OraColumn>>(),
						new HashMap<String, OraColumn>(),
						new ArrayList<Pair<String, OraColumn>>()));
			}
			final int jdbcType;
			int scale = 0;
			switch (StringUtils.upperCase(StringUtils.trim(StringUtils.substringBefore(value, '(')))) {
			case "BOOL":
			case "BOOLEAN":
				jdbcType = Types.BOOLEAN;
				break;
			case "BYTE":
			case "TINYINT":
				jdbcType = Types.TINYINT;
				break;
			case "SHORT":
			case "SMALLINT":
				jdbcType = Types.SMALLINT;
				break;
			case "INT":
			case "INTEGER":
				jdbcType = Types.INTEGER;
				break;
			case "LONG":
			case "BIGINT":
				jdbcType = Types.BIGINT;
				break;
			case "FLOAT":
				jdbcType = Types.FLOAT;
				break;
			case "DOUBLE":
				jdbcType = Types.DOUBLE;
				break;
			case "DECIMAL":
			case "NUMERIC":
				final String precisionScale = StringUtils.trim(StringUtils.substringBetween(value, "(", ")"));
				if (StringUtils.countMatches(precisionScale, ',') == 1) {
					try {
						scale = Integer.parseInt(StringUtils.trim(StringUtils.split(precisionScale, ',')[1]));
					} catch (Exception e) {
						LOGGER.error(
								"\n=====================\n" +
								"Unable to parse decimal scale in '{}' for parameter '{}'!\n" +
								"\n=====================\n",
								value, NUMBER_MAP_PREFIX + param);
						scale = -1;
					}
					if (scale == -1) {
						jdbcType = Types.NULL;
					} else {
						jdbcType = Types.DECIMAL;
					}
				} else {
					jdbcType = Types.NULL;
					LOGGER.error(
							"\n=====================\n" +
							"Mapping '{}' for parameter '' will be ignored!" +
							"\n=====================\n",
							value, NUMBER_MAP_PREFIX + param);
				}
				break;
			default:
				LOGGER.error(
						"\n=====================\n" +
						"Unable to recognize datatype '{}' for parameter '{}'!\n" +
						"\n=====================\n",
						value, NUMBER_MAP_PREFIX + param);
				jdbcType = Types.NULL;
			}
			if (jdbcType != Types.NULL) {
				if (StringUtils.endsWith(column, "%")) {
					numberColumnsMap.get(fqn).getLeft().add(Pair.of(
							StringUtils.substring(column, 0, column.length() - 1),
							new OraColumn(column, jdbcType, scale)));
				} else if (StringUtils.startsWith(column, "%")) {
					numberColumnsMap.get(fqn).getRight().add(Pair.of(
							StringUtils.substring(column, 1),
							new OraColumn(column, jdbcType, scale)));
				} else {
					numberColumnsMap.get(fqn).getMiddle().put(column, new OraColumn(column, jdbcType, scale));
				}
			}
		});
	}

	public List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>>
			tableNumberMapping(final String tableOwner, final String tableName) {
		return tableNumberMapping(null, tableOwner, tableName);
	}

	public List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>>
			tableNumberMapping(final String pdbName, final String tableOwner, final String tableName) {
		final String fqn =  tableOwner + "." + tableName;
		if (pdbName == null) {
			if (numberColumnsMap.containsKey(fqn)) {
				List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>> result =
						new ArrayList<>(1);
				result.add(numberColumnsMap.get(fqn));
				return result;
			} else {
				return null;
			}
		} else {
			final Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>> forAll =
					numberColumnsMap.get(fqn);
			final Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>> exact =
					numberColumnsMap.get(pdbName + "." + fqn);
			if (exact != null && forAll == null) {
				List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>> result =
						new ArrayList<>(1);
				result.add(exact);
				return result;
			} else if (exact != null && forAll != null) {
				List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>> result =
						new ArrayList<>(2);
				result.add(exact);
				result.add(forAll);
				return result;
			} else if (forAll != null) {
				List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>> result =
						new ArrayList<>(1);
				result.add(forAll);
				return result;
			} else {
				return null;
			}
		}
	}

	public OraColumn columnNumberMapping(
			List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>>
				numberRemap, final String columnName) {
		if (numberRemap != null)
			for (int i = 0; i < numberRemap.size(); i++) {
				Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>> reDefs =
						numberRemap.get(i);
				OraColumn result = reDefs.getMiddle().get(columnName);
				if (result != null) {
					return result;
				} else if ((result = remapUsingPattern(reDefs.getLeft(), columnName, true)) != null) {
					return result;
				} else if ((result = remapUsingPattern(reDefs.getRight(), columnName, false)) != null) {
					return result;
				}
			}
		return null;
	}

	private OraColumn remapUsingPattern(final List<Pair<String, OraColumn>> patterns, final String columnName, final boolean startsWith) {
		for (final Pair<String, OraColumn> pattern : patterns)
			if (startsWith &&
					StringUtils.startsWith(columnName, pattern.getKey()))
				return pattern.getValue();
			else if (!startsWith &&
					StringUtils.endsWith(columnName, pattern.getKey()))
				return pattern.getValue();
		return null;
	}

	public boolean useOracdcSchemas() {
		return getBoolean(ORACDC_SCHEMAS_PARAM);
	}

	public int getIncompleteDataTolerance() {
		if (incompleteDataTolerance == -1) {
			switch (getString(INCOMPLETE_REDO_TOLERANCE_PARAM)) {
			case INCOMPLETE_REDO_TOLERANCE_ERROR:
				incompleteDataTolerance = INCOMPLETE_REDO_INT_ERROR;
				break;
			case INCOMPLETE_REDO_TOLERANCE_SKIP:
				incompleteDataTolerance = INCOMPLETE_REDO_INT_SKIP;
				break;
			default:
				//INCOMPLETE_REDO_TOLERANCE_RESTORE
				incompleteDataTolerance = INCOMPLETE_REDO_INT_RESTORE;
			}
		}
		return incompleteDataTolerance;
	}

	public boolean isPrintInvalidHexValueWarning() {
		return getBoolean(PRINT_INVALID_HEX_WARNING_PARAM);
	}

	public boolean useProtobufSchemaNaming() {
		return getBoolean(PROTOBUF_SCHEMA_NAMING_PARAM);
	}

	public String getTopicNameDelimiter() {
		return getString(TOPIC_NAME_DELIMITER_PARAM);
	}

	public int getTopicNameStyle() {
		if (topicNameStyle == -1) {
			switch (getString(TOPIC_NAME_STYLE_PARAM)) {
			case TOPIC_NAME_STYLE_TABLE:
				topicNameStyle = TOPIC_NAME_STYLE_INT_TABLE;
				break;
			case TOPIC_NAME_STYLE_SCHEMA_TABLE:
				topicNameStyle = TOPIC_NAME_STYLE_INT_SCHEMA_TABLE;
				break;
			case TOPIC_NAME_STYLE_PDB_SCHEMA_TABLE:
				topicNameStyle = TOPIC_NAME_STYLE_INT_PDB_SCHEMA_TABLE;
				break;
			}
		}
		return topicNameStyle;
	}

	public int getPkType() {
		if (pkType == -1) {
			switch (getString(PK_TYPE_PARAM)) {
			case PK_TYPE_WELL_DEFINED:
				pkType = PK_TYPE_INT_WELL_DEFINED;
				break;
			case PK_TYPE_ANY_UNIQUE:
				pkType = PK_TYPE_INT_ANY_UNIQUE;
				break;
			}
		}
		return pkType;
	}

	public boolean useRowidAsKey() {
		return getBoolean(USE_ROWID_AS_KEY_PARAM);
	}

	public boolean useAllColsOnDelete() {
		return getBoolean(ConnectorParams.USE_ALL_COLUMNS_ON_DELETE_PARAM);
	}

	public boolean stopOnOra1284() {
		return getBoolean(STOP_ON_ORA_1284_PARAM);
	}

	public boolean printUnableToDeleteWarning() {
		return getBoolean(PRINT_UNABLE_TO_DELETE_WARNING_PARAM);
	}

	public TopicNameMapper getTopicNameMapper() {
		final TopicNameMapper tnm;
		final Class<?> clazz;
		final Constructor<?> constructor;
		try {
			clazz = Class.forName(getString(TOPIC_MAPPER_PARAM));
		} catch (ClassNotFoundException nfe) {
			LOGGER.error(
					"\n=====================\n" +
					"Class '{}' specified as the parameter '{}' value was not found.\n" +
					ExceptionUtils.getExceptionStackTrace(nfe) +
					"\n" +
					"=====================\n",
					getString(TOPIC_MAPPER_PARAM), TOPIC_MAPPER_PARAM);
			throw new ConnectException(nfe);
		}
		try {
			constructor = clazz.getConstructor();
		} catch (NoSuchMethodException nsme) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to get default constructor for the class '{}'.\n" +
					ExceptionUtils.getExceptionStackTrace(nsme) +
					"\n" +
					"=====================\n",
					getString(TOPIC_MAPPER_PARAM));
			throw new ConnectException(nsme);
		} 
		
		try {
			tnm = (TopicNameMapper) constructor.newInstance();
		} catch (SecurityException | 
				InvocationTargetException | 
				IllegalAccessException | 
				InstantiationException e) {
			LOGGER.error(
					"\n=====================\n" +
					"'{}' while instantinating the class '{}'.\n" +
					ExceptionUtils.getExceptionStackTrace(e) +
					"\n" +
					"=====================\n",
					e.getMessage(),getString(TOPIC_MAPPER_PARAM));
			throw new ConnectException(e);
		}
		return tnm;
	}


	public SchemaNameMapper getSchemaNameMapper() {
		final SchemaNameMapper snm;
		final Class<?> clazz;
		final Constructor<?> constructor;
		try {
			clazz = Class.forName(getString(SCHEMANAME_MAPPER_PARAM));
		} catch (ClassNotFoundException nfe) {
			LOGGER.error(
					"\n=====================\n" +
					"Class '{}' specified as the parameter '{}' value was not found.\n" +
					ExceptionUtils.getExceptionStackTrace(nfe) +
					"\n" +
					"=====================\n",
					getString(SCHEMANAME_MAPPER_PARAM), SCHEMANAME_MAPPER_PARAM);
			throw new ConnectException(nfe);
		}
		try {
			constructor = clazz.getConstructor();
		} catch (NoSuchMethodException nsme) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to get default constructor for the class '{}'.\n" +
					ExceptionUtils.getExceptionStackTrace(nsme) +
					"\n" +
					"=====================\n",
					getString(SCHEMANAME_MAPPER_PARAM));
			throw new ConnectException(nsme);
		} 
		
		try {
			snm = (SchemaNameMapper) constructor.newInstance();
		} catch (SecurityException | 
				InvocationTargetException | 
				IllegalAccessException | 
				InstantiationException e) {
			LOGGER.error(
					"\n=====================\n" +
					"'{}' while instantinating the class '{}'.\n" +
					ExceptionUtils.getExceptionStackTrace(e) +
					"\n" +
					"=====================\n",
					e.getMessage(),getString(SCHEMANAME_MAPPER_PARAM));
			throw new ConnectException(e);
		}
		return snm;
	}

	public String getOraRowScnField() {
		return getPseudoColumn(ORA_ROWSCN_PARAM);
	}

	public String getOraCommitScnField() {
		return getPseudoColumn(ORA_COMMITSCN_PARAM);
	}

	public String getOraRowTsField() {
		return getPseudoColumn(ORA_ROWTS_PARAM);
	}

	public String getOraRowOpField() {
		return getPseudoColumn(ORA_OPERATION_PARAM);
	}

	public String getOraXidField() {
		return getPseudoColumn(ORA_XID_PARAM);
	}

	public String getOraUsernameField() {
		return getPseudoColumn(ORA_USERNAME_PARAM);
	}

	public String getOraOsUsernameField() {
		return getPseudoColumn(ORA_OSUSERNAME_PARAM);
	}

	public String getOraHostnameField() {
		return getPseudoColumn(ORA_HOSTNAME_PARAM);
	}

	public String getOraAuditSessionIdField() {
		return getPseudoColumn(ORA_AUDIT_SESSIONID_PARAM);
	}

	public String getOraSessionInfoField() {
		return getPseudoColumn(ORA_SESSION_INFO_PARAM);
	}

	public String getOraClientIdField() {
		return getPseudoColumn(ORA_CLIENT_ID_PARAM);
	}

	private String getPseudoColumn(final String param) {
		final String value = getString(param);
		if (StringUtils.isBlank(value)) {
			return null;
		} else if (KafkaUtils.validAvroFieldName(value)) {
			return value;
		} else {
			LOGGER.error(
					"\n=====================\n" +
					"Invalid value [{}] for parameter '{}'.\n" +
					"The parameter value must contain only the characters below\n\t{}\n" +
					"=====================\n",
					value, param, KafkaUtils.AVRO_FIELD_VALID_CHARS);
			throw new IllegalArgumentException("Invalid value [" + value + "] for parameter " + param + "!");
		}
	}

	public String getConnectorName() {
		return connectorName;
	}

	public void setConnectorName(String connectorName) {
		this.connectorName = connectorName;
	}

	public LastProcessedSeqNotifier getLastProcessedSeqNotifier() {
		final String className = getString(LAST_SEQ_NOTIFIER_PARAM);
		if (StringUtils.isBlank(className)) {
			return null;
		} else {
			final LastProcessedSeqNotifier lpsn;
			final Class<?> clazz;
			final Constructor<?> constructor;
			try {
				clazz = Class.forName(className);
			} catch (ClassNotFoundException nfe) {
				LOGGER.error(
						"\n=====================\n" +
						"Class '{}' specified as the parameter '{}' value was not found.\n" +
						ExceptionUtils.getExceptionStackTrace(nfe) +
						"\n" +
						"=====================\n",
						className, LAST_SEQ_NOTIFIER_PARAM);
				throw new ConnectException(nfe);
			}
			try {
				constructor = clazz.getConstructor();
			} catch (NoSuchMethodException nsme) {
				LOGGER.error(
						"\n=====================\n" +
						"Unable to get default constructor for the class '{}'.\n" +
						ExceptionUtils.getExceptionStackTrace(nsme) +
						"\n" +
						"=====================\n",
						className);
				throw new ConnectException(nsme);
			} 
			
			try {
				lpsn = (LastProcessedSeqNotifier) constructor.newInstance();
			} catch (SecurityException | 
					InvocationTargetException | 
					IllegalAccessException | 
					InstantiationException e) {
				LOGGER.error(
						"\n=====================\n" +
						"'{}' while instantinating the class '{}'.\n" +
						ExceptionUtils.getExceptionStackTrace(e) +
						"\n" +
						"=====================\n",
						e.getMessage(),className);
				throw new ConnectException(e);
			}
			return lpsn;
		}
	}

	public String getLastProcessedSeqNotifierFile() {
		if (StringUtils.isNotBlank(getString(LAST_SEQ_NOTIFIER_FILE_PARAM))) {
			return getString(LAST_SEQ_NOTIFIER_FILE_PARAM);
		} else {
			final String tmpDir = System.getProperty("java.io.tmpdir");
			if (StringUtils.isNotBlank(connectorName)) {
				return tmpDir + File.separator +
						connectorName + ".seq";
			} else {
				return tmpDir + File.separator + "oracdc.seq";
			}
		} 
	}

	public Entry<OraCdcKeyOverrideTypes, String> getKeyOverrideType(final String fqtn) {
		if (keyOverrideMap == null) {
			keyOverrideMap = new HashMap<>();
			keyOverrideIndexMap = new HashMap<>();
			//Perform initial parsing
			getList(KEY_OVERRIDE_PARAM).forEach(token -> {
				try {
					final String[] pair = StringUtils.split(token, "=");
					final String fullTableName = StringUtils.upperCase(pair[0]);
					final String overrideValue = pair[1];
					if (StringUtils.equalsIgnoreCase(overrideValue, "NOKEY")) {
						keyOverrideMap.put(fullTableName, OraCdcKeyOverrideTypes.NOKEY);
					} else if (StringUtils.equalsIgnoreCase(overrideValue, "ROWID")) {
						keyOverrideMap.put(fullTableName, OraCdcKeyOverrideTypes.ROWID);
					} else if (StringUtils.startsWithIgnoreCase(overrideValue, "INDEX")) {
						keyOverrideMap.put(fullTableName, OraCdcKeyOverrideTypes.INDEX);
						keyOverrideIndexMap.put(fullTableName,
								StringUtils.substringBetween(overrideValue, "(", ")"));
					} else {
						LOGGER.error(
								"\n=====================\n" +
								"Incorrect value {} for parameter {}!" +
								"\n=====================\n",
								token, KEY_OVERRIDE_PARAM);
					}
				} catch (Exception e) {
					LOGGER.error("Unable to parse '{}'!", token);
				}
			});
			
		}
		return Map.entry(
				keyOverrideMap.getOrDefault(fqtn, OraCdcKeyOverrideTypes.NONE),
				keyOverrideIndexMap.getOrDefault(fqtn, ""));
	}

	public boolean processLobs() {
		return getBoolean(PROCESS_LOBS_PARAM);
	}

	public OraCdcLobTransformationsIntf transformLobsImpl() {
		if (transformLobsImpl == null) {
			final String transformLobsImplClass = getString(LOB_TRANSFORM_CLASS_PARAM);
			LOGGER.info("oracdc will process Oracle LOBs using {} LOB transformations implementation",
					transformLobsImplClass);
			try {
				final Class<?> classTransformLobs = Class.forName(transformLobsImplClass);
				final Constructor<?> constructor = classTransformLobs.getConstructor();
				transformLobsImpl = (OraCdcLobTransformationsIntf) constructor.newInstance();
			} catch (ClassNotFoundException nfe) {
				LOGGER.error("ClassNotFoundException while instantiating {}", transformLobsImplClass);
				throw new ConnectException("ClassNotFoundException while instantiating " + transformLobsImplClass, nfe);
			} catch (NoSuchMethodException nme) {
				LOGGER.error("NoSuchMethodException while instantiating {}", transformLobsImplClass);
				throw new ConnectException("NoSuchMethodException while instantiating " + transformLobsImplClass, nme);
			} catch (SecurityException se) {
				LOGGER.error("SecurityException while instantiating {}", transformLobsImplClass);
				throw new ConnectException("SecurityException while instantiating " + transformLobsImplClass, se);
			} catch (InvocationTargetException ite) {
				LOGGER.error("InvocationTargetException while instantiating {}", transformLobsImplClass);
				throw new ConnectException("InvocationTargetException while instantiating " + transformLobsImplClass, ite);
			} catch (IllegalAccessException iae) {
				LOGGER.error("IllegalAccessException while instantiating {}", transformLobsImplClass);
				throw new ConnectException("IllegalAccessException while instantiating " + transformLobsImplClass, iae);
			} catch (InstantiationException ie) {
				LOGGER.error("InstantiationException while instantiating {}", transformLobsImplClass);
				throw new ConnectException("InstantiationException while instantiating " + transformLobsImplClass, ie);
			}
		}
		return transformLobsImpl;
	}

	public int connectionRetryBackoff() {
		return getInt(CONNECTION_BACKOFF_PARAM);
	}

	public OraCdcPseudoColumnsProcessor pseudoColumnsProcessor() {
		if (pseudoColumns == null) {
			pseudoColumns = new OraCdcPseudoColumnsProcessor(this);
		}
		return pseudoColumns;
	}

	public boolean useRac() {
		return getBoolean(USE_RAC_PARAM);
	}

	public String useRacParamName() {
		return USE_RAC_PARAM;
	}

	public boolean activateStandby() {
		return getBoolean(MAKE_STANDBY_ACTIVE_PARAM);
	}

	public String activateStandbyParamName() {
		return MAKE_STANDBY_ACTIVE_PARAM;
	}

	public List<String> racUrls() {
		return getList(INTERNAL_RAC_URLS_PARAM);
	}

	public List<String> dg4RacThreads() {
		return getList(INTERNAL_DG4RAC_THREAD_PARAM);
	}

	public int topicPartition() {
		return topicPartition;
	}

	public void topicPartition(final int redoThread) {
		if (useRac() || 
			(activateStandby() && dg4RacThreads() != null && dg4RacThreads().size() > 1)) {
			topicPartition = redoThread - 1;
		} else {
			topicPartition = getInt(TOPIC_PARTITION_PARAM);
		}
	}

	public Path queuesRoot() throws SQLException {
		try {
			if (queuesRoot == null) {
				final String tempDir = getString(TEMP_DIR_PARAM);
				if (Files.isDirectory(Paths.get(tempDir))) {
					if (!Files.isWritable(Paths.get(tempDir))) {
						LOGGER.error(
								"\n=====================\n" +
								"Parameter '{}' points to non-writable directory '{}'." +
								"\n=====================\n",
								TEMP_DIR_PARAM, tempDir);
						throw new SQLException("Temp directory is not properly set!");
					}
				} else {
					try {
						Files.createDirectories(Paths.get(tempDir));
					} catch (IOException | UnsupportedOperationException | SecurityException  e) {
						LOGGER.error(
								"\n=====================\n" +
								"Unable to create directory! Parameter {} points to non-existent or invalid directory {}." +
								"\n=====================\n",
								TEMP_DIR_PARAM, tempDir);
						throw new SQLException(e);
					}
				}
				queuesRoot = FileSystems.getDefault().getPath(tempDir);
			}
			return queuesRoot;
		} catch (InvalidPathException ipe) {
			throw new SQLException(ipe);
		}
	}

	public long startScn() throws SQLException {
		final String scnAsString = getString(LGMNR_START_SCN_PARAM);
		try {
			return Long.parseUnsignedLong(scnAsString);
		} catch (NumberFormatException nfe) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to parse value '{}' of parameter '{}' as unsigned long!" +
					"\n=====================\n",
					scnAsString, LGMNR_START_SCN_PARAM);
			throw new SQLException(nfe);
		}
	}

	public String startScnParam() {
		return LGMNR_START_SCN_PARAM;
	}

	public boolean staticObjIds() {
		return StringUtils.equalsIgnoreCase(
				TABLE_LIST_STYLE_STATIC, getString(TABLE_LIST_STYLE_PARAM));
	}

	public long logMinerReconnectIntervalMs() {
		return getLong(LM_RECONNECT_INTERVAL_MS_PARAM);
	}

	public int transactionsThreshold() {
		int threshold = getInt(CONC_TRANSACTIONS_THRESHOLD_PARAM);
		boolean isLinux = StringUtils.containsIgnoreCase(System.getProperty("os.name"), "nux");
		if (threshold > 0) {
			return threshold;
		} else if (isLinux) {
			int maxMapCount = 0x10000;
			try (InputStream is = Files.newInputStream(Path.of("/proc/sys/vm/max_map_count"))) {
				final byte[] buffer = new byte[0x10];
				final int size = is.read(buffer, 0, buffer.length);
				maxMapCount = Integer.parseInt(StringUtils.trim(new String(buffer, 0, size)));
			} catch (IOException | NumberFormatException e) {
				LOGGER.error(
						"\n=====================\n" +
						"Unable to read and parse value of vm.max_map_count from  '/proc/sys/vm/max_map_count'!\nException:{}\n{}" +
						"\n=====================\n",
						e.getMessage(), ExceptionUtils.getExceptionStackTrace(e));
			}
			return ((int)(maxMapCount / 0x10)) * 0x7;
		} else {
			return 0x7000;
		}
	}

	public int reduceLoadMs() {
		return getInt(REDUCE_LOAD_MS_PARAM);
	}

	public int arrayListCapacity() {
		return getInt(AL_CAPACITY_PARAM);
	}

	public boolean logMiner() {
		return logMiner;
	}

	public void logMiner(final boolean logMiner) {
		this.logMiner = logMiner;
	}

	public String convertRedoFileName(final String originalName) {
		if (!fileNameConversionInited) {
			final String fileNameConvertParam = getString(REDO_FILE_NAME_CONVERT_PARAM);
			if (StringUtils.isNotEmpty(fileNameConvertParam) &&
					StringUtils.contains(fileNameConvertParam, ':')) {
				String[] elements = StringUtils.split(fileNameConvertParam, ',');
				int newSize = 0;
				boolean[] processElement = new boolean[elements.length];
				for (int i = 0; i < elements.length; i++) {
					if (StringUtils.contains(elements[i], ":")) {
						elements[i] = StringUtils.trim(elements[i]);
						processElement[i] = true;
						newSize += 1;
					} else {
						processElement[i] = false;
					}
				}
				if (newSize > 0) {
					fileNameConversionMap = new HashMap<>();
					for (int i = 0; i < elements.length; i++) {
						if (processElement[i]) {
							fileNameConversionMap.put(
								StringUtils.appendIfMissing(
									StringUtils.trim(StringUtils.substringBefore(elements[i], ":")),
									File.separator),
							StringUtils.appendIfMissing(
									StringUtils.trim(StringUtils.substringAfter(elements[i], ":")),
									File.separator));
						}
					}
					fileNameConversion = true;
				}				
				elements = null;
				processElement = null;
			}
			fileNameConversionInited = true;
		}
		if (fileNameConversion) {
			final String originalPrefix = StringUtils.trim(
					StringUtils.substring(
							originalName,
							0,
							StringUtils.lastIndexOf(originalName, File.separator) + 1));
			final String replacementPrefix =  fileNameConversionMap.get(originalPrefix);
			if (replacementPrefix == null) {
				LOGGER.error(
						"\n=====================\n" +
						"Unable to convert filename '{}' using parameter {}={} !\n" +
						"Original filename will be returned!" +
						"\n=====================\n",
						originalName, REDO_FILE_NAME_CONVERT_PARAM, getString(REDO_FILE_NAME_CONVERT_PARAM));
				return originalName;
			} else {
				return StringUtils.replace(originalName, originalPrefix, replacementPrefix);
			}
		} else {
			return originalName;
		}
	}

	public boolean useAsm() {
		return StringUtils.equals(getString(REDO_FILE_MEDIUM_PARAM), REDO_FILE_MEDIUM_ASM);
	}

	public boolean useSsh() {
		return StringUtils.equals(getString(REDO_FILE_MEDIUM_PARAM), REDO_FILE_MEDIUM_SSH);
	}

	public String asmJdbcUrl() {
		return getString(ASM_JDBC_URL_PARAM);
	}

	public String getAsmUser() {
		return getString(ASM_USER_PARAM);
	}

	public String getAsmPassword() {	
		return getPassword(ASM_PASSWORD_PARAM).value();
	}

	public boolean asmReadAhead() {	
		return getBoolean(ASM_READ_AHEAD_PARAM);
	}

	public long asmReconnectIntervalMs() {
		return getLong(ASM_RECONNECT_INTERVAL_MS_PARAM);
	}

	public String sshHostname() {
		return getString(SSH_HOST_PARAM);
	}

	public int sshPort() {
		return getInt(SSH_PORT_PARAM);
	}

	public String sshUser() {
		return getString(SSH_USER_PARAM);
	}

	public String sshKey() {
		return getPassword(SSH_USER_PARAM).value();
	}

	public String sshPassword() {
		return getPassword(SSH_PASSWORD_PARAM).value();
	}

	public long sshReconnectIntervalMs() {
		return getLong(SSH_RECONNECT_INTERVAL_MS_PARAM);
	}

	public int sshBufferSize() {
		switch (getString(SSH_BUFFER_PARAM)) {
		case SSH_BUFFER_SMALL:
			return 0x10_000;
		case SSH_BUFFER_MEDIUM:
			return 0x40_000;
		default:
			return 0x100_000;
		}
	}

}
