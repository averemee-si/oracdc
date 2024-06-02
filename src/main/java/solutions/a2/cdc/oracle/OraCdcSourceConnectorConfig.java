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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.utils.KafkaUtils;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcSourceConnectorConfig extends AbstractConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcSourceConnectorConfig.class);

	public static final int INCOMPLETE_REDO_INT_ERROR = 1;
	public static final int INCOMPLETE_REDO_INT_SKIP = 2;
	public static final int INCOMPLETE_REDO_INT_RESTORE = 3;

	public static final int TOPIC_NAME_STYLE_INT_TABLE = 1;
	public static final int TOPIC_NAME_STYLE_INT_SCHEMA_TABLE = 2;
	public static final int TOPIC_NAME_STYLE_INT_PDB_SCHEMA_TABLE = 3;

	public static final int PK_TYPE_INT_WELL_DEFINED = 1;
	public static final int PK_TYPE_INT_ANY_UNIQUE = 2;


	public static final String TASK_PARAM_MASTER = "master";
	public static final String TASK_PARAM_MV_LOG = "mv.log";
	public static final String TASK_PARAM_OWNER = "owner";
	public static final String TASK_PARAM_SCHEMA_TYPE = "schema.type";
	public static final String TASK_PARAM_MV_ROWID = "mvlog.rowid";
	public static final String TASK_PARAM_MV_PK = "mvlog.pk";
	public static final String TASK_PARAM_MV_SEQUENCE = "mvlog.seq";

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


	private int incompleteDataTolerance = -1;
	private int topicNameStyle = -1;
	private int schemaType = -1;
	private int pkType = -1;
	private String connectorName;

	public static ConfigDef config() {
		return new ConfigDef()
				.define(ConnectorParams.CONNECTION_URL_PARAM, Type.STRING, "",
						Importance.HIGH, ConnectorParams.CONNECTION_URL_DOC)
				.define(ConnectorParams.CONNECTION_USER_PARAM, Type.STRING, "",
						Importance.HIGH, ConnectorParams.CONNECTION_USER_DOC)
				.define(ConnectorParams.CONNECTION_PASSWORD_PARAM, Type.PASSWORD, "",
						Importance.HIGH, ConnectorParams.CONNECTION_PASSWORD_DOC)
				.define(ParamConstants.CONNECTION_WALLET_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.CONNECTION_WALLET_DOC)
				.define(ParamConstants.KAFKA_TOPIC_PARAM, Type.STRING, ParamConstants.KAFKA_TOPIC_PARAM_DEFAULT,
						Importance.HIGH, ParamConstants.KAFKA_TOPIC_PARAM_DOC)
				.define(ParamConstants.POLL_INTERVAL_MS_PARAM, Type.INT, ParamConstants.POLL_INTERVAL_MS_DEFAULT,
						Importance.LOW, ParamConstants.POLL_INTERVAL_MS_DOC)
				.define(ConnectorParams.BATCH_SIZE_PARAM, Type.INT,
						ConnectorParams.BATCH_SIZE_DEFAULT,
						Importance.LOW, ConnectorParams.BATCH_SIZE_DOC)
				.define(ConnectorParams.SCHEMA_TYPE_PARAM, Type.STRING,
						ConnectorParams.SCHEMA_TYPE_KAFKA,
						ConfigDef.ValidString.in(
								ConnectorParams.SCHEMA_TYPE_KAFKA,
								ConnectorParams.SCHEMA_TYPE_SINGLE,
								ConnectorParams.SCHEMA_TYPE_DEBEZIUM),
						Importance.LOW, ConnectorParams.SCHEMA_TYPE_DOC)
				.define(ConnectorParams.TOPIC_PREFIX_PARAM, Type.STRING, "",
						Importance.MEDIUM, ConnectorParams.TOPIC_PREFIX_DOC)
				.define(ParamConstants.TOPIC_PARTITION_PARAM, Type.SHORT, (short) 0,
						Importance.MEDIUM, ParamConstants.TOPIC_PARTITION_DOC)
				.define(ParamConstants.TABLE_EXCLUDE_PARAM, Type.LIST, "",
						Importance.MEDIUM, ParamConstants.TABLE_EXCLUDE_DOC)
				.define(ParamConstants.TABLE_INCLUDE_PARAM, Type.LIST, "",
						Importance.MEDIUM, ParamConstants.TABLE_INCLUDE_DOC)
				.define(ParamConstants.REDO_FILES_COUNT_PARAM, Type.SHORT, (short) 1,
						Importance.MEDIUM, ParamConstants.REDO_FILES_COUNT_DOC)
				.define(ParamConstants.REDO_FILES_SIZE_PARAM, Type.LONG, 0,
						Importance.MEDIUM, ParamConstants.REDO_FILES_SIZE_DOC)
				.define(ParamConstants.LGMNR_START_SCN_PARAM, Type.LONG, 0,
						Importance.MEDIUM, ParamConstants.LGMNR_START_SCN_DOC)
				.define(ParamConstants.TEMP_DIR_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.TEMP_DIR_DOC)
				.define(ParamConstants.MAKE_STANDBY_ACTIVE_PARAM, Type.BOOLEAN, false,
						Importance.LOW, ParamConstants.MAKE_STANDBY_ACTIVE_DOC)
				.define(ParamConstants.STANDBY_WALLET_PARAM, Type.STRING, "",
						Importance.LOW, ParamConstants.STANDBY_WALLET_DOC)
				.define(ParamConstants.STANDBY_URL_PARAM, Type.STRING, "",
						Importance.LOW, ParamConstants.STANDBY_URL_DOC)
				.define(ParamConstants.PERSISTENT_STATE_FILE_PARAM, Type.STRING, "",
						Importance.MEDIUM, ParamConstants.PERSISTENT_STATE_FILE_DOC)
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
				.define(ParamConstants.TABLE_LIST_STYLE_PARAM, Type.STRING,
						ParamConstants.TABLE_LIST_STYLE_STATIC,
						ConfigDef.ValidString.in(ParamConstants.TABLE_LIST_STYLE_STATIC,
								ParamConstants.TABLE_LIST_STYLE_DYNAMIC),
						Importance.LOW, ParamConstants.TABLE_LIST_STYLE_DOC)
				.define(ParamConstants.PROCESS_LOBS_PARAM, Type.BOOLEAN, false,
						Importance.LOW, ParamConstants.PROCESS_LOBS_DOC)
				.define(ParamConstants.CONNECTION_BACKOFF_PARAM, Type.INT, ParamConstants.CONNECTION_BACKOFF_DEFAULT,
						Importance.LOW, ParamConstants.CONNECTION_BACKOFF_DOC)
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
				.define(ParamConstants.LOB_TRANSFORM_CLASS_PARAM, Type.STRING, ParamConstants.LOB_TRANSFORM_CLASS_DEFAULT,
						Importance.LOW, ParamConstants.LOB_TRANSFORM_CLASS_DOC)
				.define(ParamConstants.RESILIENCY_TYPE_PARAM, Type.STRING,
						ParamConstants.RESILIENCY_TYPE_FAULT_TOLERANT,
						ConfigDef.ValidString.in(ParamConstants.RESILIENCY_TYPE_LEGACY,
								ParamConstants.RESILIENCY_TYPE_FAULT_TOLERANT),
						Importance.LOW, ParamConstants.RESILIENCY_TYPE_DOC)
				.define(ParamConstants.USE_RAC_PARAM, Type.BOOLEAN, false,
						Importance.LOW, ParamConstants.USE_RAC_DOC)
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
				.define(ParamConstants.LM_RECONNECT_INTERVAL_MS_PARAM, Type.LONG, Long.MAX_VALUE,
						Importance.LOW, ParamConstants.LM_RECONNECT_INTERVAL_MS_DOC)
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
				.define(ParamConstants.INTERNAL_RAC_URLS_PARAM, Type.LIST, "",
						Importance.LOW, ParamConstants.INTERNAL_PARAMETER_DOC)
				.define(ParamConstants.INTERNAL_DG4RAC_THREAD_PARAM, Type.LIST, "",
						Importance.LOW, ParamConstants.INTERNAL_PARAMETER_DOC)
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
				;
	}

	public OraCdcSourceConnectorConfig(Map<?, ?> originals) {
		super(config(), originals);
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

	public int getSchemaType() {
		if (schemaType == -1) {
			switch (getString(ConnectorParams.SCHEMA_TYPE_PARAM)) {
			case ConnectorParams.SCHEMA_TYPE_KAFKA:
				schemaType = ConnectorParams.SCHEMA_TYPE_INT_KAFKA_STD;
				break;
			case ConnectorParams.SCHEMA_TYPE_SINGLE:
				schemaType = ConnectorParams.SCHEMA_TYPE_INT_SINGLE;
				break;
			case ConnectorParams.SCHEMA_TYPE_DEBEZIUM:
				schemaType = ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM;
				break;
			}
		}
		return schemaType;
	}

	public String getTopicOrPrefix() {
		if (getSchemaType() != ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
			return getString(ConnectorParams.TOPIC_PREFIX_PARAM);
		} else {
			return getString(ParamConstants.KAFKA_TOPIC_PARAM);
		}
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

}
