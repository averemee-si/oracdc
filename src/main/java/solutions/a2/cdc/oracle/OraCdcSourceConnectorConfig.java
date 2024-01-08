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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcSourceConnectorConfig extends AbstractConfig {

	public static final int INCOMPLETE_REDO_INT_ERROR = 1;
	public static final int INCOMPLETE_REDO_INT_SKIP = 2;
	public static final int INCOMPLETE_REDO_INT_RESTORE = 3;

	public static final int TOPIC_NAME_STYLE_INT_TABLE = 1;
	public static final int TOPIC_NAME_STYLE_INT_SCHEMA_TABLE = 2;
	public static final int TOPIC_NAME_STYLE_INT_PDB_SCHEMA_TABLE = 3;

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

	static final String TOPIC_PREFIX_PARAM = "a2.topic.prefix";
	private static final String TOPIC_PREFIX_DOC = "Prefix to prepend table names to generate name of Kafka topic.";

	private int incompleteDataTolerance = -1;
	private int topicNameStyle = -1;
	private int schemaType = -1;

	public static ConfigDef config() {
		return new ConfigDef()
				.define(ParamConstants.CONNECTION_URL_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.CONNECTION_URL_DOC)
				.define(ParamConstants.CONNECTION_USER_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.CONNECTION_USER_DOC)
				.define(ParamConstants.CONNECTION_PASSWORD_PARAM, Type.PASSWORD, "",
						Importance.HIGH, ParamConstants.CONNECTION_PASSWORD_DOC)
				.define(ParamConstants.CONNECTION_WALLET_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.CONNECTION_WALLET_DOC)
				.define(ParamConstants.KAFKA_TOPIC_PARAM, Type.STRING, ParamConstants.KAFKA_TOPIC_PARAM_DEFAULT,
						Importance.HIGH, ParamConstants.KAFKA_TOPIC_PARAM_DOC)
				.define(ParamConstants.POLL_INTERVAL_MS_PARAM, Type.INT, ParamConstants.POLL_INTERVAL_MS_DEFAULT,
						Importance.LOW, ParamConstants.POLL_INTERVAL_MS_DOC)
				.define(ParamConstants.BATCH_SIZE_PARAM, Type.INT,
						ParamConstants.BATCH_SIZE_DEFAULT,
						Importance.LOW, ParamConstants.BATCH_SIZE_DOC)
				.define(ParamConstants.SCHEMA_TYPE_PARAM, Type.STRING,
						ParamConstants.SCHEMA_TYPE_KAFKA,
						ConfigDef.ValidString.in(
								ParamConstants.SCHEMA_TYPE_KAFKA,
								ParamConstants.SCHEMA_TYPE_SINGLE,
								ParamConstants.SCHEMA_TYPE_DEBEZIUM),
						Importance.LOW, ParamConstants.SCHEMA_TYPE_DOC)
				.define(TOPIC_PREFIX_PARAM, Type.STRING, "",
						Importance.MEDIUM, TOPIC_PREFIX_DOC)
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
				.define(ParamConstants.INTERNAL_RAC_URLS_PARAM, Type.LIST, "",
						Importance.LOW, ParamConstants.INTERNAL_PARAMETER_DOC)
				.define(ParamConstants.INTERNAL_DG4RAC_THREAD_PARAM, Type.LIST, "",
						Importance.LOW, ParamConstants.INTERNAL_PARAMETER_DOC)
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
			switch (getString(ParamConstants.SCHEMA_TYPE_PARAM)) {
			case ParamConstants.SCHEMA_TYPE_KAFKA:
				schemaType = ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD;
				break;
			case ParamConstants.SCHEMA_TYPE_SINGLE:
				schemaType = ParamConstants.SCHEMA_TYPE_INT_SINGLE;
				break;
			case ParamConstants.SCHEMA_TYPE_DEBEZIUM:
				schemaType = ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM;
				break;
			}
		}
		return schemaType;
	}

	public String getTopicOrPrefix() {
		if (getSchemaType() != ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
			return getString(TOPIC_PREFIX_PARAM);
		} else {
			return getString(ParamConstants.KAFKA_TOPIC_PARAM);
		}
	}

}
