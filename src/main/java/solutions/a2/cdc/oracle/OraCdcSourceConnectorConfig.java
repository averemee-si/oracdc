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

	public static final String TASK_PARAM_MASTER = "master";
	public static final String TASK_PARAM_MV_LOG = "mv.log";
	public static final String TASK_PARAM_OWNER = "owner";
	public static final String TASK_PARAM_SCHEMA_TYPE = "schema.type";
	public static final String TASK_PARAM_MV_ROWID = "mvlog.rowid";
	public static final String TASK_PARAM_MV_PK = "mvlog.pk";
	public static final String TASK_PARAM_MV_SEQUENCE = "mvlog.seq";

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
				.define(ParamConstants.TOPIC_PREFIX_PARAM, Type.STRING, "",
						Importance.MEDIUM, ParamConstants.TOPIC_PREFIX_DOC)
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
				.define(ParamConstants.ORACDC_SCHEMAS_PARAM, Type.BOOLEAN, false,
						Importance.LOW, ParamConstants.ORACDC_SCHEMAS_DOC)
				.define(ParamConstants.DICTIONARY_FILE_PARAM, Type.STRING, "",
						Importance.LOW, ParamConstants.DICTIONARY_FILE_DOC)
				.define(ParamConstants.INITIAL_LOAD_PARAM, Type.STRING,
						ParamConstants.INITIAL_LOAD_IGNORE,
						ConfigDef.ValidString.in(ParamConstants.INITIAL_LOAD_IGNORE,
								ParamConstants.INITIAL_LOAD_EXECUTE),
						Importance.LOW, ParamConstants.INITIAL_LOAD_DOC)
				.define(ParamConstants.TOPIC_NAME_STYLE_PARAM, Type.STRING,
						ParamConstants.TOPIC_NAME_STYLE_TABLE,
						ConfigDef.ValidString.in(ParamConstants.TOPIC_NAME_STYLE_TABLE,
								ParamConstants.TOPIC_NAME_STYLE_SCHEMA_TABLE,
								ParamConstants.TOPIC_NAME_STYLE_PDB_SCHEMA_TABLE),
						Importance.LOW, ParamConstants.TOPIC_NAME_STYLE_DOC)
				.define(ParamConstants.TOPIC_NAME_DELIMITER_PARAM, Type.STRING,
						ParamConstants.TOPIC_NAME_DELIMITER_UNDERSCORE,
						ConfigDef.ValidString.in(ParamConstants.TOPIC_NAME_DELIMITER_UNDERSCORE,
								ParamConstants.TOPIC_NAME_DELIMITER_DASH,
								ParamConstants.TOPIC_NAME_DELIMITER_DOT),
						Importance.LOW, ParamConstants.TOPIC_NAME_DELIMITER_DOC)
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
				.define(ParamConstants.PROTOBUF_SCHEMA_NAMING_PARAM, Type.BOOLEAN, false,
						Importance.LOW, ParamConstants.PROTOBUF_SCHEMA_NAMING_DOC)
				.define(ParamConstants.ORA_TRANSACTION_IMPL_PARAM, Type.STRING,
						ParamConstants.ORA_TRANSACTION_IMPL_CHRONICLE,
						ConfigDef.ValidString.in(ParamConstants.ORA_TRANSACTION_IMPL_CHRONICLE,
								ParamConstants.ORA_TRANSACTION_IMPL_JVM),
						Importance.LOW, ParamConstants.ORA_TRANSACTION_IMPL_DOC)
				.define(ParamConstants.PRINT_INVALID_HEX_WARNING_PARAM, Type.BOOLEAN, false,
						Importance.LOW, ParamConstants.PRINT_INVALID_HEX_WARNING_DOC)
				.define(ParamConstants.PROCESS_ONLINE_REDO_LOGS_PARAM, Type.BOOLEAN, false,
						Importance.LOW, ParamConstants.PROCESS_ONLINE_REDO_LOGS_DOC)
				.define(ParamConstants.CURRENT_SCN_QUERY_INTERVAL_PARAM, Type.INT, ParamConstants.CURRENT_SCN_QUERY_INTERVAL_DEFAULT,
						Importance.LOW, ParamConstants.CURRENT_SCN_QUERY_INTERVAL_DOC)
				.define(ParamConstants.INCOMPLETE_REDO_TOLERANCE_PARAM, Type.STRING,
						ParamConstants.INCOMPLETE_REDO_TOLERANCE_ERROR,
						ConfigDef.ValidString.in(
								ParamConstants.INCOMPLETE_REDO_TOLERANCE_ERROR,
								ParamConstants.INCOMPLETE_REDO_TOLERANCE_SKIP,
								ParamConstants.INCOMPLETE_REDO_TOLERANCE_RESTORE),
						Importance.LOW, ParamConstants.INCOMPLETE_REDO_TOLERANCE_DOC)
				.define(ParamConstants.PRINT_ALL_ONLINE_REDO_RANGES_PARAM, Type.BOOLEAN, true,
						Importance.LOW, ParamConstants.PRINT_ALL_ONLINE_REDO_RANGES_DOC)
				.define(ParamConstants.INTERNAL_RAC_URLS_PARAM, Type.LIST, "",
						Importance.LOW, ParamConstants.INTERNAL_PARAMETER_DOC)
				.define(ParamConstants.INTERNAL_DG4RAC_THREAD_PARAM, Type.LIST, "",
						Importance.LOW, ParamConstants.INTERNAL_PARAMETER_DOC)
				;
	}

	public OraCdcSourceConnectorConfig(Map<?, ?> originals) {
		super(config(), originals);
	}

}
