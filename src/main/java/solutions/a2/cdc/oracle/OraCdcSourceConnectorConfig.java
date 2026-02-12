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

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static solutions.a2.cdc.oracle.OraCdcParameters.INCOMPLETE_REDO_INT_ERROR;
import static solutions.a2.cdc.oracle.OraCdcParameters.INCOMPLETE_REDO_INT_SKIP;
import static solutions.a2.cdc.oracle.OraCdcParameters.INCOMPLETE_REDO_INT_RESTORE;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_INT_TABLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_INT_SCHEMA_TABLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_INT_PDB_SCHEMA_TABLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.PK_TYPE_INT_WELL_DEFINED;
import static solutions.a2.cdc.oracle.OraCdcParameters.PK_TYPE_INT_ANY_UNIQUE;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORACDC_SCHEMAS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORACDC_SCHEMAS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.INCOMPLETE_REDO_TOLERANCE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.INCOMPLETE_REDO_TOLERANCE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.INCOMPLETE_REDO_TOLERANCE_ERROR;
import static solutions.a2.cdc.oracle.OraCdcParameters.INCOMPLETE_REDO_TOLERANCE_SKIP;
import static solutions.a2.cdc.oracle.OraCdcParameters.INCOMPLETE_REDO_TOLERANCE_RESTORE;
import static solutions.a2.cdc.oracle.OraCdcParameters.PRINT_INVALID_HEX_WARNING_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.PRINT_INVALID_HEX_WARNING_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.PROCESS_ONLINE_REDO_LOGS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.PROCESS_ONLINE_REDO_LOGS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.CURRENT_SCN_QUERY_INTERVAL_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.CURRENT_SCN_QUERY_INTERVAL_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.CURRENT_SCN_QUERY_INTERVAL_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.PRINT_ALL_ONLINE_REDO_RANGES_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.PRINT_ALL_ONLINE_REDO_RANGES_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.PROTOBUF_SCHEMA_NAMING_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.PROTOBUF_SCHEMA_NAMING_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.INITIAL_LOAD_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.INITIAL_LOAD_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.INITIAL_LOAD_IGNORE;
import static solutions.a2.cdc.oracle.OraCdcParameters.INITIAL_LOAD_EXECUTE;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_DELIMITER_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_DELIMITER_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_DELIMITER_UNDERSCORE;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_DELIMITER_DASH;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_DELIMITER_DOT;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_TABLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_SCHEMA_TABLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_NAME_STYLE_PDB_SCHEMA_TABLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.PK_TYPE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.PK_TYPE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.PK_TYPE_WELL_DEFINED;
import static solutions.a2.cdc.oracle.OraCdcParameters.PK_TYPE_ANY_UNIQUE;
import static solutions.a2.cdc.oracle.OraCdcParameters.USE_ROWID_AS_KEY_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.USE_ROWID_AS_KEY_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_MAPPER_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_MAPPER_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_MAPPER_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.STOP_ON_ORA_1284_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.STOP_ON_ORA_1284_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.STOP_ON_ORA_1284_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.PRINT_UNABLE_TO_DELETE_WARNING_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.PRINT_UNABLE_TO_DELETE_WARNING_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.PRINT_UNABLE_TO_DELETE_WARNING_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SCHEMANAME_MAPPER_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.SCHEMANAME_MAPPER_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SCHEMANAME_MAPPER_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_ROWSCN_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_ROWSCN_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_COMMITSCN_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_COMMITSCN_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_ROWTS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_ROWTS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_OPERATION_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_OPERATION_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_XID_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_XID_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_USERNAME_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_USERNAME_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_OSUSERNAME_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_OSUSERNAME_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_HOSTNAME_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_HOSTNAME_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_AUDIT_SESSIONID_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_AUDIT_SESSIONID_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_SESSION_INFO_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_SESSION_INFO_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_CLIENT_ID_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_CLIENT_ID_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.LAST_SEQ_NOTIFIER_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.LAST_SEQ_NOTIFIER_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.LAST_SEQ_NOTIFIER_FILE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.LAST_SEQ_NOTIFIER_FILE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.KEY_OVERRIDE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.KEY_OVERRIDE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ARCHIVED_LOG_CAT_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.ARCHIVED_LOG_CAT_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ARCHIVED_LOG_CAT_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.FETCH_SIZE_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.FETCH_SIZE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.FETCH_SIZE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TRACE_LOGMINER_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TRACE_LOGMINER_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.MAKE_DISTRIBUTED_ACTIVE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.MAKE_DISTRIBUTED_ACTIVE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.DISTRIBUTED_WALLET_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.DISTRIBUTED_WALLET_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.DISTRIBUTED_URL_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.DISTRIBUTED_URL_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.DISTRIBUTED_TARGET_HOST;
import static solutions.a2.cdc.oracle.OraCdcParameters.DISTRIBUTED_TARGET_HOST_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.DISTRIBUTED_TARGET_PORT;
import static solutions.a2.cdc.oracle.OraCdcParameters.DISTRIBUTED_TARGET_PORT_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.DISTRIBUTED_TARGET_PORT_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_TRANSACTION_IMPL_CHRONICLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_TRANSACTION_IMPL_JVM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_TRANSACTION_IMPL_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_TRANSACTION_IMPL_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ORA_TRANSACTION_IMPL_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.PROCESS_LOBS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.PROCESS_LOBS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.LOB_TRANSFORM_CLASS_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.LOB_TRANSFORM_CLASS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.LOB_TRANSFORM_CLASS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONNECTION_BACKOFF_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONNECTION_BACKOFF_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONNECTION_BACKOFF_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.USE_RAC_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.USE_RAC_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.MAKE_STANDBY_ACTIVE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.MAKE_STANDBY_ACTIVE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.STANDBY_URL_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.STANDBY_URL_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.STANDBY_WALLET_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.STANDBY_WALLET_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.STANDBY_PRIVILEGE_SYSDG;
import static solutions.a2.cdc.oracle.OraCdcParameters.STANDBY_PRIVILEGE_SYSBACKUP;
import static solutions.a2.cdc.oracle.OraCdcParameters.STANDBY_PRIVILEGE_SYSDBA;
import static solutions.a2.cdc.oracle.OraCdcParameters.STANDBY_PRIVILEGE_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.STANDBY_PRIVILEGE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.STANDBY_PRIVILEGE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_PARTITION_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_PARTITION_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TEMP_DIR_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TEMP_DIR_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.FIRST_CHANGE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.FIRST_CHANGE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.INTERNAL_PARAMETER_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.INTERNAL_RAC_URLS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.INTERNAL_DG4RAC_THREAD_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TABLE_LIST_STYLE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TABLE_LIST_STYLE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TABLE_LIST_STYLE_STATIC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TABLE_LIST_STYLE_DYNAMIC;
import static solutions.a2.cdc.oracle.OraCdcParameters.LM_RECONNECT_INTERVAL_MS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.LM_RECONNECT_INTERVAL_MS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONC_TRANSACTIONS_THRESHOLD_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONC_TRANSACTIONS_THRESHOLD_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDUCE_LOAD_MS_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDUCE_LOAD_MS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDUCE_LOAD_MS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.AL_CAPACITY_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.AL_CAPACITY_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.AL_CAPACITY_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.IGNORE_STORED_OFFSET_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.IGNORE_STORED_OFFSET_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.IGNORE_STORED_OFFSET_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.NUMBER_MAP_PREFIX;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_NAME_CONVERT_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_NAME_CONVERT_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_MEDIUM_FS;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_MEDIUM_ASM;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_MEDIUM_SSH;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_MEDIUM_SMB;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_MEDIUM_BFILE;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_MEDIUM_TRANSFER;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_MEDIUM_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.REDO_FILE_MEDIUM_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_JDBC_URL_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_JDBC_URL_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_USER_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_USER_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_PASSWORD_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_PASSWORD_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_READ_AHEAD_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_READ_AHEAD_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_READ_AHEAD_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_RECONNECT_INTERVAL_MS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_RECONNECT_INTERVAL_MS_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_RECONNECT_INTERVAL_MS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_PRIVILEGE_SYSASM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_PRIVILEGE_SYSDBA;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_PRIVILEGE_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_PRIVILEGE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ASM_PRIVILEGE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_HOST_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_HOST_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_PORT_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_PORT_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_PORT_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_USER_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_USER_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_KEY_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_KEY_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_PASSWORD_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_PASSWORD_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_RECONNECT_INTERVAL_MS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_RECONNECT_INTERVAL_MS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_RECONNECT_INTERVAL_MS_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_STRICT_HOST_KEY_CHECKING_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_STRICT_HOST_KEY_CHECKING_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_PROVIDER_MAVERICK;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_PROVIDER_SSHJ;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_PROVIDER_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_PROVIDER_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_PROVIDER_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_UNCONFIRMED_READS_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_UNCONFIRMED_READS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_UNCONFIRMED_READS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_BUFFER_SIZE_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_BUFFER_SIZE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SSH_BUFFER_SIZE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_SERVER_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_SERVER_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_SHARE_ONLINE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_SHARE_ONLINE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_SHARE_ARCHIVE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_SHARE_ARCHIVE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_USER_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_USER_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_PASSWORD_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_PASSWORD_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_DOMAIN_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_DOMAIN_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_TIMEOUT_MS_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_TIMEOUT_MS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_TIMEOUT_MS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_SOCKET_TIMEOUT_MS_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_SOCKET_TIMEOUT_MS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_SOCKET_TIMEOUT_MS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_RECONNECT_INTERVAL_MS_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_RECONNECT_INTERVAL_MS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_RECONNECT_INTERVAL_MS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_BUFFER_SIZE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_BUFFER_SIZE_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.SMB_BUFFER_SIZE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.BFILE_DIR_ONLINE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.BFILE_DIR_ONLINE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.BFILE_DIR_ARCHIVE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.BFILE_DIR_ARCHIVE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.BFILE_RECONNECT_INTERVAL_MS_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.BFILE_RECONNECT_INTERVAL_MS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.BFILE_RECONNECT_INTERVAL_MS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.BFILE_BUFFER_SIZE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.BFILE_BUFFER_SIZE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.BFILE_BUFFER_SIZE_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.TDE_WALLET_PATH_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TDE_WALLET_PATH_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TDE_WALLET_PASSWORD_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TDE_WALLET_PASSWORD_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.ALL_UPDATES_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.ALL_UPDATES_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.ALL_UPDATES_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.PRINT_UNABLE2MAP_COL_ID_WARNING_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.PRINT_UNABLE2MAP_COL_ID_WARNING_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.PRINT_UNABLE2MAP_COL_ID_WARNING_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SUPPLEMENTAL_LOGGING_ALL;
import static solutions.a2.cdc.oracle.OraCdcParameters.SUPPLEMENTAL_LOGGING_NONE;
import static solutions.a2.cdc.oracle.OraCdcParameters.SUPPLEMENTAL_LOGGING_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.SUPPLEMENTAL_LOGGING_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SUPPLEMENTAL_LOGGING_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.STOP_ON_MISSED_LOG_FILE_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.STOP_ON_MISSED_LOG_FILE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.STOP_ON_MISSED_LOG_FILE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TABLES_IN_PROCESS_SIZE_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.TABLES_IN_PROCESS_SIZE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TABLES_IN_PROCESS_SIZE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TABLES_OUT_OF_SCOPE_SIZE_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.TABLES_OUT_OF_SCOPE_SIZE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TABLES_OUT_OF_SCOPE_SIZE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TRANS_IN_PROCESS_SIZE_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.TRANS_IN_PROCESS_SIZE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TRANS_IN_PROCESS_SIZE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.EMITTER_TIMEOUT_MS_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.EMITTER_TIMEOUT_MS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.EMITTER_TIMEOUT_MS_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_FULL_INT;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_FULL;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_HALF_INT;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_HALF;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_QUARTER_INT;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_QUARTER;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_HALFQUARTER_INT;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_HALFQUARTER;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.OFFHEAP_SIZE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TRANSFER_DIR_STAGE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TRANSFER_DIR_STAGE_DOC;

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
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import solutions.a2.cdc.oracle.utils.KafkaUtils;
import solutions.a2.kafka.KafkaSourceBaseConfig;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcSourceConnectorConfig extends KafkaSourceBaseConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcSourceConnectorConfig.class);


	private Map<String, OraCdcKeyOverrideTypes> keyOverrideMap = null;
	private Map<String, String> keyOverrideIndexMap = null;
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

	private boolean fileNameConversionInited = false;
	private boolean fileNameConversion = false;
	private Map<String, String> fileNameConversionMap;
	private boolean logMiner = true;
	private boolean msWindows = false;
	private String fileSeparator = File.separator;

	public static ConfigDef config() {
		return KafkaSourceBaseConfig.config()
				.define(TOPIC_PARTITION_PARAM, INT, 0, MEDIUM, TOPIC_PARTITION_DOC)
				.define(FIRST_CHANGE_PARAM, STRING, "0", MEDIUM, FIRST_CHANGE_DOC)
				.define(TEMP_DIR_PARAM, STRING, System.getProperty("java.io.tmpdir"), HIGH, TEMP_DIR_DOC)
				.define(MAKE_STANDBY_ACTIVE_PARAM, BOOLEAN, false, LOW, MAKE_STANDBY_ACTIVE_DOC)
				.define(STANDBY_WALLET_PARAM, STRING, "", LOW, STANDBY_WALLET_DOC)
				.define(STANDBY_URL_PARAM, STRING, "", LOW, STANDBY_URL_DOC)
				.define(STANDBY_PRIVILEGE_PARAM, STRING, STANDBY_PRIVILEGE_DEFAULT,
						ConfigDef.ValidString.in(
								STANDBY_PRIVILEGE_SYSDG,
								STANDBY_PRIVILEGE_SYSBACKUP,
								STANDBY_PRIVILEGE_SYSDBA),
						HIGH, STANDBY_PRIVILEGE_DOC)
				.define(ORACDC_SCHEMAS_PARAM, BOOLEAN, false, LOW, ORACDC_SCHEMAS_DOC)
				.define(INITIAL_LOAD_PARAM, STRING, INITIAL_LOAD_IGNORE,
						ConfigDef.ValidString.in(
								INITIAL_LOAD_IGNORE,
								INITIAL_LOAD_EXECUTE),
						LOW, INITIAL_LOAD_DOC)
				.define(TOPIC_NAME_STYLE_PARAM, STRING, TOPIC_NAME_STYLE_TABLE,
						ConfigDef.ValidString.in(
								TOPIC_NAME_STYLE_TABLE,
								TOPIC_NAME_STYLE_SCHEMA_TABLE,
								TOPIC_NAME_STYLE_PDB_SCHEMA_TABLE),
						LOW, TOPIC_NAME_STYLE_DOC)
				.define(TOPIC_NAME_DELIMITER_PARAM, STRING, TOPIC_NAME_DELIMITER_UNDERSCORE,
						ConfigDef.ValidString.in(
								TOPIC_NAME_DELIMITER_UNDERSCORE,
								TOPIC_NAME_DELIMITER_DASH,
								TOPIC_NAME_DELIMITER_DOT),
						LOW, TOPIC_NAME_DELIMITER_DOC)
				.define(TABLE_LIST_STYLE_PARAM, STRING, TABLE_LIST_STYLE_STATIC,
						ConfigDef.ValidString.in(
								TABLE_LIST_STYLE_STATIC,
								TABLE_LIST_STYLE_DYNAMIC),
						LOW, TABLE_LIST_STYLE_DOC)
				.define(PROCESS_LOBS_PARAM, BOOLEAN, false, LOW, PROCESS_LOBS_DOC)
				.define(CONNECTION_BACKOFF_PARAM, INT, CONNECTION_BACKOFF_DEFAULT, LOW, CONNECTION_BACKOFF_DOC)
				.define(ARCHIVED_LOG_CAT_PARAM, STRING, ARCHIVED_LOG_CAT_DEFAULT, LOW, ARCHIVED_LOG_CAT_DOC)
				.define(FETCH_SIZE_PARAM, INT, FETCH_SIZE_DEFAULT, LOW, FETCH_SIZE_DOC)
				.define(TRACE_LOGMINER_PARAM, BOOLEAN, false, LOW, TRACE_LOGMINER_DOC)
				.define(MAKE_DISTRIBUTED_ACTIVE_PARAM, BOOLEAN, false, LOW, MAKE_DISTRIBUTED_ACTIVE_DOC)
				.define(DISTRIBUTED_WALLET_PARAM, STRING, "", LOW, DISTRIBUTED_WALLET_DOC)
				.define(DISTRIBUTED_URL_PARAM, STRING, "", LOW, DISTRIBUTED_URL_DOC)
				.define(DISTRIBUTED_TARGET_HOST, STRING, "", LOW, DISTRIBUTED_TARGET_HOST_DOC)
				.define(DISTRIBUTED_TARGET_PORT, INT, DISTRIBUTED_TARGET_PORT_DEFAULT, LOW, DISTRIBUTED_TARGET_PORT_DOC)
				.define(LOB_TRANSFORM_CLASS_PARAM, STRING, LOB_TRANSFORM_CLASS_DEFAULT, LOW, LOB_TRANSFORM_CLASS_DOC)
				.define(USE_RAC_PARAM, BOOLEAN, false, LOW, USE_RAC_DOC)
				.define(PROTOBUF_SCHEMA_NAMING_PARAM, BOOLEAN, false, LOW, PROTOBUF_SCHEMA_NAMING_DOC)
				.define(ORA_TRANSACTION_IMPL_PARAM, STRING, ORA_TRANSACTION_IMPL_DEFAULT,
						ConfigDef.ValidString.in(ORA_TRANSACTION_IMPL_CHRONICLE, ORA_TRANSACTION_IMPL_JVM),
						LOW, ORA_TRANSACTION_IMPL_DOC)
				.define(PRINT_INVALID_HEX_WARNING_PARAM, BOOLEAN, false, LOW, PRINT_INVALID_HEX_WARNING_DOC)
				.define(PROCESS_ONLINE_REDO_LOGS_PARAM, BOOLEAN, false, LOW, PROCESS_ONLINE_REDO_LOGS_DOC)
				.define(CURRENT_SCN_QUERY_INTERVAL_PARAM, INT, CURRENT_SCN_QUERY_INTERVAL_DEFAULT, LOW, CURRENT_SCN_QUERY_INTERVAL_DOC)
				.define(INCOMPLETE_REDO_TOLERANCE_PARAM, STRING, INCOMPLETE_REDO_TOLERANCE_ERROR,
						ConfigDef.ValidString.in(
								INCOMPLETE_REDO_TOLERANCE_ERROR,
								INCOMPLETE_REDO_TOLERANCE_SKIP,
								INCOMPLETE_REDO_TOLERANCE_RESTORE),
						LOW, INCOMPLETE_REDO_TOLERANCE_DOC)
				.define(PRINT_ALL_ONLINE_REDO_RANGES_PARAM, BOOLEAN, true, LOW, PRINT_ALL_ONLINE_REDO_RANGES_DOC)
				.define(LM_RECONNECT_INTERVAL_MS_PARAM, LONG, Long.MAX_VALUE, LOW, LM_RECONNECT_INTERVAL_MS_DOC)
				.define(PK_TYPE_PARAM, STRING, PK_TYPE_WELL_DEFINED,
						ConfigDef.ValidString.in(
								PK_TYPE_WELL_DEFINED,
								PK_TYPE_ANY_UNIQUE),
						MEDIUM, PK_TYPE_DOC)
				.define(USE_ROWID_AS_KEY_PARAM, BOOLEAN, true, MEDIUM, USE_ROWID_AS_KEY_DOC)
				.define(OraCdcParameters.USE_ALL_COLUMNS_ON_DELETE_PARAM, BOOLEAN, OraCdcParameters.USE_ALL_COLUMNS_ON_DELETE_DEFAULT, MEDIUM, OraCdcParameters.USE_ALL_COLUMNS_ON_DELETE_DOC)
				.define(INTERNAL_RAC_URLS_PARAM, LIST, "", LOW, INTERNAL_PARAMETER_DOC)
				.define(INTERNAL_DG4RAC_THREAD_PARAM, LIST, "", LOW, INTERNAL_PARAMETER_DOC)
				.define(TOPIC_MAPPER_PARAM, STRING, TOPIC_MAPPER_DEFAULT, LOW, TOPIC_MAPPER_DOC)
				.define(STOP_ON_ORA_1284_PARAM, BOOLEAN, STOP_ON_ORA_1284_DEFAULT, LOW, STOP_ON_ORA_1284_DOC)
				.define(PRINT_UNABLE_TO_DELETE_WARNING_PARAM, BOOLEAN, PRINT_UNABLE_TO_DELETE_WARNING_DEFAULT, LOW, PRINT_UNABLE_TO_DELETE_WARNING_DOC)
				.define(SCHEMANAME_MAPPER_PARAM, STRING, SCHEMANAME_MAPPER_DEFAULT, LOW, SCHEMANAME_MAPPER_DOC)
				.define(ORA_ROWSCN_PARAM, STRING, "", LOW, ORA_ROWSCN_DOC)
				.define(ORA_COMMITSCN_PARAM, STRING, "", LOW, ORA_COMMITSCN_DOC)
				.define(ORA_ROWTS_PARAM, STRING, "", LOW, ORA_ROWTS_DOC)
				.define(ORA_OPERATION_PARAM, STRING, "", LOW, ORA_OPERATION_DOC)
				.define(ORA_XID_PARAM, STRING, "", LOW, ORA_XID_DOC)
				.define(ORA_USERNAME_PARAM, STRING, "", LOW, ORA_USERNAME_DOC)
				.define(ORA_OSUSERNAME_PARAM, STRING, "", LOW, ORA_OSUSERNAME_DOC)
				.define(ORA_HOSTNAME_PARAM, STRING, "", LOW, ORA_HOSTNAME_DOC)
				.define(ORA_AUDIT_SESSIONID_PARAM, STRING, "", LOW, ORA_AUDIT_SESSIONID_DOC)
				.define(ORA_SESSION_INFO_PARAM, STRING, "", LOW, ORA_SESSION_INFO_DOC)
				.define(ORA_CLIENT_ID_PARAM, STRING, "", LOW, ORA_CLIENT_ID_DOC)
				.define(LAST_SEQ_NOTIFIER_PARAM, STRING, "", LOW, LAST_SEQ_NOTIFIER_DOC)
				.define(LAST_SEQ_NOTIFIER_FILE_PARAM, STRING, "", LOW, LAST_SEQ_NOTIFIER_FILE_DOC)
				.define(KEY_OVERRIDE_PARAM, LIST, "", MEDIUM, KEY_OVERRIDE_DOC)
				.define(CONC_TRANSACTIONS_THRESHOLD_PARAM, INT, 0, LOW, CONC_TRANSACTIONS_THRESHOLD_DOC)
				.define(REDUCE_LOAD_MS_PARAM, INT, REDUCE_LOAD_MS_DEFAULT, LOW, REDUCE_LOAD_MS_DOC)
				.define(AL_CAPACITY_PARAM, INT, AL_CAPACITY_DEFAULT, LOW, AL_CAPACITY_DOC)
				.define(IGNORE_STORED_OFFSET_PARAM, BOOLEAN, IGNORE_STORED_OFFSET_DEFAULT, LOW, IGNORE_STORED_OFFSET_DOC)
				// Redo Miner only!
				.define(REDO_FILE_NAME_CONVERT_PARAM, STRING, "", HIGH, REDO_FILE_NAME_CONVERT_DOC)
				.define(REDO_FILE_MEDIUM_PARAM, STRING, REDO_FILE_MEDIUM_FS,
						ConfigDef.ValidString.in(
								REDO_FILE_MEDIUM_FS,
								REDO_FILE_MEDIUM_ASM,
								REDO_FILE_MEDIUM_SSH,
								REDO_FILE_MEDIUM_SMB,
								REDO_FILE_MEDIUM_BFILE,
								REDO_FILE_MEDIUM_TRANSFER),
						HIGH, REDO_FILE_MEDIUM_DOC)
				.define(ASM_JDBC_URL_PARAM, STRING, "", LOW, ASM_JDBC_URL_DOC)
				.define(ASM_USER_PARAM, STRING, "", LOW, ASM_USER_DOC)
				.define(ASM_PASSWORD_PARAM, PASSWORD, "", LOW, ASM_PASSWORD_DOC)
				.define(ASM_READ_AHEAD_PARAM, BOOLEAN, ASM_READ_AHEAD_DEFAULT, LOW, ASM_READ_AHEAD_DOC)
				.define(ASM_RECONNECT_INTERVAL_MS_PARAM, LONG, ASM_RECONNECT_INTERVAL_MS_DEFAULT, LOW, ASM_RECONNECT_INTERVAL_MS_DOC)
				.define(ASM_PRIVILEGE_PARAM, STRING, ASM_PRIVILEGE_DEFAULT,
						ConfigDef.ValidString.in(
								ASM_PRIVILEGE_SYSASM,
								ASM_PRIVILEGE_SYSDBA),
						HIGH, ASM_PRIVILEGE_DOC)
				.define(SSH_HOST_PARAM, STRING, "", LOW, SSH_HOST_DOC)
				.define(SSH_PORT_PARAM, INT, SSH_PORT_DEFAULT, LOW, SSH_PORT_DOC)
				.define(SSH_USER_PARAM, STRING, "", LOW, SSH_USER_DOC)
				.define(SSH_KEY_PARAM, PASSWORD, "", LOW, SSH_KEY_DOC)
				.define(SSH_PASSWORD_PARAM, PASSWORD, "", LOW, SSH_PASSWORD_DOC)
				.define(SSH_RECONNECT_INTERVAL_MS_PARAM, LONG, SSH_RECONNECT_INTERVAL_MS_DEFAULT, LOW, SSH_RECONNECT_INTERVAL_MS_DOC)
				.define(SSH_STRICT_HOST_KEY_CHECKING_PARAM, BOOLEAN, false, MEDIUM, SSH_STRICT_HOST_KEY_CHECKING_DOC)
				.define(SSH_PROVIDER_PARAM, STRING, SSH_PROVIDER_DEFAULT,
						ConfigDef.ValidString.in(
								SSH_PROVIDER_MAVERICK,
								SSH_PROVIDER_SSHJ),
						LOW, SSH_PROVIDER_DOC)
				.define(SSH_UNCONFIRMED_READS_PARAM, INT, SSH_UNCONFIRMED_READS_DEFAULT, LOW, SSH_UNCONFIRMED_READS_DOC)
				.define(SSH_BUFFER_SIZE_PARAM, INT, SSH_BUFFER_SIZE_DEFAULT, LOW, SSH_BUFFER_SIZE_DOC)
				.define(SMB_SERVER_PARAM, STRING, "", LOW, SMB_SERVER_DOC)
				.define(SMB_SHARE_ONLINE_PARAM, STRING, "", LOW, SMB_SHARE_ONLINE_DOC)
				.define(SMB_SHARE_ARCHIVE_PARAM, STRING, "", LOW, SMB_SHARE_ARCHIVE_DOC)
				.define(SMB_USER_PARAM, STRING, "", LOW, SMB_USER_DOC)
				.define(SMB_PASSWORD_PARAM, PASSWORD, "", LOW, SMB_PASSWORD_DOC)
				.define(SMB_DOMAIN_PARAM, STRING, "", LOW, SMB_DOMAIN_DOC)
				.define(SMB_TIMEOUT_MS_PARAM, INT, SMB_TIMEOUT_MS_DEFAULT, LOW, SMB_TIMEOUT_MS_DOC)
				.define(SMB_SOCKET_TIMEOUT_MS_PARAM, INT, SMB_SOCKET_TIMEOUT_MS_DEFAULT, LOW, SMB_SOCKET_TIMEOUT_MS_DOC)
				.define(SMB_RECONNECT_INTERVAL_MS_PARAM, LONG, SMB_RECONNECT_INTERVAL_MS_DEFAULT, LOW, SMB_RECONNECT_INTERVAL_MS_DOC)
				.define(SMB_BUFFER_SIZE_PARAM, INT, SMB_BUFFER_SIZE_DEFAULT, LOW, SMB_BUFFER_SIZE_DOC)
				.define(BFILE_DIR_ONLINE_PARAM, STRING, "", LOW, BFILE_DIR_ONLINE_DOC)
				.define(BFILE_DIR_ARCHIVE_PARAM, STRING, "", LOW, BFILE_DIR_ARCHIVE_DOC)
				.define(BFILE_RECONNECT_INTERVAL_MS_PARAM, LONG, BFILE_RECONNECT_INTERVAL_MS_DEFAULT, LOW, BFILE_RECONNECT_INTERVAL_MS_DOC)
				.define(BFILE_BUFFER_SIZE_PARAM, INT, BFILE_BUFFER_SIZE_DEFAULT, LOW, BFILE_BUFFER_SIZE_DOC)
				.define(TDE_WALLET_PATH_PARAM, STRING, "", LOW, TDE_WALLET_PATH_DOC)
				.define(TDE_WALLET_PASSWORD_PARAM, PASSWORD, "", LOW, TDE_WALLET_PASSWORD_DOC)
				.define(ALL_UPDATES_PARAM, BOOLEAN, ALL_UPDATES_DEFAULT, LOW, ALL_UPDATES_DOC)
				.define(PRINT_UNABLE2MAP_COL_ID_WARNING_PARAM, BOOLEAN, PRINT_UNABLE2MAP_COL_ID_WARNING_DEFAULT, LOW, PRINT_UNABLE2MAP_COL_ID_WARNING_DOC)
				.define(SUPPLEMENTAL_LOGGING_PARAM, STRING, SUPPLEMENTAL_LOGGING_DEFAULT,
						ConfigDef.ValidString.in(
								SUPPLEMENTAL_LOGGING_ALL,
								SUPPLEMENTAL_LOGGING_NONE),
						HIGH, SUPPLEMENTAL_LOGGING_DOC)
				.define(STOP_ON_MISSED_LOG_FILE_PARAM, BOOLEAN, STOP_ON_MISSED_LOG_FILE_DEFAULT, HIGH, STOP_ON_MISSED_LOG_FILE_DOC)
				.define(TABLES_IN_PROCESS_SIZE_PARAM, INT, TABLES_IN_PROCESS_SIZE_DEFAULT, LOW, TABLES_IN_PROCESS_SIZE_DOC)
				.define(TABLES_OUT_OF_SCOPE_SIZE_PARAM, INT, TABLES_OUT_OF_SCOPE_SIZE_DEFAULT, LOW, TABLES_OUT_OF_SCOPE_SIZE_DOC)
				.define(TRANS_IN_PROCESS_SIZE_PARAM, INT, TRANS_IN_PROCESS_SIZE_DEFAULT, LOW, TRANS_IN_PROCESS_SIZE_DOC)
				.define(EMITTER_TIMEOUT_MS_PARAM, INT, EMITTER_TIMEOUT_MS_DEFAULT, LOW, EMITTER_TIMEOUT_MS_DOC)
				.define(OFFHEAP_SIZE_PARAM, STRING, OFFHEAP_SIZE_DEFAULT,
						ConfigDef.ValidString.in(
								OFFHEAP_SIZE_FULL, OFFHEAP_SIZE_HALF, OFFHEAP_SIZE_QUARTER, OFFHEAP_SIZE_HALFQUARTER),
						LOW, OFFHEAP_SIZE_DOC)
				.define(TRANSFER_DIR_STAGE_PARAM, STRING, "", LOW, TRANSFER_DIR_STAGE_DOC);
	}

	public OraCdcSourceConnectorConfig(Map<String, String> originals) {
		super(config(), originals);
		// parse numberColumnsMap
		Map<String, String> numberMapParams = originals.entrySet().stream()
				.filter(prop -> Strings.CS.startsWith(prop.getKey(), NUMBER_MAP_PREFIX))
				.collect(Collectors.toMap(
						prop -> Strings.CS.replace(prop.getKey(), NUMBER_MAP_PREFIX, ""),
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
				if (Strings.CS.endsWith(column, "%")) {
					numberColumnsMap.get(fqn).getLeft().add(Pair.of(
							StringUtils.substring(column, 0, column.length() - 1),
							new OraColumn(column, jdbcType, scale)));
				} else if (Strings.CS.startsWith(column, "%")) {
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
					Strings.CS.startsWith(columnName, pattern.getKey()))
				return pattern.getValue();
			else if (!startsWith &&
					Strings.CS.endsWith(columnName, pattern.getKey()))
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
		return getBoolean(OraCdcParameters.USE_ALL_COLUMNS_ON_DELETE_PARAM);
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
					if (Strings.CI.equals(overrideValue, "NOKEY")) {
						keyOverrideMap.put(fullTableName, OraCdcKeyOverrideTypes.NOKEY);
					} else if (Strings.CI.equals(overrideValue, "ROWID")) {
						keyOverrideMap.put(fullTableName, OraCdcKeyOverrideTypes.ROWID);
					} else if (Strings.CI.startsWith(overrideValue, "INDEX")) {
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

	public boolean activateStandby() {
		return getBoolean(MAKE_STANDBY_ACTIVE_PARAM);
	}

	public String standbyJdbcUrl() {
		return getString(STANDBY_URL_PARAM);
	}

	public String standbyWallet() {
		return getString(STANDBY_WALLET_PARAM);
	}

	public String standbyPrivilege() {
		return getString(STANDBY_PRIVILEGE_PARAM);
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
			if (topicPartition < 0) {
				LOGGER.error(
						"""
						
						=====================
						Invalid partition: '{}'! THREAD#={}. Partition number should always be non-negative.
						=====================
						
						""", topicPartition, redoThread);
				throw new IllegalArgumentException("Invalid partition: " + topicPartition);
			}
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
								"""
								
								=====================
								Parameter '{}' points to non-writable directory '{}'.
								=====================
								
								""",
								TEMP_DIR_PARAM, tempDir);
						throw new SQLException("Temp directory is not properly set!");
					}
				} else {
					try {
						Files.createDirectories(Paths.get(tempDir));
					} catch (IOException | UnsupportedOperationException | SecurityException  e) {
						LOGGER.error(
								"""
								
								=====================
								Unable to create directory! Parameter {} points to non-existent or invalid directory {}.
								=====================
								
								""",
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
		final String scnAsString = getString(FIRST_CHANGE_PARAM);
		try {
			return Long.parseUnsignedLong(scnAsString);
		} catch (NumberFormatException nfe) {
			LOGGER.error(
					"""
					
					=====================
					Unable to parse value '{}' of parameter '{}' as unsigned long!
					=====================
					
					""",
					scnAsString, FIRST_CHANGE_PARAM);
			throw new SQLException(nfe);
		}
	}

	public String startScnParam() {
		return FIRST_CHANGE_PARAM;
	}

	public boolean staticObjIds() {
		return Strings.CI.equals(
				TABLE_LIST_STYLE_STATIC, getString(TABLE_LIST_STYLE_PARAM));
	}

	public long logMinerReconnectIntervalMs() {
		return getLong(LM_RECONNECT_INTERVAL_MS_PARAM);
	}

	public int transactionsThreshold() {
		int threshold = getInt(CONC_TRANSACTIONS_THRESHOLD_PARAM);
		boolean isLinux = Strings.CI.contains(System.getProperty("os.name"), "nux");
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
						"""
						
						=====================
						Unable to read and parse value of vm.max_map_count from  '/proc/sys/vm/max_map_count'!
						Exception: {}
						{}
						=====================
						
						""",
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

	public boolean useOffHeapMemory() {
		return Strings.CI.equals(
				getString(ORA_TRANSACTION_IMPL_PARAM), ORA_TRANSACTION_IMPL_CHRONICLE);
	}

	public int fetchSize() {
		return getInt(FETCH_SIZE_PARAM);
	}

	public boolean logMinerTrace() {
		return getBoolean(TRACE_LOGMINER_PARAM);
	}

	public Class<?> classLogMiner() throws ClassNotFoundException {
		return Class.forName(getString(ARCHIVED_LOG_CAT_PARAM));
	}

	public String classLogMinerName() {
		return getString(ARCHIVED_LOG_CAT_PARAM);
	}

	public boolean activateDistributed() {
		return getBoolean(MAKE_DISTRIBUTED_ACTIVE_PARAM);
	}

	public String distributedUrl() {
		return getString(DISTRIBUTED_URL_PARAM);
	}

	public String distributedWallet() {
		return getString(DISTRIBUTED_WALLET_PARAM);
	}

	public String distributedTargetHost() {
		return getString(DISTRIBUTED_TARGET_HOST);
	}

	public int distributedTargetPort() {
		return getInt(DISTRIBUTED_TARGET_PORT);
	}

	public boolean logMiner() {
		return logMiner;
	}

	public void logMiner(final boolean logMiner) {
		this.logMiner = logMiner;
	}

	public void msWindows(final boolean msWindows) {
		this.msWindows = msWindows;
		this.fileSeparator = msWindows ? "\\" : File.separator;
	}

	public String convertRedoFileName(final String originalName, final boolean bfile) {
		if (bfile) {
			return StringUtils.substringAfterLast(originalName, fileSeparator);
		} else {
			if (!fileNameConversionInited) {
				final String fileNameConvertParam = getString(REDO_FILE_NAME_CONVERT_PARAM);
				if (StringUtils.isNotEmpty(fileNameConvertParam) &&
						StringUtils.contains(fileNameConvertParam, '=')) {
					String[] elements = StringUtils.split(fileNameConvertParam, ',');
					int newSize = 0;
					boolean[] processElement = new boolean[elements.length];
					for (int i = 0; i < elements.length; i++) {
						if (Strings.CS.contains(elements[i], "=")) {
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
										Strings.CS.appendIfMissing(
										StringUtils.trim(StringUtils.substringBefore(elements[i], "=")),
										fileSeparator),
										Strings.CS.appendIfMissing(
										StringUtils.trim(StringUtils.substringAfter(elements[i], "=")),
										fileSeparator));
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
				int maxPrefixSize = -1;
				String originalPrefix = null;
				for (final String prefix : fileNameConversionMap.keySet()) {
					if (Strings.CS.startsWith(originalName, prefix)) {
						if (prefix.length() > maxPrefixSize) {
							maxPrefixSize = prefix.length();
							originalPrefix = prefix;
						}
					}
				}
				if (maxPrefixSize == -1) {
					LOGGER.error(
							"""
							
							=====================
							Unable to convert filename '{}' using parameter {}={} !
							Original filename will be returned!
							=====================
							
							""",
							originalName, REDO_FILE_NAME_CONVERT_PARAM, getString(REDO_FILE_NAME_CONVERT_PARAM));
					return originalName;
				} else {
					final String replacementPrefix =  fileNameConversionMap.get(originalPrefix);
					if (msWindows)
						return  Strings.CS.replace(
								Strings.CS.replace(originalName, originalPrefix, replacementPrefix),
								"\\",
								"/");
					else
						return Strings.CS.replace(originalName, originalPrefix, replacementPrefix);
				}
			} else {
				return originalName;
			}
		}
	}

	public boolean useAsm() {
		return Strings.CS.equals(getString(REDO_FILE_MEDIUM_PARAM), REDO_FILE_MEDIUM_ASM);
	}

	public boolean useSsh() {
		return Strings.CS.equals(getString(REDO_FILE_MEDIUM_PARAM), REDO_FILE_MEDIUM_SSH);
	}

	public boolean useSmb() {
		return Strings.CS.equals(getString(REDO_FILE_MEDIUM_PARAM), REDO_FILE_MEDIUM_SMB);
	}

	public boolean useBfile() {
		return Strings.CS.equals(getString(REDO_FILE_MEDIUM_PARAM), REDO_FILE_MEDIUM_BFILE);
	}

	public String asmJdbcUrl() {
		return getString(ASM_JDBC_URL_PARAM);
	}

	public String asmUser() {
		return getString(ASM_USER_PARAM);
	}

	public String asmPassword() {	
		return getPassword(ASM_PASSWORD_PARAM).value();
	}

	public boolean asmReadAhead() {	
		return getBoolean(ASM_READ_AHEAD_PARAM);
	}

	public long asmReconnectIntervalMs() {
		return getLong(ASM_RECONNECT_INTERVAL_MS_PARAM);
	}

	public String asmPrivilege() {
		return getString(ASM_PRIVILEGE_PARAM);
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
		return getPassword(SSH_KEY_PARAM).value();
	}

	public String sshPassword() {
		return getPassword(SSH_PASSWORD_PARAM).value();
	}

	public long sshReconnectIntervalMs() {
		return getLong(SSH_RECONNECT_INTERVAL_MS_PARAM);
	}

	public boolean sshStrictHostKeyChecking() {
		return getBoolean(SSH_STRICT_HOST_KEY_CHECKING_PARAM);
	}

	public boolean sshProviderMaverick() {
		return Strings.CS.equals(getString(SSH_PROVIDER_PARAM), SSH_PROVIDER_MAVERICK);
	}

	public int sshUnconfirmedReads() {
		return getInt(SSH_UNCONFIRMED_READS_PARAM);
	}

	public int sshBufferSize() {
		return getInt(SSH_BUFFER_SIZE_PARAM);
	}

	public String smbServer() {
		return getString(SMB_SERVER_PARAM);
	}

	public String smbShareOnline() {
		return getString(SMB_SHARE_ONLINE_PARAM);
	}

	public String smbShareArchive() {
		return getString(SMB_SHARE_ARCHIVE_PARAM);
	}

	public String smbUser() {
		return getString(SMB_USER_PARAM);
	}

	public String smbPassword() {
		return getPassword(SMB_PASSWORD_PARAM).value();
	}

	public String smbDomain() {
		return getString(SMB_DOMAIN_PARAM);
	}

	public int smbTimeoutMs() {
		return getInt(SMB_TIMEOUT_MS_PARAM);
	}

	public int smbSocketTimeoutMs() {
		return getInt(SMB_SOCKET_TIMEOUT_MS_PARAM);
	}

	public long smbReconnectIntervalMs() {
		return getLong(SMB_RECONNECT_INTERVAL_MS_PARAM);
	}

	public int smbBufferSize() {
		return getInt(SMB_BUFFER_SIZE_PARAM);
	}

	public String bfileDirOnline() {
		return getString(BFILE_DIR_ONLINE_PARAM);
	}

	public String bfileDirArchive() {
		return getString(BFILE_DIR_ARCHIVE_PARAM);
	}

	public long bfileReconnectIntervalMs() {
		return getLong(BFILE_RECONNECT_INTERVAL_MS_PARAM);
	}

	public int bfileBufferSize() {
		return getInt(BFILE_BUFFER_SIZE_PARAM);
	}

	public String tdeWallet() {
		return getString(TDE_WALLET_PATH_PARAM);
	}

	public String tdePassword() {
		return getPassword(TDE_WALLET_PASSWORD_PARAM).value();
	}

	public boolean allUpdates() {
		return getBoolean(ALL_UPDATES_PARAM);
	}

	public boolean processOnlineRedoLogs() {
		return getBoolean(PROCESS_ONLINE_REDO_LOGS_PARAM);
	}

	public int currentScnQueryInterval() {
		return getInt(CURRENT_SCN_QUERY_INTERVAL_PARAM);
	}

	public boolean printAllOnlineRedoRanges() {
		return getBoolean(PRINT_ALL_ONLINE_REDO_RANGES_PARAM);
	}

	public boolean printUnable2MapColIdWarning() {
		return getBoolean(PRINT_UNABLE2MAP_COL_ID_WARNING_PARAM);
	}

	public String initialLoad() {
		return getString(INITIAL_LOAD_PARAM);
	}

	public boolean ignoreStoredOffset() {
		return getBoolean(IGNORE_STORED_OFFSET_PARAM);
	}

	public boolean supplementalLogAll() {
		return Strings.CI.equals(getString(SUPPLEMENTAL_LOGGING_PARAM), SUPPLEMENTAL_LOGGING_ALL);
	}

	public boolean stopOnMissedLogFile() {
		return getBoolean(STOP_ON_MISSED_LOG_FILE_PARAM);
	}

	public int tablesInProcessSize() {
		return getInt(TABLES_IN_PROCESS_SIZE_PARAM);
	}

	public int tablesOutOfScopeSize() {
		return getInt(TABLES_OUT_OF_SCOPE_SIZE_PARAM);
	}

	public int transactionsInProcessSize() {
		return getInt(TRANS_IN_PROCESS_SIZE_PARAM);
	}

	public int emitterTimeoutMs() {
		return getInt(EMITTER_TIMEOUT_MS_PARAM);
	}

	public int[] offHeapSize() {
		if (logMiner)
			return OFFHEAP_SIZE_FULL_INT;
		else
			switch (getString(OFFHEAP_SIZE_PARAM)) {
				case OFFHEAP_SIZE_FULL: return OFFHEAP_SIZE_FULL_INT;
				case OFFHEAP_SIZE_HALF: return OFFHEAP_SIZE_HALF_INT;
				case OFFHEAP_SIZE_QUARTER: return OFFHEAP_SIZE_QUARTER_INT;
				default: return OFFHEAP_SIZE_HALFQUARTER_INT;
			}
	}

	public boolean useFileTransfer() {
		return Strings.CS.equals(getString(REDO_FILE_MEDIUM_PARAM), REDO_FILE_MEDIUM_TRANSFER);
	}

	public String fileTransferStageDir() {
		return getString(TRANSFER_DIR_STAGE_PARAM);
	}

}
