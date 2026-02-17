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

package solutions.a2.cdc.oracle.runtime.config;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.PASSWORD;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static solutions.a2.cdc.oracle.OraCdcParameters.*;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
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

import solutions.a2.cdc.oracle.LastProcessedSeqNotifier;
import solutions.a2.cdc.oracle.OraCdcKeyOverrideTypes;
import solutions.a2.cdc.oracle.OraCdcPseudoColumnsProcessor;
import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.cdc.oracle.OraColumn;
import solutions.a2.cdc.oracle.SchemaNameMapper;
import solutions.a2.cdc.oracle.TopicNameMapper;
import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import solutions.a2.cdc.oracle.utils.KafkaUtils;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class KafkaSourceConnectorConfig extends KafkaSourceBaseConfig implements OraCdcSourceConnectorConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSourceConnectorConfig.class);

	private final SourceConnectorConfig holder;
	private String connectorName;
	private OraCdcPseudoColumnsProcessor pseudoColumns = null;
	private int topicPartition = 0;

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
				.define(USE_ALL_COLUMNS_ON_DELETE_PARAM, BOOLEAN, USE_ALL_COLUMNS_ON_DELETE_DEFAULT, MEDIUM, USE_ALL_COLUMNS_ON_DELETE_DOC)
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

	public KafkaSourceConnectorConfig(Map<String, String> originals) {
		super(config(), originals);
		// parse numberColumnsMap
		Map<String, String> numberMapParams = originals.entrySet().stream()
				.filter(prop -> Strings.CS.startsWith(prop.getKey(), NUMBER_MAP_PREFIX))
				.collect(Collectors.toMap(
						prop -> Strings.CS.replace(prop.getKey(), NUMBER_MAP_PREFIX, ""),
						Map.Entry::getValue));
		holder = new SourceConnectorConfig(new ParamsRecord(
				numberMapParams,
				getString(INCOMPLETE_REDO_TOLERANCE_PARAM),
				getString(TOPIC_NAME_STYLE_PARAM),
				getString(PK_TYPE_PARAM),
				getString(LAST_SEQ_NOTIFIER_PARAM),
				getList(KEY_OVERRIDE_PARAM),
				getString(LOB_TRANSFORM_CLASS_PARAM),
				getString(TEMP_DIR_PARAM),
				getString(FIRST_CHANGE_PARAM),
				getString(TABLE_LIST_STYLE_PARAM),
				getInt(CONC_TRANSACTIONS_THRESHOLD_PARAM),
				getString(ORA_TRANSACTION_IMPL_PARAM),
				getString(REDO_FILE_NAME_CONVERT_PARAM),
				getString(REDO_FILE_MEDIUM_PARAM),
				getString(SSH_PROVIDER_PARAM),
				getString(SUPPLEMENTAL_LOGGING_PARAM),
				getString(OFFHEAP_SIZE_PARAM)));
	}

	@Override
	public List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>>
			tableNumberMapping(final String tableOwner, final String tableName) {
		return holder.tableNumberMapping(null, tableOwner, tableName);
	}

	@Override
	public List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>>
			tableNumberMapping(final String pdbName, final String tableOwner, final String tableName) {
		return holder.tableNumberMapping(null, tableOwner, tableName);
	}

	@Override
	public OraColumn columnNumberMapping(
			List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>>
				numberRemap, final String columnName) {
		return holder.columnNumberMapping(numberRemap, columnName);
	}

	@Override
	public boolean useOracdcSchemas() {
		return getBoolean(ORACDC_SCHEMAS_PARAM);
	}

	@Override
	public int getIncompleteDataTolerance() {
		return holder.incompleteDataTolerance();
	}

	@Override
	public boolean isPrintInvalidHexValueWarning() {
		return getBoolean(PRINT_INVALID_HEX_WARNING_PARAM);
	}

	@Override
	public boolean useProtobufSchemaNaming() {
		return getBoolean(PROTOBUF_SCHEMA_NAMING_PARAM);
	}

	@Override
	public String getTopicNameDelimiter() {
		return getString(TOPIC_NAME_DELIMITER_PARAM);
	}

	@Override
	public int getTopicNameStyle() {
		return holder.topicNameStyle();
	}

	@Override
	public int getPkType() {
		return holder.pkType();
	}

	@Override
	public boolean useRowidAsKey() {
		return getBoolean(USE_ROWID_AS_KEY_PARAM);
	}

	@Override
	public boolean useAllColsOnDelete() {
		return getBoolean(USE_ALL_COLUMNS_ON_DELETE_PARAM);
	}

	@Override
	public boolean stopOnOra1284() {
		return getBoolean(STOP_ON_ORA_1284_PARAM);
	}

	@Override
	public boolean printUnableToDeleteWarning() {
		return getBoolean(PRINT_UNABLE_TO_DELETE_WARNING_PARAM);
	}

	@Override
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

	@Override
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

	@Override
	public String getOraRowScnField() {
		return getPseudoColumn(ORA_ROWSCN_PARAM);
	}

	@Override
	public String getOraCommitScnField() {
		return getPseudoColumn(ORA_COMMITSCN_PARAM);
	}

	@Override
	public String getOraRowTsField() {
		return getPseudoColumn(ORA_ROWTS_PARAM);
	}

	@Override
	public String getOraRowOpField() {
		return getPseudoColumn(ORA_OPERATION_PARAM);
	}

	@Override
	public String getOraXidField() {
		return getPseudoColumn(ORA_XID_PARAM);
	}

	@Override
	public String getOraUsernameField() {
		return getPseudoColumn(ORA_USERNAME_PARAM);
	}

	@Override
	public String getOraOsUsernameField() {
		return getPseudoColumn(ORA_OSUSERNAME_PARAM);
	}

	@Override
	public String getOraHostnameField() {
		return getPseudoColumn(ORA_HOSTNAME_PARAM);
	}

	@Override
	public String getOraAuditSessionIdField() {
		return getPseudoColumn(ORA_AUDIT_SESSIONID_PARAM);
	}

	@Override
	public String getOraSessionInfoField() {
		return getPseudoColumn(ORA_SESSION_INFO_PARAM);
	}

	@Override
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

	@Override
	public String getConnectorName() {
		return connectorName;
	}

	@Override
	public void setConnectorName(String connectorName) {
		this.connectorName = connectorName;
	}

	@Override
	public LastProcessedSeqNotifier getLastProcessedSeqNotifier() {
		return holder.lastProcessedSeqNotifier();
	}

	@Override
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

	@Override
	public Entry<OraCdcKeyOverrideTypes, String> getKeyOverrideType(final String fqtn) {
		return holder.getKeyOverrideType(fqtn);
	}

	@Override
	public boolean processLobs() {
		return getBoolean(PROCESS_LOBS_PARAM);
	}

	@Override
	public OraCdcLobTransformationsIntf transformLobsImpl() {
		return holder.transformLobsImpl();
	}

	@Override
	public int connectionRetryBackoff() {
		return getInt(CONNECTION_BACKOFF_PARAM);
	}

	@Override
	public OraCdcPseudoColumnsProcessor pseudoColumnsProcessor() {
		if (pseudoColumns == null) {
			pseudoColumns = new OraCdcPseudoColumnsProcessor(this);
		}
		return pseudoColumns;
	}

	@Override
	public boolean useRac() {
		return getBoolean(USE_RAC_PARAM);
	}

	@Override
	public boolean activateStandby() {
		return getBoolean(MAKE_STANDBY_ACTIVE_PARAM);
	}

	@Override
	public String standbyJdbcUrl() {
		return getString(STANDBY_URL_PARAM);
	}

	@Override
	public String standbyWallet() {
		return getString(STANDBY_WALLET_PARAM);
	}

	@Override
	public String standbyPrivilege() {
		return getString(STANDBY_PRIVILEGE_PARAM);
	}

	@Override
	public List<String> racUrls() {
		return getList(INTERNAL_RAC_URLS_PARAM);
	}

	@Override
	public List<String> dg4RacThreads() {
		return getList(INTERNAL_DG4RAC_THREAD_PARAM);
	}

	@Override
	public int topicPartition() {
		return topicPartition;
	}

	@Override
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

	@Override
	public Path queuesRoot() {
		return holder.queuesRoot();
	}

	@Override
	public long startScn() {
		return holder.startScn();
	}

	@Override
	public boolean staticObjIds() {
		return holder.staticObjIds();
	}

	@Override
	public long logMinerReconnectIntervalMs() {
		return getLong(LM_RECONNECT_INTERVAL_MS_PARAM);
	}

	@Override
	public int transactionsThreshold() {
		return holder.transactionsThreshold();
	}

	@Override
	public int reduceLoadMs() {
		return getInt(REDUCE_LOAD_MS_PARAM);
	}

	@Override
	public int arrayListCapacity() {
		return getInt(AL_CAPACITY_PARAM);
	}

	@Override
	public boolean useOffHeapMemory() {
		return holder.useOffHeapMemory();
	}

	@Override
	public int fetchSize() {
		return getInt(FETCH_SIZE_PARAM);
	}

	@Override
	public boolean logMinerTrace() {
		return getBoolean(TRACE_LOGMINER_PARAM);
	}

	@Override
	public Class<?> classLogMiner() throws ClassNotFoundException {
		return Class.forName(getString(ARCHIVED_LOG_CAT_PARAM));
	}

	@Override
	public String classLogMinerName() {
		return getString(ARCHIVED_LOG_CAT_PARAM);
	}

	@Override
	public boolean activateDistributed() {
		return getBoolean(MAKE_DISTRIBUTED_ACTIVE_PARAM);
	}

	@Override
	public String distributedUrl() {
		return getString(DISTRIBUTED_URL_PARAM);
	}

	@Override
	public String distributedWallet() {
		return getString(DISTRIBUTED_WALLET_PARAM);
	}

	@Override
	public String distributedTargetHost() {
		return getString(DISTRIBUTED_TARGET_HOST);
	}

	@Override
	public int distributedTargetPort() {
		return getInt(DISTRIBUTED_TARGET_PORT);
	}

	@Override
	public boolean logMiner() {
		return holder.logMiner();
	}

	@Override
	public void logMiner(final boolean logMiner) {
		holder.logMiner(logMiner);
	}

	@Override
	public void msWindows(final boolean msWindows) {
		holder.msWindows(msWindows);
	}

	@Override
	public String convertRedoFileName(final String originalName, final boolean bfile) {
		return holder.convertRedoFileName(originalName, bfile);
	}

	@Override
	public boolean useAsm() {
		return holder.useAsm();
	}

	@Override
	public boolean useSsh() {
		return holder.useSsh();
	}

	@Override
	public boolean useSmb() {
		return holder.useSmb();
	}

	@Override
	public boolean useBfile() {
		return holder.useBfile();
	}

	@Override
	public boolean useFileTransfer() {
		return holder.useFileTransfer();
	}

	@Override
	public String asmJdbcUrl() {
		return getString(ASM_JDBC_URL_PARAM);
	}

	@Override
	public String asmUser() {
		return getString(ASM_USER_PARAM);
	}

	@Override
	public String asmPassword() {	
		return getPassword(ASM_PASSWORD_PARAM).value();
	}

	@Override
	public boolean asmReadAhead() {	
		return getBoolean(ASM_READ_AHEAD_PARAM);
	}

	@Override
	public long asmReconnectIntervalMs() {
		return getLong(ASM_RECONNECT_INTERVAL_MS_PARAM);
	}

	@Override
	public String asmPrivilege() {
		return getString(ASM_PRIVILEGE_PARAM);
	}

	@Override
	public String sshHostname() {
		return getString(SSH_HOST_PARAM);
	}

	@Override
	public int sshPort() {
		return getInt(SSH_PORT_PARAM);
	}

	@Override
	public String sshUser() {
		return getString(SSH_USER_PARAM);
	}

	@Override
	public String sshKey() {
		return getPassword(SSH_KEY_PARAM).value();
	}

	@Override
	public String sshPassword() {
		return getPassword(SSH_PASSWORD_PARAM).value();
	}

	@Override
	public long sshReconnectIntervalMs() {
		return getLong(SSH_RECONNECT_INTERVAL_MS_PARAM);
	}

	@Override
	public boolean sshStrictHostKeyChecking() {
		return getBoolean(SSH_STRICT_HOST_KEY_CHECKING_PARAM);
	}

	@Override
	public boolean sshProviderMaverick() {
		return holder.sshProviderMaverick();
	}

	@Override
	public boolean sshProviderMina() {
		return holder.sshProviderMina();
	}

	@Override
	public int sshUnconfirmedReads() {
		return getInt(SSH_UNCONFIRMED_READS_PARAM);
	}

	@Override
	public int sshBufferSize() {
		return getInt(SSH_BUFFER_SIZE_PARAM);
	}

	@Override
	public String smbServer() {
		return getString(SMB_SERVER_PARAM);
	}

	@Override
	public String smbShareOnline() {
		return getString(SMB_SHARE_ONLINE_PARAM);
	}

	@Override
	public String smbShareArchive() {
		return getString(SMB_SHARE_ARCHIVE_PARAM);
	}

	@Override
	public String smbUser() {
		return getString(SMB_USER_PARAM);
	}

	@Override
	public String smbPassword() {
		return getPassword(SMB_PASSWORD_PARAM).value();
	}

	@Override
	public String smbDomain() {
		return getString(SMB_DOMAIN_PARAM);
	}

	@Override
	public int smbTimeoutMs() {
		return getInt(SMB_TIMEOUT_MS_PARAM);
	}

	@Override
	public int smbSocketTimeoutMs() {
		return getInt(SMB_SOCKET_TIMEOUT_MS_PARAM);
	}

	@Override
	public long smbReconnectIntervalMs() {
		return getLong(SMB_RECONNECT_INTERVAL_MS_PARAM);
	}

	@Override
	public int smbBufferSize() {
		return getInt(SMB_BUFFER_SIZE_PARAM);
	}

	@Override
	public String bfileDirOnline() {
		return getString(BFILE_DIR_ONLINE_PARAM);
	}

	@Override
	public String bfileDirArchive() {
		return getString(BFILE_DIR_ARCHIVE_PARAM);
	}

	@Override
	public long bfileReconnectIntervalMs() {
		return getLong(BFILE_RECONNECT_INTERVAL_MS_PARAM);
	}

	@Override
	public int bfileBufferSize() {
		return getInt(BFILE_BUFFER_SIZE_PARAM);
	}

	@Override
	public String tdeWallet() {
		return getString(TDE_WALLET_PATH_PARAM);
	}

	@Override
	public String tdePassword() {
		return getPassword(TDE_WALLET_PASSWORD_PARAM).value();
	}

	@Override
	public boolean allUpdates() {
		return getBoolean(ALL_UPDATES_PARAM);
	}

	@Override
	public boolean processOnlineRedoLogs() {
		return getBoolean(PROCESS_ONLINE_REDO_LOGS_PARAM);
	}

	@Override
	public int currentScnQueryInterval() {
		return getInt(CURRENT_SCN_QUERY_INTERVAL_PARAM);
	}

	@Override
	public boolean printAllOnlineRedoRanges() {
		return getBoolean(PRINT_ALL_ONLINE_REDO_RANGES_PARAM);
	}

	@Override
	public boolean printUnable2MapColIdWarning() {
		return getBoolean(PRINT_UNABLE2MAP_COL_ID_WARNING_PARAM);
	}

	@Override
	public String initialLoad() {
		return getString(INITIAL_LOAD_PARAM);
	}

	@Override
	public boolean ignoreStoredOffset() {
		return getBoolean(IGNORE_STORED_OFFSET_PARAM);
	}

	@Override
	public boolean supplementalLogAll() {
		return holder.supplementalLogAll();
	}

	@Override
	public boolean stopOnMissedLogFile() {
		return getBoolean(STOP_ON_MISSED_LOG_FILE_PARAM);
	}

	@Override
	public int tablesInProcessSize() {
		return getInt(TABLES_IN_PROCESS_SIZE_PARAM);
	}

	@Override
	public int tablesOutOfScopeSize() {
		return getInt(TABLES_OUT_OF_SCOPE_SIZE_PARAM);
	}

	@Override
	public int transactionsInProcessSize() {
		return getInt(TRANS_IN_PROCESS_SIZE_PARAM);
	}

	@Override
	public int emitterTimeoutMs() {
		return getInt(EMITTER_TIMEOUT_MS_PARAM);
	}

	@Override
	public int[] offHeapSize() {
		return holder.offHeapSize();
	}

	@Override
	public String fileTransferStageDir() {
		return getString(TRANSFER_DIR_STAGE_PARAM);
	}

}
