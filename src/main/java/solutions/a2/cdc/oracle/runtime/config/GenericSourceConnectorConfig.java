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

import static solutions.a2.cdc.ParameterType.BOOLEAN;
import static solutions.a2.cdc.ParameterType.INT;
import static solutions.a2.cdc.ParameterType.LONG;
import static solutions.a2.cdc.ParameterType.LIST;
import static solutions.a2.cdc.ParameterType.PASSWORD;
import static solutions.a2.cdc.ParameterType.STRING;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.*;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.Configuration;
import solutions.a2.cdc.oracle.LastProcessedSeqNotifier;
import solutions.a2.cdc.oracle.OraCdcKeyOverrideTypes;
import solutions.a2.cdc.oracle.OraCdcPseudoColumnsProcessor;
import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.cdc.oracle.OraColumn;
import solutions.a2.cdc.oracle.SchemaNameMapper;
import solutions.a2.cdc.oracle.TopicNameMapper;
import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import solutions.a2.cdc.oracle.utils.KafkaUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class GenericSourceConnectorConfig extends GenericSourceBaseConfig implements OraCdcSourceConnectorConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(GenericSourceConnectorConfig.class);

	private final SourceConnectorConfig holder;
	private String connectorName;
	private OraCdcPseudoColumnsProcessor pseudoColumns = null;
	private int topicPartition = 0;

	public static Configuration config() {
		return GenericSourceBaseConfig.config()
				.define(TOPIC_PARTITION_PARAM, INT, 0)
				.define(FIRST_CHANGE_PARAM, STRING, "0")
				.define(TEMP_DIR_PARAM, STRING, System.getProperty("java.io.tmpdir"))
				.define(MAKE_STANDBY_ACTIVE_PARAM, BOOLEAN, false)
				.define(STANDBY_WALLET_PARAM, STRING, "")
				.define(STANDBY_URL_PARAM, STRING, "")
				.define(STANDBY_PRIVILEGE_PARAM, STRING, STANDBY_PRIVILEGE_DEFAULT)
				.define(ORACDC_SCHEMAS_PARAM, BOOLEAN, false)
				.define(INITIAL_LOAD_PARAM, STRING, INITIAL_LOAD_IGNORE)
				.define(TOPIC_NAME_STYLE_PARAM, STRING, TOPIC_NAME_STYLE_TABLE)
				.define(TOPIC_NAME_DELIMITER_PARAM, STRING, TOPIC_NAME_DELIMITER_UNDERSCORE)
				.define(TABLE_LIST_STYLE_PARAM, STRING, TABLE_LIST_STYLE_STATIC)
				.define(PROCESS_LOBS_PARAM, BOOLEAN, false)
				.define(CONNECTION_BACKOFF_PARAM, INT, CONNECTION_BACKOFF_DEFAULT)
				.define(ARCHIVED_LOG_CAT_PARAM, STRING, ARCHIVED_LOG_CAT_DEFAULT)
				.define(FETCH_SIZE_PARAM, INT, FETCH_SIZE_DEFAULT)
				.define(TRACE_LOGMINER_PARAM, BOOLEAN, false)
				.define(MAKE_DISTRIBUTED_ACTIVE_PARAM, BOOLEAN, false)
				.define(DISTRIBUTED_WALLET_PARAM, STRING, "")
				.define(DISTRIBUTED_URL_PARAM, STRING, "")
				.define(DISTRIBUTED_TARGET_HOST, STRING, "")
				.define(DISTRIBUTED_TARGET_PORT, INT, DISTRIBUTED_TARGET_PORT_DEFAULT)
				.define(LOB_TRANSFORM_CLASS_PARAM, STRING, LOB_TRANSFORM_CLASS_DEFAULT)
				.define(USE_RAC_PARAM, BOOLEAN, false)
				.define(PROTOBUF_SCHEMA_NAMING_PARAM, BOOLEAN, false)
				.define(ORA_TRANSACTION_IMPL_PARAM, STRING, ORA_TRANSACTION_IMPL_DEFAULT)
				.define(PRINT_INVALID_HEX_WARNING_PARAM, BOOLEAN, false)
				.define(PROCESS_ONLINE_REDO_LOGS_PARAM, BOOLEAN, false)
				.define(CURRENT_SCN_QUERY_INTERVAL_PARAM, INT, CURRENT_SCN_QUERY_INTERVAL_DEFAULT)
				.define(INCOMPLETE_REDO_TOLERANCE_PARAM, STRING, INCOMPLETE_REDO_TOLERANCE_ERROR)
				.define(PRINT_ALL_ONLINE_REDO_RANGES_PARAM, BOOLEAN, true)
				.define(LM_RECONNECT_INTERVAL_MS_PARAM, LONG, Long.MAX_VALUE)
				.define(PK_TYPE_PARAM, STRING, PK_TYPE_WELL_DEFINED)
				.define(USE_ROWID_AS_KEY_PARAM, BOOLEAN, true)
				.define(USE_ALL_COLUMNS_ON_DELETE_PARAM, BOOLEAN, USE_ALL_COLUMNS_ON_DELETE_DEFAULT)
				.define(INTERNAL_RAC_URLS_PARAM, LIST, "")
				.define(INTERNAL_DG4RAC_THREAD_PARAM, LIST, "")
				.define(TOPIC_MAPPER_PARAM, STRING, TOPIC_MAPPER_DEFAULT)
				.define(STOP_ON_ORA_1284_PARAM, BOOLEAN, STOP_ON_ORA_1284_DEFAULT)
				.define(PRINT_UNABLE_TO_DELETE_WARNING_PARAM, BOOLEAN, PRINT_UNABLE_TO_DELETE_WARNING_DEFAULT)
				.define(SCHEMANAME_MAPPER_PARAM, STRING, SCHEMANAME_MAPPER_DEFAULT)
				.define(ORA_ROWSCN_PARAM, STRING, "")
				.define(ORA_COMMITSCN_PARAM, STRING, "")
				.define(ORA_ROWTS_PARAM, STRING, "")
				.define(ORA_OPERATION_PARAM, STRING, "")
				.define(ORA_XID_PARAM, STRING, "")
				.define(ORA_USERNAME_PARAM, STRING, "")
				.define(ORA_OSUSERNAME_PARAM, STRING, "")
				.define(ORA_HOSTNAME_PARAM, STRING, "")
				.define(ORA_AUDIT_SESSIONID_PARAM, STRING, "")
				.define(ORA_SESSION_INFO_PARAM, STRING, "")
				.define(ORA_CLIENT_ID_PARAM, STRING, "")
				.define(LAST_SEQ_NOTIFIER_PARAM, STRING, "")
				.define(LAST_SEQ_NOTIFIER_FILE_PARAM, STRING, "")
				.define(KEY_OVERRIDE_PARAM, LIST, "")
				.define(CONC_TRANSACTIONS_THRESHOLD_PARAM, INT, 0)
				.define(REDUCE_LOAD_MS_PARAM, INT, REDUCE_LOAD_MS_DEFAULT)
				.define(AL_CAPACITY_PARAM, INT, AL_CAPACITY_DEFAULT)
				.define(IGNORE_STORED_OFFSET_PARAM, BOOLEAN, IGNORE_STORED_OFFSET_DEFAULT)
				// Redo Miner only!
				.define(REDO_FILE_NAME_CONVERT_PARAM, STRING, "")
				.define(REDO_FILE_MEDIUM_PARAM, STRING, REDO_FILE_MEDIUM_FS)
				.define(ASM_JDBC_URL_PARAM, STRING, "")
				.define(ASM_USER_PARAM, STRING, "")
				.define(ASM_PASSWORD_PARAM, PASSWORD, "")
				.define(ASM_READ_AHEAD_PARAM, BOOLEAN, ASM_READ_AHEAD_DEFAULT)
				.define(ASM_RECONNECT_INTERVAL_MS_PARAM, LONG, ASM_RECONNECT_INTERVAL_MS_DEFAULT)
				.define(ASM_PRIVILEGE_PARAM, STRING, ASM_PRIVILEGE_DEFAULT)
				.define(SSH_HOST_PARAM, STRING, "")
				.define(SSH_PORT_PARAM, INT, SSH_PORT_DEFAULT)
				.define(SSH_USER_PARAM, STRING, "")
				.define(SSH_KEY_PARAM, PASSWORD, "")
				.define(SSH_PASSWORD_PARAM, PASSWORD, "")
				.define(SSH_RECONNECT_INTERVAL_MS_PARAM, LONG, SSH_RECONNECT_INTERVAL_MS_DEFAULT)
				.define(SSH_STRICT_HOST_KEY_CHECKING_PARAM, BOOLEAN, false)
				.define(SSH_PROVIDER_PARAM, STRING, SSH_PROVIDER_DEFAULT)
				.define(SSH_UNCONFIRMED_READS_PARAM, INT, SSH_UNCONFIRMED_READS_DEFAULT)
				.define(SSH_BUFFER_SIZE_PARAM, INT, SSH_BUFFER_SIZE_DEFAULT)
				.define(SMB_SERVER_PARAM, STRING, "")
				.define(SMB_SHARE_ONLINE_PARAM, STRING, "")
				.define(SMB_SHARE_ARCHIVE_PARAM, STRING, "")
				.define(SMB_USER_PARAM, STRING, "")
				.define(SMB_PASSWORD_PARAM, PASSWORD, "")
				.define(SMB_DOMAIN_PARAM, STRING, "")
				.define(SMB_TIMEOUT_MS_PARAM, INT, SMB_TIMEOUT_MS_DEFAULT)
				.define(SMB_SOCKET_TIMEOUT_MS_PARAM, INT, SMB_SOCKET_TIMEOUT_MS_DEFAULT)
				.define(SMB_RECONNECT_INTERVAL_MS_PARAM, LONG, SMB_RECONNECT_INTERVAL_MS_DEFAULT)
				.define(SMB_BUFFER_SIZE_PARAM, INT, SMB_BUFFER_SIZE_DEFAULT)
				.define(BFILE_DIR_ONLINE_PARAM, STRING, "")
				.define(BFILE_DIR_ARCHIVE_PARAM, STRING, "")
				.define(BFILE_RECONNECT_INTERVAL_MS_PARAM, LONG, BFILE_RECONNECT_INTERVAL_MS_DEFAULT)
				.define(BFILE_BUFFER_SIZE_PARAM, INT, BFILE_BUFFER_SIZE_DEFAULT)
				.define(TDE_WALLET_PATH_PARAM, STRING, "")
				.define(TDE_WALLET_PASSWORD_PARAM, PASSWORD, "")
				.define(ALL_UPDATES_PARAM, BOOLEAN, ALL_UPDATES_DEFAULT)
				.define(PRINT_UNABLE2MAP_COL_ID_WARNING_PARAM, BOOLEAN, PRINT_UNABLE2MAP_COL_ID_WARNING_DEFAULT)
				.define(SUPPLEMENTAL_LOGGING_PARAM, STRING, SUPPLEMENTAL_LOGGING_DEFAULT)
				.define(STOP_ON_MISSED_LOG_FILE_PARAM, BOOLEAN, STOP_ON_MISSED_LOG_FILE_DEFAULT)
				.define(TABLES_IN_PROCESS_SIZE_PARAM, INT, TABLES_IN_PROCESS_SIZE_DEFAULT)
				.define(TABLES_OUT_OF_SCOPE_SIZE_PARAM, INT, TABLES_OUT_OF_SCOPE_SIZE_DEFAULT)
				.define(TRANS_IN_PROCESS_SIZE_PARAM, INT, TRANS_IN_PROCESS_SIZE_DEFAULT)
				.define(EMITTER_TIMEOUT_MS_PARAM, INT, EMITTER_TIMEOUT_MS_DEFAULT)
				.define(OFFHEAP_SIZE_PARAM, STRING, OFFHEAP_SIZE_DEFAULT)
				.define(TRANSFER_DIR_STAGE_PARAM, STRING, "");
	}

	public GenericSourceConnectorConfig(Map<String, String> originals) {
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
		return holder.tableNumberMapping(pdbName, tableOwner, tableName);
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
		//TODO
		//TOPO Kafka specific, will be removed after changes in OraTable
		//TODO
		return null;
	}

	@Override
	public SchemaNameMapper getSchemaNameMapper() {
		//TODO
		//TOPO Kafka specific, will be removed after changes in OraTable
		//TODO
		return null;
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
		return getPassword(ASM_PASSWORD_PARAM);
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
		return getPassword(SSH_KEY_PARAM);
	}

	@Override
	public String sshPassword() {
		return getPassword(SSH_PASSWORD_PARAM);
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
		return getPassword(SMB_PASSWORD_PARAM);
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
		return getPassword(TDE_WALLET_PASSWORD_PARAM);
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
