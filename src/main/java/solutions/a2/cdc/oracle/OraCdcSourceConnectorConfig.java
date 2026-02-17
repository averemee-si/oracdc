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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public interface OraCdcSourceConnectorConfig extends OraCdcSourceBaseConfig {
	
	public List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>>
			tableNumberMapping(final String tableOwner, final String tableName);
	public List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>>
			tableNumberMapping(final String pdbName, final String tableOwner, final String tableName);
	public OraColumn columnNumberMapping(
			List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>>
				numberRemap, final String columnName);
	public boolean useOracdcSchemas();
	public int getIncompleteDataTolerance();
	public boolean isPrintInvalidHexValueWarning();
	public boolean useProtobufSchemaNaming();
	public String getTopicNameDelimiter();
	public int getTopicNameStyle();
	public int getPkType();
	public boolean useRowidAsKey();
	public boolean useAllColsOnDelete();
	public boolean stopOnOra1284();
	public boolean printUnableToDeleteWarning();
	public TopicNameMapper getTopicNameMapper();
	public SchemaNameMapper getSchemaNameMapper();
	public String getOraRowScnField();
	public String getOraCommitScnField();
	public String getOraRowTsField();
	public String getOraRowOpField();
	public String getOraXidField();
	public String getOraUsernameField();
	public String getOraOsUsernameField();
	public String getOraHostnameField();
	public String getOraAuditSessionIdField();
	public String getOraSessionInfoField();
	public String getOraClientIdField();
	public String getConnectorName();
	public void setConnectorName(String connectorName);
	public LastProcessedSeqNotifier getLastProcessedSeqNotifier();
	public String getLastProcessedSeqNotifierFile();
	public Entry<OraCdcKeyOverrideTypes, String> getKeyOverrideType(final String fqtn);
	public boolean processLobs();
	public OraCdcLobTransformationsIntf transformLobsImpl();
	public int connectionRetryBackoff();
	public OraCdcPseudoColumnsProcessor pseudoColumnsProcessor();
	public boolean useRac();
	public boolean activateStandby();
	public String standbyJdbcUrl();
	public String standbyWallet();
	public String standbyPrivilege();
	public List<String> racUrls();
	public List<String> dg4RacThreads();
	public int topicPartition();
	public void topicPartition(final int redoThread);
	public Path queuesRoot();
	public long startScn();
	public boolean staticObjIds();
	public long logMinerReconnectIntervalMs();
	public int transactionsThreshold();
	public int reduceLoadMs();
	public int arrayListCapacity();
	public boolean useOffHeapMemory();
	public int fetchSize();
	public boolean logMinerTrace();
	public Class<?> classLogMiner() throws ClassNotFoundException;
	public String classLogMinerName();
	public boolean activateDistributed();
	public String distributedUrl();
	public String distributedWallet();
	public String distributedTargetHost();
	public int distributedTargetPort();
	public boolean logMiner();
	public void logMiner(final boolean logMiner);
	public void msWindows(final boolean msWindows);
	public String convertRedoFileName(final String originalName, final boolean bfile);
	public boolean useAsm();
	public boolean useSsh();
	public boolean useSmb();
	public boolean useBfile();
	public boolean useFileTransfer();
	public String asmJdbcUrl();
	public String asmUser();
	public String asmPassword();
	public boolean asmReadAhead();
	public long asmReconnectIntervalMs();
	public String asmPrivilege();
	public String sshHostname();
	public int sshPort();
	public String sshUser();
	public String sshKey();
	public String sshPassword();
	public long sshReconnectIntervalMs();
	public boolean sshStrictHostKeyChecking();
	public boolean sshProviderMaverick();
	public boolean sshProviderMina();
	public int sshUnconfirmedReads();
	public int sshBufferSize();
	public String smbServer();
	public String smbShareOnline();
	public String smbShareArchive();
	public String smbUser();
	public String smbPassword();
	public String smbDomain();
	public int smbTimeoutMs();
	public int smbSocketTimeoutMs();
	public long smbReconnectIntervalMs();
	public int smbBufferSize();
	public String bfileDirOnline();
	public String bfileDirArchive();
	public long bfileReconnectIntervalMs();
	public int bfileBufferSize();
	public String tdeWallet();
	public String tdePassword();
	public boolean allUpdates();
	public boolean processOnlineRedoLogs();
	public int currentScnQueryInterval();
	public boolean printAllOnlineRedoRanges();
	public boolean printUnable2MapColIdWarning();
	public String initialLoad();
	public boolean ignoreStoredOffset();
	public boolean supplementalLogAll();
	public boolean stopOnMissedLogFile();
	public int tablesInProcessSize();
	public int tablesOutOfScopeSize();
	public int transactionsInProcessSize();
	public int emitterTimeoutMs();
	public int[] offHeapSize();
	public String fileTransferStageDir();

}
