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
import solutions.a2.cdc.oracle.runtime.data.DataBinder;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public interface OraCdcSourceConnectorConfig extends OraCdcSourceBaseConfig {
	
	List<Triple<List<Pair<String, OraCdcColumn>>, Map<String, OraCdcColumn>, List<Pair<String, OraCdcColumn>>>>
			tableNumberMapping(final String tableOwner, final String tableName);
	List<Triple<List<Pair<String, OraCdcColumn>>, Map<String, OraCdcColumn>, List<Pair<String, OraCdcColumn>>>>
			tableNumberMapping(final String pdbName, final String tableOwner, final String tableName);
	OraCdcColumn columnNumberMapping(
			List<Triple<List<Pair<String, OraCdcColumn>>, Map<String, OraCdcColumn>, List<Pair<String, OraCdcColumn>>>>
				numberRemap, final String columnName);
	boolean useOracdcSchemas();
	boolean tolerateIncompleteRow();
	boolean isPrintInvalidHexValueWarning();
	boolean useProtobufSchemaNaming();
	String getTopicNameDelimiter();
	int getTopicNameStyle();
	int getPkType();
	boolean useRowidAsKey();
	boolean useAllColsOnDelete();
	boolean stopOnOra1284();
	boolean printUnableToDeleteWarning();
	String getOraRowScnField();
	String getOraCommitScnField();
	String getOraRowTsField();
	String getOraRowOpField();
	String getOraXidField();
	String getOraUsernameField();
	String getOraOsUsernameField();
	String getOraHostnameField();
	String getOraAuditSessionIdField();
	String getOraSessionInfoField();
	String getOraClientIdField();
	String getConnectorName();
	void setConnectorName(String connectorName);
	LastProcessedSeqNotifier getLastProcessedSeqNotifier();
	String getLastProcessedSeqNotifierFile();
	Entry<OraCdcKeyOverrideTypes, String> getKeyOverrideType(final String fqtn);
	boolean processLobs();
	OraCdcLobTransformationsIntf transformLobsImpl();
	int connectionRetryBackoff();
	OraCdcPseudoColumnsProcessor pseudoColumnsProcessor();
	boolean useRac();
	boolean activateStandby();
	String standbyJdbcUrl();
	String standbyWallet();
	String standbyPrivilege();
	List<String> racUrls();
	List<String> dg4RacThreads();
	Path queuesRoot();
	long startScn();
	boolean staticObjIds();
	long logMinerReconnectIntervalMs();
	int transactionsThreshold();
	int reduceLoadMs();
	int arrayListCapacity();
	boolean useOffHeapMemory();
	int fetchSize();
	boolean logMinerTrace();
	Class<?> classLogMiner();
	String classLogMinerName();
	boolean activateDistributed();
	String distributedUrl();
	String distributedWallet();
	String distributedTargetHost();
	int distributedTargetPort();
	boolean logMiner();
	void logMiner(final boolean logMiner);
	void msWindows(final boolean msWindows);
	String convertRedoFileName(final String originalName, final boolean bfile);
	boolean useAsm();
	boolean useSsh();
	boolean useSmb();
	boolean useBfile();
	boolean useFileTransfer();
	String asmJdbcUrl();
	String asmUser();
	String asmPassword();
	boolean asmReadAhead();
	long asmReconnectIntervalMs();
	String asmPrivilege();
	String sshHostname();
	int sshPort();
	String sshUser();
	String sshKey();
	String sshPassword();
	long sshReconnectIntervalMs();
	boolean sshStrictHostKeyChecking();
	boolean sshProviderMaverick();
	boolean sshProviderMina();
	int sshUnconfirmedReads();
	int sshBufferSize();
	int sshConnectTimeout();
	String smbServer();
	String smbShareOnline();
	String smbShareArchive();
	String smbUser();
	String smbPassword();
	String smbDomain();
	int smbTimeoutMs();
	int smbSocketTimeoutMs();
	long smbReconnectIntervalMs();
	int smbBufferSize();
	String bfileDirOnline();
	String bfileDirArchive();
	long bfileReconnectIntervalMs();
	int bfileBufferSize();
	String tdeWallet();
	String tdePassword();
	boolean allUpdates();
	boolean processOnlineRedoLogs();
	int currentScnQueryInterval();
	boolean printAllOnlineRedoRanges();
	boolean printUnable2MapColIdWarning();
	String initialLoad();
	boolean ignoreStoredOffset();
	boolean supplementalLogAll();
	boolean stopOnMissedLogFile();
	int tablesInProcessSize();
	int tablesOutOfScopeSize();
	int transactionsInProcessSize();
	int emitterTimeoutMs();
	int[] offHeapSize();
	String fileTransferStageDir();
	DataBinder dataBinder(OraCdcTableBase table, OraRdbmsInfo rdbmsInfo);
	boolean beforeDataImage();

}
