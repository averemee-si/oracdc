/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
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
