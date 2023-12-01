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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmtIntf;

/**
 * 
 * Wrapper for LogMiner operations (V$ARCHIVED_LOG as source) implementation
 * 
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class OraCdcV$ArchivedLogImpl implements OraLogMiner {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcV$ArchivedLogImpl.class);
//	private final static int ONLINE_REDO_THRESHOLD_MS = 60_000;

	private long firstChange;
	private long sessionFirstChange;
	private long nextChange = 0;
	private long lastSequence = -1;
	private int numArchLogs;
	private long sizeOfArchLogs;
	private final boolean useNumOfArchLogs;
	private final boolean dictionaryAvailable;
	private final boolean callDbmsLogmnrAddLogFile;
	private final boolean processOnlineRedoLogs;
	private final int onlineRedoQueryMsMin;
	private final long dbId;
	private final String dbUniqueName;
	private final OraCdcLogMinerMgmtIntf metrics;
	private PreparedStatement psGetArchivedLogs;
	private CallableStatement csAddArchivedLogs;
	private CallableStatement csStartLogMiner;
	private CallableStatement csStopLogMiner;
	private PreparedStatement psUpToCurrentScn;
	private int archLogsAvailable = 0;
	private long archLogsSize = 0;
	private List<String> fileNames = new ArrayList<>();
	private long readStartMillis;
	private final OraRdbmsInfo rdbmsInfo;
	private long lastOnlineRedoTime = 0;

	public OraCdcV$ArchivedLogImpl(
			final Connection connLogMiner,
			final OraCdcLogMinerMgmtIntf metrics, final long firstChange,
			final OraCdcSourceConnectorConfig config,
			final CountDownLatch runLatch,
			final OraRdbmsInfo rdbmsInfo,
			final OraConnectionObjects oraConnections) throws SQLException {
		LOGGER.trace("BEGIN: OraLogMiner Constructor");
		this.metrics = metrics;
		this.rdbmsInfo = rdbmsInfo;

		if (rdbmsInfo.isCdb() && rdbmsInfo.isPdbConnectionAllowed()) {
			// 19.10+ with connection to PDB
			callDbmsLogmnrAddLogFile = false;
			processOnlineRedoLogs = false;
			onlineRedoQueryMsMin = Integer.MIN_VALUE;
			if (config.getBoolean(ParamConstants.PROCESS_ONLINE_REDO_LOGS_PARAM)) {
				LOGGER.warn(
						"\n" +
						"=====================\n" +
						"Cannot process online redo logs using connection to PDB!\n" +
						"Value TRUE of the parameter 'a2.process.online.redo.logs' is ignored!\n" +
						"=====================\n");
			}
		} else {
			callDbmsLogmnrAddLogFile = true;
			processOnlineRedoLogs = config.getBoolean(ParamConstants.PROCESS_ONLINE_REDO_LOGS_PARAM);
			onlineRedoQueryMsMin = config.getInt(ParamConstants.CURRENT_SCN_QUERY_INTERVAL_PARAM);
		}

		if (config.getLong(ParamConstants.REDO_FILES_SIZE_PARAM) > 0) {
			useNumOfArchLogs = false;
			sizeOfArchLogs = config.getLong(ParamConstants.REDO_FILES_SIZE_PARAM);
			LOGGER.debug("The redo log read size limit will be set to '{}' bytes.", sizeOfArchLogs);
		} else {
			useNumOfArchLogs = true;
			numArchLogs = config.getShort(ParamConstants.REDO_FILES_COUNT_PARAM);
			LOGGER.debug("The redo log read size limit will be set to '{}' files", numArchLogs);
		}

		this.firstChange = firstChange;
		createStatements(connLogMiner);
		PreparedStatement psOpenMode = connLogMiner.prepareStatement(OraDictSqlTexts.RDBMS_OPEN_MODE,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ResultSet rsOpenMode = psOpenMode.executeQuery();
		if (rsOpenMode.next()) {
			final String openMode = rsOpenMode.getString(1);
			if ("MOUNTED".equals(openMode)) {
				LOGGER.trace("LogMiner connection database is in MOUNTED state, no dictionary available.");
				dictionaryAvailable = false;
			} else {
				LOGGER.trace("LogMiner connection database is in {} state, dictionary is available.", openMode);
				dictionaryAvailable = true;
			}
			LOGGER.info("LogMiner will start from SCN {}", firstChange);
			dbId = rsOpenMode.getLong(2);
			dbUniqueName = rsOpenMode.getString(3);
		} else {
			throw new SQLException("Unable to detect RDBMS open mode");
		}
		rsOpenMode.close();
		rsOpenMode = null;
		psOpenMode.close();
		psOpenMode = null;
		// It's time to init JMS metrics...
		metrics.start(firstChange);
		LOGGER.trace("END: OraLogMiner Constructor");
	}

	@Override
	public void createStatements(final Connection connLogMiner) throws SQLException {
		psGetArchivedLogs = connLogMiner.prepareStatement(OraDictSqlTexts.ARCHIVED_LOGS,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		csStartLogMiner = connLogMiner.prepareCall(OraDictSqlTexts.START_LOGMINER);
		csStopLogMiner = connLogMiner.prepareCall(OraDictSqlTexts.STOP_LOGMINER);
		if (callDbmsLogmnrAddLogFile) {
			csAddArchivedLogs = connLogMiner.prepareCall(OraDictSqlTexts.ADD_ARCHIVED_LOG);
			if (processOnlineRedoLogs) {
				psUpToCurrentScn = connLogMiner.prepareStatement(OraDictSqlTexts.UP_TO_CURRENT_SCN,
						ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			}
		}
	}

	/**
	 * Prepare LogMiner (exec DBMS_LOGMNR.START_LOGMNR) for given connection
	 * 
	 * @return  - true if LogMiner prepared, false if no more redo files available
	 * @throws SQLException
	 */
	@Override
	public boolean next() throws SQLException {
		return start(true);
	}

	@Override
	public boolean extend() throws SQLException {
		return start(false);
	}

	private boolean start(boolean nextLogs) throws SQLException {
		final String functionName;
		if (nextLogs) {
			functionName = "next()";
		} else {
			functionName = "extend()";
		}
		LOGGER.trace("BEGIN: {}", functionName);

		archLogsAvailable = 0;
		archLogsSize = 0;

		if (nextLogs) {
			if (firstChange == 0) {
				// oracdc started without archived logs....
				LOGGER.debug("Requerying V$ARCHIVED_LOG for FIRST_CHANGE# ...");
				firstChange = rdbmsInfo.firstScnFromArchivedLogs(psGetArchivedLogs.getConnection());
				if (firstChange == 0) {
					LOGGER.debug("Nothing found in V$ARCHIVED_LOG... Will retry");
					return false;
				}
			}
			// Initialize list of files only for "next()"
			fileNames = new ArrayList<>();
		}

		psGetArchivedLogs.setLong(1, firstChange);
		psGetArchivedLogs.setLong(2, firstChange);
		psGetArchivedLogs.setLong(3, firstChange);
		psGetArchivedLogs.setInt(4, rdbmsInfo.getRedoThread());
		psGetArchivedLogs.setInt(5, rdbmsInfo.getRedoThread());
		ResultSet rs = psGetArchivedLogs.executeQuery();
		int lagSeconds = 0;
		while (rs.next()) {
			final long sequence = rs.getLong("SEQUENCE#");
			nextChange = rs.getLong("NEXT_CHANGE#");
			if (lagSeconds == 0) {
				lagSeconds = rs.getInt("ACTUAL_LAG_SECONDS");
			}
			if (sequence > lastSequence) {
				if (firstChange < nextChange) {
					// #25 BEGIN - hole in SEQUENCE# numbering  in V$ARCHIVED_LOG
					if (nextLogs && (lastSequence - sequence) > 1) {
						LOGGER.warn("Gap in V$ARCHIVED_LOG numbering detected between SEQUENCE# {} and {}",
								lastSequence, sequence);
						if (fileNames.size() > 0) {
							break;
						} else {
							firstChange = rs.getLong("FIRST_CHANGE#");
						}
					}
					// #25 END - hole in SEQUENCE# numbering  in V$ARCHIVED_LOG
					lastSequence = sequence;
					final String fileName = rs.getString("NAME");
					fileNames.add(archLogsAvailable, fileName);
					printRedoLogInfo(true,
							fileName, rs.getShort("THREAD#"), lastSequence, rs.getLong("FIRST_CHANGE#"));
					archLogsAvailable++;
					archLogsSize += rs.getLong("BYTES"); 
					if (useNumOfArchLogs) {
						if (archLogsAvailable >= numArchLogs) {
							break;
						}
					} else {
						if (archLogsSize >= sizeOfArchLogs) {
							break;
						}
					}
				}
			}
		}
		rs.close();
		rs = null;
		psGetArchivedLogs.clearParameters();

		boolean processOnlineRedo = false;
		if (archLogsAvailable == 0) {
			if (!callDbmsLogmnrAddLogFile || !processOnlineRedoLogs) {
				LOGGER.debug("END: {} no archived redo yet, return false", functionName);
				return false;
			} else {
				// Both callDbmsLogmnrAddLogFile and processOnlineRedoLogs are TRUE
				if (lastOnlineRedoTime != 0 && 
						((int)(System.currentTimeMillis() - lastOnlineRedoTime) < onlineRedoQueryMsMin)) {
					LOGGER.debug("END: {} time is below threshold, return false", functionName);
					return false;
				}
				processOnlineRedo = true;
				psUpToCurrentScn.setLong(1, firstChange);
				long onlineSequence = 0;
				final String onlineRedoMember;
				ResultSet rsUpToCurrentScn = psUpToCurrentScn.executeQuery();
				if (rsUpToCurrentScn.next()) {
					nextChange =  rsUpToCurrentScn.getLong(1);
					lagSeconds = rsUpToCurrentScn.getInt(2);
					onlineSequence = rsUpToCurrentScn.getLong(3);
					onlineRedoMember = rsUpToCurrentScn.getString(4);
				} else {
					throw new SQLException("Unable to execute\n" + OraDictSqlTexts.UP_TO_CURRENT_SCN + "\n!!!");
				}
				rsUpToCurrentScn.close();
				rsUpToCurrentScn = null;
				if ((nextChange - 1) < firstChange) {
					LOGGER.trace("END: {} no new data in online redo, return false", functionName);
					return false;
				}
				fileNames.add(onlineRedoMember);
				printRedoLogInfo(false, onlineRedoMember, rdbmsInfo.getRedoThread(), onlineSequence, firstChange);
				// This must be here as additional warranty against ORA-1291
				lastOnlineRedoTime = System.currentTimeMillis();
			}
		}

		// Set current processing in JMX
		metrics.setNowProcessed(
				fileNames, nextLogs ? firstChange : sessionFirstChange, nextChange, lagSeconds);
		if (callDbmsLogmnrAddLogFile || processOnlineRedo) {
			LOGGER.trace("Adding files to LogMiner session and starting it");
			for (int fileNum = 0; fileNum < fileNames.size(); fileNum++) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Adding {} to LogMiner processing list.", fileNames.get(fileNum));
				}
				csAddArchivedLogs.setInt(1, fileNum);
				csAddArchivedLogs.setString(2, fileNames.get(fileNum));
				csAddArchivedLogs.addBatch();
			}
			csAddArchivedLogs.executeBatch();
			csAddArchivedLogs.clearBatch();
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Attempting to start LogMiner for SCN range from {} to {}.",
					nextLogs ? firstChange : sessionFirstChange, nextChange);
		}
		try {
			csStartLogMiner.setLong(1, nextLogs ? firstChange : sessionFirstChange); 
			csStartLogMiner.setLong(2, processOnlineRedo ? nextChange - 1 : nextChange);
			csStartLogMiner.execute();
			csStartLogMiner.clearParameters();
		} catch(SQLException sqle) {
			if (sqle.getErrorCode() == OraRdbmsInfo.ORA_1291 &&
					processOnlineRedo) {
				LOGGER.debug("ORA-1291 while processing online redo log {}.", fileNames.get(0));
				return false;
			} else {
				final StringBuilder sb = new StringBuilder(256);
				fileNames.forEach(fileName -> {
					sb
						.append(fileName)
						.append("\n");
				});
				LOGGER.error(
						"\n" +
						"=====================\n" +
						"Unable to execute\\n\\t{}\\n\\tusing STARTSCN={} and ENDSCN={}!\n" +
						"\tLogMiner redo files:\n{}" +
						"=====================\n",
							OraDictSqlTexts.START_LOGMINER,
							nextLogs ? firstChange : sessionFirstChange, nextChange, sb);
				throw sqle;
			}
		}
		if (nextLogs) {
			// Set sessionFirstChange only in call to next()
			sessionFirstChange = firstChange;
		}
		firstChange = nextChange;
		readStartMillis = System.currentTimeMillis();
		LOGGER.trace("END: {} returns true", functionName);
		return true;
	}

	@Override
	public void stop() throws SQLException {
		LOGGER.trace("BEGIN: stop()");
		csStopLogMiner.execute();
		// Add info about processed files to JMX
		metrics.addAlreadyProcessed(fileNames, archLogsAvailable, archLogsSize,
				System.currentTimeMillis() - readStartMillis);
		LOGGER.trace("END: stop()");
	}

	@Override
	public boolean isDictionaryAvailable() {
		return dictionaryAvailable;
	}

	@Override
	public long getDbId() {
		return dbId;
	}

	@Override
	public String getDbUniqueName() {
		return dbUniqueName;
	}

	private void printRedoLogInfo(final boolean archived,
			final String fileName, final int thread, final long sequence, final long logFileFirstChange) {
		LOGGER.info("Adding {} log {} thread# {} sequence# {} first change number {} next log first change {}",
				archived ? "archived" : "online", fileName, thread, sequence, logFileFirstChange, nextChange);
	}

}
