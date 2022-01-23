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

package eu.solutions.a2.cdc.oracle;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmtIntf;

/**
 * 
 * Wrapper for LogMiner operations (V$ARCHIVED_LOG as source) implementation
 * 
 * 
 * @author averemee
 */
public class OraCdcV$ArchivedLogImpl implements OraLogMiner {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcV$ArchivedLogImpl.class);

	private long firstChange;
	private long sessionFirstChange;
	private long nextChange = 0;
	private long lastSequence = -1;
	private int numArchLogs;
	private long sizeOfArchLogs;
	private final boolean useNumOfArchLogs;
	private final boolean dictionaryAvailable;
	private final boolean callDbmsLogmnrAddLogFile;
	private final long dbId;
	private final String dbUniqueName;
	private final OraCdcLogMinerMgmtIntf metrics;
	private PreparedStatement psGetArchivedLogs;
	private CallableStatement csAddArchivedLogs;
	private CallableStatement csStartLogMiner;
	private CallableStatement csStopLogMiner;
	private int archLogsAvailable = 0;
	private long archLogsSize = 0;
	private List<String> fileNames = new ArrayList<>();
	private long readStartMillis;

	public OraCdcV$ArchivedLogImpl(
			final Connection connLogMiner,
			final OraCdcLogMinerMgmtIntf metrics, final long firstChange,
			final Map<String, String> props,
			final CountDownLatch runLatch,
			final OraRdbmsInfo rdbmsInfo,
			final OraConnectionObjects oraConnections) throws SQLException {
		LOGGER.trace("BEGIN: OraLogMiner Constructor");
		this.metrics = metrics;

		if (rdbmsInfo.isCdb() && rdbmsInfo.isPdbConnectionAllowed()) {
			// 19.10+ and connection to PDB
			callDbmsLogmnrAddLogFile = false;
		} else {
			callDbmsLogmnrAddLogFile = true;
		}

		if (props.containsKey(ParamConstants.REDO_FILES_SIZE_PARAM)) {
			LOGGER.trace("Limit based of size in bytes of archived logs will be used");
			useNumOfArchLogs = false;
			this.sizeOfArchLogs = Long.parseLong(props.get(ParamConstants.REDO_FILES_SIZE_PARAM));
		} else {
			LOGGER.trace("Limit based of number of archived logs will be used");
			useNumOfArchLogs = true;
			this.numArchLogs = Integer.parseInt(props.get(ParamConstants.REDO_FILES_COUNT_PARAM));
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
				firstChange = OraRdbmsInfo.firstScnFromArchivedLogs(psGetArchivedLogs.getConnection());
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
					fileNames.add(archLogsAvailable, rs.getString("NAME"));
					LOGGER.info("Adding archived log {} thread# {} sequence# {} first change number {} next log first change {}",
							rs.getString("NAME"), rs.getShort("THREAD#"), lastSequence, rs.getLong("FIRST_CHANGE#"), nextChange);
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

		if (archLogsAvailable == 0) {
			LOGGER.trace("END: {} return false", functionName);
			return false;
		} else {
			// Set current processing in JMX
			metrics.setNowProcessed(
					fileNames, nextLogs ? firstChange : sessionFirstChange, nextChange, lagSeconds);
			if (callDbmsLogmnrAddLogFile) {
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
				csStartLogMiner.setLong(2, nextChange); 
				csStartLogMiner.execute();
				csStartLogMiner.clearParameters();
			} catch(SQLException sqle) {
				LOGGER.error("Unable to execute\n\t{}\n\tusing STARTSCN={} and ENDSCN={}",
						OraDictSqlTexts.START_LOGMINER, nextLogs ? firstChange : sessionFirstChange, nextChange);
				throw new SQLException(sqle);
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

}
