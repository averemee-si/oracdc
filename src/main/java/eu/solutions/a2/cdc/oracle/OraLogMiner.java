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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmt;

/**
 * 
 * Wrapper for LogMiner operations
 * 
 * 
 * @author averemee
 */
public class OraLogMiner {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraLogMiner.class);

	private long firstChange;
	private long nextChange = 0;
	private long lastSequence = -1;
	private int numArchLogs;
	private long sizeOfArchLogs;
	private final boolean useNumOfArchLogs;
	private final boolean dictionaryAvailable;
	private final long dbId;
	private final String dbUniqueName;
	private final OraCdcLogMinerMgmt metrics;
	private PreparedStatement psGetArchivedLogs;
	private CallableStatement csAddArchivedLogs;
	private CallableStatement csStartLogMiner;
	private CallableStatement csStopLogMiner;
	int archLogsAvailable = 0;
	long archLogsSize = 0;
	List<String> fileNames = new ArrayList<>();

	public static final short V$LOGMNR_CONTENTS_INSERT = 1;
	public static final short V$LOGMNR_CONTENTS_DELETE = 2;
	public static final short V$LOGMNR_CONTENTS_UPDATE = 3;
	public static final short V$LOGMNR_CONTENTS_COMMIT = 7;
	public static final short V$LOGMNR_CONTENTS_ROLLBACK = 36;

	public OraLogMiner(final Connection connection, final OraCdcLogMinerMgmt metrics,
			final long firstChange, final int numArchLogs) throws SQLException {
		this(connection, metrics, firstChange, numArchLogs, null);
	}

	public OraLogMiner(final Connection connection, final OraCdcLogMinerMgmt metrics,
			final long firstChange, final long sizeOfArchLogs) throws SQLException {
		this(connection, metrics, firstChange, null, sizeOfArchLogs);
	}

	private OraLogMiner(
			final Connection connection, final OraCdcLogMinerMgmt metrics, final long firstChange,
			final Integer numArchLogs, final Long sizeOfArchLogs) throws SQLException {
		LOGGER.trace("BEGIN: OraLogMiner Constructor");
		this.metrics = metrics;
		if (numArchLogs == null) {
			LOGGER.trace("Limit based of size in bytes of archived logs will be used");
			useNumOfArchLogs = false;
			this.sizeOfArchLogs = sizeOfArchLogs;
		} else {
			LOGGER.trace("Limit based of number of archived logs will be used");
			useNumOfArchLogs = true;
			this.numArchLogs = numArchLogs;
		}
		this.firstChange = firstChange;
		psGetArchivedLogs = connection.prepareStatement(OraDictSqlTexts.ARCHIVED_LOGS,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		csAddArchivedLogs = connection.prepareCall(OraDictSqlTexts.ADD_ARCHIVED_LOG);
		csStartLogMiner = connection.prepareCall(OraDictSqlTexts.START_LOGMINER);
		csStopLogMiner = connection.prepareCall(OraDictSqlTexts.STOP_LOGMINER);
		PreparedStatement psOpenMode = connection.prepareStatement(OraDictSqlTexts.RDBMS_OPEN_MODE,
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
		this.metrics.start(this.firstChange);
		LOGGER.trace("END: OraLogMiner Constructor");
	}

	/**
	 * Prepare LogMiner (exec DBMS_LOGMNR.START_LOGMNR) for given connection
	 * 
	 * 
	 * @return  - true if LogMiner prepared, false if no more redo files available
	 * @throws SQLException
	 */
	public boolean next() throws SQLException {
		LOGGER.trace("BEGIN: next()");
		archLogsAvailable = 0;
		archLogsSize = 0;

		psGetArchivedLogs.setLong(1,  firstChange);
		psGetArchivedLogs.setLong(2,  firstChange);
		psGetArchivedLogs.setLong(3,  firstChange);
		ResultSet rs = psGetArchivedLogs.executeQuery();
		fileNames = new ArrayList<>();
		while (rs.next()) {
			final long sequence = rs.getLong("SEQUENCE#");
			nextChange = rs.getLong("NEXT_CHANGE#");
			if (sequence > lastSequence) {
				if (firstChange < nextChange) {
					lastSequence = sequence;
					fileNames.add(archLogsAvailable, rs.getString("NAME"));
					LOGGER.info("Adding archived log {} thread# {} sequence# {} first change number {} next log first change {}",
							rs.getString("NAME"), rs.getShort("THREAD#"), lastSequence,
							rs.getLong("FIRST_CHANGE#"), nextChange);
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
		// Set current processing in JMX
		metrics.setNowProcessed(fileNames, firstChange, nextChange);

		if (archLogsAvailable == 0) {
			LOGGER.trace("END: next() return false");
			return false;
		} else {
			LOGGER.trace("Adding files to LogMiner session and starting it");
			for (int fileNum = 0; fileNum < fileNames.size(); fileNum++) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Adding {} to LogMiner pricessing list.", fileNames.get(fileNum));
				}
				csAddArchivedLogs.setInt(1, fileNum);
				csAddArchivedLogs.setString(2, fileNames.get(fileNum));
				csAddArchivedLogs.addBatch();
			}
			csAddArchivedLogs.executeBatch();
			csAddArchivedLogs.clearBatch();

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Attempting to start LogMiner for SCN range from {} to {}.", firstChange, nextChange);
			}
			csStartLogMiner.setLong(1, firstChange); 
			csStartLogMiner.setLong(2, nextChange); 
			csStartLogMiner.execute();
			csStartLogMiner.clearParameters();
			firstChange = nextChange;
			LOGGER.trace("END: next() return true");
			return true;
		}
	}

	public void stop() throws SQLException {
		LOGGER.trace("BEGIN: stop()");
		csStopLogMiner.execute();
		// Add info about processed files to JMX
		metrics.addAlreadyProcessed(fileNames, archLogsAvailable, archLogsSize);
		LOGGER.trace("END: stop()");
	}

	public boolean isDictionaryAvailable() {
		return dictionaryAvailable;
	}

	public long getDbId() {
		return dbId;
	}

	public String getDbUniqueName() {
		return dbUniqueName;
	}
}
