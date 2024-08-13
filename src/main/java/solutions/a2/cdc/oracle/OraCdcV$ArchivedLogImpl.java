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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.StringUtils;
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

	private long firstChange;
	private long sessionFirstChange;
	private long nextChange = 0;
	private long lastSequence = 0;
	private final String connectorName;
	private final boolean dictionaryAvailable;
	private final boolean callDbmsLogmnrAddLogFile;
	private final boolean processOnlineRedoLogs;
	private final int onlineRedoQueryMsMin;
	private final OraCdcLogMinerMgmtIntf metrics;
	private PreparedStatement psGetArchivedLogs;
	private CallableStatement csAddArchivedLogs;
	private CallableStatement csStartLogMiner;
	private CallableStatement csStopLogMiner;
	private PreparedStatement psUpToCurrentScn;
	private long archLogSize = 0;
	private final List<String> fileNames = new ArrayList<>();
	private long readStartMillis;
	private final OraRdbmsInfo rdbmsInfo;
	private long lastOnlineRedoTime = 0;
	private final boolean printAllOnlineScnRanges;
	private long lastOnlineSequence = 0;
	private final boolean useStandby;
	private final boolean stopOnOra1284;
	private final boolean useNotifier;
	private final LastProcessedSeqNotifier notifier;

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
		this.useStandby = config.getBoolean(ParamConstants.MAKE_STANDBY_ACTIVE_PARAM)  ||
							rdbmsInfo.isStandby();
		this.stopOnOra1284 = config.stopOnOra1284();
		this.connectorName = config.getConnectorName();
		this.notifier = config.getLastProcessedSeqNotifier();
		if (notifier == null) {
			useNotifier = false;
		} else {
			useNotifier = true;
			notifier.configure(config);
		}

		if (rdbmsInfo.isCdb() && rdbmsInfo.isPdbConnectionAllowed()) {
			// 19.10+ with connection to PDB
			callDbmsLogmnrAddLogFile = false;
			processOnlineRedoLogs = false;
			onlineRedoQueryMsMin = Integer.MIN_VALUE;
			if (config.getBoolean(ParamConstants.PROCESS_ONLINE_REDO_LOGS_PARAM)) {
				LOGGER.warn(
						"\n" +
						"=====================\n" +
						"Cannot process online redo logs using connection to {}!\n" +
						"Value TRUE of the parameter '{}' is ignored!\n" +
						"=====================\n",
						useStandby ? "standby database" : "PDB",
						ParamConstants.PROCESS_ONLINE_REDO_LOGS_PARAM);
			}
		} else {
			callDbmsLogmnrAddLogFile = true;
			if (useStandby) {
				processOnlineRedoLogs = false;
				onlineRedoQueryMsMin = Integer.MIN_VALUE;
			} else {
				processOnlineRedoLogs = config.getBoolean(ParamConstants.PROCESS_ONLINE_REDO_LOGS_PARAM);
				onlineRedoQueryMsMin = config.getInt(ParamConstants.CURRENT_SCN_QUERY_INTERVAL_PARAM);
			}
		}
		if (processOnlineRedoLogs) {
			printAllOnlineScnRanges = config.getBoolean(ParamConstants.PRINT_ALL_ONLINE_REDO_RANGES_PARAM);
		} else {
			printAllOnlineScnRanges = false;
		}

		this.firstChange = firstChange;
		createStatements(connLogMiner);

		final StringBuilder sb = new StringBuilder(512);
		sb.append("\n=====================\n");
		if (rdbmsInfo.isStandby()) {
			if (StringUtils.equals(OraRdbmsInfo.MOUNTED, rdbmsInfo.getOpenMode())) {
				sb.append("oracdc will use connection to Oracle DataGuard with a unique name {} in {} state to call LogMiner.\n");
				dictionaryAvailable = false;
			} else {
				sb.append("oracdc will use connection to Oracle Active DataGuard Database with a unique name {} in {} state to call LogMiner.\n");
				dictionaryAvailable = true;
			}
		} else {
			sb.append("oracdc will use connection to Oracle Database with a unique name {} in {} state to call LogMiner and query the dictionary.\n");
			dictionaryAvailable = true;
		}
		sb
			.append("Oracle Database DBID is {}, LogMiner will start from SCN {}.")
			.append("\n=====================\n");
			
		LOGGER.info(
				sb.toString(),
				rdbmsInfo.getDbUniqueName(), rdbmsInfo.getOpenMode(), rdbmsInfo.getDbId(), firstChange);
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
			// Initialize list of files only for "next()"
			fileNames.clear();
		} else {
			functionName = "extend()";
		}
		LOGGER.trace("BEGIN: {}", functionName);

		boolean archLogAvailable = false;

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
			lagSeconds = rs.getInt("ACTUAL_LAG_SECONDS");
			if (sequence >= lastSequence) {
				if (firstChange < nextChange) {
					// #25 BEGIN - hole in SEQUENCE# numbering  in V$ARCHIVED_LOG
					if (nextLogs && (lastSequence - sequence) > 1) {
						LOGGER.warn("Gap in V$ARCHIVED_LOG numbering detected between SEQUENCE# {} and {}",
								lastSequence, sequence);
						if (fileNames.size() < 1) {
							firstChange = rs.getLong("FIRST_CHANGE#");
						}
					}
					// #25 END - hole in SEQUENCE# numbering  in V$ARCHIVED_LOG

					if (sequence > lastSequence ||
							(sequence == lastSequence && firstChange < nextChange)) {
						if (sequence > lastSequence && lastSequence > 0) {
							metrics.setLastProcessedSequence(lastSequence);
							if (useNotifier) {
								notifier.notify(Instant.now(), lastSequence);
							}
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug("{} has finished processing SEQUENCE# {}", connectorName, lastSequence);
							}
						}
						lastSequence = sequence;
						final String fileName = rs.getString("NAME");
						fileNames.add(0, fileName);
						printRedoLogInfo(true, true,
								fileName, rs.getShort("THREAD#"), lastSequence, rs.getLong("FIRST_CHANGE#"));
						archLogAvailable = true;
						archLogSize = rs.getLong("BYTES");
						break;
					}
				}
			}
		}
		rs.close();
		rs = null;
		psGetArchivedLogs.clearParameters();

		boolean processOnlineRedo = false;
		if (!archLogAvailable) {
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
				psUpToCurrentScn.setInt(2, rdbmsInfo.getRedoThread());
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

				//TODO next() vs extend()
				fileNames.add(onlineRedoMember);
				if (printAllOnlineScnRanges) {
					printRedoLogInfo(false, true, onlineRedoMember, rdbmsInfo.getRedoThread(), onlineSequence, firstChange);
				}
				if (lastOnlineSequence != onlineSequence) {
					if (lastOnlineSequence > lastSequence && lastSequence > 0) {
						metrics.setLastProcessedSequence(lastSequence);
						if (useNotifier) {
							notifier.notify(Instant.now(), lastSequence, "ONLINE");
						}
						lastSequence = lastOnlineSequence;
					}					
					lastOnlineSequence = onlineSequence;
					if (!printAllOnlineScnRanges) {
						printRedoLogInfo(false, false, onlineRedoMember, rdbmsInfo.getRedoThread(), onlineSequence, firstChange);
					}
				}
				// This must be here as additional warranty against ORA-1291
				lastOnlineRedoTime = System.currentTimeMillis();
			}
		}

		// Set current processing in JMX
		boolean result = true;
		metrics.setNowProcessed(
				fileNames, nextLogs ? firstChange : sessionFirstChange, nextChange, lagSeconds);
		try {
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
			csStartLogMiner.setLong(1, nextLogs ? firstChange : sessionFirstChange); 
			csStartLogMiner.setLong(2, processOnlineRedo ? nextChange - 1 : nextChange);
			csStartLogMiner.execute();
			csStartLogMiner.clearParameters();
		} catch(SQLException sqle) {
			if (sqle.getErrorCode() == OraRdbmsInfo.ORA_1284) {
				LOGGER.error(
						"\n=====================\n" +
						"SQL errorCode = {}, SQL state = '{}' while executing DBMS_LOGMNR.START_LOGMNR!\n" +
						"SQL error message = {}\n" +
						"\n=====================\n",
						sqle.getErrorCode(), sqle.getSQLState(), sqle.getMessage());
				if (stopOnOra1284) {
					throw sqle;
				}
				result = false;
			} else if (sqle.getErrorCode() == OraRdbmsInfo.ORA_1291 &&
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
						"\n=====================\n" +
						"Unable to execute\\n\\t{}\\n\\tusing STARTSCN={} and ENDSCN={}!\n" +
						"\tLogMiner redo files:\n{}" +
						"\n=====================\n",
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
		return result;
	}

	@Override
	public void stop() throws SQLException {
		LOGGER.debug("BEGIN: stop()");
		csStopLogMiner.execute();
		// Add info about processed files to JMX
		metrics.addAlreadyProcessed(fileNames, 1, archLogSize,
				System.currentTimeMillis() - readStartMillis);
		LOGGER.debug("END: stop()");
	}

	@Override
	public void stop(final long firstChange) throws SQLException {
		LOGGER.debug("BEGIN: stop({})", firstChange);
		this.firstChange = firstChange;
		this.stop();
		LOGGER.debug("END: stop({})", firstChange);
	}

	@Override
	public boolean isDictionaryAvailable() {
		return dictionaryAvailable;
	}

	@Override
	public long getDbId() {
		return rdbmsInfo.getDbId();
	}

	@Override
	public String getDbUniqueName() {
		return rdbmsInfo.getDbUniqueName();
	}

	@Override
	public void setFirstChange(final long firstChange) throws SQLException {
		this.firstChange = firstChange;
	}

	private void printRedoLogInfo(final boolean archived, final boolean printNextChange,
			final String fileName, final int thread, final long sequence, final long logFileFirstChange) {
		final StringBuilder sb = new StringBuilder(512);
		if (archived) {
			sb.append("Adding archived log ");
		} else {
			sb.append("Processing online log ");
		}
		sb
			.append(fileName)
			.append(" thread# ")
			.append(thread)
			.append(" sequence# ")
			.append(sequence)
			.append(" first change number ")
			.append(logFileFirstChange);
		if (printNextChange) {
			sb
				.append(" next log first change ")
				.append(nextChange);
		}
		LOGGER.info(sb.toString());
	}

}
