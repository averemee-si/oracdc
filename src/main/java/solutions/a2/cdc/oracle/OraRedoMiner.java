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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.internals.OraCdcRedoLog;
import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
import solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmtIntf;

/**
 * 
 * Mine redo log's for changes
 * 
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class OraRedoMiner {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraRedoMiner.class);

	private final OraCdcSourceConnectorConfig config;
	private long firstChange;
	private long currentFirstChange;
	private long nextChange = 0;
	private long lastSequence = 0;
	private final String connectorName;
	private final boolean dictionaryAvailable;
	private final boolean processOnlineRedoLogs;
	private final int onlineRedoQueryMsMin;
	private final OraCdcLogMinerMgmtIntf metrics;
	private PreparedStatement psGetArchivedLogs;
	private PreparedStatement psUpToCurrentScn;
	private long archLogSize = 0;
	private String currentRedoLog = null;
	private String lastProcessedRedoLog = null;
	private long readStartMillis;
	private final OraRdbmsInfo rdbmsInfo;
	private long lastOnlineRedoTime = 0;
	private final boolean printAllOnlineScnRanges;
	private long lastOnlineSequence = 0;
	private final boolean useNotifier;
	private final LastProcessedSeqNotifier notifier;
	private OraCdcRedoLog redoLog;
	private Iterator<OraCdcRedoRecord> miner;

	public OraRedoMiner(
			final Connection connection,
			final OraCdcLogMinerMgmtIntf metrics,
			final long firstChange,
			final OraCdcSourceConnectorConfig config,
			final CountDownLatch runLatch,
			final OraRdbmsInfo rdbmsInfo,
			final OraConnectionObjects oraConnections) throws SQLException {
		this.config = config;
		this.metrics = metrics;
		this.rdbmsInfo = rdbmsInfo;
		this.connectorName = config.getConnectorName();
		this.notifier = config.getLastProcessedSeqNotifier();
		if (notifier == null) {
			useNotifier = false;
		} else {
			useNotifier = true;
			notifier.configure(config);
		}

		processOnlineRedoLogs = config.getBoolean(ParamConstants.PROCESS_ONLINE_REDO_LOGS_PARAM);
		if (processOnlineRedoLogs) {
			printAllOnlineScnRanges = config.getBoolean(ParamConstants.PRINT_ALL_ONLINE_REDO_RANGES_PARAM);
			onlineRedoQueryMsMin = config.getInt(ParamConstants.CURRENT_SCN_QUERY_INTERVAL_PARAM);
		} else {
			printAllOnlineScnRanges = false;
			onlineRedoQueryMsMin = Integer.MIN_VALUE;
		}

		this.firstChange = firstChange;
		createStatements(connection);

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
	}

	public void createStatements(final Connection connection) throws SQLException {
		psGetArchivedLogs = connection.prepareStatement(OraDictSqlTexts.ARCHIVED_LOGS,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		if (processOnlineRedoLogs) {
			psUpToCurrentScn = connection.prepareStatement(OraDictSqlTexts.UP_TO_CURRENT_SCN,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		}
	}

	/**
	 * Prepare next redo log for file mining
	 * 
	 * @return  - true if the redo log is prepared, false if there are no more redo log files available
	 * @throws SQLException
	 */
	public boolean next() throws SQLException {
		boolean redoLogAvailable = false;
		boolean limits = false;

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
			if (sequence >= lastSequence && firstChange < nextChange) {
				// #25 BEGIN - hole in SEQUENCE# numbering  in V$ARCHIVED_LOG
				if ((lastSequence - sequence) > 1) {
					LOGGER.warn("Gap in V$ARCHIVED_LOG numbering detected between SEQUENCE# {} and {}",
								lastSequence, sequence);
					firstChange = rs.getLong("FIRST_CHANGE#");
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
					currentRedoLog = rs.getString("NAME");
					if (firstChange > rs.getLong("FIRST_CHANGE#")) {
						limits = true;
					}
					printRedoLogInfo(true, true,
								currentRedoLog, rs.getShort("THREAD#"), lastSequence, rs.getLong("FIRST_CHANGE#"));
					redoLogAvailable = true;
					archLogSize = rs.getLong("BYTES");
					break;
				}
			}
		}
		rs.close();
		rs = null;
		psGetArchivedLogs.clearParameters();

/*
		boolean processOnlineRedo = false;
		if (!redoLogAvailable) {
			if (!processOnlineRedoLogs) {
				LOGGER.debug("END: no archived redo yet, return false");
				return false;
			} else {
				// Both callDbmsLogmnrAddLogFile and processOnlineRedoLogs are TRUE
				if (lastOnlineRedoTime != 0 && 
						((int)(System.currentTimeMillis() - lastOnlineRedoTime) < onlineRedoQueryMsMin)) {
					LOGGER.debug("END time is below threshold, return false");
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
					LOGGER.trace("END: no new data in online redo, return false");
					return false;
				}

				if (!StringUtils.equals(onlineRedoMember, currentRedoLog)) {
					lastProcessedRedoLog = currentRedoLog;
					currentRedoLog = onlineRedoMember;
				} else {
					//TODO
					//TODO - fast start from last RBA!
					//TODO
				}
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
*/
		if (redoLogAvailable) {
			metrics.setNowProcessed(List.of(currentRedoLog), firstChange, nextChange, lagSeconds);
			try {
				redoLog = new OraCdcRedoLog(config.convertRedoFileName(currentRedoLog));
				if (limits) {
					miner = redoLog.iterator(firstChange, nextChange);
				} else {
					miner = redoLog.iterator();
				}
				currentFirstChange = firstChange;
				firstChange = nextChange;
				readStartMillis = System.currentTimeMillis();
			} catch (IOException ioe) {
				throw new SQLException(ioe);
			}
		}
		return redoLogAvailable;
	}

	public void stop() throws SQLException, IOException {
		lastProcessedRedoLog = currentRedoLog;
		miner = null;
		redoLog.close();
		// Add info about processed files to JMX
		metrics.addAlreadyProcessed(List.of(currentRedoLog), 1, archLogSize,
				System.currentTimeMillis() - readStartMillis);
	}

	public Iterator<OraCdcRedoRecord> iterator() {
		return miner;
	}

	public boolean isDictionaryAvailable() {
		return dictionaryAvailable;
	}

	public long getDbId() {
		return rdbmsInfo.getDbId();
	}

	public String getDbUniqueName() {
		return rdbmsInfo.getDbUniqueName();
	}

	public void setFirstChange(final long firstChange) throws SQLException {
		this.firstChange = firstChange;
	}

	public long getFirstChange() {
		return currentFirstChange;
	}

	public long getNextChange() {
		return nextChange;
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
