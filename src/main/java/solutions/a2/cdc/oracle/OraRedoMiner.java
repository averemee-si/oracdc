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
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.internals.OraCdcRedoLog;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogAsmFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogFileFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogSmbjFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogSshjFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogSshtoolsMaverickFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
import solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmtIntf;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.utils.BinaryUtils;

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
	private RedoByteAddress firstRba;
	private boolean inited = true;
	private long nextChange = 0;
	private long lastSequence = 0;
	private final String connectorName;
	private final boolean processOnlineRedoLogs;
	private final int onlineRedoQueryMsMin;
	private final OraCdcLogMinerMgmtIntf metrics;
	private PreparedStatement psGetArchivedLogs;
	private PreparedStatement psUpToCurrentScn;
	private long redoLogSize;
	private String currentRedoLog = null;
	private RedoByteAddress lastProcessedRba;
	private long readStartMillis;
	private final OraRdbmsInfo rdbmsInfo;
	private long lastOnlineRedoTime = 0;
	private final boolean printAllOnlineScnRanges;
	private long lastOnlineSequence = 0;
	private final boolean useNotifier;
	private final LastProcessedSeqNotifier notifier;
	private OraCdcRedoLog redoLog;
	private Iterator<OraCdcRedoRecord> miner;
	private boolean processOnlineRedo = false;
	private final boolean asm;
	private final boolean ssh;
	private final boolean smb;
	private final boolean bfile;
	private final boolean reconnect;
	private final OraCdcRedoLogFactory rlf;
	private final long reconnectIntervalMs;
	private final OraConnectionObjects oraConnections;
	private long sessionStartMs = 0;
	private final int backofMs;
	private final boolean needNameChange;
	private boolean sshMaverick;

	public OraRedoMiner(
			final Connection connection,
			final OraCdcLogMinerMgmtIntf metrics,
			final Triple<Long, RedoByteAddress, Long> startFrom,
			final OraCdcSourceConnectorConfig config,
			final CountDownLatch runLatch,
			final OraRdbmsInfo rdbmsInfo,
			final OraConnectionObjects oraConnections,
			final BinaryUtils bu) throws SQLException {
		this.config = config;
		this.metrics = metrics;
		this.rdbmsInfo = rdbmsInfo;
		this.connectorName = config.getConnectorName();
		this.asm = config.useAsm();
		this.ssh = config.useSsh();
		this.smb = config.useSmb();
		//TODO
		this.bfile = false;
		this.reconnect = asm || ssh || bfile || smb;
		this.notifier = config.getLastProcessedSeqNotifier();
		this.backofMs = config.connectionRetryBackoff();
		config.msWindows(rdbmsInfo.isWindows());
		if (notifier == null) {
			useNotifier = false;
		} else {
			useNotifier = true;
			notifier.configure(config);
		}
		if (asm) {
			needNameChange = false;
			rlf = new OraCdcRedoLogAsmFactory(oraConnections.getAsmConnection(config),
					bu, true, config.asmReadAhead());
			this.oraConnections = oraConnections;
		} else if (ssh) {
			needNameChange = rdbmsInfo.isWindows();
			try {
				if (config.sshProviderMaverick()) {
					rlf = new OraCdcRedoLogSshtoolsMaverickFactory(config, bu, true);
					sshMaverick = true;
				} else {
					rlf = new OraCdcRedoLogSshjFactory(config, bu, true);
					sshMaverick = false;
				}
			} catch (IOException ioe) {
				throw new SQLException(ioe);
			}
			this.oraConnections = null;
		} else if (smb) {
			needNameChange = true;
			try {
				rlf = new OraCdcRedoLogSmbjFactory(config, bu, true);
			} catch (IOException ioe) {
				throw new SQLException(ioe);
			}
			this.oraConnections = null;
		} else {
			needNameChange = true;
			rlf = new OraCdcRedoLogFileFactory(bu, true);
			this.oraConnections = null;
		}
		if (asm)
			reconnectIntervalMs = config.asmReconnectIntervalMs();
		else if (ssh)
			reconnectIntervalMs = config.sshReconnectIntervalMs();
		else
			reconnectIntervalMs = config.smbReconnectIntervalMs();

		processOnlineRedoLogs = config.getBoolean(ParamConstants.PROCESS_ONLINE_REDO_LOGS_PARAM);
		if (processOnlineRedoLogs) {
			printAllOnlineScnRanges = config.getBoolean(ParamConstants.PRINT_ALL_ONLINE_REDO_RANGES_PARAM);
			onlineRedoQueryMsMin = config.getInt(ParamConstants.CURRENT_SCN_QUERY_INTERVAL_PARAM);
		} else {
			printAllOnlineScnRanges = false;
			onlineRedoQueryMsMin = Integer.MIN_VALUE;
		}

		this.firstChange = startFrom.getLeft();
		if (startFrom.getMiddle() != null) {
			inited = false;
			firstRba = startFrom.getMiddle();
		}
		createStatements(connection);

		final StringBuilder sb = new StringBuilder(512);
		sb.append("\n=====================\n");
		if (rdbmsInfo.isStandby()) {
			if (StringUtils.equals(OraRdbmsInfo.MOUNTED, rdbmsInfo.getOpenMode())) {
				sb.append("oracdc will use connection to Oracle DataGuard with a unique name {} in {} state.\n");
			} else {
				sb.append("oracdc will use connection to Oracle Active DataGuard Database with a unique name {} in {} state.\n");
			}
		} else {
			sb.append("oracdc will use connection to Oracle Database with a unique name {} in {} state to query the dictionary.\n");
		}
		sb
			.append("Oracle Database DBID is {}, RedoMiner will start from SCN {}.")
			.append("\n=====================\n");
			
		LOGGER.info(
				sb.toString(),
				rdbmsInfo.getDbUniqueName(), rdbmsInfo.getOpenMode(), rdbmsInfo.getDbId(), firstChange);
		// It's time to init JMS metrics...
		metrics.start(firstChange);

		if (asm || ssh) {
			sessionStartMs = System.currentTimeMillis();
		}
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
		long blocks = 0;

		psGetArchivedLogs.setLong(1, firstChange);
		psGetArchivedLogs.setLong(2, firstChange);
		psGetArchivedLogs.setLong(3, firstChange);
		psGetArchivedLogs.setInt(4, rdbmsInfo.getRedoThread());
		psGetArchivedLogs.setInt(5, rdbmsInfo.getRedoThread());
		ResultSet rs = psGetArchivedLogs.executeQuery();
		int lagSeconds = 0;
		short blockSize = 0x200;
		while (rs.next()) {
			final long sequence = rs.getLong("SEQUENCE#");
			nextChange = rs.getLong("NEXT_CHANGE#");
			lagSeconds = rs.getInt("ACTUAL_LAG_SECONDS");
			blockSize = rs.getShort("BLOCK_SIZE");
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
					blocks = rs.getLong("BLOCKS");
					redoLogSize = rs.getShort("BLOCK_SIZE") * blocks;
					break;
				}
			}
		}
		rs.close();
		rs = null;
		psGetArchivedLogs.clearParameters();

		processOnlineRedo = false;
		if (!redoLogAvailable) {
			if (!processOnlineRedoLogs) {
				LOGGER.debug("END: no archived redo yet, return false");
				return false;
			} else {
				if (lastOnlineRedoTime != 0 && 
						((int)(System.currentTimeMillis() - lastOnlineRedoTime) < onlineRedoQueryMsMin)) {
					LOGGER.debug("END time is below threshold, return false");
					return false;
				}
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
					blockSize = rsUpToCurrentScn.getShort(5);
					blocks = rsUpToCurrentScn.getLong(6)/blockSize;
				} else {
					throw new SQLException("Unable to execute\n" + OraDictSqlTexts.UP_TO_CURRENT_SCN + "\n!!!");
				}
				rsUpToCurrentScn.close();
				rsUpToCurrentScn = null;
				if ((nextChange - 1) < firstChange) {
					LOGGER.trace("END: no new data in online redo, return false");
					return false;
				}

				currentRedoLog = onlineRedoMember;
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
				lastOnlineRedoTime = System.currentTimeMillis();
				processOnlineRedo = true;
				redoLogAvailable = true;
			}
		}

		if (redoLogAvailable) {
			metrics.setNowProcessed(List.of(currentRedoLog), firstChange, nextChange, lagSeconds);
			try {
				if (reconnect) {
					final long sessionElapsed = System.currentTimeMillis() - sessionStartMs;
					if (sessionElapsed > reconnectIntervalMs) {
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("Recreating {} connection after {} ms.",
									readerName(), sessionElapsed);
						}
						boolean done = false;
						int attempt = 0;
						final long reconnectStart = System.currentTimeMillis();
						SQLException lastException = null;
						while (!done) {
							if (attempt > Byte.MAX_VALUE)
								break;
							else
								attempt++;
							try {
								if (asm)
									((OraCdcRedoLogAsmFactory) rlf).reset(oraConnections.getAsmConnection(config));
								else if (ssh)
									if (sshMaverick)
										((OraCdcRedoLogSshtoolsMaverickFactory) rlf).reset();
									else
										((OraCdcRedoLogSshjFactory) rlf).reset();
								else
									((OraCdcRedoLogSmbjFactory) rlf).reset();
								done = true;
								sessionStartMs = System.currentTimeMillis();
							} catch (SQLException sqle) {
								lastException = sqle;
								if (asm || bfile)
									LOGGER.error(
										"\n=====================\n" +
										"Failed to reconnect (attempt #{}) to {} due to '{}'.\nSQL Error Code = {}, SQL State = '{}'" +
										"oarcdc will try again to reconnect in {} ms." + 
										"\n=====================\n",
										attempt, readerName(), sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState(), backofMs);
								else
									LOGGER.error(
										"\n=====================\n" +
										"Failed to reconnect (attempt #{}) to {} due to '{}'.\n" +
										"oarcdc will try again to reconnect in {} ms." + 
										"\n=====================\n",
										readerName(), attempt, sqle.getMessage(), backofMs);
								try {Thread.sleep(backofMs);} catch (InterruptedException ie) {}
							}
						}
						if (!done) {
							LOGGER.error(
									"\n=====================\n" +
									"Failed to reconnect to {} after {} attempts in {} ms." +
									"\n=====================\n",
									readerName(), attempt, (System.currentTimeMillis() - reconnectStart));
							if (lastException != null)
								throw new SQLException(lastException);
							else
								throw new SQLException("Unable to reconnect to " + readerName());
						} else {
							LOGGER.info(
									"Reconnection to {} completed in {} ms.",
									readerName(), (System.currentTimeMillis() - reconnectStart));
						}
					}
				}
				redoLog = rlf.get(
						(needNameChange) ? config.convertRedoFileName(currentRedoLog) : currentRedoLog,
						processOnlineRedo, blockSize, blocks);
				if (inited) {
					if (limits || processOnlineRedo) {
						if (lastProcessedRba != null && redoLog.sequence() == lastProcessedRba.sqn()) {
							miner = redoLog.iterator(lastProcessedRba, nextChange);
							if (miner.hasNext()) {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug(
											"Iterator started from lastProcessedRba {} to nextChange {}",
											lastProcessedRba, nextChange);
								}
							} else {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug(
											"No next record after starting iterator from lastProcessedRba {} to nextChange {}",
											lastProcessedRba, nextChange);
								}
								LOGGER.warn("Unexpected miner.hasNext() == false after RBA {}", lastProcessedRba);
								miner = null;
								redoLog.close();
								return false;
							}
						} else {
							miner = redoLog.iterator(firstChange, nextChange);
						}
					} else {
						miner = redoLog.iterator();
					}
				} else {
					miner = redoLog.iterator(firstRba, nextChange);
					inited = true;
				}
				firstChange = nextChange;
				readStartMillis = System.currentTimeMillis();
			} catch (IOException ioe) {
				throw new SQLException(ioe);
			}
		}
		return redoLogAvailable;
	}

	public void stop(final RedoByteAddress lastRba, final long lastScn) throws SQLException, IOException {
		lastProcessedRba = lastRba;
		if (processOnlineRedo) {
			firstChange = lastScn;
		}
		miner = null;
		redoLog.close();
		// Add info about processed files to JMX
		metrics.addAlreadyProcessed(List.of(currentRedoLog), 1, redoLogSize,
				System.currentTimeMillis() - readStartMillis);
	}

	public Iterator<OraCdcRedoRecord> iterator() {
		return miner;
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

	private String readerName() {
		return
				asm ? "Oracle ASM" :
					ssh ? "SSH" :
						bfile ? "Oracle BFILE store" :
							smb ? "SMB" : "FS reader";
	}

}
