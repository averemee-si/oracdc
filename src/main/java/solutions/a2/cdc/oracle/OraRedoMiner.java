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
import java.sql.SQLRecoverableException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.internals.OraCdcRedoLog;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogAsmFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogBfileFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogFileFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogSmbjFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogSshjFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogSshtoolsMaverickFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
import solutions.a2.cdc.oracle.jmx.OraCdcSourceConnMgmt;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.utils.BinaryUtils;

import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_1170;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_2396;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_15173;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_17002;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_17008;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_17410;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_22288;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.UCP_44;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.SQL_STATE_FILE_NOT_FOUND;

/**
 * 
 * Mine redo log's for changes
 * 
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class OraRedoMiner {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraRedoMiner.class);
	private static final int MAX_RETRIES = 63;

	private final OraCdcSourceConnectorConfig config;
	private long firstChange;
	private RedoByteAddress firstRba;
	private long nextChange = 0;
	private long lastSequence = 0;
	private final String connectorName;
	private final int onlineRedoQueryMsMin;
	private final OraCdcSourceConnMgmt metrics;
	private PreparedStatement psGetArchivedLogs;
	private PreparedStatement psUpToCurrentScn;
	private long redoLogSize;
	private String currentRedoLog = null;
	private RedoByteAddress lastProcessedRba;
	private long readStartMillis;
	private final OraRdbmsInfo rdbmsInfo;
	private long lastOnlineRedoTime = 0;
	private long lastOnlineSequence = 0;
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
	final CountDownLatch runLatch;
	private static final byte FLG1_WAIT_ON_ERROR           = (byte)0x01;
	private static final byte FLG1_PRINT_ALL_ONLINE_RANGES = (byte)0x02;
	private static final byte FLG1_INITED                  = (byte)0x04;
	private static final byte FLG1_USE_NOTIFIER            = (byte)0x08;
	private static final byte FLG1_NEED_NAME_CHANGE        = (byte)0x10;
	private static final byte FLG1_PROCESS_ONLINE_REDO     = (byte)0x20;
	private static final byte FLG1_STOP_ON_MISSED_FILE     = (byte)0x40;
	private byte flags1 = FLG1_WAIT_ON_ERROR | FLG1_INITED;

	public OraRedoMiner(
			final Connection connection,
			final OraCdcSourceConnMgmt metrics,
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
		this.bfile = config.useBfile();
		this.reconnect = asm || ssh || bfile || smb;
		this.notifier = config.getLastProcessedSeqNotifier();
		this.backofMs = config.connectionRetryBackoff();
		this.runLatch = runLatch;
		config.msWindows(rdbmsInfo.isWindows());
		if (notifier != null) {
			flags1 |= FLG1_USE_NOTIFIER;
			notifier.configure(config);
		}
		this.oraConnections = oraConnections;
		if (asm) {
			reconnectIntervalMs = config.asmReconnectIntervalMs();
			rlf = new OraCdcRedoLogAsmFactory(oraConnections.getAsmConnection(config),
					bu, true, config.asmReadAhead());
		} else if (ssh) {
			if (rdbmsInfo.isWindows()) flags1 |= FLG1_NEED_NAME_CHANGE;
			reconnectIntervalMs = config.sshReconnectIntervalMs();
			if (config.sshProviderMaverick())
				rlf = new OraCdcRedoLogSshtoolsMaverickFactory(config, bu, true);
			else
				rlf = new OraCdcRedoLogSshjFactory(config, bu, true);
		} else if (bfile) {
			flags1 |= FLG1_NEED_NAME_CHANGE;
			reconnectIntervalMs = config.bfileReconnectIntervalMs();
			rlf = new OraCdcRedoLogBfileFactory(oraConnections.getConnection(),
					config.bfileDirOnline(), config.bfileDirArchive(), config.bfileBufferSize(),
					bu, true);
		} else if (smb) {
			flags1 |= FLG1_NEED_NAME_CHANGE;
			reconnectIntervalMs = config.smbReconnectIntervalMs();
			rlf = new OraCdcRedoLogSmbjFactory(config, bu, true);
		} else {
			flags1 |= FLG1_NEED_NAME_CHANGE;
			reconnectIntervalMs = Long.MAX_VALUE;
			rlf = new OraCdcRedoLogFileFactory(bu, true);
		}

		if (config.processOnlineRedoLogs()) {
			flags1 |= FLG1_PROCESS_ONLINE_REDO;
			if (config.printAllOnlineRedoRanges()) flags1 |= FLG1_PRINT_ALL_ONLINE_RANGES;
			onlineRedoQueryMsMin = config.currentScnQueryInterval();
		} else {
			onlineRedoQueryMsMin = Integer.MIN_VALUE;
		}
		if (config.stopOnMissedLogFile()) flags1 |= FLG1_STOP_ON_MISSED_FILE;
		this.firstChange = startFrom.getLeft();
		if (startFrom.getMiddle() != null) {
			flags1 &= (~FLG1_INITED);
			firstRba = startFrom.getMiddle();
		}
		createStatements(connection);

		final StringBuilder sb = new StringBuilder(512);
		sb.append("\n=====================\n");
		if (rdbmsInfo.isStandby()) {
			if (Strings.CS.equals(OraRdbmsInfo.MOUNTED, rdbmsInfo.getOpenMode())) {
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

		if (asm || ssh || smb || bfile) {
			sessionStartMs = System.currentTimeMillis();
		}
	}

	public void createStatements(final Connection connection) throws SQLException {
		psGetArchivedLogs = connection.prepareStatement(OraDictSqlTexts.ARCHIVED_LOGS,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		if ((flags1 & FLG1_PROCESS_ONLINE_REDO) > 0) {
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
		var redoLogAvailable = false;
		var limits = false;
		long blocks = 0;
		flags1 |= FLG1_WAIT_ON_ERROR;

		ResultSet rs = null;
		var rsReady = false;
		while (!rsReady && runLatch.getCount() > 0) {
			try {
				psGetArchivedLogs.setLong(1, firstChange);
				psGetArchivedLogs.setLong(2, firstChange);
				psGetArchivedLogs.setLong(3, firstChange);
				psGetArchivedLogs.setInt(4, rdbmsInfo.getRedoThread());
				psGetArchivedLogs.setInt(5, rdbmsInfo.getRedoThread());
				rs = psGetArchivedLogs.executeQuery();
				rsReady = true;
			} catch (SQLException sqle) {
				if (sqle.getErrorCode() == ORA_2396 ||
						sqle.getErrorCode() == ORA_17002 ||
						sqle.getErrorCode() == ORA_17008 ||
						sqle.getErrorCode() == ORA_17410 ||
						(sqle.getErrorCode() == UCP_44 && sqle.getSQLState() == null) ||
						sqle instanceof SQLRecoverableException ||
						(sqle.getCause() != null && sqle.getCause() instanceof SQLRecoverableException)) {
					LOGGER.warn(
							"""
							
							=====================
							Encontered an 'ORA-{}: {}', SQLState = '{}'
							Attempting to reconnect to dictionary...
							=====================
							
							""", sqle.getErrorCode(), sqle.getMessage(), sqle.getSQLState());
					try {
						var ready = false;
						var retries = 0;
						while (!ready) {
							try {
								Connection connection = oraConnections.getConnection();
								createStatements(connection);
							} catch(SQLException sqleRestore) {
								if (retries > MAX_RETRIES) {
									LOGGER.error(
											"""
											
											=====================
											Unable to restore dictionary connection after {} retries!
											=====================
											
											""", retries);
									throw sqleRestore;
								}
							}
							ready = true;
							if (!ready) {
								long waitTime = (long) Math.pow(2, retries++) + config.connectionRetryBackoff();
								LOGGER.warn("Waiting {} ms for dictionary connection to restore...", waitTime);
								try {
									this.wait(waitTime);
								} catch (InterruptedException ie) {}
							}
						}
					} catch (SQLException ucpe) {
						LOGGER.error(
								"""
								
								=====================
								SQL errorCode = {}, SQL state = '{}' while restarting connection to dictionary tables
								SQL error message = {}
								=====================
								
								""", ucpe.getErrorCode(), ucpe.getSQLState(), ucpe.getMessage());
						throw new SQLException(sqle);
					}
				} else {
					LOGGER.error(
							"""
							
							=====================
							SQL errorCode = {}, SQL state = '{}' while trying to SELECT from dictionary tables
							SQL error message = {}
							=====================
							
							""", sqle.getErrorCode(), sqle.getSQLState(), sqle.getMessage());
					throw new SQLException(sqle);
				}
			}
		}
		int lagSeconds = 0;
		short blockSize = 0x200;
		while (rs.next()) {
			final var sequence = rs.getLong("SEQUENCE#");
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
						if ((flags1 & FLG1_USE_NOTIFIER) > 0) {
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
			if ((flags1 & FLG1_PROCESS_ONLINE_REDO) == 0) {
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
				var rsUpToCurrentScn = psUpToCurrentScn.executeQuery();
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
				if ((flags1 & FLG1_PRINT_ALL_ONLINE_RANGES) > 0) {
					printRedoLogInfo(false, true, onlineRedoMember, rdbmsInfo.getRedoThread(), onlineSequence, firstChange);
				}
				if (lastOnlineSequence != onlineSequence) {
					if (lastOnlineSequence > lastSequence && lastSequence > 0) {
						metrics.setLastProcessedSequence(lastSequence);
						if ((flags1 & FLG1_USE_NOTIFIER) > 0) {
							notifier.notify(Instant.now(), lastSequence, "ONLINE");
						}
						lastSequence = lastOnlineSequence;
					}
					lastOnlineSequence = onlineSequence;
					if ((flags1 & FLG1_PRINT_ALL_ONLINE_RANGES) == 0) {
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
						var done = false;
						var attempt = 0;
						final var reconnectStart = System.currentTimeMillis();
						SQLException lastException = null;
						while (!done && runLatch.getCount() > 0) {
							if (attempt > Byte.MAX_VALUE)
								break;
							else
								attempt++;
							try {
								if (asm)
									rlf.reset(oraConnections.getAsmConnection(config));
								else if (bfile)
									rlf.reset(oraConnections.getConnection());
								else
									rlf.reset();
								done = true;
								sessionStartMs = System.currentTimeMillis();
							} catch (SQLException sqle) {
								lastException = sqle;
								if (asm || bfile)
									LOGGER.error(
										"""
										
										=====================
										Failed to reconnect (attempt #{}) to {} due to '{}'.
										SQL Error Code = {}, SQL State = '{}'
										oracdc will try again to reconnect in {} ms. 
										=====================
										
										""", attempt, readerName(),
										sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState(), backofMs);
								else
									LOGGER.error(
										"""
										
										=====================
										Failed to reconnect (attempt #{}) to {} due to '{}'.
										oracdc will try again to reconnect in {} ms. 
										=====================
										
										""", readerName(), attempt, sqle.getMessage(), backofMs);
								try {Thread.sleep(backofMs);} catch (InterruptedException ie) {}
							}
						}
						if (!done) {
							LOGGER.error(
									"""
									
									=====================
									Failed to reconnect to {} after {} attempts in {} ms.
									=====================
									
									""", readerName(), attempt, (System.currentTimeMillis() - reconnectStart));
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
				try {
					redoLog = rlf.get(
						(flags1 & FLG1_NEED_NAME_CHANGE) > 0 ? config.convertRedoFileName(currentRedoLog, bfile) : currentRedoLog,
						processOnlineRedo, blockSize, blocks);
				} catch (SQLException sqle) {
					if ((sqle.getErrorCode() == ORA_1170 &&
							Strings.CS.equals(sqle.getSQLState(), SQL_STATE_FILE_NOT_FOUND)) ||
						(sqle.getErrorCode() == ORA_15173) ||
						(sqle.getErrorCode() == ORA_22288)) {
						if ((flags1 & FLG1_STOP_ON_MISSED_FILE) > 0) {
							runLatch.countDown();
							LOGGER.error(
									"""
									
									=====================
									Missed redo log file '{}'!
									oracdc terminated.
									=====================
									
									""", (flags1 & FLG1_NEED_NAME_CHANGE) > 0 ? config.convertRedoFileName(currentRedoLog, bfile) : currentRedoLog);
							throw new ConnectException("Missed redo log file " + currentRedoLog + " !");
						} else {
							firstChange = nextChange;
							flags1 &= (~FLG1_WAIT_ON_ERROR);
						}
					}
					throw sqle;
				}
				if ((flags1 & FLG1_INITED) > 0) {
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
								try {
									redoLog.close();
								} catch (IOException ioe) {
									throw new SQLException(ioe);
								}
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
					flags1 |= FLG1_INITED;
				}
				firstChange = nextChange;
				readStartMillis = System.currentTimeMillis();
			} catch (SQLException sqle) {
				if (runLatch.getCount() > 0)
					throw sqle;
				else
					return false;
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
		final var sb = new StringBuilder(0x200);
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

	void resetRedoLogFactory(final long startScn, final RedoByteAddress startRba) throws SQLException {
		if (asm)
			rlf.reset(oraConnections.getAsmConnection(config));
		else if (bfile)
			rlf.reset(oraConnections.getConnection());
		else
			rlf.reset();
		flags1 &= (~FLG1_INITED);
		firstChange = startScn;
		firstRba = startRba;
	}

	boolean waitOnError() {
		return (flags1 & FLG1_WAIT_ON_ERROR) > 0;
	}

}
