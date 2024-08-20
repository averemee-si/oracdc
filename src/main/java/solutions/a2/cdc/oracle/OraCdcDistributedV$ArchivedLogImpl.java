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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleConnection;
import solutions.a2.cdc.oracle.jmx.OraCdcLogMinerMgmtIntf;
import solutions.a2.cdc.oracle.jmx.OraCdcRedoShipment;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * Wrapper for LogMiner operations (remote V$ARCHIVED_LOG as source) implementation
 * For more information please read
 * https://docs.oracle.com/en/database/oracle/oracle-database/21/sutil/oracle-logminer-utility.html
 * Figure 22-1 and it description https://docs.oracle.com/en/database/oracle/oracle-database/21/sutil/img_text/remote_config.html
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class OraCdcDistributedV$ArchivedLogImpl implements OraLogMiner {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcDistributedV$ArchivedLogImpl.class);

	private long sessionFirstChange;
	private final boolean dictionaryAvailable;
	private final long dbId;
	private long firstChange;
	private long nextChange;	
	private final String dbUniqueName;
	private final OraCdcLogMinerMgmtIntf metrics;
	private CallableStatement csAddArchivedLogs;
	private CallableStatement csStartLogMiner;
	private CallableStatement csStopLogMiner;
	private int archLogsAvailable = 0;
	private long archLogsSize = 0;
	private long readStartMillis;
	private final List<String> fileNames = new ArrayList<>();
	private final BlockingQueue<ArchivedRedoFile> redoFiles = new LinkedBlockingQueue<>();



	public OraCdcDistributedV$ArchivedLogImpl(
			final Connection connLogMiner,
			final OraCdcLogMinerMgmtIntf metrics, final long firstChange,
			final OraCdcSourceConnectorConfig config,
			final CountDownLatch runLatch,
			final OraRdbmsInfo rdbmsInfo,
			final OraConnectionObjects oraConnections) throws SQLException {
		LOGGER.trace("BEGIN: OraLogMiner Constructor");
		this.metrics = metrics;

		createStatements(connLogMiner);
		final String openMode = rdbmsInfo.getOpenMode();
		if (StringUtils.equals(OraRdbmsInfo.MOUNTED, openMode)) {
			LOGGER.trace("LogMiner connection database is in MOUNTED state, no dictionary available.");
			dictionaryAvailable = false;
		} else {
			LOGGER.trace("LogMiner connection database is in {} state, dictionary is available.", openMode);
			dictionaryAvailable = true;
		}
		LOGGER.info("LogMiner will start from SCN {}", firstChange);
		dbId = rdbmsInfo.getDbId();
		dbUniqueName = rdbmsInfo.getDbUniqueName();
		RedoTransportThread rtt = new RedoTransportThread(
				firstChange, config, runLatch, redoFiles, oraConnections, rdbmsInfo);
		rtt.start();
		// It's time to init JMS metrics...
		metrics.start(firstChange);
		LOGGER.trace("END: OraLogMiner Constructor");
	}

	@Override
	public void createStatements(final Connection connLogMiner) throws SQLException {
		csAddArchivedLogs = connLogMiner.prepareCall(OraDictSqlTexts.ADD_ARCHIVED_LOG);
		csStartLogMiner = connLogMiner.prepareCall(OraDictSqlTexts.START_LOGMINER);
		csStopLogMiner = connLogMiner.prepareCall(OraDictSqlTexts.STOP_LOGMINER);
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
		long currentFirst = 0, currentNext = 0;
		int currentLag = 0;

		if (nextLogs) {
			// Initialize list of files only for "next()"
			fileNames.clear();
		}

		ArchivedRedoFile redoFile = redoFiles.poll();
		if (redoFile != null) {
			fileNames.add(redoFile.NAME);
			if (archLogsAvailable == 0) {
				currentFirst = redoFile.FIRST_CHANGE;
				currentLag = redoFile.ACTUAL_LAG_SECONDS();
			}
			currentNext = redoFile.NEXT_CHANGE;
			archLogsAvailable++;
			archLogsSize += redoFile.BYTES;
			LOGGER.info("Adding archived log {} thread# {} sequence# {} first change number {} next log first change {}",
						redoFile.NAME, redoFile.THREAD, redoFile.SEQUENCE, redoFile.FIRST_CHANGE, redoFile.NEXT_CHANGE);

			// Set current processing in JMX
			metrics.setNowProcessed(
					fileNames, nextLogs ? currentFirst : sessionFirstChange, currentNext, currentLag);
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

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Attempting to start LogMiner for SCN range from {} to {}.",
						nextLogs ? currentFirst : sessionFirstChange, currentNext);
			}
			csStartLogMiner.setLong(1, nextLogs ? currentFirst : sessionFirstChange); 
			csStartLogMiner.setLong(2, currentNext);
			csStartLogMiner.execute();
			csStartLogMiner.clearParameters();
			firstChange = currentFirst;
			nextChange = currentNext;
			if (nextLogs) {
				// Set sessionFirstChange only in call to next()
				sessionFirstChange = currentFirst;
			}
			readStartMillis = System.currentTimeMillis();
			LOGGER.trace("END: {} returns true", functionName);
			return true;
		} else {
			LOGGER.trace("END: {} return false", functionName);
			return false;
		}

	}

	@Override
	public void stop() throws SQLException {
		LOGGER.debug("BEGIN: stop()");
		csStopLogMiner.execute();
		// Add info about processed files to JMX
		metrics.addAlreadyProcessed(fileNames, archLogsAvailable, archLogsSize,
				System.currentTimeMillis() - readStartMillis);
		LOGGER.debug("END: stop()");
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

	@Override
	public void setFirstChange(long firstChange) throws SQLException {
		throw new SQLException("setFirstChange(firstChange) is not supported for " + this.getClass().getName());
	}

	@Override
	public long getFirstChange() {
		return firstChange;
	}

	@Override
	public long getNextChange() {
		return nextChange;
	}

	private static class RedoTransportThread extends Thread {

		private final CountDownLatch runLatch;
		private final ZoneId oracleDbZoneId;
		private final BlockingQueue<ArchivedRedoFile> redoFiles;
		private final OraCdcRedoShipment metrics;
		private final InetSocketAddress targetServerAddress;
		private OracleConnection connDictionary;
		private PreparedStatement psGetArchivedLogs;
		private long firstChange;
		private long lastSequence = -1;
		private long nextChange = 0;
		private final OraRdbmsInfo rdbmsInfo;

		RedoTransportThread(
				final long firstChange,
				final OraCdcSourceConnectorConfig config,
				final CountDownLatch runLatch,
				final BlockingQueue<ArchivedRedoFile> redoFiles,
				final OraConnectionObjects oraConnections,
				final OraRdbmsInfo rdbmsInfo) throws SQLException {
			this.setName("OraCdcRedoTransportThread-" + System.nanoTime());
			this.firstChange = firstChange;
			this.runLatch = runLatch;
			this.redoFiles = redoFiles;
			this.rdbmsInfo = rdbmsInfo;
			connDictionary = (OracleConnection) oraConnections.getConnection();
			//TODO
			oracleDbZoneId = TimeZone.getDefault().toZoneId();
			psGetArchivedLogs = connDictionary.prepareStatement(OraDictSqlTexts.ARCHIVED_LOGS,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			final String targetHost = config.getString(ParamConstants.DISTRIBUTED_TARGET_HOST);
			if (StringUtils.isBlank(targetHost)) {
				throw new SQLException("Parameter {} must be set", ParamConstants.DISTRIBUTED_TARGET_HOST);
			}
			final int targetPort = config.getInt(ParamConstants.DISTRIBUTED_TARGET_PORT);
			metrics = new OraCdcRedoShipment(targetHost, targetPort);
			targetServerAddress = new InetSocketAddress(targetHost, targetPort);
		}

		@Override
		public void run() {
			LOGGER.info("BEGIN: RedoTransportThread.run()");
			while (runLatch.getCount() > 0) {
				try {
					psGetArchivedLogs.setLong(1, firstChange);
					psGetArchivedLogs.setLong(2, firstChange);
					psGetArchivedLogs.setLong(3, firstChange);
					psGetArchivedLogs.setInt(4, rdbmsInfo.getRedoThread());
					psGetArchivedLogs.setInt(5, rdbmsInfo.getRedoThread());
					final ResultSet rsArchivedLogFiles = psGetArchivedLogs.executeQuery();
					while (rsArchivedLogFiles.next()) {
						final long sequence = rsArchivedLogFiles.getLong("SEQUENCE#");
						nextChange = rsArchivedLogFiles.getLong("NEXT_CHANGE#");
						if (sequence > lastSequence) {
							if (firstChange < nextChange) {
								final long startNanos = System.nanoTime();
								ArchivedRedoFile redoFile = new ArchivedRedoFile(oracleDbZoneId);
								redoFile.NAME = rsArchivedLogFiles.getString("NAME");
								redoFile.THREAD = rsArchivedLogFiles.getInt("THREAD#");
								redoFile.SEQUENCE = sequence;
								redoFile.FIRST_CHANGE = rsArchivedLogFiles.getLong("FIRST_CHANGE#");
								redoFile.NEXT_CHANGE = nextChange;
								redoFile.BYTES = rsArchivedLogFiles.getLong("BYTES");
								redoFile.FIRST_TIME = rsArchivedLogFiles.getTimestamp("FIRST_TIME");
								// Get file
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("File request for reading {} sent to {}:{}",
											redoFile.NAME,
											targetServerAddress.getHostString(),
											targetServerAddress.getPort());
								}
								final ByteBuffer fileRequest = ByteBuffer.allocate(1024);
								fileRequest.put(redoFile.NAME.getBytes("UTF-8"));
								fileRequest.flip();
								SocketChannel channelToTarget = SocketChannel.open();
								channelToTarget.connect(targetServerAddress);
								channelToTarget.configureBlocking(true);
								channelToTarget.write(fileRequest);
								
								final ByteBuffer fileResponse = ByteBuffer.allocate(1024);
								channelToTarget.read(fileResponse);
								fileResponse.rewind();
								final String responseContent = StringUtils.trim(new String(fileResponse.array(), "UTF-8"));
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Response received:\t{}", responseContent);
								}
								final String[] responseLines = StringUtils.split(responseContent, "\n");
								if (responseLines.length == 3 && "OK".equals(responseLines[0])) {
									metrics.addProcessedFileInfo(
											System.nanoTime() - startNanos,
											redoFile.BYTES,
											redoFile.NAME);
									// Now we need redo log name at target server
									redoFile.NAME = responseLines[1];
								} else {
									throw new IOException("Invalid response!\t" + responseContent);
								}

								redoFiles.add(redoFile);
								firstChange = redoFile.NEXT_CHANGE;
							}
						}
					}
					psGetArchivedLogs.clearParameters();
					//TODO
					//TODO Parameter for wait timeout???
					//TODO
					try {
						Thread.sleep(50);
					} catch (InterruptedException ie) {
						throw new SQLException(ie);
					}
				} catch (SQLException | IOException sqle) {
					LOGGER.error(sqle.getMessage());
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
					//TODO
					//TODO Analyze exception for future handling
					//TODO
					try {
						connDictionary.close();
					} catch (SQLException sqleIgnore) {} 
					throw new ConnectException(sqle);
				}
			}
			try {
				if (connDictionary != null) {
					connDictionary.close();
				}
			} catch (SQLException sqle) {
				LOGGER.error(sqle.getMessage());
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			}
		}
		
	}

	private static class ArchivedRedoFile {

		private final ZoneId dbZoneId;

		String NAME;
		int THREAD;
		long SEQUENCE;
		long FIRST_CHANGE;
		long NEXT_CHANGE;
		long BYTES;
		Timestamp FIRST_TIME;

		ArchivedRedoFile(final ZoneId zi) {
			dbZoneId = zi;
		}

		int ACTUAL_LAG_SECONDS() {
			ZonedDateTime zdt = FIRST_TIME.toLocalDateTime().atZone(dbZoneId);
			return (int) Duration.between(ZonedDateTime.now(dbZoneId), zdt).getSeconds();
		}
	}

}
