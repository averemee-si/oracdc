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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.GzipUtil;
import oracle.jdbc.OraclePreparedStatement;
import oracle.sql.NUMBER;

/**
 * 
 * Wrapper for LogMiner operations
 * 
 * 
 * @author averemee
 */
public class OraCdcLargeObjectWorker {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLargeObjectWorker.class);

	private final OraLogMiner logMiner;
	private final int pollInterval;
	private final CountDownLatch runLatch;
	private final boolean isCdb;
	private final OraclePreparedStatement psReadLob;
	private boolean logMinerExtended = false;

	public OraCdcLargeObjectWorker(final OraCdcLogMinerWorkerThread parent,
			final boolean isCdb, final OraLogMiner logMiner,
			final OraclePreparedStatement psReadLob, final CountDownLatch runLatch,
			final int pollInterval) throws SQLException {
		LOGGER.trace("BEGIN: OraCdcLargeObjectWorker Constructor");
		this.pollInterval = pollInterval;
		this.runLatch = runLatch;
		this.logMiner = logMiner;
		this.psReadLob = psReadLob;
		this.isCdb = isCdb;
		LOGGER.trace("END: OraCdcLargeObjectWorker Constructor");
	}

	OraCdcLargeObjectHolder readLobData(final long scn, final String rsId, final String parentOpRsId, 
			final long dataObjectId, final String xid, final OraColumn oraColumn,
			final NUMBER srcConId) throws SQLException {

		final long processingStartMillis = System.currentTimeMillis();
		if (LOGGER.isDebugEnabled()) {
			if (isCdb) {
				LOGGER.debug("readLobData started for SCN={}, RS_ID='{}', XID='{}', DATA_OBJ#={}, Parent OP RS_ID='{}', CON_ID={} for LOB column {} (OBJECT_ID={})",
						scn, rsId, xid, dataObjectId, parentOpRsId, srcConId, oraColumn.getColumnName(), oraColumn.getLobObjectId());
			} else {
				LOGGER.debug("readLobData started for SCN={}, RS_ID='{}', XID='{}', DATA_OBJ#={}, Parent OP RS_ID='{}' for LOB column {} (OBJECT_ID={})",
						scn, rsId, xid, dataObjectId, parentOpRsId, oraColumn.getColumnName(), oraColumn.getLobObjectId());
			}
		}

		logMinerExtended = false;
		int rowsSkipped = 0;
		boolean fetchRsLogMinerNext = true;
		ResultSet rsReadLob = bindReadLobParameters(scn, xid, dataObjectId, srcConId);
		// Rewind
		while (fetchRsLogMinerNext) {
			rsReadLob.next();
			if (StringUtils.compare(rsReadLob.getString("RS_ID"), rsId) > 0) {
				fetchRsLogMinerNext = false;
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Rows skipped - {}, will start from SCN={}, RS_ID='{}'",
							rowsSkipped, rsReadLob.getLong("SCN"), rsReadLob.getString("RS_ID"));
				}
				break;
			} else {
				rowsSkipped++;
			}
		}
		boolean readLob = true;
		boolean isRsLogMinerRowAvailable = false;
		long lastProcessedScn = 0;
		String lastProcessedRsId = null;
		final StringBuilder lobHexData = new StringBuilder(65000);

		while (readLob && runLatch.getCount() > 0) {
			if (fetchRsLogMinerNext) {
				isRsLogMinerRowAvailable = rsReadLob.next();
			} else {
				isRsLogMinerRowAvailable = true;
			}
			if (isRsLogMinerRowAvailable) {
				// Exit when OPERATION_CODE == 0
				readLob = rsReadLob.getBoolean("OPERATION_CODE");
				if (readLob) {
					String redoHex = rsReadLob.getString("SQL_REDO");
					if (rsReadLob.getBoolean("CSF")) {
						// Multi-line... Add first line.....
						lobHexData.append(
								StringUtils.substringAfter(redoHex, "HEXTORAW('"));
						// Process rest
						boolean moreRedoLines = true;
						while (moreRedoLines && rsReadLob.next()) {
							moreRedoLines = rsReadLob.getBoolean("CSF");
							redoHex = rsReadLob.getString("SQL_REDO");
							if (moreRedoLines) {
								lobHexData.append(redoHex);
							} else {
								lobHexData.append(
										StringUtils.substringBefore(redoHex, "');"));
							}
						}
					} else {
						lobHexData.append(
								StringUtils.substringBetween(redoHex, "HEXTORAW('", "');"));
					}
					lastProcessedScn = rsReadLob.getLong("SCN");
					lastProcessedRsId = rsReadLob.getString("RS_ID");
					fetchRsLogMinerNext = true;
				} else {
					//We need to check next record
					final String currentRsId = rsReadLob.getString("RS_ID");

					if (LOGGER.isTraceEnabled()) {
						LOGGER.trace("Internal OPERATION at SCN={}, RS_ID='{}'",
								rsReadLob.getLong("SCN"), currentRsId);
					}
					isRsLogMinerRowAvailable = rsReadLob.next();
					if (isRsLogMinerRowAvailable) {
						final short nextOperation = rsReadLob.getShort("OPERATION_CODE");
						if (nextOperation == OraLogMiner.V$LOGMNR_LOB_WRITE) {
							fetchRsLogMinerNext = false;
							readLob = true;
							continue;
						} else if (nextOperation == OraLogMiner.V$LOGMNR_CONTENTS_INTERNAL &&
								StringUtils.compare(currentRsId, rsReadLob.getString("RS_ID")) > 0) {
							fetchRsLogMinerNext = true;
							readLob = true;
							continue;
						} else {
							break;
						}
					} else {
						continue;
					}
				}
			} else {
				// Next archived log needed...
				logMinerExtended = true;
				logMiner.stop();
				rsReadLob.close();
				rsReadLob = null;
				boolean logMinerReady = false;
				while (!logMinerReady && runLatch.getCount() > 0) {
					logMinerReady = logMiner.extend();
					if (logMinerReady) {
						rsReadLob = bindReadLobParameters(lastProcessedScn, xid, dataObjectId, srcConId);				
						fetchRsLogMinerNext = true;
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("Last processed SCN={}, RS_ID='{}'", lastProcessedScn, lastProcessedRsId);
						}
						// Get back...
						rowsSkipped = 0;
						while (fetchRsLogMinerNext) {
							rsReadLob.next();
							if (StringUtils.compare(rsReadLob.getString("RS_ID"), lastProcessedRsId) > 0) {
								fetchRsLogMinerNext = false;
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Rows skipped - {}, will continue from SCN={}, RS_ID='{}'",
											rowsSkipped, rsReadLob.getLong("SCN"), rsReadLob.getString("RS_ID"));
								}
								break;
							} else {
								rowsSkipped++;
							}
						}
						isRsLogMinerRowAvailable = true;
					} else {
						synchronized (this) {
							if (LOGGER.isDebugEnabled()) {
								LOGGER.debug("Waiting {} ms for new archived log", pollInterval);
							}
							try {
								this.wait(pollInterval);
							} catch (InterruptedException ie) {
								LOGGER.error(ie.getMessage());
								LOGGER.error(ExceptionUtils.getExceptionStackTrace(ie));
							}
						}
					}
					
				}
			}
		}

		rsReadLob.close();
		rsReadLob = null;

		final byte[] ba;
		if (oraColumn.getJdbcType() == Types.CLOB || oraColumn.getJdbcType() == Types.NCLOB) {
			final String clobAsString = OraDumpDecoder.fromClobNclob(lobHexData.toString());
			ba = GzipUtil.compress(clobAsString);
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("{} column {}, XID='{}' processing completed, processing time {} ms, data length={}, compressed data length={}",
						oraColumn.getJdbcType() == Types.CLOB ? "CLOB" : "NCLOB",
						oraColumn.getColumnName(), xid, (System.currentTimeMillis() - processingStartMillis), clobAsString.length(), ba.length);
			}
			if (LOGGER.isTraceEnabled()) {
				LOGGER.debug("{} column {} content:\n{}",
						oraColumn.getJdbcType() == Types.CLOB ? "CLOB" : "NCLOB",
						oraColumn.getColumnName(), clobAsString);
			}
		} else {
			// Types.BLOB
			ba = OraDumpDecoder.toByteArray(lobHexData.toString());
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("BLOB column {}, XID='{}' processing completed, processing time {} ms, data length={}",
						oraColumn.getColumnName(), xid, (System.currentTimeMillis() - processingStartMillis), ba.length);
			}
		}
		return new OraCdcLargeObjectHolder(oraColumn.getLobObjectId(), ba);
	}

	protected boolean isLogMinerExtended() {
		return logMinerExtended;
	}

	private ResultSet bindReadLobParameters(final long scn, final String xid,
			final long dataObjectId, final NUMBER srcConId) throws SQLException {
		psReadLob.setLong(1, dataObjectId);
		psReadLob.setString(2, xid);
		psReadLob.setLong(3, scn);
		if (isCdb) {
			psReadLob.setNUMBER(4, srcConId);
		}
		return psReadLob.executeQuery();
	}

}
