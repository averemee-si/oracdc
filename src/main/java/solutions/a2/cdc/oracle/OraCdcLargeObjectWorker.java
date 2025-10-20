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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OraclePreparedStatement;
import oracle.sql.NUMBER;
import solutions.a2.utils.ExceptionUtils;

import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
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

	/**
	 * 
	 * @param scn           SCN
	 * @param rsId          RBA
	 * @param parentOpRsId  RBA of parent operation
	 * @param dataObjectId  parent DATA_OBJ#
	 * @param lobObjectId   LOB segment DATA_OBJ#
	 * @param xid           transactionId
	 * @param oraColumn
	 * @param srcConUid     SRC_CON_UID
	 * @return
	 * @throws SQLException
	 */
	OraCdcLargeObjectHolder readLobData(final long scn, final String rsId, final String parentOpRsId, 
			final long dataObjectId, final long lobObjectId, final String xid, final OraColumn oraColumn,
			final NUMBER srcConUid) throws SQLException {

		final long processingStartMillis = System.currentTimeMillis();
		if (LOGGER.isDebugEnabled() || LOGGER.isTraceEnabled()) {
			if (isCdb) {
				LOGGER.debug("readLobData started for SCN={}, RS_ID='{}', XID='{}', DATA_OBJ#={}, Parent OP RS_ID='{}', CON_ID={} for LOB column {} (OBJECT_ID={})",
						scn, rsId, xid, dataObjectId, parentOpRsId, srcConUid.bigIntegerValue(), oraColumn.getColumnName(), lobObjectId);
				LOGGER.trace("readLobData started for SCN={}, RS_ID='{}', XID='{}', DATA_OBJ#={}, Parent OP RS_ID='{}', CON_ID={} for LOB column {} (OBJECT_ID={})",
						scn, rsId, xid, dataObjectId, parentOpRsId, srcConUid.bigIntegerValue(), oraColumn.getColumnName(), lobObjectId);
			} else {
				LOGGER.debug("readLobData started for SCN={}, RS_ID='{}', XID='{}', DATA_OBJ#={}, Parent OP RS_ID='{}' for LOB column {} (OBJECT_ID={})",
						scn, rsId, xid, dataObjectId, parentOpRsId, oraColumn.getColumnName(), lobObjectId);
				LOGGER.trace("readLobData started for SCN={}, RS_ID='{}', XID='{}', DATA_OBJ#={}, Parent OP RS_ID='{}' for LOB column {} (OBJECT_ID={})",
						scn, rsId, xid, dataObjectId, parentOpRsId, oraColumn.getColumnName(), lobObjectId);
			}
		}

		logMinerExtended = false;
		int rowsSkipped = 0;
		boolean fetchRsLogMinerNext = true;
		boolean atLastRecord = false;
		ResultSet rsReadLob = bindReadLobParameters(scn, xid, dataObjectId, srcConUid);
		// Rewind
		while (fetchRsLogMinerNext) {
			if (rsReadLob.next()) {
				if (Strings.CS.compare(rsReadLob.getString("RS_ID"), rsId) > 0) {
					fetchRsLogMinerNext = false;
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Rows skipped - {}, will start from SCN={}, RS_ID='{}'",
							rowsSkipped, rsReadLob.getLong("SCN"), rsReadLob.getString("RS_ID"));
					}
					break;
				} else {
					rowsSkipped++;
				}
			} else {
				atLastRecord = true;
				fetchRsLogMinerNext = false;
				break;
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
				isRsLogMinerRowAvailable = atLastRecord ? false : true;
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
						if (nextOperation == OraCdcV$LogmnrContents.LOB_WRITE) {
							fetchRsLogMinerNext = false;
							readLob = true;
							continue;
						} else if (nextOperation == OraCdcV$LogmnrContents.INTERNAL &&
								Strings.CS.compare(currentRsId, rsReadLob.getString("RS_ID")) > 0) {
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
						rsReadLob = bindReadLobParameters(lastProcessedScn, xid, dataObjectId, srcConUid);				
						fetchRsLogMinerNext = true;
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("Last processed SCN={}, RS_ID='{}'", lastProcessedScn, lastProcessedRsId);
						}
						// Get back...
						rowsSkipped = 0;
						while (fetchRsLogMinerNext) {
							if (rsReadLob.next()) {
								if (Strings.CS.compare(rsReadLob.getString("RS_ID"), lastProcessedRsId) > 0) {
									fetchRsLogMinerNext = false;
									if (LOGGER.isDebugEnabled()) {
										LOGGER.debug("Rows skipped - {}, will continue from SCN={}, RS_ID='{}'",
												rowsSkipped, rsReadLob.getLong("SCN"), rsReadLob.getString("RS_ID"));
									}
									break;
								} else {
									rowsSkipped++;
								}
							} else {
								//TODO
								//TODO just break here?
								//TODO
								break;
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
		switch (oraColumn.getJdbcType()) {
		case Types.CLOB:
		case Types.NCLOB:
			final String clobAsString = OraDumpDecoder.fromClobNclob(lobHexData.toString());
			ba = clobAsString.getBytes();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("{} column {}, XID='{}' processing completed, processing time {} ms, data length={}, compressed data length={}",
						oraColumn.getJdbcType() == Types.CLOB ? "CLOB" : "NCLOB",
						oraColumn.getColumnName(), xid, (System.currentTimeMillis() - processingStartMillis), clobAsString.length(), ba.length);
			}
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("{} column {} content:\n{}",
						oraColumn.getJdbcType() == Types.CLOB ? "CLOB" : "NCLOB",
						oraColumn.getColumnName(), clobAsString);
			}
			break;
		case Types.BLOB:
			ba = hexToRaw(lobHexData.toString());
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("BLOB column {}, XID='{}' processing completed, processing time {} ms, data length={}",
						oraColumn.getColumnName(), xid, (System.currentTimeMillis() - processingStartMillis), ba.length);
			}
			break;
		default:
			LOGGER.error("Type {} for column {} is not supported by {}",
					oraColumn.getJdbcType(), oraColumn.getColumnName(), OraCdcLargeObjectWorker.class.getCanonicalName());
			throw new SQLException("Unknown LOB type!!!");
		}

		return new OraCdcLargeObjectHolder(lobObjectId, ba);
	}

	protected boolean isLogMinerExtended() {
		return logMinerExtended;
	}

	private ResultSet bindReadLobParameters(final long scn, final String xid,
			final long dataObjectId, final NUMBER srcConUid) throws SQLException {
		psReadLob.setLong(1, dataObjectId);
		psReadLob.setString(2, xid);
		psReadLob.setLong(3, scn);
		if (isCdb) {
			psReadLob.setNUMBER(4, srcConUid);
		}
		return psReadLob.executeQuery();
	}

}
