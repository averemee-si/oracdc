/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle.internals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import oracle.jdbc.OracleBfile;
import oracle.jdbc.OracleCallableStatement;
import solutions.a2.utils.ExceptionUtils;

import static oracle.jdbc.LargeObjectAccessMode.MODE_READONLY;
import static oracle.jdbc.OracleTypes.BFILE;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_22288;

public class OraCdcRedoBfileReader implements OraCdcRedoReader {

	private static final Logger LOGGER = LogManager.getLogger(OraCdcRedoBfileReader.class);

	private final OracleCallableStatement read;
	private final String redoLog;
	private final String directory;
	private OracleBfile bfile;
	private InputStream is;
	private boolean initBfile;
	private final long blockCount;
	private final int bufferSize;
	private final int blockSize;
	private final byte[] buffer;
	private final int bufferBlocks;
	private long currentBlock;
	private boolean firstBlock;
	private boolean needToreadAhead;
	private long startPos;

	OraCdcRedoBfileReader(
			final OracleCallableStatement read,
			final String directory,
			final byte[] buffer,
			final String redoLog,
			final int blockSize,
			final long blockCount) throws SQLException {
		this.read = read;
		this.redoLog = redoLog;
		this.directory = directory;
		this.blockSize = blockSize;
		this.buffer = buffer;
		this.bufferSize = buffer.length;
		this.blockCount = blockCount;
		bufferBlocks = bufferSize / blockSize;
		if (bufferSize % blockSize != 0) {
			throw new SQLException("The buffer size (" + bufferSize  + 
					") must be a multiple of the block size (" + blockSize + ")!",
					"BFILE", Integer.MIN_VALUE);
		}
		needToreadAhead = true;
		currentBlock = 1;
		startPos = currentBlock;
		firstBlock = true;
		initBfile = true;
	}

	@Override
	public int read(byte b[], int off, int len) throws SQLException {
		if (firstBlock) {
			try {
				execReadStatement(blockSize + 1);
				try {
					if (is.read(b, off, len) != len) {
						throw new SQLException("Unable to read block #1 in " + redoLog, "BFILE", Integer.MIN_VALUE);
					}
				} catch (IOException ioe) {
					throw new SQLException("Unable to read block #1 in " + redoLog, "BFILE", Integer.MIN_VALUE);
				}
				try {
					is.close();
				} catch (IOException ioe) {
				} finally {
					is = null;
				}
				bfile.closeLob();
				bfile = null;
			} catch (SQLException sqle) {
				printUnableToOpenMessage(sqle);
				throw sqle;
			}
			firstBlock = false;
			currentBlock = 2;
			startPos = currentBlock;
		} else {
			if (needToreadAhead) {
				final int bytesToRead;
				if ((currentBlock + bufferBlocks) <= blockCount)  {
					bytesToRead = bufferSize;
				} else {
					final long needed = blockCount - currentBlock + 1;
					if (needed > 0)
						bytesToRead = (int) needed * blockSize;
					else
						return Integer.MIN_VALUE;
				}
				if (initBfile) {
					try {
						execReadStatement(currentBlock * blockSize + 1);
						initBfile = false;
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("BFILE = {} created with InputStream starting at {}.", redoLog, currentBlock * blockSize + 1);
						}
					} catch (SQLException sqle) {
						printUnableToOpenMessage(sqle);
						throw sqle;
					}
				}
				int actual;
				try {
					actual = is.read(buffer, 0, bytesToRead);
				} catch (IOException ioe) {
					throw new SQLException(ioe.getMessage(), "BFILE", Integer.MIN_VALUE);
				}
				if (actual != bytesToRead) {
					throw new SQLException("The actual number of bytes read (" + actual + ")" +
							" from the BFILE file is not equal to the expected number of (" + bytesToRead + ")!",
							"BFILE", Integer.MIN_VALUE);
				}
				needToreadAhead = false;
			}
			int srcPos = (int) (((currentBlock - startPos) % bufferBlocks) * blockSize);
			System.arraycopy(buffer, srcPos, b, 0, len);
			currentBlock += 1;
			if (((currentBlock - startPos) % bufferBlocks) == 0) {
				needToreadAhead = true;
			}
		}
		return len;
	}

	private void printUnableToOpenMessage(SQLException sqle) {
		if (sqle.getErrorCode() == ORA_22288)
			LOGGER.error(
				"""
				
				=====================
				Unable to open '{}' in directory {}: SQL Error Code={}, SQL State='{}'
				=====================
				
				""",
				redoLog, directory, sqle.getErrorCode(), sqle.getSQLState());
		else
			LOGGER.error(
				"""
				
				=====================
				Unable to open '{}' in directory {}: SQL Error Code={}, SQL State='{}'
				{}
				{}
				=====================
				
				""", redoLog, directory, sqle.getErrorCode(), sqle.getSQLState(),
				sqle.getMessage(), ExceptionUtils.getExceptionStackTrace(sqle));
	}

	private void execReadStatement(final long offset) throws SQLException {
		read.registerOutParameter(1, BFILE);
		read.setString(2, directory);
		read.setString(3, redoLog);
		read.execute();
		bfile = read.getBfile(1);
		if (bfile == null) {
			LOGGER.error(
					"""
					
					=====================
					BFILENAME('{}','{}') returns NULL!" +
					=====================
					
					""",
					directory, redoLog);
			throw new SQLException("BFILENAME() returns NULL!");
		}
		bfile.openLob(MODE_READONLY);
		is = bfile.getBinaryStream(offset);
	}

	@Override
	public long skip(long n) throws SQLException {
		currentBlock += n;
		startPos = currentBlock;
		needToreadAhead = true;
		close();
		if (!initBfile)
			initBfile = true;
		return n * blockSize;
	}

	@Override
	public void close() throws SQLException {
		if (!initBfile) {
			try {
				if (is != null) {
					try {
					is.close();
					} catch (IOException ioe) {
					} finally {
						is = null;
					}
				}
				if (bfile != null) {
					bfile.closeLob();
					bfile = null;
				}
			} catch (SQLException sqle) {
				LOGGER.error(
						"""
						
						=====================
						Unable to close '{}': SQL Error Code={}, SQL State='{}'!
						=====================
												
						""",
						redoLog, sqle.getErrorCode(), sqle.getSQLState());
				throw sqle;
			}
		}
	}

	@Override
	public void reset()  throws SQLException {
		currentBlock = 1;
		startPos = currentBlock;
		firstBlock = true;
		needToreadAhead = true;
		initBfile = true;
	}

	@Override
	public int blockSize() {
		return blockSize;
	}

	@Override
	public String redoLog() {
		return redoLog;
	}

}
