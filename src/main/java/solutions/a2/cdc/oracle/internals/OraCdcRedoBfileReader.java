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

package solutions.a2.cdc.oracle.internals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleBfile;
import oracle.jdbc.OracleCallableStatement;

import static oracle.jdbc.LargeObjectAccessMode.MODE_READONLY;
import static oracle.jdbc.OracleTypes.BFILE;

public class OraCdcRedoBfileReader implements OraCdcRedoReader {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoBfileReader.class);

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
			final long blockCount) throws IOException {
		this.read = read;
		this.redoLog = redoLog;
		this.directory = directory;
		this.blockSize = blockSize;
		this.buffer = buffer;
		this.bufferSize = buffer.length;
		this.blockCount = blockCount;
		bufferBlocks = bufferSize / blockSize;
		if (bufferSize % blockSize != 0) {
			throw new IOException(
					"The buffer size (" + bufferSize  + 
					") must be a multiple of the block size (" + blockSize +
					")!");
		}
		needToreadAhead = true;
		currentBlock = 1;
		startPos = currentBlock;
		firstBlock = true;
		initBfile = true;
	}

	@Override
	public int read(byte b[], int off, int len) throws IOException {
		if (firstBlock) {
			try {
				execReadStatement(blockSize + 1);
				if (is.read(b, off, len) != len) {
					throw new IOException("Unable to read block #1 in " + redoLog);
				}
				is.close();
				is = null;
				bfile.closeLob();
				bfile = null;
			} catch (SQLException sqle) {
				LOGGER.error(
						"\n=====================\n" +
						"Unable to open '{}': SQL Error Code={}, SQL State='{}'!" +
						"\n=====================\n",
						redoLog, sqle.getErrorCode(), sqle.getSQLState());
				throw new IOException(sqle);
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
						LOGGER.error(
								"\n=====================\n" +
								"Unable to open '{}': SQL Error Code={}, SQL State='{}'!" +
								"\n=====================\n",
								redoLog, sqle.getErrorCode(), sqle.getSQLState());
						throw new IOException(sqle);
					}
				}
				final int actual = is.read(buffer, 0, bytesToRead);
				if (actual != bytesToRead) {
					throw new IOException(
							"The actual number of bytes read (" + actual + ")" +
							" from the SMB remote file is not equal to the expected number of (" + bytesToRead + ")!");
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

	private void execReadStatement(final long offset) throws SQLException {
		read.registerOutParameter(1, BFILE);
		read.setString(2, directory);
		read.setString(3, redoLog);
		read.execute();
		bfile = read.getBfile(1);
		if (bfile == null) {
			LOGGER.error(
					"\n=====================\n" +
					"BFILENAME('{}','{}') returns NULL!" +
					"\n=====================\n",
					directory, redoLog);
			throw new SQLException("BFILENAME() returns NULL!");
		}
		bfile.openLob(MODE_READONLY);
		is = bfile.getBinaryStream(offset);
	}

	@Override
	public long skip(long n) throws IOException {
		currentBlock += n;
		startPos = currentBlock;
		needToreadAhead = true;
		close();
		if (!initBfile)
			initBfile = true;
		return n * blockSize;
	}

	@Override
	public void close() throws IOException {
		if (!initBfile) {
			try {
				if (is != null) {
					is.close();
					is = null;
				}
				if (bfile != null) {
					bfile.closeLob();
					bfile = null;
				}
			} catch (SQLException sqle) {
				LOGGER.error(
						"\n=====================\n" +
								"Unable to close '{}': SQL Error Code={}, SQL State='{}'!" +
						"\n=====================\n",
						redoLog, sqle.getErrorCode(), sqle.getSQLState());
				throw new IOException(sqle);
			}
		}
	}

	@Override
	public void reset()  throws IOException {
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
