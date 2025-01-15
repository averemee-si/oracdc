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
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleCallableStatement;
import oracle.sql.NUMBER;
import oracle.sql.RAW;

import static oracle.jdbc.OracleTypes.NUMBER;
import static oracle.jdbc.OracleTypes.RAW;
import static solutions.a2.cdc.oracle.internals.OraCdcRedoLog.BLOCK_SIZE_0512;
import static solutions.a2.cdc.oracle.internals.OraCdcRedoLog.BLOCK_SIZE_1024;
import static solutions.a2.cdc.oracle.internals.OraCdcRedoLog.BLOCK_SIZE_4096;

public class OraCdcRedoAsmReader implements OraCdcRedoReader {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoAsmReader.class);
	private static final int RA_BLK_0512 = 0x3F;
	private static final int RA_BLK_1024 = 0x1F;
	private static final int RA_BLK_4096 = 0x07;
	private static final int RA_BUFF_INT_0512 = 0x7E00;
	private static final int RA_BUFF_INT_1024 = 0x7C00;
	private static final int RA_BUFF_INT_4096 = 0x7000;
	private static final NUMBER RA_BUFF_0512 = new NUMBER(RA_BUFF_INT_0512);
	private static final NUMBER RA_BUFF_1024 = new NUMBER(RA_BUFF_INT_1024);
	private static final NUMBER RA_BUFF_4096 = new NUMBER(RA_BUFF_INT_4096);
	private static final NUMBER BS_0512 = new NUMBER(BLOCK_SIZE_0512);
	private static final NUMBER BS_1024 = new NUMBER(BLOCK_SIZE_1024);
	private static final NUMBER BS_4096 = new NUMBER(BLOCK_SIZE_4096);

	private final OracleCallableStatement open;
	private final OracleCallableStatement read;
	private final OracleCallableStatement close;
	private final NUMBER handle;
	private final String redoLog;
	private final NUMBER nBlockSize;
	private final int blockSize;
	private final long blockCount;
	private long currentBlock;
	private final boolean readAhead;
	private byte[] readAheadBuffer;
	private int readAheadBlocks;
	private NUMBER readAheadBytes;
	private boolean needToreadAhead = true;
	private long startPos;

	OraCdcRedoAsmReader(
			final OracleCallableStatement open,
			final OracleCallableStatement read,
			final OracleCallableStatement close,
			final String redoLog,
			final int blockSize,
			final long blockCount,
			final boolean readAhead) throws IOException {
		this.open = open;
		this.read = read;
		this.close = close;
		this.redoLog = redoLog;
		if (blockSize == BLOCK_SIZE_0512) {
			this.blockSize = BLOCK_SIZE_0512;
			this.nBlockSize = BS_0512;
			if (readAhead) {
				readAheadBlocks = RA_BLK_0512;
				readAheadBytes = RA_BUFF_0512;
				readAheadBuffer = new byte[RA_BUFF_INT_0512];
			}
		} else if (blockSize == BLOCK_SIZE_4096) {
			this.blockSize = BLOCK_SIZE_4096;
			this.nBlockSize = BS_4096;
			if (readAhead) {
				readAheadBlocks = RA_BLK_4096;
				readAheadBytes = RA_BUFF_4096;
				readAheadBuffer = new byte[RA_BUFF_INT_4096];
			}
		} else {
			this.blockSize = BLOCK_SIZE_1024;
			this.nBlockSize = BS_1024;
			if (readAhead) {
				readAheadBlocks = RA_BLK_1024;
				readAheadBytes = RA_BUFF_1024;
				readAheadBuffer = new byte[RA_BUFF_INT_1024];
			}
		}
		this.blockCount = blockCount;
		this.readAhead = readAhead;
		currentBlock = 1;
		startPos = 1;
		try {
			this.open.setString(1, redoLog);
			this.open.registerOutParameter(2, NUMBER);
			this.open.execute();
			handle = this.open.getNUMBER(2);
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("ASM file handle = {} created.", handle.stringValue());
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

	@Override
	public int read(byte b[], int off, int len) throws IOException {
		if (readAhead) {
			if (needToreadAhead) {
				final NUMBER bytesToRead;
				if ((currentBlock + readAheadBlocks) <= blockCount)  {
					bytesToRead = readAheadBytes;
				} else {
					bytesToRead = new NUMBER((blockCount - currentBlock + 1) * blockSize);
				}
				try {
					read.setNUMBER(1, handle);
					read.setLong(2, currentBlock);
					read.setNUMBER(3, bytesToRead);
					read.registerOutParameter(4, RAW);
					read.execute();
					final RAW data = read.getRAW(4);
					if (data != null) {
						final byte[] ba = data.getBytes();
						System.arraycopy(ba, 0, readAheadBuffer, 0, ba.length);
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("Copying {} bytes at block {}", ba.length, currentBlock);
						}
					} else {
						throw new IOException("RAW result is NULL while reading data from ASM!");
					}
					needToreadAhead = false;
				} catch (SQLException sqle) {
					LOGGER.error(
							"\n=====================\n" +
							"Unable to read '{}': SQL Error Code={}, SQL State='{}'!" +
							"\n=====================\n",
							redoLog, sqle.getErrorCode(), sqle.getSQLState());
					throw new IOException(sqle);
				}
			}
			int srcPos = (int) (((currentBlock - startPos) % readAheadBlocks) * blockSize);
			System.arraycopy(readAheadBuffer, srcPos, b, 0, len);
			currentBlock += 1;
			if (((currentBlock - startPos) % readAheadBlocks) == 0) {
				needToreadAhead = true;
			}
			return len;
		} else {
			try {
				read.setNUMBER(1, handle);
				read.setLong(2, currentBlock);
				read.setNUMBER(3, nBlockSize);
				read.registerOutParameter(4, RAW);
				read.execute();
				final RAW data = read.getRAW(4);
				if (data != null) {
					System.arraycopy(data.getBytes(), 0, b, 0, len);
					currentBlock += 1;
					return len;
				} else {
					throw new IOException("RAW result is NULL while reading data from ASM!");
				}
			} catch (SQLException sqle) {
				LOGGER.error(
						"\n=====================\n" +
						"Unable to read '{}': SQL Error Code={}, SQL State='{}'!" +
						"\n=====================\n",
						redoLog, sqle.getErrorCode(), sqle.getSQLState());
				throw new IOException(sqle);
			}
		}
	}
	
	@Override
	public long skip(long n) throws IOException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Changing current block in ASM file handle = {} from {} to {}.",
					handle.stringValue(), currentBlock, currentBlock + n);
		}
		currentBlock += n;
		startPos = currentBlock;
		needToreadAhead = true;
		return n * blockSize;
	}

	@Override
	public void close() throws IOException {
		if (readAhead)
			readAheadBuffer = null;
		try {
			close.setNUMBER(1, handle);
			close.execute();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("ASM file handle = {} closed.", handle.stringValue());
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

	@Override
	public void reset()  throws IOException {
		currentBlock = 1;
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
