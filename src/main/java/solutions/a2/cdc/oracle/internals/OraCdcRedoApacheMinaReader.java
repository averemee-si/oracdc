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

import static solutions.a2.cdc.oracle.internals.OraCdcRedoLog.BLOCK_SIZE_0512;
import static solutions.a2.cdc.oracle.internals.OraCdcRedoLog.BLOCK_SIZE_1024;
import static solutions.a2.cdc.oracle.internals.OraCdcRedoLog.BLOCK_SIZE_4096;

import org.apache.sshd.sftp.client.SftpClient;
import org.apache.sshd.sftp.client.SftpClient.CloseableHandle;

import java.io.IOException;
import java.sql.SQLException;

public class OraCdcRedoApacheMinaReader implements OraCdcRedoReader {

	private static final int READ_AHEAD_SIZE = 0x8000;
	private static final int RA_BLK_0512 = 0x40;
	private static final int RA_BLK_1024 = 0x20;
	private static final int RA_BLK_4096 = 0x08;
	private final SftpClient sftp;
	private final String redoLog;
	private final int blockSize;
	private final long blockCount;
	private final byte[] readAheadBuffer = new byte[READ_AHEAD_SIZE];
	private final int readAheadBlocks;
	private boolean needToreadAhead = true;
	private CloseableHandle handle;
	private long currentBlock;
	private long startPos;

	OraCdcRedoApacheMinaReader(
			final SftpClient sftp, final String redoLog, final int blockSize, final long blockCount) throws SQLException {
		this.sftp = sftp;
		this.redoLog = redoLog;
		this.blockCount = blockCount;
		if (blockSize == BLOCK_SIZE_0512) {
			this.blockSize = BLOCK_SIZE_0512;
			readAheadBlocks = RA_BLK_0512;
		} else if (blockSize == BLOCK_SIZE_4096) {
			this.blockSize = BLOCK_SIZE_4096;
				readAheadBlocks = RA_BLK_4096;
		} else {
			this.blockSize = BLOCK_SIZE_1024;
			readAheadBlocks = RA_BLK_1024;
		}
		currentBlock = 1;
		startPos = 1;
		try {
			if (sftp.isOpen()) {
				handle = sftp.open(redoLog, OraCdcRedoLogApacheMinaFactory.OPEN_MODE);
			} else {
				throw new SQLException("SFTP subsystem is closed when oening " + redoLog);
			}
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public int read(byte b[], int off, int len) throws SQLException {
		if (needToreadAhead) {
			final int bytesToRead;
			if ((currentBlock + readAheadBlocks) <= blockCount)  {
				bytesToRead = READ_AHEAD_SIZE;
			} else {
				final long needed = blockCount - currentBlock + 1;
				if (needed > 0)
					bytesToRead = (int) (needed * blockSize);
				else
					return Integer.MIN_VALUE;
			}
			try {
				if (sftp.read(handle, currentBlock * blockSize, readAheadBuffer, 0, bytesToRead) < 0)
					return -1;
				needToreadAhead = false;
			} catch (IOException ioe) {
				throw new SQLException(ioe);
			}
		}
		var srcPos = (int) (((currentBlock - startPos) % readAheadBlocks) * blockSize);
		System.arraycopy(readAheadBuffer, srcPos, b, 0, len);
		currentBlock += 1;
		if (((currentBlock - startPos) % readAheadBlocks) == 0) {
			needToreadAhead = true;
		}
		return len;
	}
	
	@Override
	public long skip(long n) throws SQLException {
		currentBlock += n;
		startPos = currentBlock;
		needToreadAhead = true;
		return n * blockSize;
	}

	@Override
	public void close() throws SQLException {
		try {
			if (handle != null) {
				if (handle.isOpen())
					handle.close();
				handle = null;
			}
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public void reset() throws SQLException {
		close();
		try {
			if (sftp.isOpen()) {
				handle = sftp.open(redoLog, OraCdcRedoLogApacheMinaFactory.OPEN_MODE);
			} else
				throw new SQLException("SFTP fisconnected, unable to read " + redoLog + "!");
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
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
