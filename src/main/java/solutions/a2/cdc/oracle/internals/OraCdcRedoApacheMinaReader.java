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
