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

 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle.internals;

import static com.hierynomus.mserref.NtStatus.STATUS_ACCESS_DENIED;
import static com.hierynomus.mserref.NtStatus.STATUS_OBJECT_PATH_NOT_FOUND;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_1170;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.SQL_STATE_FILE_NOT_FOUND;

import java.util.EnumSet;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;

import java.sql.SQLException;

public class OraCdcRedoSmbjReader implements OraCdcRedoReader {

	private final File file;
	private final String redoLog;
	private final int blockSize;
	private final long blockCount;
	private final int bufferSize;
	private final int bufferBlocks;
	private final byte[] buffer;
	private long currentBlock;
	private boolean needToreadAhead;
	private long startPos;

	OraCdcRedoSmbjReader(DiskShare share, final byte[] buffer,
			final String redoLog, final int blockSize, final long blockCount) throws SQLException {
		this.buffer = buffer;
		this.bufferSize = buffer.length;
		this.redoLog = redoLog;
		this.blockSize = blockSize;
		this.blockCount = blockCount;
		bufferBlocks = bufferSize / blockSize;
		if (bufferSize % blockSize != 0) {
			throw new SQLException("The buffer size (" + bufferSize  + 
					") must be a multiple of the block size (" + blockSize + ")!",
					"SMBJ", Integer.MIN_VALUE);
		}
		needToreadAhead = true;
		try {
			file = share.openFile(redoLog,
				EnumSet.of(AccessMask.FILE_READ_DATA), null, SMB2ShareAccess.ALL, null, null);
		} catch (SMBApiException smbae) {
			if (smbae.getStatus() == STATUS_ACCESS_DENIED ||
					smbae.getStatus() == STATUS_OBJECT_PATH_NOT_FOUND)
				throw new SQLException(redoLog, SQL_STATE_FILE_NOT_FOUND, ORA_1170, smbae);
			else
				throw new SQLException(smbae);
		}
		currentBlock = 1;
		startPos = 1;		
	}

	@Override
	public int read(byte b[], int off, int len) throws SQLException {
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
			final int actual = file.read(buffer, currentBlock * blockSize, 0, bytesToRead);
			if (actual != bytesToRead) {
				throw new SQLException("The actual number of bytes read (" + actual + ")" +
						" from the SMB remote file is not equal to the expected number of (" + bytesToRead + ")!",
						"SMBJ", Integer.MIN_VALUE);
			}
			needToreadAhead = false;
		}
		int srcPos = (int) (((currentBlock - startPos) % bufferBlocks) * blockSize);
		System.arraycopy(buffer, srcPos, b, 0, len);
		currentBlock += 1;
		if (((currentBlock - startPos) % bufferBlocks) == 0) {
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
		if (file != null) {
			file.close();
		}
	}

	@Override
	public void reset()  throws SQLException {
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
