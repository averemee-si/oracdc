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

import java.util.EnumSet;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.mssmb2.SMB2ShareAccess;
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
		file = share.openFile(redoLog,
				EnumSet.of(AccessMask.FILE_READ_DATA), null, SMB2ShareAccess.ALL, null, null);
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
