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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class OraCdcRedoSmbjReader implements OraCdcRedoReader {

	private File file;
	private InputStream is;
	private final String redoLog;
	private final int blockSize;
	private final DiskShare share;
	private final int bufferSize;

	OraCdcRedoSmbjReader(DiskShare share, final int bufferSize,
			final String redoLog, final int blockSize, final long blockCount) throws IOException {
		this.share = share;
		this.bufferSize = bufferSize;
		this.redoLog = redoLog;
		this.blockSize = blockSize;
		open();
		if (is.skip(blockSize) != blockSize) {
			throw new IOException("Unable to skip " + blockSize + " bytes!");
		}
	}

	private void open() throws IOException {
		file = share.openFile(redoLog,
				EnumSet.of(AccessMask.FILE_READ_DATA), null, SMB2ShareAccess.ALL, null, null);
		is = new BufferedInputStream(file.getInputStream(), bufferSize);
	}

	@Override
	public int read(byte b[], int off, int len) throws IOException {
		return is.read(b, off, len);
	}
	
	@Override
	public long skip(long n) throws IOException {
		return is.skip(n * blockSize);
	}

	@Override
	public void close() throws IOException {
		if (is != null) {
			is.close();
			is = null;
		}
		if (file != null) {
			file.close();
			file = null;
		}
	}

	@Override
	public void reset()  throws IOException {
		close();
		open();
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
