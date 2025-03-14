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
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;

public class OraCdcRedoSshjReader implements OraCdcRedoReader {

	private RemoteFile handle;
	private InputStream is;
	private final String redoLog;
	private final int blockSize;
	private final SFTPClient sftp;
	private final int unconfirmedReads;
	private final int bufferSize;

	OraCdcRedoSshjReader(final SFTPClient sftp, final int unconfirmedReads, final int bufferSize,
			final String redoLog, final int blockSize, final long blockCount) throws IOException {
		this.sftp = sftp;
		this.unconfirmedReads = unconfirmedReads;
		this.bufferSize = bufferSize;
		handle = sftp.open(redoLog, EnumSet.of(OpenMode.READ));
		is = new BufferedInputStream(handle.new ReadAheadRemoteFileInputStream(unconfirmedReads), bufferSize);
		if (is.skip(blockSize) != blockSize) {
			throw new IOException("Unable to skip " + blockSize + " bytes!");
		}
		this.redoLog = redoLog;
		this.blockSize = blockSize;
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
		if (handle != null) {
			handle.close();
			handle = null;
		}
	}

	@Override
	public void reset()  throws IOException {
		close();
		handle = sftp.open(redoLog, EnumSet.of(OpenMode.READ));
		is = new BufferedInputStream(handle.new ReadAheadRemoteFileInputStream(unconfirmedReads), bufferSize);
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
