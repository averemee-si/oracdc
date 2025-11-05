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
import java.sql.SQLException;

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
	private final OraCdcRedoLogSshjFactory rlf;

	OraCdcRedoSshjReader(final OraCdcRedoLogSshjFactory rlf, final int unconfirmedReads, final int bufferSize,
			final String redoLog, final int blockSize, final long blockCount) throws SQLException {
		this.rlf = rlf;
		this.sftp = rlf.sftp();
		this.unconfirmedReads = unconfirmedReads;
		this.bufferSize = bufferSize;
		try {
			if (rlf.connected()) {
				handle = sftp.open(redoLog, EnumSet.of(OpenMode.READ));
				is = new BufferedInputStream(handle.new ReadAheadRemoteFileInputStream(unconfirmedReads), bufferSize);
				if (is.skip(blockSize) != blockSize) {
					throw new IOException("Unable to skip " + blockSize + " bytes!");
				}
			} else {
				throw rlf.disconnectException();
			}
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
		this.redoLog = redoLog;
		this.blockSize = blockSize;
	}

	@Override
	public int read(byte b[], int off, int len) throws SQLException {
		try {
			if (rlf.connected()) return is.read(b, off, len);
			else throw rlf.disconnectException();
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}
	
	@Override
	public long skip(long n) throws SQLException {
		try {
			if (rlf.connected()) return is.skip(n * blockSize);
			else throw rlf.disconnectException();
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public void close() throws SQLException {
		try {
			if (is != null) {
				if (rlf.connected())
					is.close();
				is = null;
			}
			if (handle != null) {
				if (rlf.connected())
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
			if (rlf.connected()) {
				handle = sftp.open(redoLog, EnumSet.of(OpenMode.READ));
				is = new BufferedInputStream(handle.new ReadAheadRemoteFileInputStream(unconfirmedReads), bufferSize);
			} else
				throw rlf.disconnectException();
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
