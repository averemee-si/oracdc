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

import java.io.InputStream;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import com.sshtools.client.sftp.SftpClient;
import com.sshtools.common.sftp.SftpStatusException;
import com.sshtools.common.ssh.SshException;
import com.sshtools.common.ssh.SshIOException;

public class OraCdcRedoSshReader implements OraCdcRedoReader {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoSshReader.class);

	private InputStream is;
	private final String redoLog;
	private final int blockSize;
	private final SftpClient sftp;

	OraCdcRedoSshReader(final SftpClient sftp, final String redoLog, final int blockSize, final long blockCount) throws IOException {
		this.sftp = sftp;
		try {
			is = sftp.getInputStream(redoLog);
		} catch (SshException | SftpStatusException sftpe) {
			throw new IOException(sftpe);
		}
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
		try {
			return is.skip(n * blockSize);
		} catch (SshIOException sioe) {
			LOGGER.error(
					"\n=====================\n" +
					"{} while skipping {} blocks of size {} in {}!\n{}" +
					"\n=====================\n",
					sioe.getMessage(), n, blockSize, redoLog, ExceptionUtils.getStackTrace(sioe));
			throw new IOException(sioe);
		}
	}

	@Override
	public void close() throws IOException {
		is.close();
		is = null;
	}

	@Override
	public void reset()  throws IOException {
		close();
		try {
			is = sftp.getInputStream(redoLog);
		} catch (SshException | SftpStatusException sftpe) {
			throw new IOException(sftpe);
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
