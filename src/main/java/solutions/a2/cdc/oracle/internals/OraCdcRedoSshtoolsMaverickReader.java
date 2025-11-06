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

import static com.sshtools.common.sftp.SftpStatusException.SSH_FX_NO_SUCH_FILE;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_1170;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.SQL_STATE_FILE_NOT_FOUND;

import java.io.InputStream;
import java.sql.SQLException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import com.sshtools.client.sftp.SftpClient;
import com.sshtools.common.sftp.SftpStatusException;
import com.sshtools.common.ssh.SshException;

public class OraCdcRedoSshtoolsMaverickReader implements OraCdcRedoReader {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoSshtoolsMaverickReader.class);

    private InputStream is;
    private final String redoLog;
    private final int blockSize;
    private final SftpClient sftp;

	OraCdcRedoSshtoolsMaverickReader(final SftpClient sftp, final String redoLog, final int blockSize, final long blockCount) throws SQLException {
		this.sftp = sftp;
		try {
			is = sftp.getInputStream(redoLog);
		} catch (SftpStatusException sftps) {
			if (sftps.getStatus() == SSH_FX_NO_SUCH_FILE)
				throw new SQLException(redoLog, SQL_STATE_FILE_NOT_FOUND, ORA_1170, sftps);
			else
				throw new SQLException(sftps);
		} catch (SshException sshe) {
			throw new SQLException(sshe);
		}
		try {
			if (is.skip(blockSize) != blockSize) {
				throw new SQLException("Unable to skip " + blockSize + " bytes!", "MaverickSSH", Integer.MIN_VALUE);
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
			return is.read(b, off, len);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}
	
	@Override
	public long skip(long n) throws SQLException {
		try {
			return is.skip(n * blockSize);
		} catch (IOException ioe) {
			LOGGER.error(
					"""
					
					=====================
					{} while skipping {} blocks of size {} in {}!
					{}
					=====================
					
					""", ioe.getMessage(), n, blockSize, redoLog, ExceptionUtils.getStackTrace(ioe));
			throw new SQLException(ioe);
		}
	}

	@Override
	public void close() throws SQLException {
		if (is != null) {
			try {
				is.close();
			} catch (IOException ioe) {
				throw new SQLException(ioe);
			}
			is = null;
		}
	}

	@Override
	public void reset() throws SQLException {
		close();
		try {
			is = sftp.getInputStream(redoLog);
		} catch (SshException | SftpStatusException sftpe) {
			throw new SQLException(sftpe);
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
