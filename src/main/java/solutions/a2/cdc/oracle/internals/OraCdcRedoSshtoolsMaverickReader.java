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

import static com.sshtools.common.sftp.SftpStatusException.SSH_FX_NO_SUCH_FILE;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_1170;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.SQL_STATE_FILE_NOT_FOUND;

import java.io.InputStream;
import java.sql.SQLException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;

import com.sshtools.client.sftp.SftpClient;
import com.sshtools.common.sftp.SftpStatusException;
import com.sshtools.common.ssh.SshException;

public class OraCdcRedoSshtoolsMaverickReader implements OraCdcRedoReader {

	private static final Logger LOGGER = LogManager.getLogger(OraCdcRedoSshtoolsMaverickReader.class);

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
