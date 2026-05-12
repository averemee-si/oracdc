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

import static net.schmizz.sshj.sftp.Response.StatusCode.NO_SUCH_FILE;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_1170;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.SQL_STATE_FILE_NOT_FOUND;

import java.util.EnumSet;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPException;

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
		} catch (SFTPException sftpe) {
			if (sftpe.getStatusCode() == NO_SUCH_FILE)
				throw new SQLException(redoLog, SQL_STATE_FILE_NOT_FOUND, ORA_1170, sftpe);
			else
				throw new SQLException(sftpe);
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
