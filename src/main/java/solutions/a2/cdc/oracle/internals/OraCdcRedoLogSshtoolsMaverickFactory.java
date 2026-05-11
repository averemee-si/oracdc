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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;

import com.sshtools.client.SshClient;
import com.sshtools.client.SshClient.SshClientBuilder;
import com.sshtools.client.sftp.SftpClient;
import com.sshtools.client.sftp.SftpClient.SftpClientBuilder;
import com.sshtools.common.knownhosts.KnownHostsKeyVerification;
import com.sshtools.common.permissions.PermissionDeniedException;
import com.sshtools.common.sftp.SftpStatusException;
import com.sshtools.common.ssh.SshException;

import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.oracle.utils.BinaryUtils;

public class OraCdcRedoLogSshtoolsMaverickFactory extends OraCdcRedoLogFactoryBase implements OraCdcRedoLogFactory, AutoCloseable {

	
	private final String username;
	private final String hostname;
	private final int port;
	private final boolean usePassword;
	private final String secret;
	private final boolean strictHostKeyChecking;
    private SshClient ssh;
    private SftpClient sftp;

	public OraCdcRedoLogSshtoolsMaverickFactory(final OraCdcSourceConnectorConfig config,
			final BinaryUtils bu, final boolean valCheckSum) throws SQLException {
		super(bu, valCheckSum);
		username = config.sshUser();
		hostname = config.sshHostname();
		port = config.sshPort();
		strictHostKeyChecking = config.sshStrictHostKeyChecking();
		if (StringUtils.isBlank(config.sshKey())) {
			usePassword = true;
			secret = config.sshPassword();
		} else {
			usePassword = false;
			secret = config.sshKey();
		}
		create();
	}

	private void create() throws SQLException {
		SshClientBuilder sshBuilder = SshClientBuilder.create()
				.withHostname(hostname)
				.withPort(port)
				.withUsername(username);
		if (usePassword)
			sshBuilder = sshBuilder.withPassword(secret);
		else
			sshBuilder = sshBuilder.withPrivateKeyFile(Path.of(secret));
		try {
			ssh = sshBuilder.build();
			if (strictHostKeyChecking)
				ssh.getContext().setHostKeyVerification(new KnownHostsKeyVerification());
			sftp =  SftpClientBuilder.create()
					.withClient(ssh)
					.build();
			sftp.setTransferMode(SftpClient.MODE_BINARY);
		} catch (IOException | SshException | PermissionDeniedException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public OraCdcRedoLog get(final String redoLog) throws SQLException {
		try {
			InputStream fis = sftp.getInputStream(redoLog);
			long[] blockSizeAndCount = blockSizeAndCount(fis, redoLog);
			fis.close();
			fis = null;
			return get(redoLog, false, (int)blockSizeAndCount[0], blockSizeAndCount[1]); 
		} catch (SshException | SftpStatusException | IOException sftpe) {
			throw new SQLException(sftpe);
		}
	}

	@Override
	public OraCdcRedoLog get(String redoLog, boolean online, int blockSize, long blockCount) throws SQLException {
		try {
		return new OraCdcRedoLog(
				new OraCdcRedoSshtoolsMaverickReader(sftp, redoLog, blockSize, blockCount),
				valCheckSum,
				bu,
				blockCount);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public void close() {
		if (sftp != null && sftp.isConnected()) {
			try {
				sftp.close();
			} catch (IOException e) {}
			sftp = null;
		}
		if (ssh != null && ssh.isConnected()) {
			try {
				ssh.close();
			} catch (IOException ioe) {}
			ssh = null;
		}
	}

	@Override
	public void reset() throws SQLException {
		close();
		create();
	}

	@Override
	public void reset(Connection connection) throws SQLException {
		close();
		create();
	}

}
