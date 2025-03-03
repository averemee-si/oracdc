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

import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.lang3.StringUtils;

import com.sshtools.client.SshClient;
import com.sshtools.client.SshClient.SshClientBuilder;
import com.sshtools.client.sftp.SftpClient;
import com.sshtools.client.sftp.SftpClient.SftpClientBuilder;
import com.sshtools.common.knownhosts.KnownHostsKeyVerification;
import com.sshtools.common.permissions.PermissionDeniedException;
import com.sshtools.common.ssh.SshException;

import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;

public class OraCdcSshConnection implements AutoCloseable {

	private final String username;
	private final String hostname;
	private final int port;
	private final boolean usePassword;
	private final boolean strictHostKeyChecking;
	private final String secret;
	private SshClient ssh;
	private SftpClient sftp;

	public OraCdcSshConnection(final OraCdcSourceConnectorConfig config) throws IOException {
		this(config.sshUser(),
			config.sshHostname(),
			config.sshPort(),
			config.sshKey(),
			config.sshPassword(),
			config.sshStrictHostKeyChecking());
	}

	public OraCdcSshConnection(
			final String sUser, final String sHost, final int sPort,
			final String sKeyFile, final String sPassword,
			final boolean strictHostKeyChecking) throws IOException {
		username = sUser;
		hostname = sHost;
		port = sPort;
		if (StringUtils.isBlank(sKeyFile)) {
			usePassword = true;
			secret = sPassword;
		} else {
			usePassword = false;
			secret = sKeyFile;
		}
		this.strictHostKeyChecking = strictHostKeyChecking;
		try {
			createSsh();
			createSftp();
		} catch (SshException | PermissionDeniedException e) {
			throw new IOException(e);
		}
		
	}

	private void createSsh() throws IOException, SshException {
		SshClientBuilder sshBuilder = SshClientBuilder.create()
				.withHostname(hostname)
				.withPort(port)
				.withUsername(username);
		if (usePassword)
			sshBuilder = sshBuilder.withPassword(secret);
		else
			sshBuilder = sshBuilder.withPrivateKeyFile(Path.of(secret));
		ssh = sshBuilder.build();
		if (strictHostKeyChecking)
			ssh.getContext().setHostKeyVerification(new KnownHostsKeyVerification());
	}

	private void createSftp() throws IOException, SshException, PermissionDeniedException {
		try {
			sftp =  SftpClientBuilder.create()
					.withClient(ssh)
					.build();
		} catch (IllegalStateException ise) {
			if (StringUtils.startsWithIgnoreCase(ise.getMessage(), "Could not open session channel")) {
				ssh = null;
				createSsh();
				createSftp();
			} else {
				throw ise;
			}
		}
		sftp.setTransferMode(SftpClient.MODE_BINARY);
	}

	private void closeSftp() {
		if (sftp != null && sftp.isConnected()) {
			try {
				sftp.close();
			} catch (IOException ioe) {}
			sftp = null;
		}
	}

	public SftpClient getClient() {
		return sftp;
	}

	@Override
	public void close() {
		closeSftp();
		if (ssh != null && ssh.isConnected()) {
			try {
				ssh.close();
			} catch (IOException ioe) {}
		}
	}

	public void reset() throws IOException {
		try {
			closeSftp();
			createSftp();
		} catch (SshException | PermissionDeniedException e) {
			throw new IOException(e);
		}
	}

}
