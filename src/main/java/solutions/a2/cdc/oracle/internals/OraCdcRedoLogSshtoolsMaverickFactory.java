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
import java.io.InputStream;
import java.nio.file.Path;

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

	public OraCdcRedoLogSshtoolsMaverickFactory(
			final String username, final String hostname, final int port,
			final String keyFile, final String password, final boolean strictHostKeyChecking,
			final BinaryUtils bu, final boolean valCheckSum) throws IOException {
		super(bu, valCheckSum);
		this.username = username;
		this.hostname = hostname;
		this.port = port;
		this.strictHostKeyChecking = strictHostKeyChecking;
		if (StringUtils.isBlank(keyFile)) {
			usePassword = true;
			secret = password;
		} else {
			usePassword = false;
			secret = keyFile;
		}
		create();

	}

	private void create() throws IOException {
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
		} catch (SshException | PermissionDeniedException e) {
			throw new IOException(e);
		}
	}


	public OraCdcRedoLogSshtoolsMaverickFactory(final OraCdcSourceConnectorConfig config, final BinaryUtils bu, final boolean valCheckSum) throws IOException {
		this(config.sshUser(), config.sshHostname(), config.sshPort(),
			config.sshKey(), config.sshPassword(), config.sshStrictHostKeyChecking(),
			bu, valCheckSum);
	}

	@Override
	public OraCdcRedoLog get(final String redoLog) throws IOException {
		try {
			InputStream fis = sftp.getInputStream(redoLog);
			long[] blockSizeAndCount = blockSizeAndCount(fis, redoLog);
			fis.close();
			fis = null;
			return get(redoLog, false, (int)blockSizeAndCount[0], blockSizeAndCount[1]); 
		} catch (SshException | SftpStatusException sftpe) {
			throw new IOException(sftpe);
		}
	}

	@Override
	public OraCdcRedoLog get(String redoLog, boolean online, int blockSize, long blockCount) throws IOException {
		return new OraCdcRedoLog(
				new OraCdcRedoSshtoolsMaverickReader(sftp, redoLog, blockSize, blockCount),
				valCheckSum,
				bu,
				blockCount);
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

	public void reset() throws IOException {
		close();
		create();
	}

}
