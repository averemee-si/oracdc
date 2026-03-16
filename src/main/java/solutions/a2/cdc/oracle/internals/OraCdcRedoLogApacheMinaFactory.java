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
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.EnumSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.keyverifier.AcceptAllServerKeyVerifier;
import org.apache.sshd.client.keyverifier.KnownHostsServerKeyVerifier;
import org.apache.sshd.client.keyverifier.RejectAllServerKeyVerifier;
import org.apache.sshd.common.config.keys.FilePasswordProvider;
import org.apache.sshd.common.config.keys.loader.openssh.OpenSSHKeyPairResourceParser;
import org.apache.sshd.common.util.io.resource.PathResource;
import org.apache.sshd.sftp.client.SftpClient;
import org.apache.sshd.sftp.client.SftpClient.OpenMode;
import org.apache.sshd.sftp.client.SftpClientFactory;

import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.oracle.utils.BinaryUtils;

public class OraCdcRedoLogApacheMinaFactory extends OraCdcRedoLogFactoryBase
		implements OraCdcRedoLogFactory, AutoCloseable {

	static final Collection<OpenMode> OPEN_MODE = EnumSet.of(OpenMode.Read);

	private final String username;
	private final String hostname;
	private final int port;
	private final boolean usePassword;
	private final boolean strictHostKeyChecking;
	private final String secret;
	private final int connectTimeout;
	private SshClient ssh;
	private SftpClient sftp;
	private String disconnectMessage;

	public OraCdcRedoLogApacheMinaFactory(final OraCdcSourceConnectorConfig config,
			final BinaryUtils bu, final boolean valCheckSum) throws SQLException {
		super(bu, valCheckSum);
		username = config.sshUser();
		hostname = config.sshHostname();
		port = config.sshPort();
		connectTimeout = config.sshConnectTimeout();
		this.strictHostKeyChecking = config.sshStrictHostKeyChecking();
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
		try {
			ssh = SshClient.setUpDefaultClient();
			//TODO - pass file!!!
			ssh.setServerKeyVerifier(new KnownHostsServerKeyVerifier(
					strictHostKeyChecking
						? RejectAllServerKeyVerifier.INSTANCE
						: AcceptAllServerKeyVerifier.INSTANCE,
					Paths.get(System.getProperty("user.home"), ".ssh", "known_hosts")));
			ssh.start();
			var session = ssh
					.connect(username, hostname, port)
					.verify(connectTimeout)
					.getSession();
			if (usePassword)
				session.addPasswordIdentity(secret);
			else {
				var keys = OpenSSHKeyPairResourceParser.INSTANCE
						.loadKeyPairs(session, new PathResource(Paths.get(secret)), FilePasswordProvider.EMPTY);
				if (keys.isEmpty())
					throw new SQLException("Unable to load keys from " + secret + "!");
				session.addPublicKeyIdentity(keys.iterator().next());
			}
			session
				.auth()
				.verify(connectTimeout);
			sftp = SftpClientFactory.instance().createSftpClient(session);
		} catch (IOException | GeneralSecurityException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public OraCdcRedoLog get(final String redoLog) throws SQLException {
		try {
			if (sftp != null && sftp.isOpen()) {
				InputStream fis = sftp.read(redoLog, OPEN_MODE);
				long[] blockSizeAndCount = blockSizeAndCount(fis, redoLog);		
				fis.close();
				fis = null;
				return get(redoLog, false, (int)blockSizeAndCount[0], blockSizeAndCount[1]);
			} else
				throw new SQLException("Not connected to " + username + "@" + hostname + ":" + port + " !");
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public OraCdcRedoLog get(String redoLog, boolean online, int blockSize, long blockCount) throws SQLException {
		try {
			return new OraCdcRedoLog(
				new OraCdcRedoApacheMinaReader(sftp, redoLog, blockSize, blockCount),
				valCheckSum,
				bu,
				blockCount);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public void close() {
		if (sftp != null && sftp.isOpen()) {
			try {
				var session = sftp.getSession();
				sftp.close();
				session.close();
				ssh.close();
				ssh.stop();
				sftp = null;
				session = null;
				ssh = null;
			} catch (IOException ioe) {}
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

	SftpClient sftp() {
		return sftp;
	}

	IOException disconnectException() {
		return new IOException(disconnectMessage);
	}

}
