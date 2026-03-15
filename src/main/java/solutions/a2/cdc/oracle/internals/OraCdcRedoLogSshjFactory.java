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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.DisconnectReason;
import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.DisconnectListener;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.oracle.utils.BinaryUtils;

public class OraCdcRedoLogSshjFactory extends OraCdcRedoLogFactoryBase
		implements OraCdcRedoLogFactory, AutoCloseable, DisconnectListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoLogSshjFactory.class);
	
	private final String username;
	private final String hostname;
	private final int port;
	private final boolean usePassword;
	private final boolean strictHostKeyChecking;
	private final String secret;
	private final int unconfirmedReads;
	private final int bufferSize;
	private SSHClient ssh;
	private SFTPClient sftp;
	private boolean connected;
	private String disconnectMessage;

	public OraCdcRedoLogSshjFactory(final OraCdcSourceConnectorConfig config,
			final BinaryUtils bu, final boolean valCheckSum) throws SQLException {
		super(bu, valCheckSum);
		username = config.sshUser();
		hostname = config.sshHostname();
		port = config.sshPort();
		unconfirmedReads = config.sshUnconfirmedReads();
		bufferSize = config.sshBufferSize();
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
		try {
			ssh = new SSHClient();
			if (strictHostKeyChecking)
				//TODO - pass file!!!
				ssh.loadKnownHosts();
			else
				ssh.addHostKeyVerifier(new PromiscuousVerifier());
			ssh.connect(hostname, port);
			if (usePassword)
				ssh.authPassword(username, secret);
			else
				ssh.authPublickey(username, secret);
			ssh.getTransport().setDisconnectListener(this);
			sftp = ssh.newSFTPClient();
			connected = true;
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public OraCdcRedoLog get(final String redoLog) throws SQLException {
		try {
			if (connected) {
				RemoteFile handle = sftp.open(redoLog, EnumSet.of(OpenMode.READ));
				InputStream fis = handle.new RemoteFileInputStream();
				long[] blockSizeAndCount = blockSizeAndCount(fis, redoLog);		
				fis.close();
				fis = null;
				return get(redoLog, false, (int)blockSizeAndCount[0], blockSizeAndCount[1]);
			} else
				throw disconnectException();
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public OraCdcRedoLog get(String redoLog, boolean online, int blockSize, long blockCount) throws SQLException {
		try {
			return new OraCdcRedoLog(
				new OraCdcRedoSshjReader(this, unconfirmedReads, bufferSize, redoLog, blockSize, blockCount),
				valCheckSum,
				bu,
				blockCount);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public void close() {
		if (ssh.isConnected()) {
			try {
				sftp.close();
				ssh.close();
				ssh = null;
				connected = false;
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

	@Override
	public void notifyDisconnect(DisconnectReason reason, String message) {
		LOGGER.error(
				"\n=====================\n" +
				"sshj disconnected - {}. additional message - {}\n" +
				"\n=====================\n",
				reason, message);
		connected = false;
		disconnectMessage = message;
	}

	SFTPClient sftp() {
		return sftp;
	}

	boolean connected() {
		return connected;
	}

	IOException disconnectException() {
		return new IOException(disconnectMessage);
	}

}
