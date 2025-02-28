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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.Slf4jLogger;

import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;

public class OraCdcSshConnection implements AutoCloseable {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcSshConnection.class);

	private final boolean usePassword;
	private final String secret;
	private final JSch jsch;
	private final Session session;
	private ChannelSftp channel;
	private boolean connected;
	private boolean channelReady;

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
		if (JSch.getLogger() == null ||
				JSch.getLogger().getClass() == null ||
				!StringUtils.endsWith(JSch.getLogger().getClass().getCanonicalName(), "Slf4jLogger")) {
			JSch.setLogger(new Slf4jLogger());
		}
		connected = false;
		channelReady = false;
		if (StringUtils.isBlank(sKeyFile)) {
			usePassword = true;
			secret = sPassword;
		} else {
			usePassword = false;
			secret = sKeyFile;
		}
		jsch = new JSch();

		try {
			if (!usePassword) {
				jsch.addIdentity(secret);
			}
			session = jsch.getSession(sUser, sHost, sPort);
			session.setConfig("StrictHostKeyChecking", strictHostKeyChecking ? "yes" : "no");
			if (usePassword) {
				session.setConfig("PreferredAuthentications", "password,keyboard-interactive");
				session.setPassword(secret);
			}
		} catch (JSchException jsse) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to connect to {}@{}:{} !" + 
					"\n=====================\n",
					sUser, sHost, sPort);
			throw new IOException(jsse);
		}
	}

	public ChannelSftp getConnection() throws IOException {
		try {
			close();
			session.connect();
			connected = true;
			openChannel();
			return channel;
		} catch (JSchException jsse) {
			throw new IOException(jsse);
		}
	}

	@Override
	public void close() {
		closeChannel();
		if (connected) {
			session.disconnect();
			connected = false;
		}
	}

	public void openChannel() throws JSchException {
		channel = (ChannelSftp) session.openChannel("sftp");
		channel.connect();
		channelReady = true;
	}

	public void closeChannel() {
		if (channelReady) {
			channel.disconnect();
			channelReady = false;			
		}
	}

}
