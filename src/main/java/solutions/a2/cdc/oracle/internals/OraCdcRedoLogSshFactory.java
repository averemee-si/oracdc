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

import com.sshtools.client.sftp.SftpClient;
import com.sshtools.common.sftp.SftpStatusException;
import com.sshtools.common.ssh.SshException;

import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.oracle.utils.BinaryUtils;

public class OraCdcRedoLogSshFactory extends OraCdcRedoLogFactoryBase implements OraCdcRedoLogFactory {

	private final OraCdcSshConnection ssh;
	private SftpClient sftp;

	public OraCdcRedoLogSshFactory(final OraCdcSshConnection ssh, final BinaryUtils bu, final boolean valCheckSum) throws IOException {
		super(bu, valCheckSum);
		this.ssh = ssh;
		this.sftp = ssh.getClient();
	}

	public OraCdcRedoLogSshFactory(final OraCdcSourceConnectorConfig config, final BinaryUtils bu, final boolean valCheckSum) throws IOException {
		this(new OraCdcSshConnection(config), bu, valCheckSum);
	}

	@Override
	public OraCdcRedoLog get(final String redoLog) throws IOException {
		try {
			InputStream fis = sftp.getInputStream(redoLog);
			long[] blockSizeAndCount = blockSizeAndCount(fis, redoLog);		
			fis.close();
			fis = null;
			return get(redoLog, (int)blockSizeAndCount[0], blockSizeAndCount[1]); 
		} catch (SshException | SftpStatusException sftpe) {
			throw new IOException(sftpe);
		}
	}

	@Override
	public OraCdcRedoLog get(String redoLog, int blockSize, long blockCount) throws IOException {
		return new OraCdcRedoLog(
				new OraCdcRedoSshReader(sftp, redoLog, blockSize, blockCount),
				valCheckSum,
				bu,
				blockCount);
	}

	public void reset() throws IOException {
		ssh.reset();
		sftp = ssh.getClient();
	}

}
