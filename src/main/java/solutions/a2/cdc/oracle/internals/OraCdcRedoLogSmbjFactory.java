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
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Strings;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;

import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.oracle.utils.BinaryUtils;

public class OraCdcRedoLogSmbjFactory extends OraCdcRedoLogFactoryBase implements OraCdcRedoLogFactory, AutoCloseable {

	
	private final String server;
	private final String username;
	private final char[] password;
	private final String domain;
	private final String shareNameOnline;
	private final String shareNameArchive;
	private final boolean singleShare;
	private final int bufferSize;
	private final byte[] buffer;
	
	private final SMBClient smb;
	private Connection connection;
	private Session session;
	private DiskShare shareOnline;
	private DiskShare shareArchive;
	
	public OraCdcRedoLogSmbjFactory(final OraCdcSourceConnectorConfig config, final BinaryUtils bu, final boolean valCheckSum) throws IOException {
		super(bu, valCheckSum);
		server = config.smbServer();
		username = config.smbUser();
		password = config.smbPassword().toCharArray();
		domain = config.smbDomain();
		shareNameOnline = config.smbShareOnline();
		shareNameArchive = config.smbShareArchive();
		if (Strings.CS.equals(shareNameOnline, shareNameArchive))
			singleShare = true;
		else
			singleShare = false;
		bufferSize = config.smbBufferSize();
		buffer = new byte[bufferSize];
		final SmbConfig smbConfig = SmbConfig.builder()
				.withTimeout(config.smbTimeoutMs(), TimeUnit.MILLISECONDS)
				.withSoTimeout(config.smbSocketTimeoutMs(), TimeUnit.MILLISECONDS)
				.build();
		smb = new SMBClient(smbConfig);

		create();
	}

	private void create() throws IOException {
		connection = smb.connect(server);
		final AuthenticationContext ac = new AuthenticationContext(username, password, domain);
		session = connection.authenticate(ac);
		shareOnline = (DiskShare) session.connectShare(shareNameOnline);
		if (singleShare)
			shareArchive = shareOnline;
		else
			shareArchive = (DiskShare) session.connectShare(shareNameArchive);
	}


	@Override
	public OraCdcRedoLog get(final String redoLog) throws IOException {
		File file = shareOnline.openFile(redoLog,
				EnumSet.of(AccessMask.FILE_READ_DATA), null, SMB2ShareAccess.ALL, null, null);
		InputStream fis = file.getInputStream();
		long[] blockSizeAndCount = blockSizeAndCount(fis, redoLog);		
		fis.close();
		fis = null;
		file.close();
		file = null;
		return get(redoLog, false, (int)blockSizeAndCount[0], blockSizeAndCount[1]); 
	}

	@Override
	public OraCdcRedoLog get(String redoLog, boolean online, int blockSize, long blockCount) throws IOException {
		return new OraCdcRedoLog(
				new OraCdcRedoSmbjReader(
						online ? shareOnline : shareArchive, buffer, redoLog, blockSize, blockCount),
				valCheckSum,
				bu,
				blockCount);
	}

	@Override
	public void close() {
		try {
			closeObjects();
		} catch(IOException ioe) {}
		smb.close();
	}

	private void closeObjects() throws IOException {
		shareOnline.close();
		shareOnline = null;
		if (!singleShare)
			shareArchive.close();
		shareArchive = null;
		session.close();
		session = null;
		connection.close();
		connection = null;
	}

	public void reset() throws IOException {
		closeObjects();
		create();
	}

}
