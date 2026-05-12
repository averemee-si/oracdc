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

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
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
	
	public OraCdcRedoLogSmbjFactory(final OraCdcSourceConnectorConfig config, final BinaryUtils bu, final boolean valCheckSum) throws SQLException {
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

	private void create() throws SQLException {
		try {
			connection = smb.connect(server);
			final AuthenticationContext ac = new AuthenticationContext(username, password, domain);
			session = connection.authenticate(ac);
			shareOnline = (DiskShare) session.connectShare(shareNameOnline);
			if (singleShare)
				shareArchive = shareOnline;
			else
				shareArchive = (DiskShare) session.connectShare(shareNameArchive);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}


	@Override
	public OraCdcRedoLog get(final String redoLog) throws SQLException {
		try {
			File file = shareOnline.openFile(redoLog,
				EnumSet.of(AccessMask.FILE_READ_DATA), null, SMB2ShareAccess.ALL, null, null);
			InputStream fis = file.getInputStream();
			long[] blockSizeAndCount = blockSizeAndCount(fis, redoLog);		
			fis.close();
			fis = null;
			file.close();
			file = null;
			return get(redoLog, false, (int)blockSizeAndCount[0], blockSizeAndCount[1]);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public OraCdcRedoLog get(String redoLog, boolean online, int blockSize, long blockCount) throws SQLException {
		try {
			return new OraCdcRedoLog(
				new OraCdcRedoSmbjReader(
						online ? shareOnline : shareArchive, buffer, redoLog, blockSize, blockCount),
				valCheckSum,
				bu,
				blockCount);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public void close() {
		try {
			closeObjects();
		} catch(SQLException ioe) {}
		smb.close();
	}

	private void closeObjects() throws SQLException {
		try {
			shareOnline.close();
			shareOnline = null;
			if (!singleShare)
				shareArchive.close();
			shareArchive = null;
			session.close();
			session = null;
			connection.close();
			connection = null;
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public void reset() throws SQLException {
		closeObjects();
		create();
	}

	@Override
	public void reset(java.sql.Connection connection) throws SQLException {
		closeObjects();
		create();
	}

}
