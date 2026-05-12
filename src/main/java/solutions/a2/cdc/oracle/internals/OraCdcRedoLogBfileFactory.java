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
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleBfile;
import oracle.jdbc.OracleCallableStatement;
import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.oracle.utils.BinaryUtils;

import static oracle.jdbc.LargeObjectAccessMode.MODE_READONLY;
import static oracle.jdbc.OracleTypes.BFILE;

public class OraCdcRedoLogBfileFactory extends OraCdcRedoLogFactoryBase implements OraCdcRedoLogFactory {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoLogBfileFactory.class);
	private static final String GET_BFILE =
			"""
			begin
			  ? := BFILENAME(?, ?);
			end;
			
			""";

	final String dirOnline;
	final String dirArchive;
	final byte[] buffer;
	Connection connection;
	OracleCallableStatement read;

	public OraCdcRedoLogBfileFactory(final Connection connection,
			final OraCdcSourceConnectorConfig config,
			final BinaryUtils bu, final boolean valCheckSum) throws SQLException {
		super(bu, valCheckSum);
		this.dirOnline = config.bfileDirOnline();
		this.dirArchive = config.bfileDirArchive();
		this.buffer = new byte[config.bfileBufferSize()];
		try {
			reset(connection);
		} catch (SQLException sqle) {
			LOGGER.error(
					"""
					
					=====================
					Unable to create OraCdcRedoLogBfileFactory for directories
					ONLINE='{}', ARCHIVE='{}'  and buffer size={}
					'{}', SQL Error Code={}, SQL State='{}'!
					=====================
					
					""", dirOnline, dirArchive, config.bfileBufferSize(),
					sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
			throw sqle;
		}
	}

	@Override
	public OraCdcRedoLog get(final String redoLog) throws SQLException {
		try {
			read.registerOutParameter(1, BFILE);
			read.setString(2, dirArchive);
			read.setString(3, redoLog);
			read.execute();
			OracleBfile bfile = read.getBfile(1);
			bfile.openLob(MODE_READONLY);
			InputStream is = bfile.getBinaryStream();
			long[] blockSizeAndCount = blockSizeAndCount(is, redoLog);
			is.close();
			is = null;
			bfile.closeLob();
			bfile = null;
			return get(redoLog, false, (int)blockSizeAndCount[0], blockSizeAndCount[1]); 
		} catch (SQLException sqle) {
			LOGGER.error(
					"""
					
					=====================
					Unable to detect attributes of {}:
					'{}', SQL Error Code={}, SQL State='{}'!
					=====================
					
					""", redoLog, sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
			throw sqle;
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public OraCdcRedoLog get(String redoLog, boolean online, int blockSize, long blockCount) throws SQLException {
		try {
			return new OraCdcRedoLog(
					new OraCdcRedoBfileReader(
							read,
							online ? dirOnline : dirArchive,
							buffer,
							redoLog, blockSize, blockCount),
					valCheckSum,
					bu,
					blockCount);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public void reset(final Connection connection) throws SQLException {
		if (read != null) {
			try {read.close();} catch (SQLException sqle) {
									printCloseWarningMessage("read BFILE anonymous PL/SQL block", sqle);}
			read = null;
		}
		if (this.connection != null) {
			try {this.connection.close();} catch (SQLException sqle) {
									printCloseWarningMessage("close RDBMS connection", sqle);}
			this.connection = null;
		}
		this.connection = connection;
		read = (OracleCallableStatement) connection.prepareCall(GET_BFILE);
		read.setLobPrefetchSize(-1);
	}

	@Override
	public void reset() throws SQLException {
		throw new SQLException("Not implemented!");
	}

	void printCloseWarningMessage(final String blockName, final SQLException sqle) {
		LOGGER.warn(
				"""
				
				=====================
				Unable to '{}' due to SQL Exception '{}'
					SQL Error code={}, SQL State='{}'!
				=====================
				
				""", blockName, sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
	}

}
