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
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleCallableStatement;
import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.oracle.utils.BinaryUtils;

public class OraCdcRedoLogFileTransferFactory extends OraCdcRedoLogBfileFactory {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoLogFileTransferFactory.class);
	private static final String COPY =
			"""
			begin
			  DBMS_FILE_TRANSFER.COPY_FILE(?, ?, ?, ?);
			end;
			
			""";
	private static final String DELETE =
			"""
			begin
			  UTL_FILE.FREMOVE(?, ?);
			end;
			
			""";

	private final String dirStage;
	private OracleCallableStatement copy;
	private OracleCallableStatement delete;
	private boolean execDelete = false;
	private String file2Delete = null;

	public OraCdcRedoLogFileTransferFactory(final Connection connection,
			final OraCdcSourceConnectorConfig config,
			final BinaryUtils bu, final boolean valCheckSum) throws SQLException {
		super(connection, config, bu, valCheckSum);
		dirStage = config.fileTransferStageDir();
		try {
			reset(connection);
		} catch (SQLException sqle) {
			LOGGER.error(
					"""
					
					=====================
					Unable to create OraCdcRedoLogBfileFactory for directories
					ONLINE='{}', ARCHIVE='{}', STAGE='{}'  and buffer size={}
					'{}', SQL Error Code={}, SQL State='{}'!
					=====================
					
					""", config.bfileDirOnline(), config.bfileDirArchive(), dirStage,
					config.bfileBufferSize(), sqle.getMessage(), sqle.getErrorCode(),
					sqle.getSQLState());
			throw sqle;
		}
	}

	@Override
	public OraCdcRedoLog get(final String redoLog) throws SQLException {
		throw new SQLException("Not implemented!");
	}

	@Override
	public OraCdcRedoLog get(String redoLog, boolean online, int blockSize, long blockCount) throws SQLException {
		if (execDelete) {
			try {
				delete.setString(1, dirStage);
				delete.setString(2, file2Delete);
				delete.execute();
			} catch (SQLException sqle) {
				LOGGER.warn(
						"""
						
						=====================
						Unable to delete file {} in directory {}!
						Please delete it manually.
						'{}', SQL Error Code={}, SQL State='{}'!
						
						=====================
						
						""", redoLog, dirStage, sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
			}
		} else
			execDelete = true;
		file2Delete = redoLog;
		copy.setString(1, online ? dirOnline : dirArchive);
		copy.setString(2, redoLog);
		copy.setString(3, dirStage);
		copy.setString(4, redoLog);
		copy.execute();
		try {
			return new OraCdcRedoLog(
					new OraCdcRedoBfileReader(
							read,
							dirStage,
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
		if (copy != null) {
			try {copy.close();} catch (SQLException sqle) {
									printCloseWarningMessage("copy FILE anonymous PL/SQL block", sqle);}
			copy = null;
		}
		if (delete != null) {
			try {delete.close();} catch (SQLException sqle) {
									printCloseWarningMessage("delete FILE anonymous PL/SQL block", sqle);}
			delete = null;
		}
		super.reset(connection);
		copy = (OracleCallableStatement) connection.prepareCall(COPY);
		delete = (OracleCallableStatement) connection.prepareCall(DELETE);
	}

	@Override
	public void reset() throws SQLException {
		throw new SQLException("Not implemented!");
	}

}
