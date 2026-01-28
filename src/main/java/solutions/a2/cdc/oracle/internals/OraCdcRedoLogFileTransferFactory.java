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

import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_19504;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private boolean execDelete = false;
	private String file2Delete = null;

	public OraCdcRedoLogFileTransferFactory(final Connection connection,
			final OraCdcSourceConnectorConfig config,
			final BinaryUtils bu, final boolean valCheckSum) throws SQLException {
		super(connection, config, bu, valCheckSum);
		dirStage = config.fileTransferStageDir();
	}

	@Override
	public OraCdcRedoLog get(final String redoLog) throws SQLException {
		throw new SQLException("Not implemented!");
	}

	@Override
	public OraCdcRedoLog get(String redoLog, boolean online, int blockSize, long blockCount) throws SQLException {
		if (execDelete) {
			try {
				var delete = connection.prepareCall(DELETE);
				delete.setString(1, dirStage);
				delete.setString(2, file2Delete);
				delete.execute();
				delete.close();
				delete = null;
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
		try {
			var copy = connection.prepareCall(COPY);
			copy.setString(1, online ? dirOnline : dirArchive);
			copy.setString(2, redoLog);
			copy.setString(3, dirStage);
			copy.setString(4, redoLog);
			copy.execute();
			copy.close();
			copy = null;
		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == ORA_19504)
				get(redoLog, online, blockSize, blockCount);
			else
				throw sqle;
		}
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
	public void reset() throws SQLException {
		throw new SQLException("Not implemented!");
	}

}
