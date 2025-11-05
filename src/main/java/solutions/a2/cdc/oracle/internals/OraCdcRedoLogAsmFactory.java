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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleCallableStatement;
import solutions.a2.oracle.utils.BinaryUtils;

import static oracle.jdbc.OracleTypes.INTEGER;
import static oracle.jdbc.OracleTypes.BIGINT;

public class OraCdcRedoLogAsmFactory extends OraCdcRedoLogFactoryBase implements OraCdcRedoLogFactory {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoLogAsmFactory.class);
	private static final String ASM_OPEN =
			"declare\n" +
			"  l_ASM_FILENAME varchar2(4000);\n" +
			"  l_HANDLE binary_integer;\n" +
			"  l_PLK_SIZE binary_integer;\n" +
			"  l_FILETYPE binary_integer;\n" +
			"  l_BLOCKS binary_integer;\n" +
			"  l_BLOCK_SIZE binary_integer;\n" +
			"begin\n" +
			"  l_ASM_FILENAME := ?;\n" +
			"  DBMS_DISKGROUP.GETFILEATTR(l_ASM_FILENAME, l_FILETYPE, l_BLOCKS, l_BLOCK_SIZE);\n" +
			"  DBMS_DISKGROUP.OPEN(l_ASM_FILENAME, 'r', l_FILETYPE, l_BLOCK_SIZE, l_HANDLE, l_PLK_SIZE, l_BLOCKS);\n" +
			"  ? := l_HANDLE;\n" +
			"end;\n";
	private static final String ASM_READ =
			"begin\n" +
			"  DBMS_DISKGROUP.READ(?, ?, ?, ?);\n" +
			"end;\n";
	private static final String ASM_CLOSE =
			"begin\n" +
			"  DBMS_DISKGROUP.CLOSE(?);\n" +
			"end;\n";
	private static final String ASM_FILE_ATTR =
			"declare\n" +
			"  l_FILETYPE binary_integer;\n" +
			"begin\n" +
			"  DBMS_DISKGROUP.GETFILEATTR(?, l_FILETYPE, ?, ?);\n" +
			"end;\n";

	private final boolean readAhead;
	private Connection connection;
	private OracleCallableStatement open;
	private OracleCallableStatement read;
	private OracleCallableStatement close;

	public OraCdcRedoLogAsmFactory(final Connection connection, final BinaryUtils bu, final boolean valCheckSum, final boolean readAhead) throws SQLException {
		super(bu, valCheckSum);
		this.readAhead = readAhead;
		try {
			reset(connection);
		} catch (SQLException sqle) {
			LOGGER.error(
					"""
					
					=====================
					Unable to create OraCdcRedoLogAsmFactory: SQL Error Code={}, SQL State='{}'!
					=====================
					
					""", sqle.getErrorCode(), sqle.getSQLState());
			throw sqle;
		}
	}

	@Override
	public OraCdcRedoLog get(final String redoLog) throws SQLException {
		try {
			CallableStatement cs = open.getConnection().prepareCall(ASM_FILE_ATTR);
			cs.setString(1, redoLog);
			cs.registerOutParameter(2, BIGINT);
			cs.registerOutParameter(3, INTEGER);
			cs.execute();
			final long blockCount = cs.getLong(2);
			final int blockSize = cs.getInt(3);
			try {
				return new OraCdcRedoLog(
					new OraCdcRedoAsmReader(open, read, close, redoLog, blockSize, blockCount, readAhead),
					valCheckSum,
					bu,
					blockCount);
			} catch (IOException ioe) {
				throw new SQLException(ioe);
			}
		} catch (SQLException sqle) {
			LOGGER.error(
					"""
					
					=====================
					Unable to detect attributes of {}: SQL Error Code={}, SQL State='{}'!
					=====================
					
					""", redoLog, sqle.getErrorCode(), sqle.getSQLState());
			throw sqle;
		}
	}

	@Override
	public OraCdcRedoLog get(String redoLog, boolean online, int blockSize, long blockCount) throws SQLException {
		try {
			return new OraCdcRedoLog(
				new OraCdcRedoAsmReader(open, read, close, redoLog, blockSize, blockCount, readAhead),
				valCheckSum,
				bu,
				blockCount);
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	@Override
	public void reset(final Connection connection) throws SQLException {
		if (open != null) {
			try {open.close();} catch (SQLException sqle) {
									printCloseWarningMessage("open file handle anonymous PL/SQL block", sqle);}
			open = null;
		}
		if (read != null) {
			try {read.close();} catch (SQLException sqle) {
									printCloseWarningMessage("read file anonymous PL/SQL block", sqle);}
			read = null;
		}
		if (close != null) {
			try {close.close();} catch (SQLException sqle) {
									printCloseWarningMessage("close file handle anonymous PL/SQL block", sqle);}
			close = null;
		}
		if (this.connection != null) {
			try {this.connection.close();} catch (SQLException sqle) {
									printCloseWarningMessage("close ASM connection", sqle);}
			this.connection = null;
		}
		this.connection = connection;

		open = (OracleCallableStatement) connection.prepareCall(ASM_OPEN);
		read = (OracleCallableStatement) connection.prepareCall(ASM_READ);
		close = (OracleCallableStatement) connection.prepareCall(ASM_CLOSE);
	}

	@Override
	public void reset() throws SQLException {
		throw new SQLException("Not implemented!");
	}

	private void printCloseWarningMessage(final String blockName, final SQLException sqle) {
		LOGGER.warn(
				"""
				
				=====================
				Unable to '{}' due to SQL Exception {}
					SQL Error code={}, SQL State='{}'!
				=====================
				
				""", blockName, sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
	}

}
