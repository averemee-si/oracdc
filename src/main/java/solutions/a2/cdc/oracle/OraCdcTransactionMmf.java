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
	
package solutions.a2.cdc.oracle;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.OffHeapMmf;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcTransactionMmf extends OraCdcTransaction {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTransactionMmf.class);

	private String mmfStatements;
	private OffHeapMmf statements;
	private int queueSize;
	private int tailerOffset;
	private String  mmfLobs;
	private OffHeapMmf lobs;
	private final int blockSize;
	private final OraCdcStatementBase lmStmt = new OraCdcStatementBase();

	/**
	 * 
	 * Creates OraCdcTransaction for new transaction
	 * 
	 * @param processLobs
	 * @param rootDir
	 * @param xid
	 * @param firstChange
	 * @param isCdb
	 * @param sizeArray
	 * @throws IOException
	 */
	public OraCdcTransactionMmf(final LobProcessingStatus processLobs,
			final Path rootDir, final String xid, final long firstChange, final boolean isCdb, final int[] sizeArray) throws IOException {
		super(xid, firstChange, isCdb, processLobs, rootDir);
		transSize = 0;
		blockSize = sizeArray[0];
	}

	/**
	 * 
	 * Creates OraCdcTransaction for new transaction without LOBs
	 * 
	 * @param rootDir
	 * @param xid
	 * @param firstStatement
	 * @param isCdb
	 * @throws IOException
	 */
	public OraCdcTransactionMmf(
			final Path rootDir, final String xid, final OraCdcStatementBase firstStatement, final boolean isCdb) throws IOException {
		super(xid, firstStatement.getScn(), isCdb, LobProcessingStatus.NOT_AT_ALL, rootDir);
		transSize = 0;
		//TODO - temporary, constructor used only in test!
		blockSize = 0x400000;
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("MMF blocksize is set to {}", blockSize);
		addStatement(firstStatement);
	}

	/**
	 * 
	 * Creates OraCdcTransaction from list of OraCdcRedoRecords
	 * 
	 * @param raw
	 * @param isCdb
	 * @param processLobs
	 * @param rootDir
	 * @param sizeArray
	 */
	public OraCdcTransactionMmf(
			final OraCdcRawTransaction raw, final boolean isCdb,
			final LobProcessingStatus processLobs, final Path rootDir,
			final int[] sizeArray) throws SQLException, IOException {
		super(raw, isCdb, processLobs, rootDir);
		blockSize = sizeArray[0];
	}

	void processRollbackEntries() {
		// Step I
		byte[] data;
		var index = queueSize - rollbackEntriesList.size();
		var nonRollback = new PartialRollbackEntry[index];
		while ((data = statements.readNext()) != null) {
			lmStmt.content(data);
			if (!lmStmt.isRollback())
				nonRollback[--index] = new PartialRollbackEntry(statements.readIndex(), lmStmt);
		}
		statements.resetReadIndex();
		// Step 2
		for (var pre : rollbackEntriesList) {
			printPartialRollbackEntryDebug(pre);
			var pairFound = false;
			var startFrom = nonRollback.length - 1;
			for (var i = startFrom; i > -1; i--) {
				if (pre.match(nonRollback[i])) {
					final var rba = nonRollback[i].rba();
					final var rowid = nonRollback[i].rowId();
					final var uniqueAddr = Objects.hash(rba.sqn(), rba.blk(), rba.offset(),rowid.dataBlk(),rowid.rowNum());
					if (!rollbackPairs.contains(uniqueAddr)) {
						rollbackPairs.add(uniqueAddr);
						pairFound = true;
						startFrom = i;
						break;
					}
				}
			}
			if (!pairFound)
				printUnpairedRollbackEntryError(pre);
		}
		nonRollback = null;
	}

	void addToPrintOutput(final StringBuilder sb) {
		byte[] data;
		while ((data = statements.readNext()) != null) {
			lmStmt.content(data);
			sb.append(lmStmt.toDelimitedRow());
		}
		statements.resetReadIndex();
	}

	void init() {
		needInit = false;
		LOGGER.debug("BEGIN: create OraCdcTransactionMmf for new transaction");
		try {
			initLobs();
			mmfStatements = rootDir + File.separator + getXid() + "." + System.nanoTime();
			statements = new OffHeapMmf(mmfStatements, blockSize);
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Created row data queue directory {} for transaction XID {}.",
						mmfStatements, getXid());
			}
			queueSize = 0;
			tailerOffset = 0;
			if (processLobs == LobProcessingStatus.LOGMINER) {
				mmfLobs = rootDir + File.separator + getXid() + ".LOBDATA." + System.nanoTime();
				lobs = new OffHeapMmf(mmfLobs, blockSize);
			}
		} catch (IOException ioe) {
			LOGGER.error(
					"""
					
					=====================
					'{}' while creating memory-mapped files for transaction {}
					{}
					=====================
					
					""", ioe.getMessage(), getXid(), ExceptionUtils.getExceptionStackTrace(ioe));
			throw new OraCdcException(ioe);
		}
		LOGGER.debug("END: create OraCdcTransactionMmf for new transaction");
	}

	private void addStatementInt(final OraCdcStatementBase oraSql) throws IOException {
		if (needInit) {
			init();
		}
		checkForRollback(oraSql, queueSize++);
		statements.writeRecord(oraSql.content());
		transSize += oraSql.size();
	}

	@Override
	public void addStatement(final OraCdcStatementBase oraSql) throws IOException {
		addStatementInt(oraSql);
	}

	public void addStatement(final OraCdcStatementBase oraSql, final List<OraCdcLargeObjectHolder> lobs) throws IOException {
		final boolean lobsExists;
		if (lobs == null) {
			lobsExists = false;
			oraSql.setLobCount((byte) 0);
		} else {
			lobsExists = true;
			oraSql.setLobCount((byte) lobs.size());
		}
		addStatementInt(oraSql); 
		if (lobsExists) {
			for (int i = 0; i < lobs.size(); i++) {
				//TODO
				//TODO
				//TODO
//				lobsAppender.writeDocument(lobs.get(i));
				transSize += lobs.get(i).size();
			}
		}
	}

	@Override
	public boolean getStatement(OraCdcStatementBase oraSql) {
		while (true) {
			var data = statements.readNext();
			if (data != null) {
				oraSql.content(data);
				if (!willItRolledBack(oraSql))
					return true;
				else
					continue;
			} else
				return false;
		}
	}

	public boolean getStatement(OraCdcStatementBase oraSql, List<OraCdcLargeObjectHolder> lobs) {
		boolean result = getStatement(oraSql);
		if (result) {
			for (int i = 0; i < oraSql.getLobCount(); i++) {
				OraCdcLargeObjectHolder lobHolder = new OraCdcLargeObjectHolder();
				//TODO
				//TODO
				//TODO
//				if (!lobsTailer.readDocument(lobHolder)) {
//					break;
//				} else {
//					lobs.add(lobHolder);
//				}
			}
		}
		return result;
	}

	public boolean getLobs(final int lobCount, final List<OraCdcLargeObjectHolder> lobs) {
		boolean result = true;
		for (int i = 0; i < lobCount; i++) {
			OraCdcLargeObjectHolder lobHolder = new OraCdcLargeObjectHolder();
			//TODO
			//TODO
			//TODO
//			result = result && lobsTailer.readDocument(lobHolder);
			if (!result) {
				break;
			} else {
				lobs.add(lobHolder);
			}
		}
		return result;
	}

	@Override
	public void close() {
		if (needInit) {
			return;
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Closing and deleting memory-mapped files for transaction {}.", getXid());
		}
		try {
			if (processLobs != LobProcessingStatus.NOT_AT_ALL) {
				if (processLobs == LobProcessingStatus.REDOMINER) {
					closeLobFiles();
				} else {
					// LobProcessingStatus.LOGMINER
					if (lobs != null) {
						lobs.close();
						lobs = null;
					}
				}
			}
			if (statements != null) {
				statements.close();
				statements = null;
			}
		} catch (IOException ioe) {
			LOGGER.error(
					"""
					
					=====================
					'{}' Unable to delete memory-mapped files for transaction {}
					{}
					=====================
					
					""", ioe.getMessage(), getXid(), ExceptionUtils.getExceptionStackTrace(ioe));
		}
	}

	@Override
	public int length() {
		return queueSize;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(128);
		sb.append("oracdc Transaction: xid = ");
		sb.append(getXid());
		sb.append(" located in the '");
		sb.append(mmfStatements);
		sb.append("', processLobs = ");
		sb.append(processLobs);
		sb.append(", queueSize = ");
		sb.append(queueSize);
		sb.append(", firstChange = ");
		sb.append(getFirstChange());
		sb.append(", nextChange = ");
		sb.append(getNextChange());
		if (getCommitScn() != 0) {
			sb.append(", commitScn = ");
			sb.append(getCommitScn());
		}
		if (tailerOffset > 0) {
			sb.append(", tailerOffset = ");
			sb.append(tailerOffset);
		}
		sb.append(".");

		return sb.toString();
	}

	public String mmfStatements() {
		return mmfStatements;
	}

	@Override
	public long size() {
		return transSize;
	}

	@Override
	public void setCommitScn(long commitScn) {
		if (queueSize > 0) {
			if (processLobs == LobProcessingStatus.REDOMINER)
				closeLobFiles();
		}
		super.setCommitScn(commitScn);
	}

	@Override
	public void setCommitScn(long commitScn, OraCdcPseudoColumnsProcessor pseudoColumns, ResultSet resultSet) throws SQLException {
		super.setCommitScn(commitScn, pseudoColumns, resultSet);
	}

}
