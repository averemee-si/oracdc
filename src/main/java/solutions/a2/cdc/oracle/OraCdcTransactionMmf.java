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

import static solutions.a2.cdc.oracle.OraCdcRawStatementBase.BIG_ENDIAN;
import static solutions.a2.cdc.oracle.OraCdcStatementBase.OPERATION_POS;
import static solutions.a2.cdc.oracle.OraCdcStatementBase.RBA_PART1_POS;
import static solutions.a2.cdc.oracle.OraCdcStatementBase.RBA_PART2_POS;
import static solutions.a2.cdc.oracle.OraCdcStatementBase.RBA_PART3_POS;
import static solutions.a2.cdc.oracle.OraCdcStatementBase.ROWID_POS_START;
import static solutions.a2.cdc.oracle.OraCdcStatementBase.ROWID_POS_END;
import static solutions.a2.cdc.oracle.OraCdcStatementBase.ROLLBACK_POS;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.OffHeapMmf;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;
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
	private final int segmentSize;

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
		segmentSize = sizeArray[0];
	}

	/**
	 * 
	 * Creates OraCdcTransaction for new transaction without LOBs (only for testing!)
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
		segmentSize = 0x400000;
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("MMF blocksize is set to {}", segmentSize);
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
		//TODO - in Java 25 with Prologue init must go back to super constructor
		segmentSize = sizeArray[0];
		init(raw);
	}

	void processRollbackEntries() {
		// Step I
		byte[] data;
		var index = queueSize - rollbackEntriesList.size();
		var nonRollback = new PartialRollbackEntry[index];
		while ((data = statements.readNext()) != null) {
			if (data[ROLLBACK_POS] != 1)
				nonRollback[--index] = new PartialRollbackEntry(
						statements.readIndex(),
						BIG_ENDIAN.getU16(data, OPERATION_POS),
						new RedoByteAddress(
								BIG_ENDIAN.getU32(data, RBA_PART1_POS),
								BIG_ENDIAN.getU32(data, RBA_PART2_POS),
								BIG_ENDIAN.getU16(data, RBA_PART3_POS)),
						//TODO need allocation-free method!
						new RowId(
								Arrays.copyOfRange(data, ROWID_POS_START, ROWID_POS_END)));
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
			sb.append(new OraCdcStatementBase(data).toDelimitedRow());
		}
		statements.resetReadIndex();
	}

	void init() {
		needInit = false;
		LOGGER.debug("BEGIN: create OraCdcTransactionMmf for new transaction");
		try {
			initLobs();
			mmfStatements = rootDir + File.separator + getXid() + "." + System.nanoTime();
			statements = new OffHeapMmf(mmfStatements, segmentSize);
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Created row data queue directory {} for transaction XID {}.",
						mmfStatements, getXid());
			}
			queueSize = 0;
			tailerOffset = 0;
			if (processLobs == LobProcessingStatus.LOGMINER) {
				mmfLobs = rootDir + File.separator + getXid() + ".LOBDATA." + System.nanoTime();
				lobs = new OffHeapMmf(mmfLobs, segmentSize);
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
		if (needInit) init();
		checkForRollback(oraSql, queueSize++);
		statements.writeRecord(oraSql.content());
		transSize += oraSql.size();
	}

	@Override
	public void addStatement(final OraCdcStatementBase oraSql) throws IOException {
		addStatementInt(oraSql);
	}

	public void addStatement(final OraCdcStatementBase oraSql, final List<OraCdcLargeObjectHolder> lobHolders) throws IOException {
		final boolean lobsExists;
		if (lobs == null) {
			lobsExists = false;
			oraSql.setLobCount((byte) 0);
		} else {
			lobsExists = true;
			oraSql.setLobCount((byte) lobHolders.size());
		}
		addStatementInt(oraSql); 
		if (lobsExists) {
			for (int i = 0; i < lobHolders.size(); i++) {
				lobs.writeRecord(lobHolders.get(i).content());
				transSize += lobHolders.get(i).size();
			}
		}
	}

	@Override
	public boolean getStatement(OraCdcStatementBase oraSql) {
		while (true) {
			var data = statements.readNext();
			if (data != null) {
				oraSql.restore(data);
				if (!willItRolledBack(oraSql))
					return true;
				else
					continue;
			} else
				return false;
		}
	}

	public boolean getStatement(OraCdcStatementBase oraSql, List<OraCdcLargeObjectHolder> lobHolders) {
		boolean result = getStatement(oraSql);
		if (result) {
			for (int i = 0; i < oraSql.getLobCount(); i++) {
				var content = lobs.readNext();
				if (content != null) {
					var lobHolder = new OraCdcLargeObjectHolder();
					lobHolder.restore(content);
					lobHolders.add(lobHolder);
				} else
					break;
			}
		}
		return result;
	}

	public boolean getLobs(final int lobCount, final List<OraCdcLargeObjectHolder> lobHolders) {
		var result = false;
		for (int i = 0; i < lobCount; i++) {
			var content = lobs.readNext();
			if (content != null) {
				if (!result) result = true;
				var lobHolder = new OraCdcLargeObjectHolder();
				lobHolder.restore(content);
				lobHolders.add(lobHolder);
			} else
				break;
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
			if (processLobs == LobProcessingStatus.REDOMINER) {
				closeLobFiles();
				deleteLobFiles();
			} else if (processLobs == LobProcessingStatus.LOGMINER) {
				if (lobs != null) {
					lobs.close();
					lobs = null;
				}
			}
			if (processLobs != LobProcessingStatus.NOT_AT_ALL) {
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
