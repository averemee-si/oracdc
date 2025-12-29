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
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import solutions.a2.utils.ExceptionUtils;

import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcTransactionChronicleQueue extends OraCdcTransaction {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTransactionChronicleQueue.class);
	private static final String CQ_ISSUE_1446_RETRY_MSG = "Received https://github.com/OpenHFT/Chronicle-Queue/issues/1446, will try again";
	private static final String CQ_ISSUE_1446_MSG =
			"""
			
			=====================
			'{}' while initializing Chronicle Queue.\n
			Perhaps this is https://github.com/OpenHFT/Chronicle-Queue/issues/1446
			Please suggest increase the value of system property "chronicle.table.store.timeoutMS".
				For more information on Chronicle Queue parameters please visit
					https://github.com/OpenHFT/Chronicle-Queue/blob/ea/systemProperties.adoc .
			=====================
			
			""";
	private static final int LOCK_RETRY = 5;
	private static final int PARTIAL_ROLLBACK_HEAP_THRESHOLD = 10;

	private Path queueDirectory;
	private ChronicleQueue statements;
	private ExcerptAppender appender;
	private ExcerptTailer tailer;
	private int queueSize;
	private int tailerOffset;
	private ChronicleQueue lobs;
	private ExcerptAppender lobsAppender;
	private ExcerptTailer lobsTailer;
	private long lastIndexAppended;

	/**
	 * 
	 * Creates OraCdcTransaction for new transaction
	 * 
	 * @param processLobs
	 * @param rootDir
	 * @param xid
	 * @param firstChange
	 * @param isCdb
	 * @throws IOException
	 */
	public OraCdcTransactionChronicleQueue(final LobProcessingStatus processLobs,
			final Path rootDir, final String xid, final long firstChange, final boolean isCdb) throws IOException {
		super(xid, firstChange, isCdb, processLobs, rootDir);
		transSize = 0;
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
	public OraCdcTransactionChronicleQueue(
			final Path rootDir, final String xid, final OraCdcStatementBase firstStatement, final boolean isCdb) throws IOException {
		this(LobProcessingStatus.NOT_AT_ALL, rootDir, xid, firstStatement.getScn(), isCdb);
		this.addStatement(firstStatement);
	}

	/**
	 * 
	 * Creates OraCdcTransaction from list of OraCdcRedoRecords
	 * 
	 * @param raw
	 * @param isCdb
	 * @param processLobs
	 * @param rootDir
	 */
	public OraCdcTransactionChronicleQueue(
			final OraCdcRawTransaction raw, final boolean isCdb,
			final LobProcessingStatus processLobs, final Path rootDir) throws SQLException, IOException {
		super(raw, isCdb, processLobs, rootDir);
	}

	void processRollbackEntries() {
		long nanos = System.nanoTime();
		final ExcerptTailer reverse = statements.createTailer();
		final OraCdcStatementBase lmStmt = new OraCdcStatementBase();
		reverse.direction(TailerDirection.BACKWARD);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Spent {} nanos to open the reverse tailer.", (System.nanoTime() - nanos));
		}

		boolean readResult = false;
		nanos = System.nanoTime();

		if (rollbackEntriesList.size() < PARTIAL_ROLLBACK_HEAP_THRESHOLD) {
			for (final PartialRollbackEntry pre : rollbackEntriesList) {
				printPartialRollbackEntryDebug(pre);
				reverse.moveToIndex(pre.index);
				boolean pairFound = false;
				do {
					readResult = reverse.readDocument(lmStmt);
					if (readResult &&
							!lmStmt.isRollback() &&
							((pre.operation == DELETE && lmStmt.getOperation() == INSERT) ||
							(pre.operation == INSERT && lmStmt.getOperation() == DELETE) ||
							(pre.operation == UPDATE && lmStmt.getOperation() == UPDATE)) &&
							pre.rowId.equals(lmStmt.getRowId())) {
						final var rba = lmStmt.getRba();
						final var rowid = lmStmt.getRowId();
						final var uniqueAddr = Objects.hash(rba.sqn(), rba.blk(), rba.offset(),rowid.dataBlk(),rowid.rowNum());
						if (!rollbackPairs.contains(uniqueAddr)) {
							rollbackPairs.add(uniqueAddr);
							pairFound = true;
						}
					}
				} while (readResult && !pairFound);
				if (!pairFound) {
					printUnpairedRollbackEntryError(pre);
				}
			}
		} else {
			// Step I
			reverse.moveToIndex(lastIndexAppended);
			int i= 0;
			final PartialRollbackEntry[] nonRollback = new PartialRollbackEntry[this.queueSize - rollbackEntriesList.size()];
			do {
				readResult = reverse.readDocument(lmStmt);
				if (readResult && !lmStmt.isRollback()) {
					final PartialRollbackEntry pre = new PartialRollbackEntry();
					pre.index = reverse.index();
					pre.operation = lmStmt.getOperation();
					pre.rowId = lmStmt.getRowId();
					pre.rba = lmStmt.getRba();
					nonRollback[i++] = pre;
				}
			} while (readResult);
			// Step 2
			for (final PartialRollbackEntry pre : rollbackEntriesList) {
				printPartialRollbackEntryDebug(pre);
				boolean pairFound = false;
				for (i = nonRollback.length - 1; i >= 0; i--) {
					if (((pre.operation == DELETE && nonRollback[i].operation == INSERT) ||
							(pre.operation == INSERT && nonRollback[i].operation == DELETE) ||
							(pre.operation == UPDATE && nonRollback[i].operation == UPDATE)) &&
							pre.rowId.equals(nonRollback[i].rowId)) {
						final var rba = nonRollback[i].rba;
						final var rowid = nonRollback[i].rowId;
						final var uniqueAddr = Objects.hash(rba.sqn(), rba.blk(), rba.offset(),rowid.dataBlk(),rowid.rowNum());
						if (!rollbackPairs.contains(uniqueAddr)) {
							rollbackPairs.add(uniqueAddr);
							pairFound = true;
							break;
						}
					}
				}
				if (!pairFound) {
					printUnpairedRollbackEntryError(pre);
				}
			}
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Spent {} nanos to pair {} partial rollback entries in transaction XID='{}' with size={}.",
					(System.nanoTime() - nanos), rollbackEntriesList.size(), getXid(), queueSize);
		}
		reverse.close();
	}

	void addToPrintOutput(final StringBuilder sb) {
		final ExcerptTailer printTailer = statements.createTailer();
		final OraCdcStatementBase lmStmt = new OraCdcStatementBase();
		boolean readResult = false;
		do {
			readResult = printTailer.readDocument(lmStmt);
			if (readResult) {
				sb.append(lmStmt.toDelimitedRow());
			}
		} while (readResult);
		printTailer.close();
	}

	void init() {
		needInit = false;
		LOGGER.debug("BEGIN: create OraCdcTransactionChronicleQueue for new transaction");
		try {
			queueDirectory = Files.createTempDirectory(rootDir, getXid() + ".");
			initLobs();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Created row data queue directory {} for transaction XID {}.",
						queueDirectory.toString(), getXid());
			}
			boolean cqDone = false;
			Exception lastException = null;
			for (int i = 0; i < LOCK_RETRY; i++) {
				try {
					statements = ChronicleQueue
							.singleBuilder(queueDirectory)
							.build();
					cqDone = true;
				} catch (IllegalStateException ise) {
					LOGGER.error(CQ_ISSUE_1446_RETRY_MSG);
					deleteDir(queueDirectory);
					if (i == LOCK_RETRY - 1) {
						lastException = ise;
					}
				}
				if (cqDone) {
					break;
				}
			}
			if (!cqDone) {
				LOGGER.error(CQ_ISSUE_1446_MSG, lastException.getMessage());
				throw lastException;
			}
			appender = statements.createAppender();
			queueSize = 0;
			tailerOffset = 0;
			if (processLobs == LobProcessingStatus.LOGMINER) {
				cqDone = false;
				for (int i = 0; i < LOCK_RETRY; i++) {
					try {
						lobs = ChronicleQueue
								.singleBuilder(lobsQueueDirectory)
								.build();
						cqDone = true;
					} catch (IllegalStateException ise) {
						LOGGER.error(CQ_ISSUE_1446_RETRY_MSG);
						deleteDir(lobsQueueDirectory);
						if (i == LOCK_RETRY - 1) {
							lastException = ise;
						}
					}
					if (cqDone) {
						break;
					}
				}
				if (!cqDone) {
					LOGGER.error(CQ_ISSUE_1446_MSG, lastException.getMessage());
					throw lastException;
				}
				lobsTailer = lobs.createTailer();
				lobsAppender = lobs.createAppender();
			}
		} catch (Exception e) {
			LOGGER.error(
					"""
					
					=====================
					'{}' while creating Chronicle Queue for transaction {}
					{}
					=====================
					
					""", e.getMessage(), getXid(), ExceptionUtils.getExceptionStackTrace(e));
			throw new RuntimeException(e);
		}
		LOGGER.debug("END: create OraCdcTransactionChronicleQueue for new transaction");
	}

	private void addStatementInt(final OraCdcStatementBase oraSql) {
		if (needInit) {
			init();
		}
		checkForRollback(oraSql, firstRecord ? - 1 : appender.lastIndexAppended());
		appender.writeDocument(oraSql);
		queueSize++;
		transSize += oraSql.size();
	}

	@Override
	public void addStatement(final OraCdcStatementBase oraSql) {
		addStatementInt(oraSql);
	}

	public void addStatement(final OraCdcStatementBase oraSql, final List<OraCdcLargeObjectHolder> lobs) {
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
				lobsAppender.writeDocument(lobs.get(i));
				transSize += lobs.get(i).size();
			}
		}
	}

	@Override
	public boolean getStatement(OraCdcStatementBase oraSql) {
		boolean result = false;
		do {
			result = tailer.readDocument(oraSql);
			if (result) {
				if (!willItRolledBack(oraSql)) {
					tailerOffset++;
					break;
				}
			} else {
				tailerOffset++;
				continue;
			}
		} while (result);
		return result;
	}

	public boolean getStatement(OraCdcStatementBase oraSql, List<OraCdcLargeObjectHolder> lobs) {
		boolean result = getStatement(oraSql);
		if (result) {
			for (int i = 0; i < oraSql.getLobCount(); i++) {
				OraCdcLargeObjectHolder lobHolder = new OraCdcLargeObjectHolder();
				if (!lobsTailer.readDocument(lobHolder)) {
					break;
				} else {
					lobs.add(lobHolder);
				}
			}
		}
		return result;
	}

	public boolean getLobs(final int lobCount, final List<OraCdcLargeObjectHolder> lobs) {
		boolean result = true;
		for (int i = 0; i < lobCount; i++) {
			OraCdcLargeObjectHolder lobHolder = new OraCdcLargeObjectHolder();
			result = result && lobsTailer.readDocument(lobHolder);
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
			LOGGER.debug("Closing Cronicle Queue and deleting memory-mapped files for transaction {}.", getXid());
		}
		if (tailer != null) {
			tailer.close();
			tailer = null;
		}
		if (appender != null) {
			appender.close();
			appender = null;
		}
		if (processLobs != LobProcessingStatus.NOT_AT_ALL) {
			if (processLobs == LobProcessingStatus.REDOMINER) {
				closeLobFiles();
			} else {
				// LobProcessingStatus.LOGMINER
				if (lobs != null) {
					if (lobsTailer != null) {
						lobsTailer.close();
						lobsTailer = null;
					}
					if (lobsAppender != null) {
						lobsAppender.close();
						lobsAppender = null;
					}
					lobs.close();
					lobs = null;
				}
			}
			deleteDir(lobsQueueDirectory);
		}
		if (statements != null) {
			statements.close();
			statements = null;
		}
		deleteDir(queueDirectory);
	}

	private void deleteDir(final Path directory) {
		try {
			Files.walk(directory)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile)
				.forEach(File::delete);
		} catch (NoSuchFileException nsf) {
			LOGGER.error(nsf.getMessage());
		} catch (IOException ioe) {
			LOGGER.error(ioe.getMessage());
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
		sb.append(queueDirectory.toString());
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

	public Path getPath() {
		return queueDirectory;
	}

	@Override
	public long size() {
		return transSize;
	}

	@Override
	public void setCommitScn(long commitScn) {
		if (queueSize > 0)
			lastIndexAppended = appender.lastIndexAppended();
		else
			lastIndexAppended = 0;
		appender.close();
		appender = null;
		tailer = statements.createTailer();
		if (processLobs == LobProcessingStatus.REDOMINER)
			closeLobFiles();
		super.setCommitScn(commitScn);
	}

	@Override
	public void setCommitScn(long commitScn, OraCdcPseudoColumnsProcessor pseudoColumns, ResultSet resultSet) throws SQLException {
		super.setCommitScn(commitScn, pseudoColumns, resultSet);
	}

}
