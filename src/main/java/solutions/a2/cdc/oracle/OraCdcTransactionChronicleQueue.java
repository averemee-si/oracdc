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
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcTransactionChronicleQueue extends OraCdcTransactionBase {


	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTransactionChronicleQueue.class);
	private static final String QUEUE_DIR = "queueDirectory";
	private static final String QUEUE_OFFSET = "tailerOffset";
	private static final String PROCESS_LOBS = "processLobs";
	private static final String CQ_ISSUE_1446_RETRY_MSG = "Received https://github.com/OpenHFT/Chronicle-Queue/issues/1446, will try again";
	private static final String CQ_ISSUE_1446_MSG =
			"\n=====================\n" +
			"'{}' while initializing Chronicle Queue.\n" +
			"Perhaps this is https://github.com/OpenHFT/Chronicle-Queue/issues/1446\n" +
			"Please suggest increase the value of system property \"chronicle.table.store.timeoutMS\".\n" +
			"\tFor more information on Chronicle Queue parameters please visit https://github.com/OpenHFT/Chronicle-Queue/blob/ea/systemProperties.adoc .\n" +
			"=====================\n";
	private static final int LOCK_RETRY = 5;
	private static final int PARTIAL_ROLLBACK_HEAP_THRESHOLD = 10;

	private long nextChange;
	private final Path queueDirectory;
	private final Path lobsQueueDirectory;
	private final boolean processLobs;
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
	 * @throws IOException
	 */
	public OraCdcTransactionChronicleQueue(final boolean processLobs, final Path rootDir, final String xid) throws IOException {
		super(xid);
		LOGGER.debug("BEGIN: create OraCdcTransactionChronicleQueue for new transaction");
		this.processLobs = processLobs;
		queueDirectory = Files.createTempDirectory(rootDir, xid + ".");
		if (processLobs) {
			final String lobDirectory = queueDirectory.toString() + ".LOBDATA";
			lobsQueueDirectory = Files.createDirectory(Paths.get(lobDirectory));
		} else {
			lobsQueueDirectory = null;
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Created row data queue directory {} for transaction XID {}.",
					queueDirectory.toString(), xid);
			if (processLobs) {
				LOGGER.debug("Created LOB data queue directory {} for transaction XID {}.",
						lobsQueueDirectory.toString(), xid);
			}
		}
		try {
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
			if (processLobs) {
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
			LOGGER.error("Unable to create Chronicle Queue!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new IOException(e);
		}
		transSize = 0;
		LOGGER.debug("END: create OraCdcTransactionChronicleQueue for new transaction");
	}

	/**
	 * 
	 * Creates OraCdcTransaction for new transaction without LOBs
	 * 
	 * @param rootDir
	 * @param xid
	 * @param firstStatement
	 * @throws IOException
	 */
	public OraCdcTransactionChronicleQueue(
			final Path rootDir, final String xid, final OraCdcStatementBase firstStatement) throws IOException {
		this(false, rootDir, xid);
		this.addStatement(firstStatement);
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
							lmStmt.getTableId() == pre.tableId &&
							((pre.operation == OraCdcV$LogmnrContents.DELETE &&
								lmStmt.getOperation() == OraCdcV$LogmnrContents.INSERT) ||
							(pre.operation == OraCdcV$LogmnrContents.INSERT &&
							lmStmt.getOperation() == OraCdcV$LogmnrContents.DELETE) ||
							(pre.operation == OraCdcV$LogmnrContents.UPDATE &&
							lmStmt.getOperation() == OraCdcV$LogmnrContents.UPDATE)) &&
							pre.rowId.equals(lmStmt.getRowId())) {
						final Map.Entry<RedoByteAddress, Long> uniqueAddr = Map.entry(lmStmt.getRba(), lmStmt.getSsn());
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
					pre.tableId = lmStmt.getTableId();
					pre.operation = lmStmt.getOperation();
					pre.rowId = lmStmt.getRowId();
					pre.scn = lmStmt.getScn();
					pre.rsId = lmStmt.getRba();
					pre.ssn = lmStmt.getSsn();
					nonRollback[i++] = pre;
				}
			} while (readResult);
			// Step 2
			for (final PartialRollbackEntry pre : rollbackEntriesList) {
				printPartialRollbackEntryDebug(pre);
				boolean pairFound = false;
				for (i = nonRollback.length - 1; i >= 0; i--) {
					if (nonRollback[i].tableId == pre.tableId &&
							((pre.operation == OraCdcV$LogmnrContents.DELETE &&
								nonRollback[i].operation == OraCdcV$LogmnrContents.INSERT) ||
							(pre.operation == OraCdcV$LogmnrContents.INSERT &&
									nonRollback[i].operation == OraCdcV$LogmnrContents.DELETE) ||
							(pre.operation == OraCdcV$LogmnrContents.UPDATE &&
									nonRollback[i].operation == OraCdcV$LogmnrContents.UPDATE)) &&
							pre.rowId.equals(nonRollback[i].rowId)) {
						final Map.Entry<RedoByteAddress, Long> uniqueAddr = Map.entry(nonRollback[i].rsId, nonRollback[i].ssn);
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
			LOGGER.debug("List of rollback pairs:");
			rollbackPairs.forEach(entry -> {
				LOGGER.debug("\tRBA={}, SSN={}", entry.getKey(), entry.getValue());
			});
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

	private void addStatementInt(final OraCdcStatementBase oraSql) {
		checkForRollback(oraSql, firstRecord ? - 1 : appender.lastIndexAppended());
		appender.writeDocument(oraSql);
		nextChange = oraSql.getScn();
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
		if (processLobs) {
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
	public int offset() {
		return tailerOffset;
	}

	public Map<String, Object> attrsAsMap() {
		final Map<String, Object> transAsMap = new LinkedHashMap<>();
		transAsMap.put(QUEUE_DIR, queueDirectory.toString());
		transAsMap.put(TRANS_XID, getXid());
		transAsMap.put(PROCESS_LOBS, processLobs);
		transAsMap.put(TRANS_FIRST_CHANGE, getFirstChange());
		transAsMap.put(TRANS_NEXT_CHANGE, nextChange);
		transAsMap.put(QUEUE_SIZE, queueSize);
		transAsMap.put(QUEUE_OFFSET, tailerOffset);
		if (getCommitScn() != 0) {
			transAsMap.put(TRANS_COMMIT_SCN, getCommitScn());
		}
		return transAsMap;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(128);
		sb.append("oracdc Transaction: ");
		sb.append(TRANS_XID);
		sb.append(" = ");
		sb.append(getXid());
		sb.append(" located in the '");
		sb.append(queueDirectory.toString());
		sb.append("', ");
		sb.append(PROCESS_LOBS);
		sb.append(" = ");
		sb.append(processLobs);
		sb.append(", ");
		sb.append(QUEUE_SIZE);
		sb.append(" = ");
		sb.append(queueSize);
		sb.append(", ");
		sb.append(TRANS_FIRST_CHANGE);
		sb.append(" = ");
		sb.append(getFirstChange());
		sb.append(", ");
		sb.append(TRANS_NEXT_CHANGE);
		sb.append(" = ");
		sb.append(nextChange);
		if (getCommitScn() != 0) {
			sb.append(", ");
			sb.append(TRANS_COMMIT_SCN);
			sb.append(" = ");
			sb.append(getCommitScn());
		}
		if (tailerOffset > 0) {
			sb.append(", ");
			sb.append(QUEUE_OFFSET);
			sb.append(" = ");
			sb.append(tailerOffset);
		}
		sb.append(".");

		return sb.toString();
	}

	@Override
	public long getNextChange() {
		return nextChange;
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
		lastIndexAppended = appender.lastIndexAppended();
		appender.close();
		appender = null;
		tailer = statements.createTailer();		
		super.setCommitScn(commitScn);
	}

	@Override
	public void setCommitScn(long commitScn, OraCdcPseudoColumnsProcessor pseudoColumns, ResultSet resultSet) throws SQLException {
		super.setCommitScn(commitScn, pseudoColumns, resultSet);
	}

}
