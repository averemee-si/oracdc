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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import solutions.a2.oracle.internals.CMapInflater;
import solutions.a2.oracle.internals.LobId;
import solutions.a2.oracle.internals.LobLocator;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.utils.ExceptionUtils;

import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.LOB_OP_END;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.LOB_OP_ERASE;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.LOB_OP_PREPARE;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.LOB_OP_TRIM;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.LOB_OP_UNKNOWN;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.LOB_OP_WRITE;
import static solutions.a2.oracle.internals.LobLocator.BLOB;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcTransactionChronicleQueue extends OraCdcTransaction {

	public enum LobProcessingStatus {NOT_AT_ALL, LOGMINER, REDOMINER};


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

	private final Path queueDirectory;
	private final Path lobsQueueDirectory;
	private final String lobDirectory;
	private final LobProcessingStatus processLobs;
	private final Map<LobId, LobHolder> transLobs;
	private final Map<Long, LobId> lobCols;
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
		super(xid, firstChange, isCdb);
		LOGGER.debug("BEGIN: create OraCdcTransactionChronicleQueue for new transaction");
		this.processLobs = processLobs;
		queueDirectory = Files.createTempDirectory(rootDir, xid + ".");
		if (processLobs == LobProcessingStatus.NOT_AT_ALL) {
			lobDirectory = null;
			lobsQueueDirectory = null;
			transLobs = null;
			lobCols = null;
		} else {
			lobDirectory = queueDirectory.toString() + ".LOBDATA";
			lobsQueueDirectory = Files.createDirectory(Paths.get(lobDirectory));
			if (processLobs == LobProcessingStatus.REDOMINER) {
				transLobs = new HashMap<>();
				lobCols = new HashMap<>();
			} else {
				// processLobs == LobProcessingStatus.LOGMINER
				transLobs = null;
				lobCols = null;
			}
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Created row data queue directory {} for transaction XID {}.",
					queueDirectory.toString(), xid);
			if (processLobs != LobProcessingStatus.NOT_AT_ALL) {
				LOGGER.debug("Created LOB data queue directory {} for transaction XID {}.",
						lobDirectory, xid);
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
			if (processLobs == LobProcessingStatus.REDOMINER) {
				
			} else if (processLobs == LobProcessingStatus.LOGMINER) {
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
	 * @param isCdb
	 * @throws IOException
	 */
	public OraCdcTransactionChronicleQueue(
			final Path rootDir, final String xid, final OraCdcStatementBase firstStatement, final boolean isCdb) throws IOException {
		this(LobProcessingStatus.NOT_AT_ALL, rootDir, xid, firstStatement.getScn(), isCdb);
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
	public int offset() {
		return tailerOffset;
	}

	public Map<String, Object> attrsAsMap() {
		final Map<String, Object> transAsMap = new LinkedHashMap<>();
		transAsMap.put(QUEUE_DIR, queueDirectory.toString());
		transAsMap.put(TRANS_XID, getXid());
		transAsMap.put(PROCESS_LOBS, processLobs);
		transAsMap.put(TRANS_FIRST_CHANGE, getFirstChange());
		transAsMap.put(TRANS_NEXT_CHANGE, getNextChange());
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
		sb.append(getNextChange());
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

	public void openLob(final LobId lid, final int obj, final short col, final byte lobOp, final RedoByteAddress rba, final boolean open) {
		LobHolder holder = transLobs.get(lid);
		if (holder == null) {
			holder = new LobHolder(lid, obj, col, lobsQueueDirectory);
			transLobs.put(lid, holder);
		}
		final LobId otherLid = lobCols.put(objCol(obj, col), lid);
		if (otherLid != null && transLobs.get(otherLid).chunks.size() > 0) {
			LOGGER.error(
					"\n=====================\n" +
					"Double entry in object {} column {} for lid '{}' and lid = {} at RBA {}, XID {}" +
					"\n=====================\n",
					Integer.toUnsignedLong(obj), Short.toUnsignedInt(col), lid, otherLid, rba, getXid());
		}
		if (open)
			holder.open(lobOp);
	}

	public void openLob(final int obj, final short col, final RedoByteAddress rba) {
		final LobId lobId = lobCols.get(objCol(obj, col));
		if (lobId == null) {
			LOGGER.error(
					"\n=====================\n" +
					"Attempting to open unknown LOB for OBJ {}/COL {} at RBA {}, XID {}" +
					"\n=====================\n",
					Integer.toUnsignedLong(obj), Short.toUnsignedInt(col), rba, getXid());
		} else {
			LobHolder holder = transLobs.get(lobId);
			if (!holder.binaryXml)
				holder.binaryXml = true;
			holder.open(LOB_OP_WRITE);
		}
	}
	public void closeLob(final int obj, final short col, final int size, final RedoByteAddress rba) throws IOException {
		final long objCol = objCol(obj, col);
		final LobId lobId = lobCols.get(objCol);
		if (lobId == null) {
			LOGGER.error(
					"\n=====================\n" +
					"Attempting to execute OP:11.17 TYP 3 on OBJ {}/COL {} without corresponding OP:11.17 TYP 1 at RBA {}, XID {}" +
					"\n=====================\n",
					Integer.toUnsignedLong(obj), Short.toUnsignedInt(col), rba, getXid());
		} else {
			LobHolder holder = transLobs.get(lobId);
			lobCols.remove(objCol);
			holder.close(size);
		}
	}

	private static long objCol(final int obj, final short col) {
		// col = ((byte)objCol) | ((objCol & 0x0000FF0000000000L) >> 40);
		// obj = (objCol & 0x000000FFFFFFFF00L) >> 8;
		return (Integer.toUnsignedLong((col & 0xFF00) << 8) << 40) | (Integer.toUnsignedLong(obj) << 8 ) | (byte)col;
	}

	public Set<LobId> lobIds(final boolean all) {
		if (processLobs == LobProcessingStatus.REDOMINER) {
			if (all)
				return transLobs.keySet();
			else {
				if (transLobs.keySet().size() > 0) {
					final Set<LobId> result = new HashSet<>();
					for (final LobId lid : transLobs.keySet()) {
						final LobHolder lh = transLobs.get(lid);
						if (lh.chunks.size() > 0)
//							&& lh.chunks.stream().mapToInt(c -> c.size).sum() > 0)
							result.add(lid);
					}
					return result;
				} else
					return transLobs.keySet();
			}
		}
		else
			return null;
	}

	public void writeLobChunk(final int obj, final short col, final byte[] data, final int off, final int len) throws SQLException {
		final LobId lobId = lobCols.get(objCol(obj, col));
		if (lobId == null) {
			LOGGER.error(
					"\n=====================\n" +
					"Attempting to write to unknown LOB for OBJ {}/COL {} in XID {}" +
					"\n=====================\n",
					Integer.toUnsignedLong(obj), Short.toUnsignedInt(col), getXid());
		} else {
			writeLobChunk(lobId, data, off, len, false, false);
		}
	}

	public void writeLobChunk(final LobId lid, final byte[] data, final int off, final int len, final boolean colb, final boolean cmap) throws SQLException {
		LobHolder holder = transLobs.get(lid);
		if (holder == null) {
			holder = new LobHolder(lid, -1, (short)0, lobsQueueDirectory);
			transLobs.put(lid, holder);
			holder.open(LOB_OP_WRITE);
		} else {
			try {
				holder.write(data, off, len, colb, cmap);
			} catch (IOException ioe) {
				throw new SQLException(ioe);
			}
		}
	}

	public void writeLobChunk(final LobId lid, final int obj, final byte[] data, final int off, final int len, final boolean cmap) throws SQLException {
		LobHolder holder = transLobs.get(lid);
		if (holder == null) {
			holder = new LobHolder(lid, obj, (short)0, lobsQueueDirectory);
			transLobs.put(lid, holder);
		}
		holder.open(LOB_OP_WRITE);
		try {
			holder.write(data, off, len, false, cmap);
			holder.close(len);	
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	private void closeLobFiles() {
		for (LobHolder closeIt : transLobs.values())
			if (closeIt.lastChunk != null) {
				LOGGER.warn(
						"Output stream for LOB '{}', chunk # is in incorrect OPEN state!",
						closeIt.lid.toString(), closeIt.chunks.size());
				try { closeIt.lastChunk.os.close();} catch (Exception e) {}
			}
	}

	public byte[] getLob(final LobLocator ll) throws SQLException {
		LobHolder holder = transLobs.get(ll.lid());
		if (holder == null) {
			LOGGER.error(
					"\n=====================\n" +
					"Attempt to read from unknown LOB {}!" +
					"\n=====================\n",
					ll.lid());
			throw new SQLException("Attempt to read from unknown LOB " + ll.lid() + " !");
		} else {
			if (holder.chunks.size() > 0) {
				final boolean clob = ll.type() != BLOB;
				final int expected = ll.dataLength() == 0 
										? holder.chunks.stream().mapToInt(c -> c.size).sum() 
										:ll.dataLength();
				//TODO int overflow?
				final ByteArrayOutputStream baos = new ByteArrayOutputStream(clob ? expected * 2 : expected);
				for (int i = 0; i < holder.chunks.size();) {
					final LobChunk chunk = holder.chunks.get(i);
					try {
						final byte[] ba = Files.readAllBytes(Paths.get(holder.directory, ll.lid().toString() + "." + (++i)));
						if (holder.colb) {
							baos.write(ba, 0, clob ? chunk.size * 2 : chunk.size);
						} else {
							if (holder.cmap) {
								CMapInflater.inflate(ba, baos);
							} else if (ll.dataCompressed() && !holder.binaryXml) {
								Inflater inflater = new Inflater();
								inflater.setInput(ba);
								final byte[] buffer = new byte[0x2000];
								try {
									while (!inflater.finished()) {
										int processed = inflater.inflate(buffer);
										baos.write(buffer, 0, processed);
									}
								} catch (DataFormatException dfe) {
									throw new SQLException(dfe);
								}
							} else {
								baos.write(ba);
							}
						}
					} catch (IOException ioe) {
						throw new SQLException(ioe);
					}
				}
				return baos.toByteArray();
			} else {
				return LobHolder.EMPTY;
			}
		}		
	}

	private static class LobHolder {
		private static final byte[] EMPTY = {};
		private final LobId lid;
		private final int obj;
		private final short col;
		private final List<LobChunk> chunks;
		private final String directory;
		private byte status = LOB_OP_UNKNOWN;
		private boolean colb;
		private boolean cmap;
		private LobChunk lastChunk;
		private boolean binaryXml = false;

		private LobHolder(final LobId lid, final int obj, final short col, final Path path) {
			this.lid = lid;
			this.obj = obj;
			this.col = col;
			this.directory = path.toString();
			chunks = new ArrayList<>();
		}

		private void open(final byte status) {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug(
						"Changing LOB {} status from {} to {}", lid.toString(), this.status, status);
			this.status = status;
		}

		private void close(final int size) throws IOException {
			if (status == LOB_OP_UNKNOWN || status == LOB_OP_PREPARE || status == LOB_OP_END)
				LOGGER.error(
						"\n=====================\n" +
						"Attempting to execute OP:11.17 TYP 3 on OBJ {}/COL {}/LID {} without corresponding OP:11.17 TYP 1" +
						"\n=====================\n",
						Integer.toUnsignedLong(obj), Short.toUnsignedInt(col), lid.toString());
			if (lastChunk != null) {
				if (!binaryXml)
					lastChunk.size = size;
				lastChunk.os.close();
				lastChunk = null;
			}
		}

		void write(final byte[] data, final int off, final int len, final boolean colb, final boolean cmap) throws IOException {
			switch (status) {
			case LOB_OP_WRITE:
				if (lastChunk == null) {
					lastChunk = new LobChunk();
					chunks.add(lastChunk);
					final Path path = Paths.get(directory, lid.toString() + "." + chunks.size());
					lastChunk.os = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW);
					if (LOGGER.isDebugEnabled())
						LOGGER.debug("Successfully created {} for writing large object {}",
								path.toAbsolutePath().toString(), path.toString());
					this.colb = colb;
					this.cmap = cmap;
				}
				lastChunk.os.write(data, off, len);
				if (binaryXml)
					lastChunk.size += len;
				break;
			case LOB_OP_ERASE:
				//TODO
				break;
			case LOB_OP_TRIM:
				//TODO
				break;
			default:
				if (LOGGER.isDebugEnabled()) LOGGER.debug("Write to LOB {} in incorrect status {}", lid, status);
			}
		}

	}

	private static class LobChunk {
		private int size = 0;
		private OutputStream os;
	}

}
