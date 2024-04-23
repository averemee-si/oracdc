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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
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

	private long firstChange;
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
	private long transSize;

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
			tailer = statements.createTailer();
			appender = statements.acquireAppender();
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
				lobsAppender = lobs.acquireAppender();
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
			final Path rootDir, final String xid, final OraCdcLogMinerStatement firstStatement) throws IOException {
		this(false, rootDir, xid);
		this.addStatement(firstStatement);
	}

	/**
	 * 
	 * Restores OraCdcTransaction from previously created Chronicle queue file
	 * 
	 * @param processLobs
	 * @param queueDirectory
	 * @param xid
	 * @param firstChange
	 * @param nextChange
	 * @param commitScn
	 * @param queueSize
	 * @param savedTailerOffset
	 */
	public OraCdcTransactionChronicleQueue(
			final boolean processLobs, final Path queueDirectory, final String xid,
			final long firstChange, final long nextChange, final Long commitScn,
			final int queueSize, final int savedTailerOffset) throws IOException {
		super(xid);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("BEGIN: restore OraCdcTransaction for XID={} from {}",
					xid, queueDirectory);
		}
		this.processLobs = processLobs;
		this.queueDirectory = queueDirectory;
		this.queueSize = queueSize;
		if (processLobs) {
			lobsQueueDirectory = Paths.get(queueDirectory.toString() + ".LOBDATA");
		} else {
			lobsQueueDirectory = null;
		}
		try {
			statements = ChronicleQueue
				.singleBuilder(queueDirectory)
				.build();
			tailer = statements.createTailer();
			appender = statements.acquireAppender();
			if (lobsQueueDirectory != null) {
				lobs = ChronicleQueue
						.singleBuilder(lobsQueueDirectory)
						.build();
					lobsTailer = lobs.createTailer();
					lobsAppender = lobs.acquireAppender();
			}
		} catch (Exception e) {
			LOGGER.error("Unable to create Chronicle Queue!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new IOException(e);
		}
		this.firstChange = firstChange;
		this.nextChange = nextChange;
		setCommitScn(commitScn);
		tailerOffset = 0;
		while (tailerOffset < savedTailerOffset) {
			OraCdcLogMinerStatement oraSql = new OraCdcLogMinerStatement();
			final boolean result = getStatement(oraSql);
			if (!result) {
				throw new IOException("Chronicle Queue for data corruption!!!");
			}
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("Chronicle Queue for data rewind current offset={}, SCN={}",
					tailerOffset, oraSql.getScn());
			}
			if (processLobs) {
				for (int i = 0; i < (int) oraSql.getLobCount(); i++) {
					OraCdcLargeObjectHolder oraLob = new OraCdcLargeObjectHolder();
					final boolean lobResult = lobsTailer.readDocument(oraLob);
					if (!lobResult) {
						throw new IOException("Chronicle Queue for LOBS corruption!!!");
					}
				}
			}
		}
		//TODO - additional manipulations are required
		transSize = statements.lastIndex();

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Chronicle Queue Successfully restored in directory {} for transaction XID {} with {} records.",
					queueDirectory.toString(), xid, queueSize);
		}
	}

	/**
	 * 
	 * Restores OraCdcTransaction from previously created Chronicle queue file
	 * 
	 * @param queueDirectory
	 * @param xid
	 * @param firstChange
	 * @param nextChange
	 * @param commitScn
	 * @param queueSize
	 * @param savedTailerOffset
	 */
	public OraCdcTransactionChronicleQueue(
			final Path queueDirectory, final String xid,
			final long firstChange, final long nextChange, final Long commitScn,
			final int queueSize, final int savedTailerOffset) throws IOException {
		this(false, queueDirectory, xid, firstChange, nextChange, commitScn, queueSize, savedTailerOffset);
	}

	private boolean addStatementInt(final OraCdcLogMinerStatement oraSql) {
		if (firstChange == 0) {
			firstChange = oraSql.getScn();
		}
		final boolean isRollback = checkForRollback(oraSql);
		if (!isRollback) {
			appender.writeDocument(oraSql);
			nextChange = oraSql.getScn();
			queueSize++;
			transSize += oraSql.size();
		}
		return isRollback;
	}
	

	@Override
	public void addStatement(final OraCdcLogMinerStatement oraSql) {
		addStatementInt(oraSql);
	}

	public void addStatement(final OraCdcLogMinerStatement oraSql, final List<OraCdcLargeObjectHolder> lobs) {
		final boolean lobsExists;
		if (lobs == null) {
			lobsExists = false;
			oraSql.setLobCount((byte) 0);
		} else {
			lobsExists = true;
			oraSql.setLobCount((byte) lobs.size());
		}
		final boolean isRollback = addStatementInt(oraSql); 
		if (lobsExists && !isRollback) {
			for (int i = 0; i < lobs.size(); i++) {
				lobsAppender.writeDocument(lobs.get(i));
				transSize += lobs.get(i).size();
			}
		}
	}

	@Override
	public boolean getStatement(OraCdcLogMinerStatement oraSql) {
		boolean result;
		boolean rollback = false;
		do {
			result = tailer.readDocument(oraSql);
			if (result) {
				rollback = willItRolledBack(oraSql);
				if (!rollback) {
					firstChange = oraSql.getScn();
				}
				tailerOffset++;
			} else {
				break;
			}
		} while (rollback);
		return result;
	}

	public boolean getStatement(OraCdcLogMinerStatement oraSql, List<OraCdcLargeObjectHolder> lobs) {
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
		if (processLobs) {
			if (lobs != null) {
				lobs.close();
			}
			lobs = null;
		}
		if (statements != null) {
			statements.close();
		}
		statements = null;
		if (processLobs) {
			deleteDir(lobsQueueDirectory);
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
		transAsMap.put(TRANS_FIRST_CHANGE, firstChange);
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
		sb.append(firstChange);
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

	public static OraCdcTransaction restoreFromMap(Map<String, Object> attrs) throws IOException {
		final Path transDir = Paths.get((String) attrs.get(QUEUE_DIR));
		final String transXid = (String) attrs.get(TRANS_XID);
		final long transFirstChange = valueAsLong(attrs.get(TRANS_FIRST_CHANGE));
		final long transNextChange = valueAsLong(attrs.get(TRANS_NEXT_CHANGE));
		final int transQueueSize = (int) attrs.get(QUEUE_SIZE);
		final int transOffset = (int) attrs.get(QUEUE_OFFSET);
		final Object transCommitScnObj = attrs.get(TRANS_COMMIT_SCN);
		final Long transCommitScn = transCommitScnObj == null ? null : valueAsLong(transCommitScnObj);
		final Object transProcessLobsObj = attrs.get(PROCESS_LOBS);
		final Boolean transProcessLobs = transProcessLobsObj == null ? false : (Boolean) transProcessLobsObj;
		return new OraCdcTransactionChronicleQueue(transProcessLobs, transDir, transXid,
				transFirstChange, transNextChange, transCommitScn, transQueueSize, transOffset);
	}

	@Override
	public long getFirstChange() {
		return firstChange;
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

	private static long valueAsLong(Object value) {
		if (value instanceof Integer) {
			return ((Integer) value).intValue();
		} else {
			return (long) value;
		}
	}

}
