package eu.solutions.a2.cdc.oracle;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcTransaction {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTransaction.class);
	private static final String QUEUE_DIR = "queueDirectory";
	private static final String TRANS_XID = "xid";
	private static final String TRANS_FIRST_CHANGE = "firstChange";
	private static final String TRANS_NEXT_CHANGE = "nextChange";
	private static final String QUEUE_SIZE = "queueSize";
	private static final String QUEUE_OFFSET = "tailerOffset";
	private static final String TRANS_COMMIT_SCN = "commitScn";
	private static final String PROCESS_LOBS = "processLobs";

	private final String xid;
	private long firstChange;
	private long nextChange;
	private Long commitScn;
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
	public OraCdcTransaction(final boolean processLobs, final Path rootDir, final String xid) throws IOException {
		LOGGER.trace("BEGIN: create OraCdcTransaction for new transaction");
		this.xid = xid;
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
			statements = ChronicleQueue
				.singleBuilder(queueDirectory)
				.build();
			tailer = statements.createTailer();
			appender = statements.acquireAppender();
			queueSize = 0;
			tailerOffset = 0;
			if (processLobs) {
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
		transSize = 0;
		LOGGER.trace("END: create OraCdcTransaction for new transaction");
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
	public OraCdcTransaction(
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
	public OraCdcTransaction(
			final boolean processLobs, final Path queueDirectory, final String xid,
			final long firstChange, final long nextChange, final Long commitScn,
			final int queueSize, final int savedTailerOffset) throws IOException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("BEGIN: restore OraCdcTransaction for XID={} from {}",
					xid, queueDirectory);
		}
		this.processLobs = processLobs;
		this.queueDirectory = queueDirectory;
		this.xid = xid;
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
		this.commitScn = commitScn;
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
	public OraCdcTransaction(
			final Path queueDirectory, final String xid,
			final long firstChange, final long nextChange, final Long commitScn,
			final int queueSize, final int savedTailerOffset) throws IOException {
		this(false, queueDirectory, xid, firstChange, nextChange, commitScn, queueSize, savedTailerOffset);
	}

	public void addStatement(final OraCdcLogMinerStatement oraSql) {
		if (firstChange == 0) {
			firstChange = oraSql.getScn();
		}
		appender.writeDocument(oraSql);
		nextChange = oraSql.getScn();
		queueSize++;
		transSize += oraSql.size();
	}

	public void addStatement(final OraCdcLogMinerStatement oraSql, final List<OraCdcLargeObjectHolder> lobs) {
		final boolean lobsExists;
		if (lobs == null) {
			lobsExists = false;
		} else {
			lobsExists = true;
		}
		if (lobsExists) {
			oraSql.setLobCount((byte) lobs.size());
		} else {
			oraSql.setLobCount((byte) 0);
		}
		addStatement(oraSql);
		if (lobsExists) {
			for (int i = 0; i < lobs.size(); i++) {
				lobsAppender.writeDocument(lobs.get(i));
				transSize += lobs.get(i).size();
			}
		}
	}

	public boolean getStatement(OraCdcLogMinerStatement oraSql) {
		final boolean result = tailer.readDocument(oraSql);
		firstChange = oraSql.getScn();
		tailerOffset++;
		return result;
	}

	public boolean getStatement(OraCdcLogMinerStatement oraSql, List<OraCdcLargeObjectHolder> lobs) {
		boolean result = tailer.readDocument(oraSql);
		firstChange = oraSql.getScn();
		tailerOffset++;
		for (int i = 0; i < oraSql.getLobCount(); i++) {
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

	public void close() {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Closing Cronicle Queue and deleting memory-mapped files for transaction {}.", xid);
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
		try {
			if (processLobs) {
				Files.walk(lobsQueueDirectory)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile)
				.forEach(File::delete);
			}
			Files.walk(queueDirectory)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile)
				.forEach(File::delete);
		} catch (IOException ioe) {
			LOGGER.error("Unable to delete Cronicle Queue files.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
		}
	}

	public int length() {
		return queueSize;
	}

	public int offset() {
		return tailerOffset;
	}

	public Map<String, Object> attrsAsMap() {
		final Map<String, Object> transAsMap = new LinkedHashMap<>();
		transAsMap.put(QUEUE_DIR, queueDirectory.toString());
		transAsMap.put(TRANS_XID, xid);
		transAsMap.put(PROCESS_LOBS, processLobs);
		transAsMap.put(TRANS_FIRST_CHANGE, firstChange);
		transAsMap.put(TRANS_NEXT_CHANGE, nextChange);
		transAsMap.put(QUEUE_SIZE, queueSize);
		transAsMap.put(QUEUE_OFFSET, tailerOffset);
		if (commitScn != null) {
			transAsMap.put(TRANS_COMMIT_SCN, commitScn);
		}
		return transAsMap;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(128);
		sb.append("oracdc Transaction: ");
		sb.append(TRANS_XID);
		sb.append(" = ");
		sb.append(xid);
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
		if (commitScn != null) {
			sb.append(", ");
			sb.append(TRANS_COMMIT_SCN);
			sb.append(" = ");
			sb.append(commitScn);
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
		return new OraCdcTransaction(transProcessLobs, transDir, transXid,
				transFirstChange, transNextChange, transCommitScn, transQueueSize, transOffset);
	}

	public String getXid() {
		return xid;
	}

	public long getFirstChange() {
		return firstChange;
	}

	public long getNextChange() {
		return nextChange;
	}

	public Long getCommitScn() {
		return commitScn;
	}

	public void setCommitScn(Long commitScn) {
		this.commitScn = commitScn;
	}

	public Path getPath() {
		return queueDirectory;
	}

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
