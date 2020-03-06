package eu.solutions.a2.cdc.oracle;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
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

	public static final String QUEUE_DIR = "queueDirectory";
	public static final String TRANS_XID = "xid";
	public static final String TRANS_FIRST_CHANGE = "firstChange";
	public static final String TRANS_NEXT_CHANGE = "nextChange";
	public static final String QUEUE_SIZE = "queueSize";
	public static final String QUEUE_OFFSET = "tailerOffset";
	public static final String TRANS_COMMIT_SCN = "commitScn";

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTransaction.class);

	private final String xid;
	private long firstChange;
	private long nextChange;
	private Long commitScn;
	private final Path queueDirectory;
	private ChronicleQueue statements;
	private ExcerptAppender appender;
	private ExcerptTailer tailer;
	private int queueSize;
	private int tailerOffset;

	/**
	 * 
	 * Creates OraCdcTransaction for new transaction
	 * 
	 * @param rootDir
	 * @param xid
	 * @param firstStatement
	 * @throws IOException
	 */
	public OraCdcTransaction(
			final Path rootDir, final String xid, final OraCdcLogMinerStatement firstStatement) throws IOException {
		LOGGER.trace("BEGIN: create OraCdcTransaction for new transaction");
		this.xid = xid;
		firstChange = firstStatement.getScn();
		nextChange = firstChange;

		queueDirectory = Files.createTempDirectory(rootDir, xid + ".");
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Created queue directory {} for transaction XID {}.",
					queueDirectory.toString(), xid);
		}
		try {
			statements = ChronicleQueue
				.singleBuilder(queueDirectory)
				.build();
			tailer = this.statements.createTailer();
			appender = this.statements.acquireAppender();
			appender.writeDocument(firstStatement);
			queueSize = 1;
			tailerOffset = 0;
		} catch (Exception e) {
			LOGGER.error("Unable to create Chronicle Queue!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new IOException(e);
		}
		LOGGER.trace("END: create OraCdcTransaction for new transaction");
	}

	/**
	 * 
	 * Restores OraCdcTransaction from previously creates Chronicle queue file
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
			final int queueSize, final Integer savedTailerOffset) throws IOException {
		LOGGER.trace("BEGIN: restore OraCdcTransaction from filesystem");
		this.queueDirectory = queueDirectory;
		this.xid = xid;
		this.queueSize = queueSize;
		try {
			statements = ChronicleQueue
				.singleBuilder(queueDirectory)
				.build();
			tailer = this.statements.createTailer();
			appender = this.statements.acquireAppender();
		} catch (Exception e) {
			LOGGER.error("Unable to create Chronicle Queue!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new IOException(e);
		}
		this.firstChange = firstChange;
		this.nextChange = nextChange;
		this.commitScn = commitScn;
		this.tailerOffset = 0;
		while (this.tailerOffset < savedTailerOffset) {
			LOGGER.trace("rewind to stored offset");
			OraCdcLogMinerStatement oraSql = new OraCdcLogMinerStatement();
			final boolean result = this.getStatement(oraSql);
			if (!result) {
				throw new IOException("Chronicle Queue corruption!!!");
			}
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Chronicle Queue Successfully restored in directory {} for transaction XID {} with {} records.",
					queueDirectory.toString(), xid, queueSize);
		}
		LOGGER.trace("END: restore OraCdcTransaction from filesystem");
	}

	public void addStatement(final OraCdcLogMinerStatement oraSql) {
		appender.writeDocument(oraSql);
		nextChange = oraSql.getScn();
		queueSize++;
	}

	public boolean getStatement(OraCdcLogMinerStatement oraSql) {
		final boolean result = tailer.readDocument(oraSql);
		firstChange = oraSql.getScn();
		tailerOffset++;
		return result;
	}

	public void close() {
		LOGGER.trace("Closing Cronicle Queue and deleting files.");
		if (statements != null) {
			statements.close();
		}
		statements = null;
		try {
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
		transAsMap.put(TRANS_FIRST_CHANGE, firstChange);
		transAsMap.put(TRANS_NEXT_CHANGE, nextChange);
		transAsMap.put(QUEUE_SIZE, queueSize);
		transAsMap.put(QUEUE_OFFSET, tailerOffset);
		if (commitScn != null) {
			transAsMap.put(TRANS_COMMIT_SCN, commitScn);
		}
		return transAsMap;
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

}
