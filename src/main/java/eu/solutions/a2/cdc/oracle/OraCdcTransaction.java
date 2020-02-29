package eu.solutions.a2.cdc.oracle;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

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

	private final String xid;
	private long firstChange;
	private long nextChange;
	private long commitScn;
	private final Path queueDirectory;
	private ChronicleQueue statements;
	private ExcerptAppender appender;
	private ExcerptTailer tailer;
	private int queueSize = 0;

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
			appender = this.statements.acquireAppender();
			appender.writeDocument(firstStatement);
			queueSize = 1;
		} catch (Exception e) {
			LOGGER.error("Unable to create Chronicle Queue!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new IOException(e);
		}
	}

	public void createTailer() {
		tailer = statements.createTailer();
	}

	public void addStatement(final OraCdcLogMinerStatement oraSql) {
		appender.writeDocument(oraSql);
		nextChange = oraSql.getScn();
		queueSize++;
	}

	public boolean getStatement(OraCdcLogMinerStatement oraSql) {
		final boolean result = tailer.readDocument(oraSql);
		firstChange = oraSql.getScn();
		return result;
	}

	public void close() {
		LOGGER.trace("Closing Cronicle Queue and deleting files.");
		statements.close();
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

	public String getXid() {
		return xid;
	}

	public long getFirstChange() {
		return firstChange;
	}

	public long getNextChange() {
		return nextChange;
	}

	public long getCommitScn() {
		return commitScn;
	}

	public void setCommitScn(long commitScn) {
		this.commitScn = commitScn;
	}

}
