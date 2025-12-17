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

package solutions.a2.cdc.oracle.jmx;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.commons.math3.util.Precision;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.OraRdbmsInfo;
import solutions.a2.cdc.oracle.utils.LimitedSizeQueue;
import solutions.a2.utils.ExceptionUtils;
import solutions.a2.utils.OraCdcMBeanUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcSourceConnMgmt implements OraCdcSourceConnMgmtMBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcSourceConnMgmt.class);

	private List<String> tablesInProcessing = new ArrayList<>();
	private List<String> nowProcessedRedologs;
	private LimitedSizeQueue<String> lastHundredProcessed = new LimitedSizeQueue<>(100);
	private long currentFirstScn;
	private long currentNextScn;
	private int processedArchivedRedoCount;
	private long processedArchivedRedoSize;
	private long startTimeMillis;
	private LocalDateTime startTime;
	private long startScn;
	private String lastRedoLog;
	private LocalDateTime lastRedoLogTime;
	private long lastScn = 0;
	private long redoReadTimeElapsed = 0;
	private float redoReadMbPerSec = 0;
	private int lagSeconds = -1;
	private int partitionsCount = 0;
	private int tableOutOfScopeCount = 0;

	private long totalRecordsCount = 0;
	private long recordsRolledBackCount = 0;
	private int transactionsRolledBackCount = 0;
	private long recordsCommittedCount = 0;
	private int transactionsCommittedCount = 0;
	private long recordsSentCount = 0;
	private int batchesSentCount = 0;
	private long parseTimeElapsed = 0;
	private int parsePerSecond = 0;
	private int ddlColumnsCount = 0;
	private long ddlTimeElapsed = 0;
	private long bytesWrittenCQ = 0;
	private long maxTransSizeBytes = 0;
	private int currentTransSendCount = 0;
	private int maxTransSendCount = 0;
	private int currentTransProcessingCount = 0;
	private int maxTransProcessingCount = 0;
	private long lastProcessedSequence = 0;

	public OraCdcSourceConnMgmt(
			final OraRdbmsInfo rdbmsInfo, final String connectorName, final String jmxTypeName) {
		final StringBuilder sb = new StringBuilder(96);
		sb.append("solutions.a2.oracdc:type=");
		sb.append(jmxTypeName);
		sb.append(",name=");
		sb.append(connectorName);
		sb.append(",database=");
		sb.append(rdbmsInfo.getInstanceName());
		sb.append("_");
		sb.append(rdbmsInfo.getHostName());
		try {
			final ObjectName name = new ObjectName(sb.toString());
			final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			if (mbs.isRegistered(name)) {
				LOGGER.warn("JMX MBean {} already registered, trying to remove it.", name.getCanonicalName());
				try {
					mbs.unregisterMBean(name);
				} catch (InstanceNotFoundException nfe) {
					LOGGER.error("Unable to unregister MBean {}", name.getCanonicalName());
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(nfe));
					throw new ConnectException(nfe);
				}
			}
			mbs.registerMBean(this, name);
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
			LOGGER.error("Unable to register MBean {} !!! ", sb.toString());
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
	}

	public void start(long startScn) {
		this.startTimeMillis = System.currentTimeMillis();
		this.startTime = LocalDateTime.now();
		this.startScn = startScn;
	}
	@Override
	public long getStartScn() {
		return startScn;
	}
	@Override
	public String getStartTime() {
		return startTime.format(DateTimeFormatter.ISO_DATE_TIME);
	}

	public void setNowProcessed(
			final List<String> nowProcessedRedoLogs, final long currentFirstScn, final long currentNextScn,
			final int lagSeconds) {
		this.nowProcessedRedologs = nowProcessedRedoLogs;
		this.currentFirstScn = currentFirstScn;
		this.currentNextScn = currentNextScn;
		this.lagSeconds = lagSeconds;
	}
	@Override
	public String getCurrentlyProcessedRedoLog() {
		return nowProcessedRedologs.get(0);
	}
	@Override
	public long getCurrentFirstScn() {
		return currentFirstScn;
	}
	@Override
	public long getCurrentNextScn() {
		return currentNextScn;
	}

	public void addAlreadyProcessed(final List<String> lastProcessed, final int count, final long size,
			final long redoReadMillis) {
		final int listSize = lastProcessed.size();
		for (int i = 0; i < listSize; i++) {
			lastHundredProcessed.add(lastProcessed.get(i));
		}
		processedArchivedRedoCount += count;
		processedArchivedRedoSize += size;
		lastRedoLog = lastProcessed.get(listSize - 1);
		lastRedoLogTime = LocalDateTime.now();
		lastScn = currentNextScn;
		currentFirstScn = 0;
		currentNextScn = 0;
		redoReadTimeElapsed += redoReadMillis;
		if (redoReadTimeElapsed != 0) {
			float seconds = redoReadTimeElapsed / 1000;
			redoReadMbPerSec = Precision.round((processedArchivedRedoSize / (1024 * 1024)) / seconds, 3);
		}
	}
	@Override
	public String[] getLast100ProcessedRedoLogs() {
		return lastHundredProcessed.toArray(new String[0]);
	}
	@Override
	public int getProcessedRedoLogsCount() {
		return processedArchivedRedoCount;
	}
	@Override
	public float getProcessedRedoLogsSizeGb() {
		return Precision.round((float)((float)processedArchivedRedoSize / (float)(1024*1024*1024)), 3);
	}
	@Override
	public String getLastProcessedRedoLog() {
		return lastRedoLog;
	}
	@Override
	public long getLastProcessedScn() {
		return lastScn;
	}
	@Override
	public String getLastProcessedRedoLogTime() {
		if (lastRedoLog != null) {
			return lastRedoLogTime.format(DateTimeFormatter.ISO_DATE_TIME);
		} else {
			return null;
		}
	}

	public void addRecord() {
		totalRecordsCount++;
	}
	@Override
	public long getTotalRecordsCount() {
		return totalRecordsCount;
	}

	public void addCommittedRecords(
			final int committedRecords, final long transSize,
			final int currentSentSize, final int currentProcessingSize) {
		recordsCommittedCount += committedRecords;
		totalRecordsCount += committedRecords;
		transactionsCommittedCount++;
		bytesWrittenCQ += transSize;
		if (transSize > maxTransSizeBytes) {
			maxTransSizeBytes = transSize;
		}
		currentTransSendCount = currentSentSize;
		if (currentTransSendCount > maxTransSendCount) {
			maxTransSendCount = currentTransSendCount;
		}
		currentTransProcessingCount = currentProcessingSize;
		if (currentTransProcessingCount > maxTransProcessingCount) {
			maxTransProcessingCount = currentTransProcessingCount;
		}
	}
	@Override
	public long getCommittedRecordsCount() {
		return recordsCommittedCount;
	}
	@Override
	public int getCommittedTransactionsCount() {
		return transactionsCommittedCount;
	}

	public void addRolledBackRecords(
			final int rolledBackRecords, final long transSize, final int currentProcessingSize) {
		recordsRolledBackCount += rolledBackRecords;
		totalRecordsCount += rolledBackRecords;
		transactionsRolledBackCount++;
		bytesWrittenCQ += transSize;
		if (transSize > maxTransSizeBytes) {
			maxTransSizeBytes = transSize;
		}
		currentTransProcessingCount = currentProcessingSize;
		if (currentTransProcessingCount > maxTransProcessingCount) {
			maxTransProcessingCount = currentTransProcessingCount;
		}
	}
	@Override
	public long getRolledBackRecordsCount() {
		return recordsRolledBackCount;
	}
	@Override
	public int getRolledBackTransactionsCount() {
		return transactionsRolledBackCount;
	}

	public void addSentRecords(int sentRecords, int parseTime) {
		recordsSentCount += sentRecords;
		batchesSentCount++;
		parseTimeElapsed += parseTime;
		if (parseTimeElapsed != 0) {
			parsePerSecond = (int) Math.ceil(((double) recordsSentCount)/((double) (parseTimeElapsed / 1000)));
		} else {
			parsePerSecond = 0;
		}
	}
	@Override
	public long getSentRecordsCount() {
		return recordsSentCount;
	}
	@Override
	public int getSentBatchesCount() {
		return batchesSentCount;
	}
	@Override
	public long getParseElapsedMillis() {
		return parseTimeElapsed;
	}
	@Override
	public String getParseElapsed() {
		final Duration duration = Duration.ofMillis(parseTimeElapsed);
		return OraCdcMBeanUtils.formatDuration(duration);
	}
	@Override
	public int getParsePerSecond() {
		return parsePerSecond;
	}

	public void setCurrentFirstScn(long currentFirstScn) {
		this.currentFirstScn = currentFirstScn;
	}

	public long getStartTimeMillis() {
		return startTimeMillis;
	}

	@Override
	public long getRedoReadElapsedMillis() {
		return redoReadTimeElapsed;
	}
	@Override
	public String getRedoReadElapsed() {
		final Duration duration = Duration.ofMillis(redoReadTimeElapsed);
		return OraCdcMBeanUtils.formatDuration(duration);
	}
	@Override
	public float getRedoReadMbPerSecond() {
		return redoReadMbPerSec;
	}

	public void addPartitionInProcessing() {
		partitionsCount++;
	}
	@Override
	public int getPartitionsInProcessingCount() {
		return partitionsCount;
	}

	public void addTableInProcessing(final String tableName) {
		tablesInProcessing.add(tableName);
	}
	@Override
	public int getTablesInProcessingCount() {
		return tablesInProcessing.size();
	}
	@Override
	public String[] getTablesInProcessing() {
		return tablesInProcessing.toArray(new String[0]);
	}

	public void addTableOutOfScope() {
		tableOutOfScopeCount++;
	}
	@Override
	public int getTableOutOfScopeCount() {
		return tableOutOfScopeCount;
	}


	@Override
	public long getElapsedTimeMillis() {
		return System.currentTimeMillis() - startTimeMillis;
	}
	@Override
	public String getElapsedTime() {
		final Duration duration = Duration.ofMillis(System.currentTimeMillis() - startTimeMillis);
		return OraCdcMBeanUtils.formatDuration(duration);
	}

	@Override
	public int getActualLagSeconds() {
		return lagSeconds;
	}
	@Override
	public String getActualLagText() {
		if (lagSeconds > -1) {
			final Duration duration = Duration.ofSeconds(lagSeconds); 
			return OraCdcMBeanUtils.formatDuration(duration);
		} else {
			return "";
		}
	}


	public void addDdlMetrics(final int ddlCount, final long ddlElapsed) {
		ddlColumnsCount += ddlCount;
		ddlTimeElapsed += ddlElapsed;
	}
	@Override
	public int getDdlColumnsCount() {
		return ddlColumnsCount;
	}
	@Override
	public long getDdlElapsedMillis() {
		return ddlTimeElapsed;
	}
	@Override
	public String getDdlElapsed() {
		final Duration duration = Duration.ofMillis(ddlTimeElapsed);
		return OraCdcMBeanUtils.formatDuration(duration);
	}

	@Override
	public long getNumBytesWrittenUsingOffHeapMem() {
		return bytesWrittenCQ;
	}
	@Override
	public float getGiBWrittenUsingOffHeapMem() {
		if (bytesWrittenCQ == 0) {
			return 0;
		} else {
			return Precision.round((bytesWrittenCQ / (1024 * 1024 * 1024)), 3);
		}
	}
	@Override
	public long getMaxTransactionSizeBytes() {
		return maxTransSizeBytes;
	}
	@Override
	public float getMaxTransactionSizeMiB() {
		if (maxTransSizeBytes == 0) {
			return 0;
		} else {
			return Precision.round((maxTransSizeBytes / (1024 * 1024)), 3);
		}
	}

	@Override
	public int getMaxNumberOfTransInSendQueue() {
		return maxTransSendCount;
	}
	@Override
	public int getCurrentNumberOfTransInSendQueue() {
		return currentTransSendCount;
	}
	@Override
	public int getMaxNumberOfTransInProcessingQueue() {
		return maxTransProcessingCount;
	}
	@Override
	public int getCurrentNumberOfTransInProcessingQueue() {
		return currentTransProcessingCount;
	}
	@Override
	public long getLastProcessedSequence() {
		return lastProcessedSequence;
	}
	public void setLastProcessedSequence(final long lastProcessedSequence) {
		this.lastProcessedSequence = lastProcessedSequence;
	}
}
