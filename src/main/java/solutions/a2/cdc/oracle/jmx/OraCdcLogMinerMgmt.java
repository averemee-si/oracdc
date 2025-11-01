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

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.commons.math3.util.Precision;

import solutions.a2.cdc.oracle.OraRdbmsInfo;
import solutions.a2.utils.OraCdcMBeanUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcLogMinerMgmt extends OraCdcMgmtBase implements OraCdcLogMinerMgmtMBean, OraCdcLogMinerMgmtIntf {

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

	public OraCdcLogMinerMgmt(
			final OraRdbmsInfo rdbmsInfo, final String connectorName) {
		super(rdbmsInfo, connectorName, "LogMiner-metrics");
	}

	@Override
	public void start(long startScn) {
		super.start(startScn);
	}
	@Override
	public String getStartTime() {
		return super.getStartTimeLdt().format(DateTimeFormatter.ISO_DATE_TIME);
	}
	@Override
	public long getStartScn() {
		return super.getStartScn();
	}

	@Override
	public int getTablesInProcessingCount() {
		return tablesInProcessing().size();
	}
	@Override
	public String[] getTablesInProcessing() {
		return tablesInProcessing().toArray(new String[0]);
	}

	@Override
	public int getTableOutOfScopeCount() {
		return tableOutOfScopeCount();
	}

	@Override
	public int getPartitionsInProcessingCount() {
		return partitionsCount();
	}

	@Override
	public void setNowProcessed(
			final List<String> nowProcessedArchiveLogs, final long currentFirstScn, final long currentNextScn,
			final int lagSeconds) {
		super.setNowProcessed(nowProcessedArchiveLogs, currentFirstScn, currentNextScn, lagSeconds);
	}
	@Override
	public String[] getNowProcessedArchivelogs() {
		return super.getNowProcessedArchiveLogsList().toArray(new String[0]);
	}
	@Override
	public long getCurrentFirstScn() {
		return super.getCurrentFirstScn();
	}
	@Override
	public long getCurrentNextScn() {
		return super.getCurrentNextScn();
	}

	@Override
	public void addAlreadyProcessed(final List<String> lastProcessed, final int count, final long size,
			final long redoReadMillis) {
		super.addAlreadyProcessed(lastProcessed, count, size, redoReadMillis);
	}
	@Override
	public String[] getLast100ProcessedArchivelogs() {
		return super.getLastHundredProcessed().toArray(new String[0]);
	}
	@Override
	public int getProcessedArchivelogsCount() {
		return super.getProcessedArchivedRedoCount();
	}
	@Override
	public float getProcessedArchivelogsSizeGb() {
		return Precision.round((float)((float)super.getProcessedArchivedRedoSize() / (float)(1024*1024*1024)), 3);
	}
	@Override
	public String getLastProcessedArchivelog() {
		return super.getLastRedoLog();
	}
	@Override
	public long getLastProcessedScn() {
		return super.getLastScn();
	}
	@Override
	public String getLastProcessedArchivelogTime() {
		if (super.getLastRedoLogTime() != null) {
			return super.getLastRedoLogTime().format(DateTimeFormatter.ISO_DATE_TIME);
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

	@Override
	public long getRedoReadElapsedMillis() {
		return super.getRedoReadTimeElapsed();
	}
	@Override
	public String getRedoReadElapsed() {
		final Duration duration = Duration.ofMillis(super.getRedoReadTimeElapsed());
		return OraCdcMBeanUtils.formatDuration(duration);
	}
	@Override
	public float getRedoReadMbPerSecond() {
		return super.getRedoReadMbPerSec();
	}

	@Override
	public long getElapsedTimeMillis() {
		return System.currentTimeMillis() - super.getStartTimeMillis();
	}
	@Override
	public String getElapsedTime() {
		final Duration duration = Duration.ofMillis(System.currentTimeMillis() - super.getStartTimeMillis());
		return OraCdcMBeanUtils.formatDuration(duration);
	}

	@Override
	public int getActualLagSeconds() {
		return super.getLagSeconds();
	}
	@Override
	public String getActualLagText() {
		int lagSeconds = super.getLagSeconds();
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
	public long getNumBytesWrittenUsingChronicleQueue() {
		return bytesWrittenCQ;
	}
	@Override
	public float getGiBWrittenUsingChronicleQueue() {
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
	@Override
	public void setLastProcessedSequence(final long lastProcessedSequence) {
		this.lastProcessedSequence = lastProcessedSequence;
	}


}
