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

package eu.solutions.a2.cdc.oracle.jmx;

import java.io.IOException;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.OraCdcLogMinerTask;
import eu.solutions.a2.cdc.oracle.OraRdbmsInfo;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcLogMinerMgmt extends OraCdcLogMinerMgmtBase implements OraCdcLogMinerMgmtMBean, OraCdcLogMinerMgmtIntf {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerMgmt.class);

	private List<String> tablesInProcessing = new ArrayList<>();
	private int tableOutOfScopeCount = 0;
	private int partitionsCount = 0;
	private long totalRecordsCount = 0;
	private long recordsRolledBackCount = 0;
	private int transactionsRolledBackCount = 0;
	private long recordsCommittedCount = 0;
	private int transactionsCommittedCount = 0;
	private long recordsSentCount = 0;
	private int batchesSentCount = 0;
	private long parseTimeElapsed = 0;
	private int parsePerSecond = 0;

	private final OraCdcLogMinerTask task;

	public OraCdcLogMinerMgmt(
			final OraRdbmsInfo rdbmsInfo, final String connectorName, final OraCdcLogMinerTask task) {
		super(rdbmsInfo, connectorName, "LogMiner-metrics");
		this.task = task;
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

	public void addPartitionInProcessing() {
		partitionsCount++;
	}
	@Override
	public int getPartitionsInProcessingCount() {
		return partitionsCount;
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

	public void addCommittedRecords(int committedRecords) {
		recordsCommittedCount += committedRecords;
		transactionsCommittedCount++;
	}
	@Override
	public long getCommittedRecordsCount() {
		return recordsCommittedCount;
	}
	@Override
	public int getCommittedTransactionsCount() {
		return transactionsCommittedCount;
	}

	public void addRolledBackRecords(int rolledBackRecords) {
		recordsRolledBackCount += rolledBackRecords;
		transactionsRolledBackCount++;
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

	@Override
	public void saveCurrentState() {
		if (task != null) {
			try {
				task.saveState(false);
			} catch (IOException ioe) {
				LOGGER.error("Unable to save state to file from JMX subsys!");
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
			}
		}
	}

	@Override
	public void saveCurrentTablesSchema() {
		if (task != null) {
			try {
				task.saveTablesSchema();
			} catch (IOException ioe) {
				LOGGER.error("Unable to schemas to file from JMX subsys!");
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
			}
		}
	}

}
