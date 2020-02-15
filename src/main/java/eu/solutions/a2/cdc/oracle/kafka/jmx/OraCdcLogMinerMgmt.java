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

package eu.solutions.a2.cdc.oracle.kafka.jmx;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import eu.solutions.a2.cdc.oracle.utils.LimitedSizeQueue;

public class OraCdcLogMinerMgmt implements OraCdcLogMinerMgmtMBean {


	private List<String> tablesInProcessing = new ArrayList<>();
	private int tableOutOfScopeCount = 0;
	private List<String> nowProcessedArchivelogs;
	private LimitedSizeQueue<String> lastHundredProcessed = new LimitedSizeQueue<>(100);
	private long currentFirstScn;
	private long currentNextScn;
	private int processedArchivedRedoCount;
	private long processedArchivedRedoSize;
	private long startTimeMillis;
	private LocalDateTime startTime;
	private long startScn;
	private long recordCount = 0;

	public void start(long startScn) {
		this.startTimeMillis = System.currentTimeMillis();
		this.startTime = LocalDateTime.now();
		this.startScn = startScn;
	}
	@Override
	public String getStartTime() {
		return startTime.format(DateTimeFormatter.ISO_DATE_TIME);
	}
	@Override
	public long getStartScn() {
		return startScn;
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

	public void setNowProcessed(
			final List<String> nowProcessedArchiveLogs, final long currentFirstScn, final long currentNextScn) {
		this.nowProcessedArchivelogs = nowProcessedArchiveLogs;
		this.currentFirstScn = currentFirstScn;
		this.currentNextScn = currentNextScn;
	}
	@Override
	public String[] getNowProcessedArchivelogs() {
		return nowProcessedArchivelogs.toArray(new String[0]);
	}
	@Override
	public long getCurrentFirstScn() {
		return currentFirstScn;
	}
	@Override
	public long getCurrentNextScn() {
		return currentNextScn;
	}

	public void addAlreadyProcessed(final List<String> lastProcessed, final int count, final long size) {
		for (int i = 0; i < lastProcessed.size(); i++) {
			lastHundredProcessed.add(lastProcessed.get(i));
		}
		processedArchivedRedoCount += count;
		processedArchivedRedoSize += size;
	}
	@Override
	public String[] getLast100ProcessedArchivelogs() {
		return lastHundredProcessed.toArray(new String[0]);
	}
	@Override
	public int getProcessedArchivelogsCount() {
		return processedArchivedRedoCount;
	}
	@Override
	public long getProcessedArchivelogsSize() {
		return processedArchivedRedoSize;
	}

	public void addRecordCount(int count) {
		recordCount += count;
	}
	@Override
	public long getRecordCount() {
		return recordCount;
	}

	@Override
	public long getElapsedTimeMillis() {
		return System.currentTimeMillis() - startTimeMillis;
	}

	@Override
	public String getElapsedTime() {
		Duration duration = Duration.ofMillis(System.currentTimeMillis() - startTimeMillis);
		return String.format("%sdays %shrs %smin %ssec.\n",
				duration.toDays(),
				duration.toHours() % 24,
				duration.toMinutes() % 60,
				duration.getSeconds() % 60);
	}


}
