/**
 * Copyright (c) 2018-present, http://a2-solutions.eu
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

package eu.solutions.a2.cdc.oracle.standalone;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public class CommonJobMgmt implements CommonJobMgmtMBean {

	private final long startTimeMillis = System.currentTimeMillis();
	private int tableCount;
	private final AtomicLong recordCount = new AtomicLong(0L);
	private final AtomicLong bytesSent = new AtomicLong(0L);
	private final AtomicLong processingTime = new AtomicLong(0L);

	public void addRecordData(long recordSize, long elapsedMillis) {
		recordCount.incrementAndGet();
		bytesSent.addAndGet(recordSize);
		processingTime.addAndGet(elapsedMillis);
	}

	public int getTableCount() {
		return tableCount;
	}

	public void setTableCount(int tableCount) {
		this.tableCount = tableCount;
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

	@Override
	public long getRecordCount() {
		return recordCount.longValue();
	}

	@Override
	public long getTotalBytesSent() {
		return bytesSent.longValue();
	}

	@Override
	public long getTransferTimeMillis() {
		return processingTime.longValue();
	}

	@Override
	public String getTransferTime() {
		Duration duration = Duration.ofMillis(processingTime.longValue());
		return String.format("%sdays %shrs %smin %ssec.\n",
				duration.toDays(),
				duration.toHours() % 24,
				duration.toMinutes() % 60,
				duration.getSeconds() % 60);
	}

}
