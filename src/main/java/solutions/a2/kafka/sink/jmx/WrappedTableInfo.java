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

package solutions.a2.kafka.sink.jmx;

import java.time.Duration;

import static org.apache.commons.math3.util.Precision.round;
import static solutions.a2.utils.OraCdcMBeanUtils.formatDuration;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class WrappedTableInfo extends SinkInfoBase implements WrappedTableInfoMBean {

	private long elapsedInsertNanos;
	private long insertRecordsCount;
	private long elapsedUpdateNanos;
	private long updateRecordsCount;

	public WrappedTableInfo(final String tableName) {
		super();
		elapsedInsertNanos = 0;
		insertRecordsCount = 0;
		elapsedUpdateNanos = 0;
		updateRecordsCount = 0;
		registerMBean(this, tableName);
	}

	@Override
	public long getProcessingTimeMillis() {
		return ((elapsedInsertNanos + elapsedUpdateNanos + elapsedDeleteNanos) / 1_000_000);
	}
	@Override
	public String getProcessingTime() {
		Duration duration = Duration.ofNanos(elapsedInsertNanos + elapsedUpdateNanos + elapsedDeleteNanos);
		return formatDuration(duration);
	}
	@Override
	public long getProcessedRecordsCount() {
		return (insertRecordsCount + updateRecordsCount + deleteRecordsCount);
	}

	public void addInsert(int processed, long opNanos) {
		insertRecordsCount += processed;
		elapsedInsertNanos += opNanos;
	}
	@Override
	public long getInsertCount() {
		return insertRecordsCount;
	}
	@Override
	public long getInsertProcessingMillis() {
		return elapsedInsertNanos / 1_000_000;
	}
	@Override
	public String getInsertProcessingTime() {
		Duration duration = Duration.ofNanos(elapsedInsertNanos);
		return formatDuration(duration);
	}
	@Override
	public double getInsertsPerSecond() {
		if (insertRecordsCount == 0 || elapsedInsertNanos == 0) {
			return 0;
		} else {
			return round(((double)(insertRecordsCount * 1_000_000_000)) / ((double) elapsedInsertNanos), 2);
		}
	}

	public void addUpdate(int processed, long opNanos) {
		updateRecordsCount += processed;
		elapsedUpdateNanos += opNanos;
	}
	@Override
	public long getUpdateCount() {
		return updateRecordsCount;
	}
	@Override
	public long getUpdateProcessingMillis() {
		return elapsedUpdateNanos / 1_000_000;
	}
	@Override
	public String getUpdateProcessingTime() {
		Duration duration = Duration.ofNanos(elapsedUpdateNanos);
		return formatDuration(duration);
	}
	@Override
	public double getUpdatesPerSecond() {
		if (updateRecordsCount == 0 || elapsedUpdateNanos == 0) {
			return 0;
		} else {
			return round(((double)(updateRecordsCount * 1_000_000_000)) / ((double) elapsedUpdateNanos), 2);
		}
	}

}
