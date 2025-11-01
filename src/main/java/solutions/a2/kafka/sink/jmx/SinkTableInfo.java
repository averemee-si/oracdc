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
public class SinkTableInfo extends SinkInfoBase implements SinkTableInfoMBean {

	private long elapsedUpsertNanos;
	private long upsertRecordsCount;

	public SinkTableInfo(final String tableName) {
		super();
		elapsedUpsertNanos = 0;
		upsertRecordsCount = 0;
		registerMBean(this, tableName);
	}

	@Override
	public long getProcessingTimeMillis() {
		return ((elapsedUpsertNanos + elapsedDeleteNanos) / 1_000_000);
	}
	@Override
	public String getProcessingTime() {
		Duration duration = Duration.ofNanos(elapsedUpsertNanos + elapsedDeleteNanos);
		return formatDuration(duration);
	}
	@Override
	public long getProcessedRecordsCount() {
		return (upsertRecordsCount + deleteRecordsCount);
	}

	public void addUpsert(int processed, long opNanos) {
		upsertRecordsCount += processed;
		elapsedUpsertNanos += opNanos;
	}
	@Override
	public long getUpsertCount() {
		return upsertRecordsCount;
	}
	@Override
	public long getUpsertProcessingMillis() {
		return elapsedUpsertNanos / 1_000_000;
	}
	@Override
	public String getUpsertProcessingTime() {
		Duration duration = Duration.ofNanos(elapsedUpsertNanos);
		return formatDuration(duration);
	}
	@Override
	public double getUpsertsPerSecond() {
		if (upsertRecordsCount == 0 || elapsedUpsertNanos == 0) {
			return 0;
		} else {
			return round(((double)(upsertRecordsCount * 1_000_000_000)) / ((double) elapsedUpsertNanos), 2);
		}
	}

}
