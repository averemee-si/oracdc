/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.

 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.sink.jmx;

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
