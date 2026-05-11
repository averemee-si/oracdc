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
