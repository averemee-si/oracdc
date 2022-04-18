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

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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

import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcSinkTableInfo implements OraCdcSinkTableInfoMBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcSinkTableInfoMBean.class);

	private long startTimeMillis;
	private LocalDateTime startTime;
	private long elapsedUpsertNanos;
	private long upsertRecordsCount;
	private long elapsedDeleteNanos;
	private long deleteRecordsCount;

	public OraCdcSinkTableInfo(final String tableName) {
		this.startTimeMillis = System.currentTimeMillis();
		this.startTime = LocalDateTime.now();
		this.elapsedUpsertNanos = 0;
		this.upsertRecordsCount = 0;
		this.elapsedDeleteNanos = 0;
		this.deleteRecordsCount = 0;
		final StringBuilder sb = new StringBuilder(64);
		sb.append("eu.solutions.a2.oracdc:type=Sink-metrics,tableName=");
		sb.append(tableName);
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
			LOGGER.debug("MBean {} registered.", name.getCanonicalName());
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
			LOGGER.error("Unable to register MBean {} !!! ", sb.toString());
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
	}

	@Override
	public String getStartTime() {
		return startTime.format(DateTimeFormatter.ISO_DATE_TIME);
	}
	@Override
	public long getElapsedTimeMillis() {
		return System.currentTimeMillis() - startTimeMillis;
	}
	@Override
	public String getElapsedTime() {
		Duration duration = Duration.ofMillis(System.currentTimeMillis() - startTimeMillis);
		return OraCdcMBeanUtils.formatDuration(duration);
	}

	@Override
	public long getProcessingTimeMillis() {
		return ((elapsedUpsertNanos + elapsedDeleteNanos) / 1_000_000);
	}
	@Override
	public String getProcessingTime() {
		Duration duration = Duration.ofNanos(elapsedUpsertNanos + elapsedDeleteNanos);
		return OraCdcMBeanUtils.formatDuration(duration);
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
		return OraCdcMBeanUtils.formatDuration(duration);
	}
	@Override
	public double getUpsertsPerSecond() {
		if (upsertRecordsCount == 0 || elapsedUpsertNanos == 0) {
			return 0;
		} else {
			return Precision.round(((double)(upsertRecordsCount * 1_000_000_000)) / ((double) elapsedUpsertNanos), 2);
		}
	}

	public void addDelete(int processed, long opNanos) {
		deleteRecordsCount += processed;
		elapsedDeleteNanos += opNanos;
	}
	@Override
	public long getDeleteCount() {
		return deleteRecordsCount;
	}
	@Override
	public long getDeleteProcessingMillis() {
		return elapsedDeleteNanos / 1_000_000;
	}
	@Override
	public String getDeleteProcessingTime() {
		Duration duration = Duration.ofNanos(elapsedDeleteNanos);
		return OraCdcMBeanUtils.formatDuration(duration);
	}
	@Override
	public double getDeletesPerSecond() {
		if (deleteRecordsCount == 0 || elapsedDeleteNanos == 0) {
			return 0;
		} else {
			return Precision.round(((double)(deleteRecordsCount * 1_000_000_000)) / ((double) elapsedDeleteNanos), 2);
		}
	}

}
