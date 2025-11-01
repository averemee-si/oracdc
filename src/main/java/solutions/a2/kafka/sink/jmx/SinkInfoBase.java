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

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.LocalDateTime;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.utils.ExceptionUtils;

import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static org.apache.commons.math3.util.Precision.round;
import static solutions.a2.utils.OraCdcMBeanUtils.formatDuration;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public abstract class SinkInfoBase implements SinkInfoBaseMBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(SinkInfoBase.class);

	private final long startTimeMillis;
	private final LocalDateTime startTime;
	long elapsedDeleteNanos;
	long deleteRecordsCount;
	
	SinkInfoBase() {
		startTimeMillis = System.currentTimeMillis();
		startTime = LocalDateTime.now();
		elapsedDeleteNanos = 0;
		deleteRecordsCount = 0;
	}

	void registerMBean(final SinkInfoBase sib, final String tableName) {
		final var sb = new StringBuilder(0x40);
		sb
			.append(sib instanceof SinkTableInfo
					? "solutions.a2.oracdc:type=Sink-metrics,tableName="
					: "solutions.a2.oracdc:type=Sink-metrics4Wrapped-data,tableName=")
			.append(tableName);
		try {
			final var name = new ObjectName(sb.toString());
			final var mbs = ManagementFactory.getPlatformMBeanServer();
			if (mbs.isRegistered(name)) {
				LOGGER.warn(
						"""
						
						=====================
						JMX MBean {} is already registered, trying to delete it.
						=====================
						
						""", name.getCanonicalName());
				try {
					mbs.unregisterMBean(name);
				} catch (InstanceNotFoundException nfe) {
					LOGGER.error("Unable to unregister MBean {}", name.getCanonicalName());
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(nfe));
					throw new ConnectException(nfe);
				}
			}
			mbs.registerMBean(sib, name);
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("MBean {} registered.", name.getCanonicalName());
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
			LOGGER.warn(
					"""
					
					=====================
					Error '{}; while registering MBean {}!!!
					=====================
					
					""", e.getMessage(), sb.toString());
			throw new ConnectException(e);
		}
	}

	@Override
	public String getStartTime() {
		return startTime.format(ISO_DATE_TIME);
	}
	@Override
	public long getElapsedTimeMillis() {
		return System.currentTimeMillis() - startTimeMillis;
	}
	@Override
	public String getElapsedTime() {
		Duration duration = Duration.ofMillis(System.currentTimeMillis() - startTimeMillis);
		return formatDuration(duration);
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
		return formatDuration(duration);
	}
	@Override
	public double getDeletesPerSecond() {
		if (deleteRecordsCount == 0 || elapsedDeleteNanos == 0) {
			return 0;
		} else {
			return round(((double)(deleteRecordsCount * 1_000_000_000)) / ((double) elapsedDeleteNanos), 2);
		}
	}

}
