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
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle.jmx;

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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import solutions.a2.cdc.oracle.OraCdcException;
import solutions.a2.cdc.oracle.utils.LimitedSizeQueue;
import solutions.a2.utils.ExceptionUtils;
import solutions.a2.utils.OraCdcMBeanUtils;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcRedoShipment implements OraCdcRedoShipmentMBean {

	private static final Logger LOGGER = LogManager.getLogger(OraCdcRedoShipment.class);

	private long startTimeMillis;
	private LocalDateTime startTime;
	private long totalNanos;
	private int numberOfFiles;
	private long totalBytes;
	private String lastProcessedFile;
	private LimitedSizeQueue<String> lastHundredProcessed;

	public OraCdcRedoShipment(final String targetServer, final int targetPort) {
		final StringBuilder sb = new StringBuilder(64);
		sb.append("solutions.a2.oracdc:type=Shipment-metrics,targetServer=");
		sb.append(targetServer);
		sb.append("-");
		sb.append(targetPort);
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
					throw new OraCdcException(nfe);
				}
			}
			mbs.registerMBean(this, name);
			LOGGER.debug("MBean {} registered.", name.getCanonicalName());
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
			LOGGER.error("Unable to register MBean {} !!! ", sb.toString());
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new OraCdcException(e);
		}
		startTimeMillis = System.currentTimeMillis();
		startTime = LocalDateTime.now();
		totalNanos = 0;
		numberOfFiles = 0;
		totalBytes = 0;
		lastProcessedFile = "";
		lastHundredProcessed = new LimitedSizeQueue<>(100);
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

	public void addProcessedFileInfo(final long nanos, final long bytes, final String fileName) {
		totalNanos += nanos;
		numberOfFiles += 1;
		totalBytes += bytes;
		lastProcessedFile = fileName;
		lastHundredProcessed.add(fileName);
	}
	@Override
	public long getShipmentTimeMillis() {
		return totalNanos / 1_000_000;
	}
	@Override
	public String getShipmentTime() {
		Duration duration = Duration.ofNanos(totalNanos);
		return OraCdcMBeanUtils.formatDuration(duration);
	}
	@Override
	public int getProcessedFilesCount() {
		return numberOfFiles;
	}
	@Override
	public long getProcessedBytesCount() {
		return totalBytes;
	}
	@Override
	public float getProcessedMiB() {
		return totalBytes / (1024 * 1024);
	}
	@Override
	public float getProcessedGiB() {
		return totalBytes / (1024 * 1024 * 1024);
	}
	@Override
	public String getLastProcessedFile() {
		return lastProcessedFile;
	}
	@Override
	public String[] getLast100ProcessedFiles() {
		return lastHundredProcessed.toArray(new String[0]);
	}
	@Override
	public float getMiBPerSecond() {
		if (totalNanos == 0) {
			return 0;
		} else {
			return Precision.round(
					(float) (getProcessedMiB() * 1_000_000_000) / totalNanos, 3);
		}
	}
	@Override
	public float getGiBPerSecond() {
		if (totalNanos == 0) {
			return 0;
		} else {
			return Precision.round(
					(float) (getProcessedGiB() * 1_000_000_000) / totalNanos, 3);
		}
	}

}
