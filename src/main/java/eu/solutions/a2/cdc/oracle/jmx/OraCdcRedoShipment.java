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
import eu.solutions.a2.cdc.oracle.utils.LimitedSizeQueue;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcRedoShipment implements OraCdcRedoShipmentMBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoShipment.class);

	private long startTimeMillis;
	private LocalDateTime startTime;
	private long totalNanos;
	private int numberOfFiles;
	private long totalBytes;
	private String lastProcessedFile;
	private LimitedSizeQueue<String> lastHundredProcessed;

	public OraCdcRedoShipment(final String targetServer, final int targetPort) {
		try {
			final StringBuilder sb = new StringBuilder(64);
			sb.append("eu.solutions.a2.oracdc:type=Shipment-metrics,targetServer=");
			sb.append(targetServer);
			sb.append("-");
			sb.append(targetPort);
			ObjectName name = new ObjectName(sb.toString());
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			mbs.registerMBean(this, name);
			LOGGER.debug("MBean {} registered.", sb.toString());
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
			LOGGER.error("Unable to register MBean - " + e.getMessage() + " !!!!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
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
