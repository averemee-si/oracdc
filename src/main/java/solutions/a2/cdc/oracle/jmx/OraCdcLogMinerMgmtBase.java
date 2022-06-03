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

package solutions.a2.cdc.oracle.jmx;

import java.lang.management.ManagementFactory;
import java.time.LocalDateTime;
import java.util.List;

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

import solutions.a2.cdc.oracle.OraRdbmsInfo;
import solutions.a2.cdc.oracle.utils.ExceptionUtils;
import solutions.a2.cdc.oracle.utils.LimitedSizeQueue;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcLogMinerMgmtBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerMgmtBase.class);

	private List<String> nowProcessedArchivelogs;
	private LimitedSizeQueue<String> lastHundredProcessed = new LimitedSizeQueue<>(100);
	private long currentFirstScn;
	private long currentNextScn;
	private int processedArchivedRedoCount;
	private long processedArchivedRedoSize;
	private long startTimeMillis;
	private LocalDateTime startTime;
	private long startScn;
	private String lastRedoLog;
	private LocalDateTime lastRedoLogTime;
	private long lastScn = 0;
	private long redoReadTimeElapsed = 0;
	private float redoReadMbPerSec = 0;
	private int lagSeconds = -1;

	public OraCdcLogMinerMgmtBase(
			final OraRdbmsInfo rdbmsInfo, final String connectorName, final String jmxTypeName) {
		final StringBuilder sb = new StringBuilder(96);
		sb.append("eu.solutions.a2.oracdc:type=");
		sb.append(jmxTypeName);
		sb.append(",name=");
		sb.append(connectorName);
		sb.append(",database=");
		sb.append(rdbmsInfo.getInstanceName());
		sb.append("_");
		sb.append(rdbmsInfo.getHostName());
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
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
			LOGGER.error("Unable to register MBean {} !!! ", sb.toString());
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
	}

	public void start(long startScn) {
		this.startTimeMillis = System.currentTimeMillis();
		this.startTime = LocalDateTime.now();
		this.startScn = startScn;
	}


	public void setNowProcessed(
			final List<String> nowProcessedArchiveLogs, final long currentFirstScn, final long currentNextScn,
			final int lagSeconds) {
		this.nowProcessedArchivelogs = nowProcessedArchiveLogs;
		this.currentFirstScn = currentFirstScn;
		this.currentNextScn = currentNextScn;
		this.lagSeconds = lagSeconds;
	}

	public void addAlreadyProcessed(final List<String> lastProcessed, final int count, final long size,
			final long redoReadMillis) {
		final int listSize = lastProcessed.size();
		for (int i = 0; i < listSize; i++) {
			lastHundredProcessed.add(lastProcessed.get(i));
		}
		processedArchivedRedoCount += count;
		processedArchivedRedoSize += size;
		lastRedoLog = lastProcessed.get(listSize - 1);
		lastRedoLogTime = LocalDateTime.now();
		lastScn = currentNextScn;
		currentFirstScn = 0;
		currentNextScn = 0;
		redoReadTimeElapsed += redoReadMillis;
		if (redoReadTimeElapsed != 0) {
			float seconds = redoReadTimeElapsed / 1000;
			redoReadMbPerSec = Precision.round((processedArchivedRedoSize / (1024 * 1024)) / seconds, 3);
		}
	}

	public List<String> getNowProcessedArchiveLogsList() {
		return nowProcessedArchivelogs;
	}

	public LimitedSizeQueue<String> getLastHundredProcessed() {
		return lastHundredProcessed;
	}

	public long getCurrentFirstScn() {
		return currentFirstScn;
	}

	public void setCurrentFirstScn(long currentFirstScn) {
		this.currentFirstScn = currentFirstScn;
	}

	public long getCurrentNextScn() {
		return currentNextScn;
	}

	public int getProcessedArchivedRedoCount() {
		return processedArchivedRedoCount;
	}

	public long getProcessedArchivedRedoSize() {
		return processedArchivedRedoSize;
	}

	public long getStartTimeMillis() {
		return startTimeMillis;
	}

	public LocalDateTime getStartTimeLdt() {
		return startTime;
	}

	public long getStartScn() {
		return startScn;
	}

	public String getLastRedoLog() {
		return lastRedoLog;
	}

	public LocalDateTime getLastRedoLogTime() {
		return lastRedoLogTime;
	}

	public long getLastScn() {
		return lastScn;
	}

	public long getRedoReadTimeElapsed() {
		return redoReadTimeElapsed;
	}

	public float getRedoReadMbPerSec() {
		return redoReadMbPerSec;
	}

	public int getLagSeconds() {
		return lagSeconds;
	}

}
