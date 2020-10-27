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
import java.time.LocalDateTime;
import java.util.List;

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

import eu.solutions.a2.cdc.oracle.OraRdbmsInfo;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.LimitedSizeQueue;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcLogMinerMgmtBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerMgmtBase.class);

	protected List<String> nowProcessedArchivelogs;
	protected LimitedSizeQueue<String> lastHundredProcessed = new LimitedSizeQueue<>(100);
	protected long currentFirstScn;
	protected long currentNextScn;
	protected int processedArchivedRedoCount;
	protected long processedArchivedRedoSize;
	protected long startTimeMillis;
	protected LocalDateTime startTime;
	protected long startScn;
	protected String lastRedoLog;
	protected LocalDateTime lastRedoLogTime;
	protected long lastScn = 0;
	protected long redoReadTimeElapsed = 0;
	protected float redoReadMbPerSec = 0;

	public OraCdcLogMinerMgmtBase(
			final OraRdbmsInfo rdbmsInfo, final String connectorName, final String jmxTypeName) {
		try {
			final StringBuilder sb = new StringBuilder(96);
			sb.append("eu.solutions.a2.oracdc:type=");
			sb.append(jmxTypeName);
			sb.append(",name=");
			sb.append(connectorName);
			sb.append(",database=");
			sb.append(rdbmsInfo.getInstanceName());
			sb.append("_");
			sb.append(rdbmsInfo.getHostName());
			ObjectName name = new ObjectName(sb.toString());
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			mbs.registerMBean(this, name);
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
			LOGGER.error("Unable to register MBean - " + e.getMessage() + " !!!!");
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
			final List<String> nowProcessedArchiveLogs, final long currentFirstScn, final long currentNextScn) {
		this.nowProcessedArchivelogs = nowProcessedArchiveLogs;
		this.currentFirstScn = currentFirstScn;
		this.currentNextScn = currentNextScn;
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

}
