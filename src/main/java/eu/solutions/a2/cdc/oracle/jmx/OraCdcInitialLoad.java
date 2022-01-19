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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicLong;

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

import eu.solutions.a2.cdc.oracle.OraRdbmsInfo;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.LimitedSizeQueue;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcInitialLoad implements OraCdcInitialLoadMBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcInitialLoad.class);

	private long startTimeMillis;
	private LocalDateTime startTime;
	private HashSet<String> tablesSelect;
	private HashSet<String> tablesSend;
	private int processedTableCount;
	private LimitedSizeQueue<String> lastProcessedTables;
	private final AtomicLong sqlSelectNanos;
	private final AtomicLong sqlSelectRows;
	private final AtomicLong sqlSelectRowsColumns;
	private final AtomicLong sendNanos;
	private final AtomicLong sendRows;
	private final AtomicLong sendRowsColumns;

	public OraCdcInitialLoad(final OraRdbmsInfo rdbmsInfo, final String connectorName) {
		this.startTimeMillis = System.currentTimeMillis();
		this.startTime = LocalDateTime.now();
		this.tablesSelect = new LinkedHashSet<>();
		this.tablesSend  = new LinkedHashSet<>();
		this.processedTableCount = 0;
		this.lastProcessedTables = new LimitedSizeQueue<>(500);
		this.sqlSelectNanos = new AtomicLong(0);
		this.sqlSelectRows = new AtomicLong(0);
		this.sqlSelectRowsColumns = new AtomicLong(0);
		this.sendNanos = new AtomicLong(0);
		this.sendRows = new AtomicLong(0);
		this.sendRowsColumns = new AtomicLong(0);
		try {
			final StringBuilder sb = new StringBuilder(96);
			sb.append("eu.solutions.a2.oracdc:type=Initial-Load-metrics,name=");
			sb.append(connectorName);
			sb.append(",database=");
			sb.append(rdbmsInfo.getInstanceName());
			sb.append("_");
			sb.append(rdbmsInfo.getHostName());
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
			LOGGER.debug("MBean {} registered.", sb.toString());
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
			LOGGER.error("Unable to register MBean {} !!! ", e.getMessage());
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
		return ((sqlSelectNanos.get() + sendNanos.get()) / 1_000_000);
	}
	@Override
	public String getProcessingTime() {
		Duration duration = Duration.ofNanos(sqlSelectNanos.get() + sendNanos.get());
		return OraCdcMBeanUtils.formatDuration(duration);
	}
	@Override
	public long getProcessedRowsCount() {
		return sendRows.get();
	}
	@Override
	public long getProcessedRowsColumnsCount() {
		return sendRowsColumns.get();
	}

	public void startSelectTable(String fqn) {
		synchronized (tablesSelect) {
			tablesSelect.add(fqn);
		}
	}
	public void finishSelectTable(String fqn, long numRows, long numRowsColumns, long nanos) {
		synchronized (tablesSelect) {
			tablesSelect.remove(fqn);
		}
		sqlSelectNanos.addAndGet(nanos);
		sqlSelectRows.addAndGet(numRows);
		sqlSelectRowsColumns.addAndGet(numRowsColumns);
	}
	@Override
	public String[] getCurrentSelectTableList() {
		return tablesSelect.toArray(new String[0]);
	}

	public void startSendTable(String fqn) {
		synchronized (tablesSend) {
			tablesSend.add(fqn);
		}
	}
	public void finishSendTable(String fqn) {
		synchronized (tablesSend) {
			tablesSend.remove(fqn);
		}
		// This is also end of processing for table...
		processedTableCount += 1;
		lastProcessedTables.add(fqn);
	}
	@Override
	public String[] getCurrentSendTableList() {
		return tablesSend.toArray(new String[0]);
	}
	@Override
	public int getProcessedTableCount() {
		return processedTableCount;
	}
	@Override
	public String[] getLast500ProcessedTables() {
		return lastProcessedTables.toArray(new String[0]);
	}

	@Override
	public long getSqlSelectTimeMillis() {
		return sqlSelectNanos.get() / 1_000_000;
	}
	@Override
	public String getSqlSelectTime() {
		Duration duration = Duration.ofNanos(sqlSelectNanos.get());
		return OraCdcMBeanUtils.formatDuration(duration);
	}
	@Override
	public long getSelectedRowsCount() {
		return sqlSelectRows.get();
	}
	@Override
	public long getSelectedRowsColumnsCount() {
		return sqlSelectRowsColumns.get();
	}

	public void addSendInfo(long numRowsColumns, long nanos) {
		sendNanos.addAndGet(nanos);
		sendRows.addAndGet(1);
		sendRowsColumns.addAndGet(numRowsColumns);
	}
	@Override
	public long getSendTimeMillis() {
		return sendNanos.get() / 1_000_000;
	}
	@Override
	public String getSendTime() {
		Duration duration = Duration.ofNanos(sendNanos.get());
		return OraCdcMBeanUtils.formatDuration(duration);
	}

	@Override
	public double getRowsPerSecond() {
		if (sendRows.get() == 0 || sendNanos.get() == 0) {
			return 0;
		} else {
			return Precision.round(((double)(sendRows.get() * 1_000_000_000)) / ((double) (sqlSelectNanos.get() + sendNanos.get())), 2);
		}
	}
	@Override
	public double getRowsColumnsPerSecond() {
		if (sendRowsColumns.get() == 0 || sendNanos.get() == 0) {
			return 0;
		} else {
			return Precision.round(((double)(sendRowsColumns.get() * 1_000_000_000)) / ((double) (sqlSelectNanos.get() + sendNanos.get())), 2);
		}
	}

}
