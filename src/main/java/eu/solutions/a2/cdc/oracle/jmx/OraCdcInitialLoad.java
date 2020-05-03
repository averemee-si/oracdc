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
public class OraCdcInitialLoad implements OraCdcInitialLoadMBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcInitialLoad.class);

	private long startTimeMillis;
	private LocalDateTime startTime;
	private HashSet<String> tablesSelect;
	private HashSet<String> tablesSend;
	private int processedTableCount;
	private LimitedSizeQueue<String> lastProcessedTables;
	private long sqlSelectNanos;
	private long sqlSelectRows;
	private long sqlSelectRowsColumns;
	private long sendNanos;
	private long sendRows;
	private long sendRowsColumns;

	public OraCdcInitialLoad() {
		this.startTimeMillis = System.currentTimeMillis();
		this.startTime = LocalDateTime.now();
		this.tablesSelect = new LinkedHashSet<>();
		this.tablesSend  = new LinkedHashSet<>();
		this.processedTableCount = 0;
		this.lastProcessedTables = new LimitedSizeQueue<>(500);
		this.sqlSelectNanos = 0;
		this.sqlSelectRows = 0;
		this.sqlSelectRowsColumns = 0;
		this.sendNanos = 0;
		this.sendRows = 0;
		this.sendRowsColumns = 0;
		try {
			final String jmxName = "eu.solutions.a2.oracdc:type=Initial-Load";
			ObjectName name = new ObjectName(jmxName);
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			mbs.registerMBean(this, name);
			LOGGER.debug("MBean {} registered.", jmxName);
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
			LOGGER.error("Unable to register MBean - " + e.getMessage() + " !!!!");
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
		return ((sqlSelectNanos + sendNanos) / 1_000_000);
	}
	@Override
	public String getProcessingTime() {
		Duration duration = Duration.ofNanos(sqlSelectNanos + sendNanos);
		return OraCdcMBeanUtils.formatDuration(duration);
	}
	@Override
	public long getProcessedRowsCount() {
		return sendRows;
	}
	@Override
	public long getProcessedRowsColumnsCount() {
		return sendRowsColumns;
	}

	public void startSelectTable(String fqn) {
		tablesSelect.add(fqn);
	}
	public void finishSelectTable(String fqn) {
		tablesSelect.remove(fqn);
	}
	@Override
	public String[] getCurrentSelectTableList() {
		return tablesSelect.toArray(new String[0]);
	}

	public void startSendTable(String fqn) {
		tablesSend.add(fqn);
	}
	public void finishSendTable(String fqn) {
		tablesSend.remove(fqn);
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

	public void addSelectInfo(long numRows, long numRowsColumns, long nanos) {
		sqlSelectNanos += nanos;
		sqlSelectRows += numRows;
		sqlSelectRowsColumns += numRowsColumns;
	}
	@Override
	public long getSqlSelectTimeMillis() {
		return sqlSelectNanos / 1_000_000;
	}
	@Override
	public String getSqlSelectTime() {
		Duration duration = Duration.ofMillis(sqlSelectNanos);
		return OraCdcMBeanUtils.formatDuration(duration);
	}
	@Override
	public long getSelectedRowsCount() {
		return sqlSelectRows;
	}
	@Override
	public long getSelectedRowsColumnsCount() {
		return sqlSelectRowsColumns;
	}

	public void addSendInfo(long numRows, long numRowsColumns, long nanos) {
		sendNanos += nanos;
		sendRows += numRows;
		sendRowsColumns += numRowsColumns;
	}
	@Override
	public long getSendTimeMillis() {
		return sendNanos / 1_000_000;
	}
	@Override
	public String getSendTime() {
		Duration duration = Duration.ofMillis(sendNanos);
		return OraCdcMBeanUtils.formatDuration(duration);
	}

	@Override
	public double getRowsPerSecond() {
		if (sendRows == 0 || sendNanos == 0) {
			return 0;
		} else {
			return Precision.round(((double)(sendRows * 1_000_000_000)) / ((double) (sqlSelectNanos + sendNanos)), 2);
		}
	}
	@Override
	public double getRowsColumnsPerSecond() {
		if (sendRowsColumns == 0 || sendNanos == 0) {
			return 0;
		} else {
			return Precision.round(((double)(sendRowsColumns * 1_000_000_000)) / ((double) (sqlSelectNanos + sendNanos)), 2);
		}
	}

}
