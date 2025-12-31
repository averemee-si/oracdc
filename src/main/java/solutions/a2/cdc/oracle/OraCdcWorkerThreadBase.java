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

package solutions.a2.cdc.oracle;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.oracle.internals.RedoByteAddress;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public abstract class OraCdcWorkerThreadBase extends Thread {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcWorkerThreadBase.class);

	final CountDownLatch runLatch;
	final AtomicBoolean running;
	final OraRdbmsInfo rdbmsInfo;
	final OraCdcSourceConnectorConfig config;
	final OraConnectionObjects oraConnections;
	final boolean processLobs;
	final int backoffMs;
	final boolean isCdb;
	final int pollInterval;
	final int initialCapacity;
	long lastScn;
	RedoByteAddress lastRba;
	long lastSubScn;

	public OraCdcWorkerThreadBase(final OraCdcTaskBase task) throws SQLException {
		runLatch = task.runLatch();
		running = new AtomicBoolean(false);
		rdbmsInfo = task.rdbmsInfo();
		config = task.config();
		oraConnections = task.oraConnections();
		processLobs = config.processLobs();
		backoffMs = config.connectionRetryBackoff();
		isCdb = rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed();
		pollInterval = config.pollIntervalMs();
		initialCapacity = config.arrayListCapacity();
	}

	public boolean isRunning() {
		return running.get();
	}

	public void shutdown() {
		LOGGER.info("Stopping oracdc worker thread...");
		while (runLatch.getCount() > 0) {
			runLatch.countDown();
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("call to shutdown() completed");
		}
	}

	public long lastScn() {
		return lastScn;
	}

	public RedoByteAddress lastRba() {
		return lastRba;
	}

	public long lastSubScn() {
		return lastSubScn;
	}

	abstract void rewind(final long firstScn, final RedoByteAddress firstRba, final long firstSubScn) throws SQLException;

}
