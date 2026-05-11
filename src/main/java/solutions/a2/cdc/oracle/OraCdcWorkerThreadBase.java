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
	final int reduceLoadMs;

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
		reduceLoadMs = config.reduceLoadMs();
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

	public abstract void rewind(final long firstScn, final RedoByteAddress firstRba, final long firstSubScn) throws SQLException;

}
