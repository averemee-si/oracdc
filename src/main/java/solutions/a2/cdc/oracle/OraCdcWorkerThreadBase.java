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

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
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
	final Set<Long> lobObjects;
	final Set<Long> nonLobObjects;
	final boolean useChronicleQueue;
	final Path queuesRoot;
	final BlockingQueue<OraCdcTransaction> committedTransactions;
	final boolean isCdb;
	final int pollInterval;
	long lastScn;
	RedoByteAddress lastRba;
	long lastSubScn;

	public OraCdcWorkerThreadBase(final CountDownLatch runLatch,
			final OraRdbmsInfo rdbmsInfo, final OraCdcSourceConnectorConfig config,
			final OraConnectionObjects oraConnections, final Path queuesRoot,
			final BlockingQueue<OraCdcTransaction> committedTransactions) {
		this.runLatch = runLatch;
		this.running = new AtomicBoolean(false);
		this.rdbmsInfo = rdbmsInfo;
		this.config = config;
		this.oraConnections = oraConnections;
		this.processLobs = config.processLobs();
		if (processLobs) {
			lobObjects = new HashSet<>();
			nonLobObjects = new HashSet<>();
		} else {
			lobObjects = null;
			nonLobObjects = null;
		}
		this.useChronicleQueue = StringUtils.equalsIgnoreCase(
				config.getString(ParamConstants.ORA_TRANSACTION_IMPL_PARAM),
				ParamConstants.ORA_TRANSACTION_IMPL_CHRONICLE);
		this.queuesRoot = queuesRoot;
		this.committedTransactions = committedTransactions;
		this.isCdb = rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed();
		this.pollInterval = config.pollIntervalMs();
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
