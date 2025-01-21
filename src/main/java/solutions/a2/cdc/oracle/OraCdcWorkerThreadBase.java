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

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.utils.ExceptionUtils;

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
	final int backofMs;
	final Path queuesRoot;
	final BlockingQueue<OraCdcTransaction> committedTransactions;
	final boolean isCdb;
	final int pollInterval;
	long lastScn;
	RedoByteAddress lastRba;
	long lastSubScn;

	public OraCdcWorkerThreadBase(final CountDownLatch runLatch,
			final OraRdbmsInfo rdbmsInfo, final OraCdcSourceConnectorConfig config,
			final OraConnectionObjects oraConnections,
			final BlockingQueue<OraCdcTransaction> committedTransactions) throws SQLException {
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
		this.backofMs = config.connectionRetryBackoff();
		this.queuesRoot = config.queuesRoot();
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

	OraCdcTransactionChronicleQueue getChronicleQueue(final String xidAsString) {
		int attempt = 0;
		final long createQueueStart = System.currentTimeMillis();
		Exception lastException = null;
		while (true) {
			if (attempt > Byte.MAX_VALUE)
				break;
			else
				attempt++;
			try {
				return new OraCdcTransactionChronicleQueue(processLobs, queuesRoot, xidAsString);
			} catch (Exception cqe) {
				lastException = cqe;
				if (cqe.getCause() != null &&
						cqe.getCause() instanceof IOException &&
						StringUtils.containsIgnoreCase(cqe.getCause().getMessage(), "Too") &&
						StringUtils.containsIgnoreCase(cqe.getCause().getMessage(), "many") &&
						StringUtils.containsIgnoreCase(cqe.getCause().getMessage(), "open") &&
						StringUtils.containsIgnoreCase(cqe.getCause().getMessage(), "files")) {
					try {Thread.sleep(backofMs);} catch (InterruptedException ie) {}
				} else {
					LOGGER.error(
							"\n=====================\n" +
							"'{}' while initializing Chronicle Queue.\n" +
							"\tThis might be issue https://github.com/OpenHFT/Chronicle-Queue/issues/1446 or you don't have enough open files limit.\n" +
							"Please send errorstack below to oracle@a2.solutions\n{}\n" +
							"=====================\n",
							cqe.getMessage(), ExceptionUtils.getExceptionStackTrace(cqe));
					throw new ConnectException(cqe);
				}
			}
		}
		LOGGER.error(
					"\n=====================\n" +
					"Failed to reconnect to create Chronicle Queue after {} attempts in {} ms.\n{}" +
					"\n=====================\n",
					attempt, (System.currentTimeMillis() - createQueueStart),
					lastException != null ? ExceptionUtils.getExceptionStackTrace(lastException) : "");
		if (lastException != null)
			throw new ConnectException(lastException);
		else
			throw new ConnectException("Unable to create Chronicle Queue!");
	}
}
