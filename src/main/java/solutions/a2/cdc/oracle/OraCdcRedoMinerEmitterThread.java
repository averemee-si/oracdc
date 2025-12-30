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

import static solutions.a2.cdc.oracle.OraCdcTransaction.LobProcessingStatus.NOT_AT_ALL;
import static solutions.a2.cdc.oracle.OraCdcTransaction.LobProcessingStatus.REDOMINER;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.OraCdcTransaction.LobProcessingStatus;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcRedoMinerEmitterThread extends Thread {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoMinerEmitterThread.class);

	private final OraCdcRedoMinerTask task;
	private final CountDownLatch runLatch;
	private final BlockingQueue<OraCdcTransaction> committedTransactions;
	private final BlockingQueue<OraCdcRawTransaction> rawTransactions;
	private final int timeout;
	private final boolean useChronicleQueue;
	private final LobProcessingStatus lobStatus;
	private final boolean isCdb;
	private final Path queuesRoot;
	private final int backoffMs;

	public OraCdcRedoMinerEmitterThread(final OraCdcRedoMinerTask task, final BlockingQueue<OraCdcRawTransaction> rawTransactions, final BlockingQueue<OraCdcTransaction> committedTransactions) throws SQLException {
		LOGGER.info("Initializing oracdc Redo Miner emitter thread");
		this.task = task;
		runLatch = task.runLatch();
		this.committedTransactions = committedTransactions;
		this.rawTransactions = rawTransactions;
		timeout = task.config().emitterTimeoutMs();
		useChronicleQueue = task.useChronicleQueue;
		if (task.processLobs)
			lobStatus = REDOMINER;
		else
			lobStatus = NOT_AT_ALL;
		isCdb = task.rdbmsInfo().isCdb() && !task.rdbmsInfo().isPdbConnectionAllowed();
		queuesRoot = task.config().queuesRoot();
		backoffMs = task.config().connectionRetryBackoff();
		this.setDaemon(true);
		this.setName("OraCdcRedoMinerEmitterThread-" + System.nanoTime());
	}

	@Override
	public void run()  {
		LOGGER.info("BEGIN: OraCdcRedoMinerEmitterThread.run()");
		while (runLatch.getCount() > 0) {
			var raw = rawTransactions.poll();
			if (raw != null) {
				try {
					OraCdcTransaction transaction = null;
					if (useChronicleQueue) {
						var start = System.currentTimeMillis();
						var attempt = 0;
						var ready = false;
						Exception lastException = null;
						while (!ready) {
							if (attempt > Byte.MAX_VALUE)
								break;
							else
								attempt++;
							try {
								transaction = new OraCdcTransactionChronicleQueue(raw, isCdb, lobStatus, queuesRoot);
								ready = true;
								break;
							} catch (Exception cqe) {
								lastException = cqe;
								if (cqe.getCause() != null &&
										cqe.getCause() instanceof IOException &&
										Strings.CI.contains(cqe.getCause().getMessage(), "Too") &&
										Strings.CI.contains(cqe.getCause().getMessage(), "many") &&
										Strings.CI.contains(cqe.getCause().getMessage(), "open") &&
										Strings.CI.contains(cqe.getCause().getMessage(), "files")) {
									try {
										LOGGER.info("Wait {} ms until OS resources become available to create a Chronicle Queue", backoffMs);
										Thread.sleep(backoffMs);
									} catch (InterruptedException ie) {}
								} else {
									LOGGER.error(
											"""
											
											=====================
											'{}' while initializing Chronicle Queue.
												This might be issue https://github.com/OpenHFT/Chronicle-Queue/issues/1446 or you don't have enough open files limit.
											Please send errorstack below to oracle@a2.solutions
											{}
											=====================
											
											""", cqe.getMessage(), ExceptionUtils.getExceptionStackTrace(cqe));
									throw new ConnectException(cqe);
								}
							}
							if (!ready) {
								LOGGER.error(
										"""
										
										=====================
										Failed to reconnect to create Chronicle Queue after {} attempts in {} ms.
										{}
										=====================
										
										""", attempt, (System.currentTimeMillis() - start),
										ExceptionUtils.getExceptionStackTrace(lastException));
								throw new ConnectException(lastException);
							}
						}
					} else
						transaction = new OraCdcTransactionArrayList(raw, isCdb, lobStatus, queuesRoot);
					if (transaction.hasRows())
						committedTransactions.add(transaction);
					else
						transaction = null;
				} catch (SQLException | IOException e) {
					task.stop();
					throw new ConnectException(e);
				}
			} else {
				try {
					Thread.sleep(timeout);
				} catch (InterruptedException ie) {}
			}

		}
		LOGGER.info("END: OraCdcRedoMinerEmitterThread.run()");
	}

}
