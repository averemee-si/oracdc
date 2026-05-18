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
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle;

import static solutions.a2.cdc.oracle.OraCdcTransaction.LobProcessingStatus.NOT_AT_ALL;
import static solutions.a2.cdc.oracle.OraCdcTransaction.LobProcessingStatus.REDOMINER;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import solutions.a2.cdc.oracle.OraCdcTransaction.LobProcessingStatus;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcRedoMinerEmitterThread extends Thread {

	private static final Logger LOGGER = LogManager.getLogger(OraCdcRedoMinerEmitterThread.class);

	private final OraCdcTaskBase task;
	private final CountDownLatch runLatch;
	private final BlockingQueue<OraCdcTransaction> committedTransactions;
	private final BlockingQueue<OraCdcRawTransaction> rawTransactions;
	private final int timeout;
	private final boolean useChronicleQueue;
	private final LobProcessingStatus lobStatus;
	private final boolean isCdb;
	private final Path queuesRoot;
	private final int reduceLoadMs;
	private final int[] offHeapSize;
	private boolean coolDown = false;

	public OraCdcRedoMinerEmitterThread(final OraCdcTaskBase task, final BlockingQueue<OraCdcRawTransaction> rawTransactions, final BlockingQueue<OraCdcTransaction> committedTransactions) throws SQLException {
		LOGGER.info("Initializing oracdc Redo Miner emitter thread");
		this.task = task;
		runLatch = task.runLatch();
		this.committedTransactions = committedTransactions;
		this.rawTransactions = rawTransactions;
		timeout = task.config().emitterTimeoutMs();
		useChronicleQueue = task.config().useOffHeapMemory();
		if (task.config().processLobs())
			lobStatus = REDOMINER;
		else
			lobStatus = NOT_AT_ALL;
		isCdb = task.rdbmsInfo().isCdb() && !task.rdbmsInfo().isPdbConnectionAllowed();
		queuesRoot = task.config().queuesRoot();
		reduceLoadMs = task.config().reduceLoadMs();
		offHeapSize = task.config().offHeapSize();
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
					while (coolDown = committedTransactions.remainingCapacity() == 0) {
						LOGGER.info(
								"Currently {} transactions are ready to send and {} are in the process of reading from RDBMS. Wait {} ms to reduce the load on the system",
								committedTransactions.size(), rawTransactions.size(), reduceLoadMs);
						try {
							Thread.sleep(reduceLoadMs);
						} catch (InterruptedException ie) {}
					}
					OraCdcTransaction transaction = null;
					if (useChronicleQueue)
						transaction = getOffHeap(raw);
					else
						transaction = new OraCdcTransactionArrayList(raw, isCdb, lobStatus, queuesRoot);
					if (transaction.hasRows())
						committedTransactions.add(transaction);
					else
						transaction = null;
				} catch (SQLException | IOException e) {
					task.stop();
					throw new IllegalArgumentException(e);
				}
			} else {
				try {
					Thread.sleep(timeout);
				} catch (InterruptedException ie) {}
			}

		}
		LOGGER.info("END: OraCdcRedoMinerEmitterThread.run()");
	}

	private OraCdcTransactionMmf getOffHeap(final OraCdcRawTransaction raw) {
		try {
			return new OraCdcTransactionMmf(raw, isCdb, lobStatus, queuesRoot, offHeapSize);
		} catch (SQLException | IOException e) {
			LOGGER.error(
					"""
					
					=====================
					Failed to create memory mapped file!
					{}
					=====================
					
					""", ExceptionUtils.getExceptionStackTrace(e));
			task.stop();
		}
		return null;
	}

	boolean coolDown() {
		return coolDown;
	}

}
