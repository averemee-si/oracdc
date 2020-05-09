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

package eu.solutions.a2.cdc.oracle;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.jmx.OraCdcInitialLoad;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcInitialLoadThread extends Thread {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcInitialLoadThread.class);

	private final int waitInterval;
	private final long asOfScn;
	private final OraCdcInitialLoad metrics;
	private final CountDownLatch runLatch;
	private final Map<Long, OraTable4LogMiner> tablesInProcessing;
	private final Path queuesRoot;
	private final BlockingQueue<OraTable4InitialLoad> tablesQueue;
	private final AtomicBoolean running;
	private final int selectThreadCount;

	public OraCdcInitialLoadThread(
			final int waitInterval,
			final long asOfScn,
			final Map<Long, OraTable4LogMiner> tablesInProcessing,
			final Path queuesRoot,
			final OraRdbmsInfo rdbmsInfo,
			final OraCdcInitialLoad metrics,
			final BlockingQueue<OraTable4InitialLoad> tablesQueue) throws SQLException {
		LOGGER.info("Initializing oracdc initial load thread");
		this.setName("OraCdcInitialLoadThread-" + System.nanoTime());
		this.waitInterval = waitInterval;
		this.asOfScn = asOfScn;
		this.tablesInProcessing = tablesInProcessing;
		this.queuesRoot = queuesRoot;
		this.tablesQueue = tablesQueue;
		final int coreCount = Runtime.getRuntime().availableProcessors();
		this.selectThreadCount = Math.min(coreCount, rdbmsInfo.getCpuCoreCount());
		LOGGER.info("DB cores available {}, Kafka Cores available {}.", rdbmsInfo.getCpuCoreCount(), coreCount);
		LOGGER.info("{} parallel loaders for select phase will be used.");
		this.metrics = metrics;
		// Set latch to number of tables for load...
		this.runLatch = new CountDownLatch(tablesInProcessing.size());
		// Need running status set here!!!
		running = new AtomicBoolean(true);
	}

	@Override
	public void run()  {
		LOGGER.info("BEGIN: OraCdcInitialLoadThread.run()");
		final long startMillis = System.currentTimeMillis();
		if (tablesInProcessing != null && tablesInProcessing.size() > 0) {
			final BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(tablesInProcessing.size());
			final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
				selectThreadCount, selectThreadCount, waitInterval,
				TimeUnit.MILLISECONDS, workQueue, new ThreadPoolExecutor.AbortPolicy());
			tablesInProcessing.forEach((k, oraTable) -> {
				try {
					final OraTable4InitialLoad table4Load =
						new OraTable4InitialLoad(queuesRoot, oraTable, metrics);
					threadPool.submit(() -> {
						table4Load.readTableData(asOfScn, runLatch, tablesQueue);
					});
				} catch (IOException ioe) {
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
					throw new ConnectException(ioe);
				}
			});
			try {
				LOGGER.debug("Start waiting for initial load jobs completition...");
				runLatch.await();
				threadPool.shutdown();
			} catch (InterruptedException ie) {
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(ie));
				throw new ConnectException(ie);
			}
		} else {
			LOGGER.warn("No tables for initial load!!!");
		}
		running.set(false);
		LOGGER.info("END: OraCdcInitialLoadThread.run(), elapsed time {} ms",
				(System.currentTimeMillis() - startMillis));
	}

	public boolean isRunning() {
		return running.get();
	}

}