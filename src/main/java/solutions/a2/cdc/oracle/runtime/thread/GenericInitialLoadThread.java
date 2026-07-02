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

package solutions.a2.cdc.oracle.runtime.thread;

import static oracle.jdbc.OracleTypes.CURSOR;


import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleResultSet;
import org.apache.commons.lang3.exception.ExceptionUtils;
import solutions.a2.cdc.oracle.OraCdcException;
import solutions.a2.cdc.oracle.jmx.OraCdcInitialLoad;
import solutions.a2.cdc.oracle.runtime.data.DataBinder;
import solutions.a2.cdc.oracle.runtime.data.GenericAbstractMapDataBinder;
import solutions.a2.cdc.oracle.runtime.data.GenericInitialLoadTable;
import solutions.a2.cdc.oracle.runtime.data.GenericAbstractMapDataBinder.KeyValuePair;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class GenericInitialLoadThread extends Thread {

	private static final Logger LOGGER = LogManager.getLogger(GenericInitialLoadThread.class);

	private final CountDownLatch latch;
	private final int rowNumStart;
	private final int rowNumEnd;
	private final GenericInitialLoadTable table;
	private final DataBinder dataBinder; 
	private final OracleConnection connSource;
	private final ArrayBlockingQueue<GenericAbstractMapDataBinder.Batch> queue;
	private final int batchSize;
	private final int columnCount;
	private final OraCdcInitialLoad metrics;
	private final boolean useDefaultFetchSize;

	private static final int MAX_ROWS = 32767;
	private static final int ORA_1461 = 1461;

	GenericInitialLoadThread(
			final int workerNum,
			final CountDownLatch latch,
			final int rowNumStart,
			final int rowNumEnd,
			final GenericInitialLoadTable table,
			final Connection connSource,
			final ArrayBlockingQueue<GenericAbstractMapDataBinder.Batch> queue,
			final boolean useDefaultFetchSize,
			final int columnCount,
			final int batchSize,
			final OraCdcInitialLoad metrics) {
		this.setDaemon(true);
		this.setName("IL:" + table.fqn() + "-" + workerNum);
		this.latch = latch;
		this.rowNumStart = rowNumStart;
		this.rowNumEnd = rowNumEnd;
		this.table = table;
		this.dataBinder = table.dataBinder(true);
		this.connSource = (OracleConnection) connSource;
		this.queue = queue;
		this.batchSize = batchSize;
		this.useDefaultFetchSize = useDefaultFetchSize;
		this.columnCount = columnCount;
		this.metrics = metrics;
	}

	@Override
	public void run() {
		try {
			var elapsed = System.currentTimeMillis();
			var rowsProcessed = 0;
			var selectData = table.prepareSource(connSource);
			if (!useDefaultFetchSize)
				selectData.setFetchSize(rowNumEnd - rowNumStart);
			final var batchCount = (int) Math.ceil( ((double)(rowNumEnd - rowNumStart))/MAX_ROWS);
			LOGGER.info("Thread {}: will process rows from {} to {} in {} batch(es).",
					getName(), rowNumStart, rowNumEnd - 1, batchCount);
			var currentBatch = new GenericAbstractMapDataBinder.Batch(batchSize);
			for (var batchNum = 0; batchNum < batchCount; batchNum++) {
				var batchElapsedMillis = System.currentTimeMillis();
				final int batchStart;
				final int batchEnd;
				if (batchCount == 1) {
					batchStart = rowNumStart;
					batchEnd = rowNumEnd;
				} else {
					batchStart = rowNumStart + (MAX_ROWS * batchNum);
					batchEnd = (batchNum == batchCount - 1)
							? rowNumEnd
							: batchStart + MAX_ROWS;
					LOGGER.info("Thread {}, Batch {}: will process rows from {} to {}. Prepared to run in {} milliseconds.",
							getName(), batchNum, batchStart, batchEnd - 1, System.currentTimeMillis() - batchElapsedMillis);
				}
				final Array rowIdArray = table.getRowIdArray(connSource, batchStart, batchEnd);
				batchElapsedMillis = System.currentTimeMillis();

				selectData.registerOutParameter(1, CURSOR);
				selectData.setArray(2, rowIdArray);
				selectData.execute();
				var rs = (OracleResultSet) selectData.getCursor(1);
				KeyValuePair row = null;
				var nanos = System.nanoTime();
				while ((row = table.getSourceRecord(rs, dataBinder)) != null) {
					currentBatch.add(row);
					rowsProcessed++;
					if (currentBatch.processed() == batchSize) {
						try { queue.put(currentBatch);} catch (InterruptedException ie) {}
						metrics.addSendInfo(currentBatch.processed() * columnCount, System.nanoTime() - nanos);
						nanos = System.nanoTime();
						currentBatch = new GenericAbstractMapDataBinder.Batch(batchSize);
					}
				}
				if (currentBatch.processed() > 0) {
					try { queue.put(currentBatch);} catch (InterruptedException ie) {}
					metrics.addSendInfo(currentBatch.processed() * columnCount, System.nanoTime() - nanos);
				}
				rs.close();
				rs = null;
			}
			selectData.close();
			selectData = null;
			LOGGER.info("Completion of thread {}: {} rows processed in {} milliseconds",
					getName(), rowsProcessed, System.currentTimeMillis() - elapsed);
		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == ORA_1461)
				LOGGER.error(
						"""
						
						=====================
						ORA-01461: Please add to destination URL parameter
							'defaultNChar=true'
						and restart process!
						=====================
						
						""", sqle.getMessage(),
						sqle.getErrorCode(), sqle.getSQLState(), ExceptionUtils.getStackTrace(sqle));
			else
				LOGGER.error(
						"""
						
						=====================
						'{}' while copying data between systems.
						errorCode={}, SQL State = '{}'
						{}
						=====================
						
						""", sqle.getMessage(),
						sqle.getErrorCode(), sqle.getSQLState(), ExceptionUtils.getStackTrace(sqle));
			latch.countDown();
			throw new OraCdcException(sqle);
		}	
		latch.countDown();
	}

}
