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

package solutions.a2.cdc.oracle.jmx;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public interface OraCdcSourceConnMgmtMBean {
	public int getTablesInProcessingCount();
	public int getPartitionsInProcessingCount();
	public String[] getTablesInProcessing();
	public String[] getLast100ProcessedRedoLogs();
	public String getLastProcessedRedoLog();
	public long getLastProcessedScn();
	public long getLastProcessedSequence();
	public String getLastProcessedRedoLogTime();
	public int getTableOutOfScopeCount();
	public String getCurrentlyProcessedRedoLog();
	public long getCurrentFirstScn();
	public long getCurrentNextScn();
	public int getProcessedRedoLogsCount();
	public float getProcessedRedoLogsSizeGb();
	public String getStartTime();
	public long getStartScn();
	public long getElapsedTimeMillis();
	public String getElapsedTime();
	public long getTotalRecordsCount();
	public long getRolledBackRecordsCount();
	public int getRolledBackTransactionsCount();
	public long getCommittedRecordsCount();
	public int getCommittedTransactionsCount();
	public long getSentRecordsCount();
	public int getSentBatchesCount();
	public long getParseElapsedMillis();
	public String getParseElapsed();
	public int getParsePerSecond();
	public long getRedoReadElapsedMillis();
	public String getRedoReadElapsed();
	public float getRedoReadMbPerSecond();

	public int getActualLagSeconds();
	public String getActualLagText();

	public int getDdlColumnsCount();
	public long getDdlElapsedMillis();
	public String getDdlElapsed();

	public long getNumBytesWrittenUsingOffHeapMem();
	public float getGiBWrittenUsingOffHeapMem();
	public long getMaxTransactionSizeBytes();
	public float getMaxTransactionSizeMiB();
	public int getMaxNumberOfTransInSendQueue();
	public int getCurrentNumberOfTransInSendQueue();
	public int getMaxNumberOfTransInProcessingQueue();
	public int getCurrentNumberOfTransInProcessingQueue();

}
