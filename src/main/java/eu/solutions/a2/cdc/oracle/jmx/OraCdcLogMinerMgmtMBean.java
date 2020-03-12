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

/**
 * 
 * @author averemee
 *
 */
public interface OraCdcLogMinerMgmtMBean {
	public int getTablesInProcessingCount();
	public String[] getTablesInProcessing();
	public String[] getLast100ProcessedArchivelogs();
	public String getLastProcessedArchivelog();
	public long getLastProcessedScn();
	public String getLastProcessedArchivelogTime();
	public int getTableOutOfScopeCount();
	public String[] getNowProcessedArchivelogs();
	public long getCurrentFirstScn();
	public long getCurrentNextScn();
	public int getProcessedArchivelogsCount();
	public long getProcessedArchivelogsSize();
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

	public void saveCurrentState();

}
