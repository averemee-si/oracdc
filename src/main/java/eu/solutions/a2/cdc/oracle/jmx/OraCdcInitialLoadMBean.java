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
public interface OraCdcInitialLoadMBean {
	public String getStartTime();
	public long getElapsedTimeMillis();
	public String getElapsedTime();
	public long getProcessingTimeMillis();
	public String getProcessingTime();
	public long getSqlSelectTimeMillis();
	public String getSqlSelectTime();
	public long getSendTimeMillis();
	public String getSendTime();
	public long getSelectedRowsCount();
	public long getSelectedRowsColumnsCount();
	public long getProcessedRowsCount();
	public long getProcessedRowsColumnsCount();
	public double getRowsPerSecond();
	public double getRowsColumnsPerSecond();
	public String[] getCurrentSelectTableList();
	public String[] getCurrentSendTableList();
	public int getProcessedTableCount();
	public String[] getLast500ProcessedTables();
}
