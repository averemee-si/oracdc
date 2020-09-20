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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * 
 * @author averemee
 *
 */
@JsonInclude(Include.NON_EMPTY)
public class OraCdcPersistentState implements Serializable {

	private static final long serialVersionUID = -1124449350380428483L;

	private String instanceName;
	private String hostName;
	private Long dbId;
	private Long lastScn;
	private String lastRsId;
	private Long lastSsn;
	private Long lastOpTsMillis;
	private String initialLoad;

	private Map<String, Object> currentTransaction;
	private List<Map<String, Object>> committedTransactions;
	private List<Map<String, Object>> inProgressTransactions;
	private List<Long> processedTablesIds;
	private List<Long> outOfScopeTablesIds;

	public OraCdcPersistentState() {
	}

	public static OraCdcPersistentState fromFile(String fileName) throws IOException {
		OraCdcPersistentState result;
		final ObjectReader reader = new ObjectMapper()
					.readerFor(OraCdcPersistentState.class);
		InputStream is = new FileInputStream(fileName);
		result = reader.readValue(is);
		is.close();
		return result;
	}

	public void toFile(String fileName) throws IOException {
		final ObjectWriter writer = new ObjectMapper()
					.enable(SerializationFeature.INDENT_OUTPUT)
					.writer();
		OutputStream os = new FileOutputStream(fileName);
		writer.writeValue(os, this);
		os.flush();
		os.close();
	}

	public String getInstanceName() {
		return instanceName;
	}

	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public Long getDbId() {
		return dbId;
	}

	public void setDbId(Long dbId) {
		this.dbId = dbId;
	}

	public Long getLastScn() {
		return lastScn;
	}

	public void setLastScn(Long lastScn) {
		this.lastScn = lastScn;
	}

	public String getLastRsId() {
		return lastRsId;
	}

	public void setLastRsId(String lastRsId) {
		this.lastRsId = lastRsId;
	}

	public Long getLastSsn() {
		return lastSsn;
	}

	public void setLastSsn(Long lastSsn) {
		this.lastSsn = lastSsn;
	}

	public Long getLastOpTsMillis() {
		return lastOpTsMillis;
	}

	public void setLastOpTsMillis(Long lastOpTsMillis) {
		this.lastOpTsMillis = lastOpTsMillis;
	}

	public String getInitialLoad() {
		return initialLoad;
	}

	public void setInitialLoad(String initialLoad) {
		this.initialLoad = initialLoad;
	}

	public Map<String, Object> getCurrentTransaction() {
		return currentTransaction;
	}

	public void setCurrentTransaction(Map<String, Object> currentTransaction) {
		this.currentTransaction = currentTransaction;
	}

	public List<Map<String, Object>> getCommittedTransactions() {
		return committedTransactions;
	}

	public void setCommittedTransactions(List<Map<String, Object>> committedTransactions) {
		this.committedTransactions = committedTransactions;
	}

	public List<Map<String, Object>> getInProgressTransactions() {
		return inProgressTransactions;
	}

	public void setInProgressTransactions(List<Map<String, Object>> inProgressTransactions) {
		this.inProgressTransactions = inProgressTransactions;
	}

	public List<Long> getProcessedTablesIds() {
		return processedTablesIds;
	}

	public void setProcessedTablesIds(List<Long> processedTablesIds) {
		this.processedTablesIds = processedTablesIds;
	}

	public List<Long> getOutOfScopeTablesIds() {
		return outOfScopeTablesIds;
	}

	public void setOutOfScopeTablesIds(List<Long> outOfScopeTablesIds) {
		this.outOfScopeTablesIds = outOfScopeTablesIds;
	}

	@Override
	public String toString() {
		String result = "";
		final ObjectWriter writer = new ObjectMapper()
				.enable(SerializationFeature.INDENT_OUTPUT)
				.writer();
		try {
			result = writer.writeValueAsString(this);
		} catch (JsonProcessingException e) {
		}
		return result;
	}

}