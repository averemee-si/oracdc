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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;


/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public abstract class OraCdcTransaction {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTransaction.class);

	protected static final String TRANS_XID = "xid";
	protected static final String TRANS_FIRST_CHANGE = "firstChange";
	protected static final String TRANS_NEXT_CHANGE = "nextChange";
	protected static final String QUEUE_SIZE = "queueSize";
	protected static final String QUEUE_OFFSET = "tailerOffset";
	protected static final String TRANS_COMMIT_SCN = "commitScn";

	boolean firstRecord = true;
	private long firstChange = 0;
	private long nextChange = 0;
	private final String xid;
	private long commitScn;
	private boolean startsWithBeginTrans = true;
	long transSize;

	boolean partialRollback = false;
	List<PartialRollbackEntry> rollbackEntriesList;
	Set<Map.Entry<RedoByteAddress, Long>> rollbackPairs;
	private boolean suspicious = false;

	private String username;
	private String osUsername;
	private String hostname;
	private long auditSessionId;
	private String sessionInfo;
	private String clientId;

	OraCdcTransaction(final String xid) {
		this.xid = xid;
		this.transSize = 0;
	}

	void checkForRollback(final OraCdcStatementBase oraSql, final long index) {
		nextChange = oraSql.getScn();
		if (firstRecord) {
			firstRecord = false;
			firstChange = oraSql.getScn();
			if (oraSql.isRollback()) {
				suspicious = true;
				LOGGER.error(
						"\n=====================\n" +
						"The partial rollback redo record in transaction {} is the first statement in that transaction.\n" +
						"\nDetailed information about redo record\n" +
						oraSql.toStringBuilder().toString() +
						"\n=====================\n",
						xid);
			}
		} else {
			if (startsWithBeginTrans &&
					Long.compareUnsigned(firstChange, oraSql.getScn()) > 0) {
				firstChange = oraSql.getScn();
				startsWithBeginTrans = false;
			}
			if (oraSql.isRollback()) {
				if (!partialRollback) {
					partialRollback = true;
					rollbackEntriesList = new ArrayList<>();
				}
				final PartialRollbackEntry pre = new PartialRollbackEntry();
				pre.index = index;
				pre.tableId = oraSql.getTableId();
				pre.operation = oraSql.getOperation();
				pre.rowId = oraSql.getRowId();
				pre.scn = oraSql.getScn();
				pre.rsId = oraSql.getRba();
				pre.ssn = oraSql.getSsn();

				rollbackEntriesList.add(pre);
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("New partial rollback entry at SCN={}, RS_ID(RBA)='{}' for ROWID={} added.",
							oraSql.getScn(), oraSql.getRba(), oraSql.getRowId());
				}
			}
		}
	}

	abstract void processRollbackEntries();

	boolean willItRolledBack(final OraCdcStatementBase oraSql) {
		if (partialRollback) {
			if (oraSql.isRollback()) {
				return true;
			} else {
				final Map.Entry<RedoByteAddress, Long> uniqueAddr = Map.entry(oraSql.getRba(), oraSql.getSsn());
				return rollbackPairs.contains(uniqueAddr);
			}
		} else {
			return false;
		}
	}

	public String getXid() {
		return xid;
	}

	public long getCommitScn() {
		return commitScn;
	}

	private void print(boolean errorOutput) {
		final StringBuilder sb = new StringBuilder((int) transSize);
		sb
			.append("\n=====================\n")
			.append("Information about suspicious transaction with XID=")
			.append(getXid())
			.append("\n")
			.append("COMMIT_SCN=")
			.append(commitScn)
			.append("\n")
			.append(OraCdcStatementBase.delimitedRowHeader());
		addToPrintOutput(sb);
		sb.append("\n=====================\n");
		if (errorOutput) {
			LOGGER.error(sb.toString());
		} else {
			LOGGER.trace(sb.toString());
		}
	}

	abstract void addToPrintOutput(final StringBuilder sb);

	public void setCommitScn(long commitScn) {
		this.commitScn = commitScn;
		if (partialRollback) {
			// Need to process all entries in reverse order
			rollbackPairs = new HashSet<>();
			processRollbackEntries();
		}
		if (suspicious) {
			print(true);
		} else if (LOGGER.isTraceEnabled()) {
			print(false);
		}
	}

	public void setCommitScn(long commitScn, OraCdcPseudoColumnsProcessor pseudoColumns, ResultSet resultSet) throws SQLException {
		setCommitScn(commitScn);
		if (pseudoColumns.isAuditNeeded()) {
			if (pseudoColumns.isUsername()) {
				username = resultSet.getString("USERNAME");
			}
			if (pseudoColumns.isOsUsername()) {
				osUsername = resultSet.getString("OS_USERNAME");
			}
			if (pseudoColumns.isHostname()) {
				hostname = resultSet.getString("MACHINE_NAME");
			}
			if (pseudoColumns.isAuditSessionId()) {
				auditSessionId = resultSet.getLong("AUDIT_SESSIONID");
			}
			if (pseudoColumns.isSessionInfo()) {
				sessionInfo = resultSet.getString("SESSION_INFO");
			}
			clientId = resultSet.getString("CLIENT_ID");
		}
	}

	public boolean startsWithBeginTrans() {
		return startsWithBeginTrans;
	}

	public long getFirstChange() {
		return firstChange;
	}

	public long getNextChange() {
		return nextChange;
	}

	void setSuspicious() {
		suspicious = true;
	}

	public String getUsername() {
		return username;
	}

	public String getOsUsername() {
		return osUsername;
	}

	public String getHostname() {
		return hostname;
	}

	public long getAuditSessionId() {
		return auditSessionId;
	}

	public String getSessionInfo() {
		return sessionInfo;
	}

	public String getClientId() {
		return clientId;
	}

	void printPartialRollbackEntryDebug(final PartialRollbackEntry pre) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Working with partial rollback statement for ROWID={} at SCN={}, RBA(RS_ID)='{}', SSN={}",
					pre.rowId, pre.scn, pre.rsId, pre.ssn);
		}
	}

	void printUnpairedRollbackEntryError(final PartialRollbackEntry pre) {
		suspicious = true;
		LOGGER.error(
				"\n=====================\n" +
				"No pair for partial rollback statement with ROWID={} at SCN={}, RBA(RS_ID)='{}' in transaction XID='{}'!\n" +
				"\n=====================\n",
				pre.rowId, pre.scn, pre.rsId, getXid());
	}

	abstract void addStatement(final OraCdcStatementBase oraSql);
	abstract boolean getStatement(OraCdcStatementBase oraSql);
	abstract long size();
	abstract int length();
	abstract int offset();
	abstract void close();

	static class PartialRollbackEntry {
		long index;
		long tableId;
		short operation;
		RowId rowId;
		long scn;
		RedoByteAddress rsId;
		long ssn;
	}

}
