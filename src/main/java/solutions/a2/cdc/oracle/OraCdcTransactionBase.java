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
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public abstract class OraCdcTransactionBase implements OraCdcTransaction {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTransactionBase.class);

	protected static final String TRANS_XID = "xid";
	protected static final String TRANS_FIRST_CHANGE = "firstChange";
	protected static final String TRANS_NEXT_CHANGE = "nextChange";
	protected static final String QUEUE_SIZE = "queueSize";
	protected static final String QUEUE_OFFSET = "tailerOffset";
	protected static final String TRANS_COMMIT_SCN = "commitScn";

	private final String xid;
	private long commitScn;

	private final List<String> excludedRbas = new ArrayList<>();

	private OraCdcLogMinerStatement lastSql;
	private boolean firstStatement = true;
	private int offset = 0;
	private boolean moreMessages = false;
	private StringBuilder sbMessages;

	private String username;
	private String osUsername;
	private String hostname;
	private long auditSessionId;
	private String sessionInfo;
	private String clientId;

	OraCdcTransactionBase(final String xid) {
		this.xid = xid;
	}

	boolean checkForRollback(OraCdcLogMinerStatement oraSql) {
		if (firstStatement) {
			if (!ignore(oraSql)) {
				// First statement in transaction or after successful pairing
				firstStatement = false;
				lastSql = oraSql;
			}
			return oraSql.isRollback();
		} else if (lastSql.isRollback()) {
			// Last processed statement is with ROLLBACK=1 and unpaired
			if (oraSql.isRollback()) {
				// Rollback after unpaired rollback - error condition!!!
				if (ignore(oraSql)) {
					firstStatement = true;
				} else {
					allocMessages();
					addMessage(
							"Partial rollback redo record after another unpaired partial rollback record for " +
							(lastSql.getTableId() == oraSql.getTableId() ? "same tables" : "different tables"),
							"Detailed information about unpaired partial rollback redo record:",
							lastSql);
					addMessage(
							null,
							"Detailed information about second partial rollback redo record",
							oraSql);
					lastSql = oraSql;
				}
				return oraSql.isRollback();
			} else {
				// Potenitial partial rollback pair....
				if (lastSql.getTableId() == oraSql.getTableId() && 
						valid4Rollback(lastSql) && valid4Rollback(oraSql) &&
						StringUtils.equals(lastSql.getRowId(), oraSql.getRowId())) {
					// Done with pairing
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Skipping redo record at SCN={}, RBA='{}' with ROWID={}",
								oraSql.getScn(), oraSql.getRsId(), oraSql.getRowId());
					}
					if (LOGGER.isTraceEnabled()) {
						LOGGER.trace(
								"\n=====================\n" +
								"Redo record is paired with partial rollback in transaction '{}'.\n" +
								"\nDetailed information about partial rollback redo record (ROLLBACK=1)\n" +
								lastSql.toStringBuilder().toString() +
								"\nDetailed information about redo record (ROLLBACK=0)\n" +
								oraSql.toStringBuilder().toString() +
								"\n=====================\n",
								xid);
					}
					// In this case we do need this record at all
					firstStatement = true;
					return true;
				} else {
					if (ignore(oraSql)) {
						firstStatement = true;
					} else {
						allocMessages();
						addMessage(
								"Redo record with ROLLBACK=0 after unpaired record with ROLLBACK=1 does not match it",
								"Detailed information about unpaired partial rollback redo record:",
								lastSql);
						addMessage(
								null,
								"Detailed information about redo record with ROLLBACK=0",
								oraSql);
						lastSql = oraSql;
					}
					return oraSql.isRollback();
				}
			}
		} else {
			// Last processed statement is with ROLLBACK=0
			if (oraSql.isRollback()) {
				// Potenitial partial rollback pair....
				if (lastSql.getTableId() == oraSql.getTableId() && 
						valid4Rollback(lastSql) && valid4Rollback(oraSql) &&
						StringUtils.equals(lastSql.getRowId(), oraSql.getRowId())) {
					// Done with pairing
					excludedRbas.add(lastSql.getRsId());
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug(
								"Redo record with RBA='{}' added to the list of rolled back entries.",
								lastSql.getRsId());
					}
					if (LOGGER.isTraceEnabled()) {
						LOGGER.trace(
								"\n=====================\n" +
								"Redo record is paired with partial rollback in transaction '{}'.\n" +
								"\nDetailed information about redo record (ROLLBACK=0)\n" +
								lastSql.toStringBuilder().toString() +
								"\nDetailed information about partial rollback redo record (ROLLBACK=1)\n" +
								oraSql.toStringBuilder().toString() +
								"\n=====================\n",
								xid);
					}
					firstStatement = true;
					return true;
				} else {
					if (ignore(oraSql)) {
						firstStatement = true;
					} else {
						lastSql = oraSql;
					}
					return true;
				}
			} else {
				lastSql = oraSql;
				return false;
			}
		}
	}

	boolean willItRolledBack(final OraCdcLogMinerStatement oraSql) {
		if (excludedRbas.size() > 0 && offset < excludedRbas.size()) {
			final String rba = oraSql.getRsId();
			if (StringUtils.equals(rba, excludedRbas.get(offset))) {
				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace(
							"Redo record with RBA='{}' will be skipped.",
							rba);
				}
				offset++;
				return true;
			} else {
				return false;
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

	public void setCommitScn(long commitScn) {
		if (moreMessages) {
			sbMessages.append("\n=====================\n");
			LOGGER.error(sbMessages.toString(), xid);
		}
		this.commitScn = commitScn;
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

	private boolean valid4Rollback(final OraCdcLogMinerStatement stmt) {
		return stmt.getOperation() == OraCdcV$LogmnrContents.INSERT ||
				stmt.getOperation() == OraCdcV$LogmnrContents.UPDATE ||
						stmt.getOperation() == OraCdcV$LogmnrContents.DELETE;
	}

	private void allocMessages() {
		if (!moreMessages) {
			moreMessages = true;
			sbMessages = new StringBuilder(1048576);
			sbMessages
				.append("\n=====================\n")
				.append("\nUnpaired redo records for transaction XID '{}':\n")
				.append("Please send information below to oracle@a2-solutions.eu\n\n");
		}
	}

	private void addMessage(final String firstLine, final String recordHeader, final OraCdcLogMinerStatement stmt) {
		if (firstLine != null) {
			sbMessages
				.append("\n")
				.append(firstLine)
				.append(".\n");
		}
		sbMessages
			.append(recordHeader)
			.append("\n")
			.append(stmt.toStringBuilder());
	}

	private boolean ignore(final OraCdcLogMinerStatement stmt) {
		final boolean result =
				stmt.isRollback() &&
				stmt.getOperation() == OraCdcV$LogmnrContents.UPDATE &&
				!StringUtils.contains(stmt.getSqlRedo(), " where ");
		if (result) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(
						"Partial rollback redo record at RBA='{}' in XID={} without WHERE clause.",
						stmt.getRsId(), xid);
			}
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace(
						"\n=====================\n" +
						"Partial rollback redo record in transaction {} without WHERE clause'.\n" +
						"\nDetailed information about redo record\n" +
						stmt.toStringBuilder().toString() +
						"\n=====================\n",
						xid);
			}
		}
		return result;
	}

	public static Logger getLogger() {
		return LOGGER;
	}

	public static String getTransXid() {
		return TRANS_XID;
	}

	public static String getTransFirstChange() {
		return TRANS_FIRST_CHANGE;
	}

	public static String getTransNextChange() {
		return TRANS_NEXT_CHANGE;
	}

	public static String getQueueSize() {
		return QUEUE_SIZE;
	}

	public static String getQueueOffset() {
		return QUEUE_OFFSET;
	}

	public static String getTransCommitScn() {
		return TRANS_COMMIT_SCN;
	}

	public List<String> getExcludedRbas() {
		return excludedRbas;
	}

	public OraCdcLogMinerStatement getLastSql() {
		return lastSql;
	}

	public boolean isFirstStatement() {
		return firstStatement;
	}

	public int getOffset() {
		return offset;
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

}
