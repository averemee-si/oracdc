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
public abstract class OraCdcTransactionBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTransactionBase.class);

	protected static final String TRANS_XID = "xid";
	protected static final String TRANS_FIRST_CHANGE = "firstChange";
	protected static final String TRANS_NEXT_CHANGE = "nextChange";
	protected static final String QUEUE_SIZE = "queueSize";
	protected static final String QUEUE_OFFSET = "tailerOffset";
	protected static final String TRANS_COMMIT_SCN = "commitScn";


	private final String xid;

	private final List<String> excludedRbas = new ArrayList<>();

	private OraCdcLogMinerStatement lastSql;
	private boolean firstStatement = true;
	private int offset = 0;

	OraCdcTransactionBase(final String xid) {
		this.xid = xid;
	}

	boolean checkForRollback(OraCdcLogMinerStatement oraSql) {
		if (firstStatement) {
			// First statement in transaction or after successful pairing
			firstStatement = false;
			lastSql = oraSql;
			return oraSql.isRollback();
		} else if (lastSql.isRollback()) {
			// Last processed statement is with ROLLBACK=1 and unpaired
			if (oraSql.isRollback()) {
				// Rollback after unpaired rollback - error condition!!!
				LOGGER.error(
						"\n=====================\n" +
						"Partial rollback redo record after another unpaired partial rollback record!\n" +
						"Please send information below to oracle@a2-solutions.eu\n" +
						"\tXID = {}\n" +
						"\nDetailed information about unpaired partial rollback redo record\n" +
						"\tSCN = {}\n" +
						"\tTIMESTAMP = {}\n" +
						"\tRS_ID = {}\n" +
						"\tSSN = {}\n" +
						"\tROW_ID = {}\n" +
						"\tOPERATION_CODE = {}\n" +
						"\tSQL_REDO = {}\n" +
						"\nDetailed information about second partial rollback redo record\n" +
						"\tSCN = {}\n" +
						"\tTIMESTAMP = {}\n" +
						"\tRS_ID = {}\n" +
						"\tSSN = {}\n" +
						"\tROW_ID = {}\n" +
						"\tOPERATION_CODE = {}\n" +
						"\tSQL_REDO = {}\n" +
						"\n=====================\n",
						xid,
						lastSql.getScn(), lastSql.getTs(), lastSql.getRsId(),
						lastSql.getSsn(), lastSql.getRowId(),
						lastSql.getOperation(), lastSql.getSqlRedo(),
						oraSql.getScn(), oraSql.getTs(), oraSql.getRsId(),
						oraSql.getSsn(), oraSql.getRowId(),
						oraSql.getOperation(), oraSql.getSqlRedo());
				lastSql = oraSql;
				return false;
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
								"\tSCN = {}\n" +
								"\tTIMESTAMP = {}\n" +
								"\tRS_ID = {}\n" +
								"\tSSN = {}\n" +
								"\tROW_ID = {}\n" +
								"\tOPERATION_CODE = {}\n" +
								"\tSQL_REDO = {}\n" +
								"\nDetailed information about redo record (ROLLBACK=0)\n" +
								"\tSCN = {}\n" +
								"\tTIMESTAMP = {}\n" +
								"\tRS_ID = {}\n" +
								"\tSSN = {}\n" +
								"\tROW_ID = {}\n" +
								"\tOPERATION_CODE = {}\n" +
								"\tSQL_REDO = {}\n" +
								"\n=====================\n",
								xid,
								lastSql.getScn(), lastSql.getTs(), lastSql.getRsId(),
								lastSql.getSsn(), lastSql.getRowId(),
								lastSql.getOperation(), lastSql.getSqlRedo(),
								oraSql.getScn(), oraSql.getTs(), oraSql.getRsId(),
								oraSql.getSsn(), oraSql.getRowId(),
								oraSql.getOperation(), oraSql.getSqlRedo());
					}
					// In this case we do need this record at all
					firstStatement = true;
					return true;
				} else {
					LOGGER.error(
							"\n=====================\n" +
							"Redo record with ROLLBACK=0 after unpaired record with ROLLBACK=1 does not match it!\n" +
							"Please send information below to oracle@a2-solutions.eu\n" +
							"\tXID = {}\n" +
							"\nDetailed information about unbounded partial rollback redo record\n" +
							"\tSCN = {}\n" +
							"\tTIMESTAMP = {}\n" +
							"\tRS_ID = {}\n" +
							"\tSSN = {}\n" +
							"\tROW_ID = {}\n" +
							"\tOPERATION_CODE = {}\n" +
							"\tSQL_REDO = {}\n" +
							"\nDetailed information about redo record with ROLLBACK=0\n" +
							"\tSCN = {}\n" +
							"\tTIMESTAMP = {}\n" +
							"\tRS_ID = {}\n" +
							"\tSSN = {}\n" +
							"\tROW_ID = {}\n" +
							"\tOPERATION_CODE = {}\n" +
							"\tSQL_REDO = {}\n" +
							"\n=====================\n",
							xid,
							lastSql.getScn(), lastSql.getTs(), lastSql.getRsId(),
							lastSql.getSsn(), lastSql.getRowId(),
							lastSql.getOperation(), lastSql.getSqlRedo(),
							oraSql.getScn(), oraSql.getTs(), oraSql.getRsId(),
							oraSql.getSsn(), oraSql.getRowId(),
							oraSql.getOperation(), oraSql.getSqlRedo());
					lastSql = oraSql;
					return false;
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
								"\tSCN = {}\n" +
								"\tTIMESTAMP = {}\n" +
								"\tRS_ID = {}\n" +
								"\tSSN = {}\n" +
								"\tROW_ID = {}\n" +
								"\tOPERATION_CODE = {}\n" +
								"\tSQL_REDO = {}\n" +
								"\nDetailed information about partial rollback redo record (ROLLBACK=1)\n" +
								"\tSCN = {}\n" +
								"\tTIMESTAMP = {}\n" +
								"\tRS_ID = {}\n" +
								"\tSSN = {}\n" +
								"\tROW_ID = {}\n" +
								"\tOPERATION_CODE = {}\n" +
								"\tSQL_REDO = {}\n" +
								"\n=====================\n",
								xid,
								lastSql.getScn(), lastSql.getTs(), lastSql.getRsId(),
								lastSql.getSsn(), lastSql.getRowId(),
								lastSql.getOperation(), lastSql.getSqlRedo(),
								oraSql.getScn(), oraSql.getTs(), oraSql.getRsId(),
								oraSql.getSsn(), oraSql.getRowId(),
								oraSql.getOperation(), oraSql.getSqlRedo());
					}
					firstStatement = true;
					return true;
				} else {
					lastSql = oraSql;
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

	protected String xid() {
		return xid;
	}

	private boolean valid4Rollback(final OraCdcLogMinerStatement stmt) {
		return stmt.getOperation() == OraCdcV$LogmnrContents.INSERT ||
				stmt.getOperation() == OraCdcV$LogmnrContents.UPDATE ||
						stmt.getOperation() == OraCdcV$LogmnrContents.DELETE;
	}

}
