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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.core.util.StringUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public abstract class OraCdcTransactionBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTransactionBase.class);

	private final Map<String, Integer> rollbackInserts = new HashMap<>();
	private final Map<String, Integer> rollbackUpdates = new HashMap<>();
	private final Map<String, Integer> rollbackDeletes = new HashMap<>();

	private OraCdcLogMinerStatement lastSql;
	private boolean firstStatement = true;

	boolean checkForRollback(final OraCdcLogMinerStatement oraSql, final int offset) {
		final boolean result;
		if (firstStatement) {
			firstStatement = false;
			result = false;
		} else {
			if (oraSql.isRollback()) {
				// Add previous record to exclusion list
				final String rowId = lastSql.getRowId();
				if (lastSql.getTableId() == oraSql.getTableId() &&
						StringUtils.isEqual(rowId, oraSql.getRowId())) {
					if (lastSql.getOperation() == OraCdcV$LogmnrContents.INSERT) {
						rollbackInserts.put(rowId, offset);
					} else if (lastSql.getOperation() == OraCdcV$LogmnrContents.UPDATE) {
						rollbackUpdates.put(rowId, offset);
					} else {
						// OraCdcV$LogmnrContents.DELETE
						rollbackDeletes.put(rowId, offset);
					}
				} else {
					final StringBuilder sb = new StringBuilder(2048);
					sb
						.append("\n=====================\n")
						.append("Partial rollback record ROWID does not match previous record ROWID!\nPlease send information below to oracle@a2-solutions.eu\n")
						.append("\nDetailed information about record with ROLLBACK=0")
						.append("\tSCN = {}\n")
						.append("\tTIMESTAMP = {}\n")
						.append("\tRS_ID = {}\n")
						.append("\tSSN = {}\n")
						.append("\tROW_ID = {}\n")
						.append("\tOPERATION_CODE = {}\n")
						.append("\tSQL_REDO = {}\n")
						.append("\nDetailed information about record with ROLLBACK=1")
						.append("\tSCN = {}\n")
						.append("\tTIMESTAMP = {}\n")
						.append("\tRS_ID = {}\n")
						.append("\tSSN = {}\n")
						.append("\tROW_ID = {}\n")
						.append("\tOPERATION_CODE = {}\n")
						.append("\tSQL_REDO = {}\n")
						.append("=====================\n");
					LOGGER.error(sb.toString(),
							lastSql.getScn(), lastSql.getTs(), lastSql.getRsId(),
							lastSql.getSsn(), lastSql.getRowId(),
							lastSql.getOperation(), lastSql.getSqlRedo(),
							oraSql.getScn(), oraSql.getTs(), oraSql.getRsId(),
							oraSql.getSsn(), oraSql.getRowId(),
							oraSql.getOperation(), oraSql.getSqlRedo());
				}
				result = true;
			} else {
				result = false;
			}
		}
		if (!result) {
			lastSql = oraSql;
		}
		return result;
	}

	boolean willItRolledBack(final short opCode, final String rowId, final int offset) {
		final Integer rollbackOffset;
		if (opCode == OraCdcV$LogmnrContents.INSERT) {
			rollbackOffset = rollbackInserts.get(rowId);
		} else if (opCode == OraCdcV$LogmnrContents.UPDATE) {
			rollbackOffset = rollbackUpdates.get(rowId);
		} else {
			// OraCdcV$LogmnrContents.DELETE
			rollbackOffset = rollbackDeletes.get(rowId);
		}
		if (rollbackOffset == null) {
			return false;
		} else {
			return (offset == rollbackOffset);
		}
	}

}
