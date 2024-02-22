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

	private final List<String> excludedRbas = new ArrayList<>();

	private OraCdcLogMinerStatement lastSql;
	private boolean firstStatement = true;
	private int offset = 0;

	boolean checkForRollback(final OraCdcLogMinerStatement oraSql) {
		final boolean result;
		if (firstStatement) {
			firstStatement = false;
			result = false;
		} else {
			if (oraSql.isRollback()) {
				// Add previous record to exclusion list for INSERT/UPDATE/DELETE
				final String rowId = lastSql.getRowId();
				if (lastSql.getTableId() == oraSql.getTableId() &&
						(lastSql.getOperation() == OraCdcV$LogmnrContents.INSERT ||
						lastSql.getOperation() == OraCdcV$LogmnrContents.UPDATE ||
						lastSql.getOperation() == OraCdcV$LogmnrContents.DELETE) &&
						StringUtils.equals(rowId, oraSql.getRowId())) {
					excludedRbas.add(lastSql.getRsId());
					if (LOGGER.isTraceEnabled()) {
						LOGGER.trace(
								"Redo record with RBA='{}' added to the list of rolled back entries.",
								lastSql.getRsId());
					}
					result = true;
				} else {
					final StringBuilder sb = new StringBuilder(2048);
					sb
						.append("\n=====================\n")
						.append("Partial rollback record ROWID does not match previous record ROWID or unsupported operation code!\nPlease send information below to oracle@a2-solutions.eu\n")
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
					result = false;
				}
			} else {
				result = false;
			}
		}
		if (!result) {
			lastSql = oraSql;
		}
		return result;
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

}
