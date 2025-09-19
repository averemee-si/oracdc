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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.oracle.internals.RedoByteAddress;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcTransactionArrayList extends OraCdcTransaction {


	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTransactionArrayList.class);

	private final List<OraCdcStatementBase> statements;
	private int queueSize;
	private int tailerOffset;

	/**
	 * 
	 * Creates OraCdcTransaction for new transaction
	 * 
	 * @param xid
	 * @param capacity
	 * @param isCdb
	 */
	public OraCdcTransactionArrayList(
			final String xid, final long firstChange, final int capacity, final boolean isCdb) {
		super(xid, firstChange, isCdb);
		statements = new ArrayList<>(capacity);
		queueSize = 0;
		tailerOffset = 0;
	}

	/**
	 * 
	 * Creates OraCdcTransaction for new transaction
	 * 
	 * @param xid
	 * @param firstStatement
	 */
	public OraCdcTransactionArrayList(final String xid, final OraCdcStatementBase firstStatement, final boolean isCdb) {
		this(xid, firstStatement.getScn(), 0x20, isCdb);
		this.addStatement(firstStatement);
	}

	void processRollbackEntries() {
		long nanos = System.nanoTime();
		for (final PartialRollbackEntry pre : rollbackEntriesList) {
			printPartialRollbackEntryDebug(pre);
			boolean pairFound = false;
			for (int i = (int) pre.index; i > -1; i--) {
				final OraCdcStatementBase lmStmt = statements.get(i);
				if (!lmStmt.isRollback() &&
					lmStmt.getTableId() == pre.tableId &&
					((pre.operation == OraCdcV$LogmnrContents.DELETE &&
					lmStmt.getOperation() == OraCdcV$LogmnrContents.INSERT) ||
					(pre.operation == OraCdcV$LogmnrContents.INSERT &&
					lmStmt.getOperation() == OraCdcV$LogmnrContents.DELETE) ||
					(pre.operation == OraCdcV$LogmnrContents.UPDATE &&
					lmStmt.getOperation() == OraCdcV$LogmnrContents.UPDATE)) &&
					pre.rowId.equals(lmStmt.getRowId())) {
					final Map.Entry<RedoByteAddress, Long> uniqueAddr = Map.entry(lmStmt.getRba(), lmStmt.getSsn());
					if (!rollbackPairs.contains(uniqueAddr)) {
						rollbackPairs.add(uniqueAddr);
						pairFound = true;
						break;
					}
				}
			}
			if (!pairFound) {
				printUnpairedRollbackEntryError(pre);
			}
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Spent {} nanos to pair {} partial rollback entries in transaction XID='{}' with size={}.",
					(System.nanoTime() - nanos), rollbackEntriesList.size(), getXid(), queueSize);
		}
		LOGGER.debug("List of rollback pairs:");
		rollbackPairs.forEach(entry -> {
			LOGGER.debug("\tRBA={}, SSN={}", entry.getKey(), entry.getValue());
		});
	}

	void addToPrintOutput(final StringBuilder sb) {
		statements.forEach(stmt -> sb.append(stmt.toDelimitedRow()));
	}

	@Override
	public void addStatement(final OraCdcStatementBase oraSql) {
		checkForRollback(oraSql, statements.size() - 1);

		statements.add(oraSql);
		queueSize++;
		transSize += oraSql.size();
	}

	@Override
	public boolean getStatement(OraCdcStatementBase oraSql) {
		while (tailerOffset < statements.size()) {
			final OraCdcStatementBase fromQueue = statements.get(tailerOffset++);
			if (!willItRolledBack(fromQueue)) {
				fromQueue.clone(oraSql);
				return true;
			}
		}
		return false;
	}

	@Override
	public void close() {
		statements.clear();
	}


	@Override
	public int length() {
		return queueSize;
	}

	@Override
	public int offset() {
		return tailerOffset;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(128);
		sb.append("oracdc Transaction: ");
		sb.append(TRANS_XID);
		sb.append(" = ");
		sb.append(getXid());
		sb.append("', ");
		sb.append(QUEUE_SIZE);
		sb.append(" = ");
		sb.append(queueSize);
		sb.append(", ");
		sb.append(TRANS_FIRST_CHANGE);
		sb.append(" = ");
		sb.append(getFirstChange());
		sb.append(", ");
		sb.append(TRANS_NEXT_CHANGE);
		sb.append(" = ");
		sb.append(getNextChange());
		if (getCommitScn() != 0) {
			sb.append(", ");
			sb.append(TRANS_COMMIT_SCN);
			sb.append(" = ");
			sb.append(getCommitScn());
		}
		if (tailerOffset > 0) {
			sb.append(", ");
			sb.append(QUEUE_OFFSET);
			sb.append(" = ");
			sb.append(tailerOffset);
		}
		sb.append(".");

		return sb.toString();
	}

	@Override
	public long size() {
		return transSize;
	}

}
