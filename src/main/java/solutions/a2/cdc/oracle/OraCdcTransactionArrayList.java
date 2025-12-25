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

import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcTransactionArrayList extends OraCdcTransaction {


	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTransactionArrayList.class);

	private List<OraCdcStatementBase> statements;
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
		super(xid, firstChange, isCdb, LobProcessingStatus.NOT_AT_ALL, null);
		this.capacity = capacity;
	}

	void processRollbackEntries() {
		long nanos = System.nanoTime();
		for (final PartialRollbackEntry pre : rollbackEntriesList) {
			printPartialRollbackEntryDebug(pre);
			boolean pairFound = false;
			for (int i = (int) pre.index; i > -1; i--) {
				final OraCdcStatementBase lmStmt = statements.get(i);
				if (!lmStmt.isRollback() &&
					((pre.operation == DELETE && lmStmt.getOperation() == INSERT) ||
					(pre.operation == INSERT && lmStmt.getOperation() == DELETE) ||
					(pre.operation == UPDATE && lmStmt.getOperation() == UPDATE)) &&
					pre.rowId.equals(lmStmt.getRowId())) {
					final var rba = lmStmt.getRba();
					final var rowid = lmStmt.getRowId();
					final var uniqueAddr = Objects.hash(rba.sqn(), rba.blk(), rba.offset(),rowid.dataBlk(),rowid.rowNum());
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
	}

	void addToPrintOutput(final StringBuilder sb) {
		statements.forEach(stmt -> sb.append(stmt.toDelimitedRow()));
	}

	@Override
	public void addStatement(final OraCdcStatementBase oraSql) {
		if (needInit) {
			init();
		}
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
	public String toString() {
		final StringBuilder sb = new StringBuilder(128);
		sb.append("oracdc Transaction: xid = ");
		sb.append(getXid());
		sb.append("', queueSize = ");
		sb.append(queueSize);
		sb.append(", firstChange = ");
		sb.append(getFirstChange());
		sb.append(", nextChange = ");
		sb.append(getNextChange());
		if (getCommitScn() != 0) {
			sb.append(", commitScn = ");
			sb.append(getCommitScn());
		}
		if (tailerOffset > 0) {
			sb.append(", tailerOffset = ");
			sb.append(tailerOffset);
		}
		sb.append(".");

		return sb.toString();
	}

	@Override
	public long size() {
		return transSize;
	}

	void init() {
		statements = new ArrayList<>(capacity);
		queueSize = 0;
		tailerOffset = 0;
		needInit = false;
	}

}
