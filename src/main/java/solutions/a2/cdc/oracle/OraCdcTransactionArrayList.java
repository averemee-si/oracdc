/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.

 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
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

	/**
	 * 
	 * Creates OraCdcTransaction from list of OraCdcRedoRecords
	 * 
	 * @param raw
	 * @param isCdb
	 * @param processLobs
	 * @param rootDir
	 */
	public OraCdcTransactionArrayList(
			final OraCdcRawTransaction raw, final boolean isCdb,
			final LobProcessingStatus processLobs, final Path rootDir) throws SQLException, IOException {
		super(raw, isCdb, processLobs, rootDir);
		init(raw);
	}

	void processRollbackEntries() {
		long nanos = System.nanoTime();
		for (var pre : rollbackEntriesList) {
			printPartialRollbackEntryDebug(pre);
			var pairFound = false;
			for (var i = pre.index; i > -1; i--) {
				if (pre.match(statements.get(i))) {
					final var rba = statements.get(i).getRba();
					final var rowid = statements.get(i).getRowId();
					final var uniqueAddr = Objects.hash(rba.sqn(), rba.blk(), rba.offset(), rowid.dataBlk(), rowid.rowNum());
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
