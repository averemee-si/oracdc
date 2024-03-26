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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcTransactionArrayList extends OraCdcTransactionBase implements OraCdcTransaction {


	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTransactionArrayList.class);

	private final List<OraCdcLogMinerStatement> statements;
	private long firstChange;
	private long nextChange;
	private Long commitScn;
	private int queueSize;
	private int tailerOffset;
	private long transSize;

	/**
	 * 
	 * Creates OraCdcTransaction for new transaction
	 * 
	 * @param xid
	 */
	public OraCdcTransactionArrayList(final String xid) {
		super(xid);
		LOGGER.debug("BEGIN: create OraCdcTransactionArrayList for new transaction");
		statements = new ArrayList<>();
		firstChange = 0;
		queueSize = 0;
		tailerOffset = 0;
		transSize = 0;
		LOGGER.trace("END: create OraCdcTransactionArrayList for new transaction");
	}

	/**
	 * 
	 * Creates OraCdcTransaction for new transaction
	 * 
	 * @param xid
	 * @param firstStatement
	 */
	public OraCdcTransactionArrayList(final String xid, final OraCdcLogMinerStatement firstStatement) {
		this(xid);
		this.addStatement(firstStatement);
	}


	@Override
	public void addStatement(final OraCdcLogMinerStatement oraSql) {
		if (firstChange == 0) {
			firstChange = oraSql.getScn();
		}
		if (!checkForRollback(oraSql)) {
			statements.add(oraSql);
			nextChange = oraSql.getScn();
			queueSize++;
			transSize += oraSql.size();
		}
	}

	@Override
	public boolean getStatement(OraCdcLogMinerStatement oraSql) {
		while (tailerOffset < statements.size()) {
			final OraCdcLogMinerStatement fromQueue = statements.get(tailerOffset);
			final boolean rollback = willItRolledBack(fromQueue);
			tailerOffset++;
			if (!rollback) {
				firstChange = oraSql.getScn();
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
		sb.append(xid());
		sb.append("', ");
		sb.append(QUEUE_SIZE);
		sb.append(" = ");
		sb.append(queueSize);
		sb.append(", ");
		sb.append(TRANS_FIRST_CHANGE);
		sb.append(" = ");
		sb.append(firstChange);
		sb.append(", ");
		sb.append(TRANS_NEXT_CHANGE);
		sb.append(" = ");
		sb.append(nextChange);
		if (commitScn != null) {
			sb.append(", ");
			sb.append(TRANS_COMMIT_SCN);
			sb.append(" = ");
			sb.append(commitScn);
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
	public String getXid() {
		return xid();
	}

	@Override
	public long getFirstChange() {
		return firstChange;
	}

	@Override
	public long getNextChange() {
		return nextChange;
	}

	@Override
	public Long getCommitScn() {
		return commitScn;
	}

	@Override
	public void setCommitScn(Long commitScn) {
		printMessages();
		this.commitScn = commitScn;
	}

	@Override
	public long size() {
		return transSize;
	}

}
