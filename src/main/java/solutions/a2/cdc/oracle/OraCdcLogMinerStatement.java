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
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle;

import java.nio.charset.StandardCharsets;

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;

/**
 * Minimlistic presentation of V$LOGMNR_CONTENTS row for OPERATION_CODE = 1|2|3
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcLogMinerStatement extends OraCdcStatementBase {


	/**
	 * 
	 * Default constructor
	 * 
	 */
	public OraCdcLogMinerStatement() {
		super();
	}

	/**
	 * 
	 * @param tableId     (((long)V$LOGMNR_CONTENTS.CON_ID) {@literal <}{@literal <} 32) | (V$LOGMNR_CONTENTS.DATA_OBJ# {@literal &} 0xFFFFFFFFL)
	 * @param operation   V$LOGMNR_CONTENTS.OPERATION_CODE
	 * @param redoData    V$LOGMNR_CONTENTS.SQL_REDO (concatenated!)
	 * @param ts          V$LOGMNR_CONTENTS.TIMESTAMP (in millis)
	 * @param scn         V$LOGMNR_CONTENTS.SCN
	 * @param rba         V$LOGMNR_CONTENTS.RS_ID
	 * @param ssn         V$LOGMNR_CONTENTS.SSN
	 * @param rowId       V$LOGMNR_CONTENTS.ROW_ID
	 * @param rollback    V$LOGMNR_CONTENTS.ROLLBACK
	 */
	public OraCdcLogMinerStatement(long tableId, short operation,
			byte[] redoData, long ts, long scn, RedoByteAddress rba, long ssn, RowId rowId, boolean rollback) {
		super(tableId, operation, redoData, ts, scn, rba, ssn, rowId, rollback);
	}

	@Override
	public String getSqlRedo() {
		return new String(redoData, StandardCharsets.US_ASCII);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(APPROXIMATE_SIZE);
		final int conId = (int) (tableId >> 32);
		sb.append("OraCdcLogMinerStatement [");
			if (conId != 0) {
				sb
					.append("\n\tCON_ID=")
					.append(conId);
			}
			sb
				.append("\n")
				.append(toStringBuilder());
			if (lobCount != 0) {
				sb
					.append("\tLOB_COUNT=")
					.append(lobCount)
					.append("\n");
					
			}
			sb.append("]");
		return sb.toString();
	}

}
