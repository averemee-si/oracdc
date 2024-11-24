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

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.RowId;

import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;

/**
 * Minimlistic presentation of V$LOGMNR_CONTENTS row for OPERATION_CODE = 1|2|3
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcRedoMinerStatement extends OraCdcStatementBase {

	/**
	 * 
	 * Default constructor
	 * 
	 */
	public OraCdcRedoMinerStatement() {
		super();
	}

	/**
	 * 
	 * @param tableId     OBJECT_ID {@literal &} CON_ID
	 * @param operation   V$LOGMNR_CONTENTS.OPERATION_CODE
	 * @param redoData    redo bytes
	 * @param ts          change record timestamp
	 * @param scn         SCN
	 * @param rba         Redo byte address
	 * @param ssn         Subscn
	 * @param rowId       ROWID
	 * @param rollback    Partial rollback flag
	 */
	public OraCdcRedoMinerStatement(long tableId, short operation,
			byte[] redoData, long ts, long scn, RedoByteAddress rba, long ssn, RowId rowId, boolean rollback) {
		super(tableId, operation, redoData, ts, scn, rba, ssn, rowId, rollback);
	}

	@Override
	public String getSqlRedo() {
		final StringBuilder sql = new StringBuilder(APPROXIMATE_SIZE);
		final int objId = (int) tableId;
		if (operation == INSERT || operation == DELETE) {
			final short colCount = (short) (redoData[0] << 8 | (redoData[1] & 0xFF));
			sql
				.append(operation == INSERT ?
							"insert into" :
							"delete from")
				.append(" \"UNKNOWN\".\"OBJ# ")
				.append(objId)
				.append('"');
			if (operation == INSERT) {
				sql.append('(');
				boolean first = true;
				for (int col = 1; col <= colCount; col++) {
					if (first) {
						first = false;
					} else {
						sql.append(',');
					}
					sql
						.append("\"COL ")
						.append(col)
						.append('"');
				}
				sql.append(") values(");
			} else {
				sql.append(" where ");
			}
			int pos = Short.BYTES;
			boolean first = true;
			for (int col = 1; col <= colCount; col++) {
				boolean isNull = false;
				if (first) {
					first = false;
				} else {
					sql.append(operation == INSERT ? "," : " and ");
				}
				int colSize = Byte.toUnsignedInt(redoData[pos++]);
				if (colSize ==  0xFE) {
					colSize = (redoData[pos++] << 8 | (redoData[pos++] & 0xFF));
				} else if (colSize == 0xFF) {
					colSize = 0;
					isNull = true;
				}
				if (operation == DELETE) {
					sql
						.append("\"COL ")
						.append(col)
						.append("\"");
				}
				if (isNull) {
					if (operation == DELETE) {
						sql.append(" IS ");
					}
					sql.append("NULL");
				} else {
					if (operation == DELETE) {
						sql.append(" = ");
					}
					sql.append('\'');
					for (int i = 0; i < colSize; i++) {
						sql.append(String.format("%02x", redoData[pos++]));
					}
					sql.append('\'');
				}
			}
			if (operation == INSERT) {
				sql.append(')');
			}
		} else if (operation == UPDATE) {
			boolean first;
			final short setColCount = (short) (redoData[0] << 8 | (redoData[1] & 0xFF));
			sql
				.append("update \"UNKNOWN\".\"OBJ# ")
				.append(objId)
				.append("\" set ");
			first = true;
			int pos = Short.BYTES;
			for (int col = 1; col <= setColCount; col++) {
				boolean isNull = false;
				if (first) {
					first = false;
				} else {
					sql.append(", ");
				}
				final int colNum = (redoData[pos++] << 8 | (redoData[pos++] & 0xFF));
				int colSize = Byte.toUnsignedInt(redoData[pos++]);
				if (colSize ==  0xFE) {
					colSize = (redoData[pos++] << 8 | (redoData[pos++] & 0xFF));
				} else if (colSize == 0xFF) {
					colSize = 0;
					isNull = true;
				}
				sql
					.append("\"COL ")
					.append(colNum)
					.append("\" = ");
				if (isNull) {
					sql.append("NULL");
				} else {
					sql.append('\'');
					for (int i = 0; i < colSize; i++) {
						sql.append(String.format("%02x", redoData[pos++]));
					}
					sql.append('\'');
				}				
			}
			sql.append(" where ");
			final short whereColCount = (short) (redoData[pos++] << 8 | (redoData[pos++] & 0xFF));
			first = true;
			for (int col = 1; col <= whereColCount; col++) {
				boolean isNull = false;
				if (first) {
					first = false;
				} else {
					sql.append(" and ");
				}
				final int colNum = (redoData[pos++] << 8 | (redoData[pos++] & 0xFF));
				int colSize = Byte.toUnsignedInt(redoData[pos++]);
				if (colSize ==  0xFE) {
					colSize = (redoData[pos++] << 8 | (redoData[pos++] & 0xFF));
				} else if (colSize == 0xFF) {
					colSize = 0;
					isNull = true;
				}
				sql
					.append("\"COL ")
					.append(colNum)
					.append('"');		
				if (isNull) {
					sql.append(" IS NULL");
				} else {
					sql.append(" = '");
					for (int i = 0; i < colSize; i++) {
						sql.append(String.format("%02x", redoData[pos++]));
					}
					sql.append('\'');
				}
			}
		} else {
			sql.append("TODO!\nNot implemented yet!!!\nTODO!");
		}
		return sql.toString();
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(APPROXIMATE_SIZE);
		final int conId = (int) (tableId >> 32);
		sb.append("OraCdcRedoMinerStatement [");
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
