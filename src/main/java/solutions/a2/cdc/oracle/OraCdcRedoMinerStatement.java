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

import java.util.Arrays;

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
		boolean first;
		final int objId = (int) tableId;
		if (operation == INSERT || operation == DELETE) {
			final int colCount = (redoData[0] << 8) | (redoData[1] & 0xFF);
			final int[][] colDefs = new int[colCount][3];
			readAndSortColDefs(colDefs, Short.BYTES);
			sql
				.append(operation == INSERT ?
							"insert into" :
							"delete from")
				.append(" \"UNKNOWN\".\"OBJ# ")
				.append(objId)
				.append('"');
			if (operation == INSERT) {
				sql.append('(');
				first = true;
				for (int i = 0; i < colCount; i++) {
					if (first) {
						first = false;
					} else {
						sql.append(',');
					}
					sql
						.append("\"COL ")
						.append(colDefs[i][0])
						.append('"');
				}
				sql.append(") values(");
			} else {
				sql.append(" where ");
			}
			first = true;
			for (int i = 0; i < colCount; i++) {
				if (first) {
					first = false;
				} else {
					sql.append(operation == INSERT ? "," : " and ");
				}
				final int colSize = colDefs[i][1];
				if (operation == DELETE) {
					sql
						.append("\"COL ")
						.append(colDefs[i][0])
						.append("\"");
				}
				if (colSize < 0) {
					if (operation == DELETE) {
						sql.append(" IS ");
					}
					sql.append("NULL");
				} else {
					if (operation == DELETE) {
						sql.append(" = ");
					}
					sql.append('\'');
					for (int j = 0; j < colSize; j++) {
						sql.append(String.format("%02x", redoData[colDefs[i][2] + j]));
					}
					sql.append('\'');
				}
			}
			if (operation == INSERT) {
				sql.append(')');
			}
		} else if (operation == UPDATE) {
			final int setColCount = redoData[0] << 8 | (redoData[1] & 0xFF);
			final int[][] setColDefs = new int[setColCount][3];
			int pos = readAndSortColDefs(setColDefs, Short.BYTES);
			
			sql
				.append("update \"UNKNOWN\".\"OBJ# ")
				.append(objId)
				.append("\" set ");
			first = true;
			for (int i = 0; i < setColCount; i++) {
				if (first) {
					first = false;
				} else {
					sql.append(", ");
				}
				final int colSize = setColDefs[i][1];
				sql
					.append("\"COL ")
					.append(setColDefs[i][0])
					.append("\" = ");
				if (colSize < 0) {
					sql.append("NULL");
				} else {
					sql.append('\'');
					for (int j = 0; j < colSize; j++) {
						sql.append(String.format("%02x", redoData[setColDefs[i][2] + j]));
					}
					sql.append('\'');
				}				
			}
	
			sql.append(" where ");
			final int whereColCount = redoData[pos++] << 8 | (redoData[pos++] & 0xFF);
			final int[][] whereColDefs = new int[whereColCount][3];
			readAndSortColDefs(whereColDefs, pos);
			
			first = true;
			for (int i = 0; i < whereColCount; i++) {
				if (first) {
					first = false;
				} else {
					sql.append(" and ");
				}
				final int colSize = whereColDefs[i][1];
				sql
					.append("\"COL ")
					.append(whereColDefs[i][0])
					.append('"');		
				if (colSize < 0) {
					sql.append(" IS NULL");
				} else {
					sql.append(" = '");
					for (int j = 0; j < colSize; j++) {
						sql.append(String.format("%02x", redoData[whereColDefs[i][2] + j]));
					}
					sql.append('\'');
				}
			}
		} else {
			sql.append("TODO!\nNot implemented yet!!!\nTODO!");
		}
		return sql.toString();
	}

	private int readAndSortColDefs(final int[][] colDefs, int pos) {
		final int colCount = colDefs.length;
		for (int i = 0; i < colCount; i++) {
			colDefs[i][0] = redoData[pos++] << 8 | redoData[pos++] & 0xFF;
			int colSize = Byte.toUnsignedInt(redoData[pos++]);
			if (colSize ==  0xFE) {
				colSize = (redoData[pos++] << 8 | (redoData[pos++] & 0xFF));
			} else if (colSize == 0xFF) {
				colSize = -1;
			}
			colDefs[i][1] = colSize;
			colDefs[i][2] = pos;
			if (colSize > 0) {
				pos += colSize;
			}
		}
		pos = colDefs[colCount - 1][2] + colDefs[colCount - 1][1]; 
		Arrays.sort(colDefs, (a, b) -> Integer.compare(a[0], b[0]));
		return pos;
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
