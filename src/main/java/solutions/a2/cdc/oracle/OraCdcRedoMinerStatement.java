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
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DDL;
import static solutions.a2.oracle.utils.BinaryUtils.getU16BE;
import static solutions.a2.oracle.utils.BinaryUtils.getU24BE;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimlistic presentation of V$LOGMNR_CONTENTS row for OPERATION_CODE = 1|2|3
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcRedoMinerStatement extends OraCdcStatementBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoMinerStatement.class);

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

	byte[] redoData() {
		return redoData;
	}

	@Override
	public String getSqlRedo() {
		final StringBuilder sql = new StringBuilder(APPROXIMATE_SIZE);
		boolean first;
		final int objId = (int) tableId;
		if (operation == INSERT || operation == DELETE) {
			sql
				.append(operation == INSERT ?
						"insert into" :
						"delete from")
				.append(" \"UNKNOWN\".\"OBJ# ")
				.append(objId)
				.append('"');
			if (operation == DELETE && rollback) {
				sql
					.append(" where ROWID = '")
					.append(rowId)
					.append('\'');
			} else {
				final int colCount = (redoData[0] << 8) | (redoData[1] & 0xFF);
				final int[][] colDefs = new int[colCount][3];
				readAndSortColDefs(colDefs, Short.BYTES);
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
			}
		} else if (operation == UPDATE) {
			final int setColCount = redoData[0] << 8 | (redoData[1] & 0xFF);
			final Set<Integer> changedCols = new HashSet<>(setColCount);
			int[][] setColDefs = new int[setColCount][3];
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
				final int colNum = setColDefs[i][0];
				final int colSize = setColDefs[i][1];
				changedCols.add(colNum);
				sql
					.append("\"COL ")
					.append(colNum)
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

			final int beforeDataLength = getU24BE(redoData, pos);
			pos += 0x3;
			int beforeColCount = 0;
			for (int dbPos = pos; dbPos < pos + beforeDataLength; ) {
				beforeColCount++;
				dbPos += Short.BYTES;
				int colSize = Byte.toUnsignedInt(redoData[dbPos++]);
				if (colSize ==  0xFE) {
					colSize = (redoData[dbPos++] << 8 | (redoData[dbPos++] & 0xFF));
				} else if (colSize == 0xFF) {
					colSize = -1;
				}
				if (colSize > 0) {
					dbPos += colSize;
				}
			}

			sql.append(" where ");
			first = true;

			if (beforeColCount < setColCount && beforeColCount > 0) {
				setColDefs = null;
				setColDefs = new int[beforeColCount][3];
				if (LOGGER.isDebugEnabled())
					LOGGER.debug("Reducing setColDefs array dimension from {} to {}", setColCount, beforeColCount);
			}
			if (beforeColCount > 0) {
				pos = readAndSortColDefs(setColDefs, pos);
				for (int i = 0; i < beforeColCount; i++) {
					if (first) {
						first = false;
					} else {
						sql.append(" and ");
					}
					final int colSize = setColDefs[i][1];
					sql
						.append("\"COL ")
						.append(setColDefs[i][0])
						.append('"');		
					if (colSize < 0) {
						sql.append(" IS NULL");
					} else {
						sql.append(" = '");
						for (int j = 0; j < colSize; j++) {
							sql.append(String.format("%02x", redoData[setColDefs[i][2] + j]));
						}
						sql.append('\'');
					}
				}
			}

			final int whereColCount = redoData[pos++] << 8 | (redoData[pos++] & 0xFF);
			if (whereColCount > 0) {
				final int[][] whereColDefs = new int[whereColCount][3];
				readAndSortColDefs(whereColDefs, pos);
				
				for (int i = 0; i < whereColCount; i++) {
					final int colNum = whereColDefs[i][0];
					if (changedCols.contains(colNum)) {
						continue;
					}
					if (first) {
						first = false;
					} else {
						sql.append(" and ");
					}
					final int colSize = whereColDefs[i][1];
					sql
						.append("\"COL ")
						.append(colNum)
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
			}
		} else if (operation == DDL) {
			sql.append(new String(redoData));
		} else {
			sql.append("TODO!\nNot implemented yet!!!\nTODO!");
		}
		return sql.toString();
	}

	int readColDefs(final int[][] colDefs, int pos) {
		final int colCount = colDefs.length;
		int i = 0;
		try {
			for (; i < colCount; i++) {
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
			pos = colDefs[colCount - 1][2] + 
					(colDefs[colCount - 1][1] > 0 ? colDefs[colCount - 1][1] : 0);
			return pos;
		} catch (ArrayIndexOutOfBoundsException oob) {
			LOGGER.error(
					"\n=====================\n" +
					"{} when reading columns for {} statement starting at SCN/SUBSCN/RBA {}/{}/{}\n" +
					"Column count = {}, last column = {}, pos = {}. Byte array content:\n{}" +
					"\n=====================\n",
					oob.getMessage(),
					operation == INSERT ? "INSERT" : operation == DELETE ? "DELETE" : "UPDATE",
					scn, ssn, rba, colCount, i, pos, rawToHex(redoData));
			throw oob;
		}
	}

	private int readAndSortColDefs(final int[][] colDefs, int pos) {
		pos = readColDefs(colDefs, pos);
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

	boolean updateWithoutChanges() {
		if (operation == UPDATE) {
			final int setColCount = getU16BE(redoData, 0);
			int i = 0;
			int lastSetAfterIndex = Short.BYTES;
			try {
				for (; i < setColCount; i++) {
					lastSetAfterIndex += Short.BYTES;
					int colSize = Byte.toUnsignedInt(redoData[lastSetAfterIndex++]);
					if (colSize ==  0xFE) {
						colSize =  getU16BE(redoData, lastSetAfterIndex);
						lastSetAfterIndex += Short.BYTES;
					} else if (colSize == 0xFF) {
						colSize = -1;
					}
					if (colSize > 0) {
						lastSetAfterIndex += colSize;
					}
				}
			} catch (ArrayIndexOutOfBoundsException oob) {
				LOGGER.error(
						"\n=====================\n" +
						"{} when reading columns for {} statement starting at SCN/SUBSCN/RBA {}/{}/{}\n" +
						"Column count = {}, last column = {}, pos = {}. Byte array content:\n{}" +
						"\n=====================\n",
						oob.getMessage(),
						operation == INSERT ? "INSERT" : operation == DELETE ? "DELETE" : "UPDATE",
						scn, ssn, rba, setColCount, i, lastSetAfterIndex, rawToHex(redoData));
				throw oob;
			}
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("UPDATE statement at SCN/SUBSCN/RBA {}/{}/{}\nBefore update:\n{}\nAfter update:\n{}",
						scn, ssn, rba,
						rawToHex(Arrays.copyOfRange(redoData, Short.BYTES, lastSetAfterIndex)),
						rawToHex(Arrays.copyOfRange(redoData, lastSetAfterIndex + 3, 3 + lastSetAfterIndex + getU24BE(redoData, lastSetAfterIndex))));
			return Arrays.equals(
					redoData, Short.BYTES, lastSetAfterIndex,
					redoData, lastSetAfterIndex + 3, 3 + lastSetAfterIndex + getU24BE(redoData, lastSetAfterIndex));
		} else {
			return false;
		}
	}

}
