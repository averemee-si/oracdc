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
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_DEBEZIUM;
import static solutions.a2.oracle.utils.BinaryUtils.getU24BE;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcRedoMinerTable extends OraCdcTableBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoMinerTable.class);

	private final Map<Integer, OraCdcColumn> pureIdMap = new HashMap<>();
	private final Set<Integer> setColumns = new HashSet<>();
	private Set<Integer> lobColumnIds;
	private final boolean beforeData;

	/**
	 * 
	 * For Redo Miner worker thread
	 * 
	 * @param pdbName
	 * @param conId
	 * @param tableOwner
	 * @param tableName
	 * @param rowLevelScn
	 * @param config
	 * @param rdbmsInfo
	 * @param connection
	 */
	public OraCdcRedoMinerTable(
			final String pdbName, final short conId, final String tableOwner,
			final String tableName, final boolean rowLevelScn,
			final OraCdcSourceConnectorConfig config,
			final OraRdbmsInfo rdbmsInfo, final Connection connection, final int version) {
		super(pdbName, tableOwner, tableName, rowLevelScn, conId, config, rdbmsInfo, connection, version);
		beforeData = (schemaType == SCHEMA_TYPE_INT_DEBEZIUM);
		try {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Preparing column list and mining SQL statements for table {}.", tableFqn);
			}
			readAndParseOraColumns(connection, true);
		} catch (SQLException sqle) {
			throw sqlExceptionOnInit(sqle);
		}
	}

	@Override
	void addToIdMap(final OraCdcColumn column) {
		pureIdMap.put(column.getColumnId(), column);
	}

	@Override
	void clearIdMap() {
		pureIdMap.clear();
	}

	@Override
	void removeUnusedColumn(final OraCdcColumn unusedColumn) {
		final int unusedColId = unusedColumn.getColumnId();
		pureIdMap.remove(unusedColId);
		if ((flags & FLG_WITH_LOBS) > 0 && lobColumnIds != null)
			lobColumnIds.remove(unusedColId);
	}

	@Override
	void shiftColumnId(final OraCdcColumn column) {
		final int oldColumnId = column.getColumnId();
		pureIdMap.put(oldColumnId - 1, column);
		if ((flags & FLG_WITH_LOBS) > 0 && lobColumnIds != null && lobColumnIds.contains(oldColumnId)) {
			lobColumnIds.remove(oldColumnId);
			lobColumnIds.add(oldColumnId - 1);
		}
	}

	@Override
	void clearLobHolders() {
		if (lobColumnIds != null)
			lobColumnIds.clear();
	}

	@Override
	void createLobHolders() {
	}

	@Override
	void addLobColumnId(final int columnId) {
		if (lobColumnIds == null)
			lobColumnIds = new HashSet<>();
		lobColumnIds.add(columnId);
	}

	public Object parseRedoRecord(
			final OraCdcRedoMinerStatement stmt,
			final OraCdcTransaction transaction,
			final Map<String, Object> offset) throws SQLException {
		if (stmt.rollback) {
			LOGGER.error(
					"""
					
					=====================
					Redo record with partial rollback set to true in XID {}!
					DML operation details:
					{}
					=====================
					
					""",
						transaction.getXid(), stmt.toString());
			return null;
		}
		dataBinder.init(stmt);
		boolean skipRedoRecord = false;

		if (LOGGER.isTraceEnabled()) {
			printErrorMessage(
					Level.TRACE,
					"Parsing REDO record for table {}\nRedo record information:\n",
					stmt, transaction);
		}
		if ((flags & FLG_PSEUDO_KEY) > 0)
			dataBinder.addRowId(stmt);

		final byte[] redoData = stmt.redoData();
		if (stmt.getOperation() == INSERT) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing INSERT");
			}
			final int colCount = (redoData[0] << 8) | (redoData[1] & 0xFF);
			final int[][] colDefs = new int[colCount][3];
			stmt.readColDefs(colDefs, Short.BYTES);
			for (int i = 0; i < colCount; i++) {
				final OraCdcColumn oraColumn = pureIdMap.get(colDefs[i][0]);
				final int colSize = colDefs[i][1];
				if (oraColumn != null) {
					if (colSize < 0) {
						if (oraColumn.mandatory()) {
							if (oraColumn.defaultValuePresent()) {
								dataBinder.insert(oraColumn, oraColumn.typedDefaultValue());
								if (LOGGER.isDebugEnabled())
									LOGGER.debug("Value of column {} in table {} is set to default value of '{}'",
											oraColumn.getColumnName(), tableFqn, oraColumn.defaultValue());
							} else {
								if ((flags & FLG_TOLERATE_INCOMPLETE_ROW) > 0) {
									printSkippingRedoRecordMessage(stmt, transaction);
									return null;
								} else {
									printInvalidFieldValue(oraColumn, stmt, transaction);
									throw new OraCdcDataException("Mandatory field " + oraColumn.getColumnName() + " is NULL!");
								}
							}
						}
					} else {
						try {
							dataBinder.insert(oraColumn,
								parseRedoRecordValues(oraColumn, redoData,
									colDefs[i][2], colSize, transaction));
						} catch (OraCdcDataException de) {
							LOGGER.error("Invalid value {} for column {} in table {}",
									rawToHex(Arrays.copyOfRange(redoData, colDefs[i][2], colDefs[i][2] + colSize)),
									oraColumn.getColumnName(), tableFqn);
							printInvalidFieldValue(oraColumn, stmt, transaction);
							throw de;
						} catch (SQLException sqle) {
							if (oraColumn.isNullable()) {
								printToLogInvalidHexValueWarning(
										rawToHex(Arrays.copyOfRange(redoData, colDefs[i][2], colDefs[i][2] + colSize)),
										oraColumn.getColumnName(), stmt);
							} else {
								LOGGER.error("Invalid value {} for column {} in table {}",
									rawToHex(Arrays.copyOfRange(redoData, colDefs[i][2], colDefs[i][2] + colSize)),
									oraColumn.getColumnName(), tableFqn);
								printInvalidFieldValue(oraColumn, stmt, transaction);
								throw sqle;
							}
						}
					}
				} else if ((flags & FLG_PROCESS_LOBS) == 0 && lobColumnIds != null && lobColumnIds.contains(colDefs[i][0])) {
					if (LOGGER.isDebugEnabled())
						LOGGER.debug(
								"""
								
								=====================
								Unable to map column with id {} to dictionary for table {} in XID {}!
								DML operation details:
									{}
								=====================
								
								""", colDefs[i][0], tableFqn, transaction.getXid(), stmt.toString());
				} else
					printUnableToMapColIdWarning(colDefs[i][0], transaction, stmt);
			}
		} else if (stmt.getOperation() == DELETE) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing DELETE");
			}
			final int colCount = (redoData[0] << 8) | (redoData[1] & 0xFF);
			final int[][] colDefs = new int[colCount][3];
			stmt.readColDefs(colDefs, Short.BYTES);
			if ((flags & FLG_TABLE_WITH_PK) > 0 || (flags & FLG_PSEUDO_KEY) > 0) {
				for (int i = 0; i < colCount; i++) {
					final int colSize = colDefs[i][1];
						final OraCdcColumn oraColumn = pureIdMap.get(colDefs[i][0]);
						if (oraColumn != null) {
							if (colSize < 0) {
								if (oraColumn.defaultValuePresent()) {
									dataBinder.delete(oraColumn, oraColumn.typedDefaultValue());
									if (LOGGER.isDebugEnabled())
										LOGGER.debug("Value of column {} in table {} is set to default value of '{}'",
												oraColumn.getColumnName(), tableFqn, oraColumn.defaultValue());
								} else {
									if (oraColumn.mandatory()) {
										if ((flags & FLG_TOLERATE_INCOMPLETE_ROW) > 0) {
											printSkippingRedoRecordMessage(stmt, transaction);
											return null;
										} else  {
											printInvalidFieldValue(oraColumn, stmt, transaction);
											throw new OraCdcDataException("Mandatory field " + oraColumn.getColumnName() + " is NULL!");
										}
									}
								}
							} else if ((flags & FLG_ALL_COLS_ON_DELETE) > 0 || oraColumn.isPartOfPk()) {
								try {
									dataBinder.delete(oraColumn, 
										parseRedoRecordValues(oraColumn, redoData,
											colDefs[i][2], colSize, transaction));
								} catch (OraCdcDataException de) {
									LOGGER.error("Invalid value {} for column {} in table {}",
											rawToHex(Arrays.copyOfRange(redoData, colDefs[i][2], colDefs[i][2] + colSize)),
											oraColumn.getColumnName(), tableFqn);
									printInvalidFieldValue(oraColumn, stmt, transaction);
									throw de;
								} catch (SQLException sqle) {
									if (oraColumn.isNullable()) {
										printToLogInvalidHexValueWarning(
												rawToHex(Arrays.copyOfRange(redoData, colDefs[i][2], colDefs[i][2] + colSize)),
												oraColumn.getColumnName(), stmt);
									} else {
										LOGGER.error("Invalid value {} for column {} in table {}",
												rawToHex(Arrays.copyOfRange(redoData, colDefs[i][2], colDefs[i][2] + colSize)),
												oraColumn.getColumnName(), tableFqn);
										printInvalidFieldValue(oraColumn, stmt, transaction);
										throw sqle;
									}
								}
							}
						} else
							printUnableToMapColIdWarning(colDefs[i][0], transaction, stmt);
				}
			} else if ((flags & FLG_ONLY_VALUE) > 0) {
				// skip delete operation only when schema don't have key
				skipRedoRecord = true;
				printUnableToDeleteWarning(stmt);
			}
		} else if (stmt.getOperation() == UPDATE) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing UPDATE");
			}
			if ((flags & FLG_ALL_UPDATES) == 0 && stmt.updateWithoutChanges()) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(
							"UPDATE without real changes at  SCN/SUBSCN/RBA {}/{}/{}",
							stmt.getScn(), stmt.getSsn(), stmt.getRba());
				}
				return null;
			}
			setColumns.clear();
			final int setColCount = redoData[0] << 8 | (redoData[1] & 0xFF);
			final int[][] setColDefs = new int[setColCount][3];
			int pos = stmt.readColDefs(setColDefs, Short.BYTES);
			for (int i = 0; i < setColCount; i++) {
				final int colSize = setColDefs[i][1];
				final OraCdcColumn oraColumn = pureIdMap.get(setColDefs[i][0]);
				if (oraColumn != null) {
					setColumns.add(setColDefs[i][0]);
					if (colSize < 0) {
						if (oraColumn.mandatory()) {
							if (!oraColumn.defaultValuePresent()) {
								// throw error only if we don't expect to get value from WHERE clause
								printInvalidFieldValue(oraColumn, stmt, transaction);
								throw new OraCdcDataException("Mandatory field " + oraColumn.getColumnName() + " is NULL!");
							} else {
								dataBinder.update(oraColumn, oraColumn.typedDefaultValue(), true);
								if (LOGGER.isDebugEnabled())
									LOGGER.debug("Value of column {} in table {} is set to default value of '{}'",
											oraColumn.getColumnName(), tableFqn, oraColumn.defaultValue());
							}
						}
					} else {
						try {
							dataBinder.update(oraColumn,
								parseRedoRecordValues(oraColumn, redoData,
									setColDefs[i][2], colSize, transaction),
								true);
						} catch (SQLException sqle ) {
							if (oraColumn.isNullable()) {
								printToLogInvalidHexValueWarning(
										rawToHex(Arrays.copyOfRange(redoData, setColDefs[i][2], setColDefs[i][2] + colSize)),
										oraColumn.getColumnName(), stmt);
							} else {
								LOGGER.error("Invalid value {} for column {} in table {}",
										rawToHex(Arrays.copyOfRange(redoData, setColDefs[i][2], setColDefs[i][2] + colSize)),
										oraColumn.getColumnName(), tableFqn);
								printInvalidFieldValue(oraColumn, stmt, transaction);
								throw sqle;
							}
						}
					}
				} else
					printUnableToMapColIdWarning(setColDefs[i][0], transaction, stmt);
			}
			int origValLen = getU24BE(redoData, pos);
			int origDataPos = pos + 3;
			pos = origDataPos + origValLen;
			//BEGIN: where clause processing...
			final int whereColCount = redoData[pos++] << 8 | (redoData[pos++] & 0xFF);
			if (whereColCount > 0) {
				final int[][] whereColDefs = new int[whereColCount][3];
				stmt.readColDefs(whereColDefs, pos);
				for (int i = 0; i < whereColCount; i++) {
					if (!setColumns.contains(whereColDefs[i][0])) {
						final OraCdcColumn oraColumn = pureIdMap.get(whereColDefs[i][0]);
						if (oraColumn != null) {
							final int colSize = whereColDefs[i][1];
							if (colSize < 0) {
								if (oraColumn.mandatory()) {
									boolean throwDataException = true;
									if (oraColumn.defaultValuePresent()) {
										final Object columnDefaultValue = oraColumn.typedDefaultValue();
										if (columnDefaultValue != null) {
											printSubstDefaultValueWarning(oraColumn, stmt);
											dataBinder.update(oraColumn, columnDefaultValue, true);
											throwDataException = false;
										}
									} else {
										printNullValueError(oraColumn, stmt);
									}
									if (throwDataException) {
										if ((flags & FLG_TOLERATE_INCOMPLETE_ROW) > 0) {
											printSkippingRedoRecordMessage(stmt, transaction);
											return null;
										} else  {
											printInvalidFieldValue(oraColumn, stmt, transaction);
											throw new OraCdcDataException("Mandatory field " + oraColumn.getColumnName() + " is NULL!");
										}
									}
								}
							} else {
								try {
									dataBinder.update(oraColumn,
										parseRedoRecordValues(oraColumn, redoData,
											whereColDefs[i][2], colSize, transaction),
										true);
								} catch (OraCdcDataException de) {
									LOGGER.error("Invalid value {} for column {} in table {}",
											rawToHex(Arrays.copyOfRange(redoData, whereColDefs[i][2], whereColDefs[i][2] + colSize)),
											oraColumn.getColumnName(), tableFqn);
									printInvalidFieldValue(oraColumn, stmt, transaction);
									throw de;
								} catch (SQLException sqle) {
									if (oraColumn.isNullable()) {
										printToLogInvalidHexValueWarning(
											rawToHex(Arrays.copyOfRange(redoData, whereColDefs[i][2], whereColDefs[i][2] + colSize)),
											oraColumn.getColumnName(), stmt);
									} else {
										LOGGER.error("Invalid value {} for column {} in table {}",
												rawToHex(Arrays.copyOfRange(redoData, whereColDefs[i][2], whereColDefs[i][2] + colSize)),
												oraColumn.getColumnName(), tableFqn);
										printInvalidFieldValue(oraColumn, stmt, transaction);
										throw sqle;
									}
								}
							}
						} else
							printUnableToMapColIdWarning(whereColDefs[i][0], transaction, stmt);
					}
				}
			} else {
				if ((flags & FLG_TOLERATE_INCOMPLETE_ROW) > 0) {
					final String message = 
							"Missed where clause in UPDATE record for the table {}.\n";
					printErrorMessage(Level.WARN,  message, stmt, transaction);
					skipRedoRecord = true;
				} else {
					final String message = 
							"Missed where clause in UPDATE record for the table {}.\n";
					printErrorMessage(Level.ERROR,  message + "Exiting!\n", stmt, transaction);
					throw new OraCdcException("Incomplete redo record!");
				}
			}
			//END: where clause processing...
			dataBinder.afterBefore();
			if (beforeData) {
				for (int i = 0; i < setColDefs.length; i++)
					for (int j = 0; j < setColDefs[i].length; j++)
						setColDefs[i][j] = 0;
				pos = stmt.readColDefs(setColDefs, origDataPos);
				for (int i = 0; i < setColCount; i++) {
					final int colSize = setColDefs[i][1];
					final OraCdcColumn oraColumn = pureIdMap.get(setColDefs[i][0]);
					if (oraColumn != null) {
						if (colSize < 0) {
							try {
								dataBinder.update(oraColumn, null, false);
							} catch (OraCdcDataException de) {
								printInvalidFieldValue(oraColumn, stmt, transaction);
								throw de;
							}
						} else {
							try {
								dataBinder.update(oraColumn,
									parseRedoRecordValues(oraColumn, redoData,
										setColDefs[i][2], colSize, transaction),
									false);
							} catch (SQLException sqle ) {
								if (oraColumn.isNullable()) {
									printToLogInvalidHexValueWarning(
											rawToHex(Arrays.copyOfRange(redoData, setColDefs[i][2], setColDefs[i][2] + colSize)),
											oraColumn.getColumnName(), stmt);
								} else {
									LOGGER.error("Invalid value {} for column {} in table {}",
											rawToHex(Arrays.copyOfRange(redoData, setColDefs[i][2], setColDefs[i][2] + colSize)),
											oraColumn.getColumnName(), tableFqn);
									printInvalidFieldValue(oraColumn, stmt, transaction);
									throw sqle;
								}
							}
						}
					} else
						printUnableToMapColIdWarning(setColDefs[i][0], transaction, stmt);
				}
			}
		} else {
			// We expect here only 1,2,3 as valid values for OPERATION_CODE
			printErrorMessage(
					Level.ERROR,
					"Corrupted record for table {} found!!!\nPlease send e-mail to oracle@a2.solutions with record details below:\n",
					stmt, transaction);
			throw new SQLException("Unknown OPERATION_CODE while parsing redo record!");
		}

		return dataBinder.changeVector(transaction, offset, skipRedoRecord);
	}

	
	private Object parseRedoRecordValues(
			final OraCdcColumn oraColumn, final byte[] data, final int offset, final int length,
			final OraCdcTransaction transaction) throws SQLException {
		if (oraColumn.decodeWithoutTrans())
			return oraColumn.decoder().decode(data, offset, length);
		else {
//			if (oraColumn.transformLob()) {
//				return transformLobs.transformData(pdbName,
//						tableOwner, tableName, oraColumn, data, keyStruct,
//						lobColumnSchemas.get(oraColumn.getColumnName()));
//			} else
				return oraColumn.decoder().decode(data, offset, length, transaction);
		}
	}

	private void printUnableToMapColIdWarning(final int colId, final OraCdcTransaction transaction, final OraCdcRedoMinerStatement stmt) {
		if ((flags & FLG_PRINT_UNABLE_MAP_COL_ID) > 0)
			LOGGER.warn(
					"""
					
					=====================
					Unable to map column with id {} to dictionary for table {} in XID {}!
					DML operation details:
					{}
					=====================
					
					""",
					colId, tableFqn, transaction.getXid(), stmt.toString());
	}

}
