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

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.SQLXML;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static oracle.jdbc.OracleTypes.JSON;
import static oracle.jdbc.OracleTypes.VECTOR;
import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_ERROR;
import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_SKIP;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;
import static solutions.a2.cdc.oracle.schema.JdbcTypes.getTypeName;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toByte;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toDouble;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toFloat;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toInt;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toLong;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toShort;
import static solutions.a2.oracle.utils.BinaryUtils.getU24BE;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import oracle.jdbc.OracleTypes;
import solutions.a2.cdc.oracle.data.OraInterval;
import solutions.a2.cdc.oracle.data.OraTimestamp;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.oracle.internals.LobId;
import solutions.a2.oracle.internals.LobLocator;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraTable4RedoMiner extends OraTable {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraTable4RedoMiner.class);

	private final Map<Integer, OraColumn> pureIdMap = new HashMap<>();
	private final Set<Integer> setColumns = new HashSet<>();
	private Set<Integer> lobColumnIds;

	/**
	 * 
	 * For Redo Miner worker thread
	 * 
	 * @param pdbName
	 * @param conId
	 * @param tableOwner
	 * @param tableName
	 * @param rowLevelScnDependency
	 * @param config
	 * @param rdbmsInfo
	 * @param connection
	 */
	public OraTable4RedoMiner(
			final String pdbName, final short conId, final String tableOwner,
			final String tableName, final boolean rowLevelScnDependency,
			final OraCdcSourceConnectorConfig config,
			final OraRdbmsInfo rdbmsInfo, final Connection connection, final int version) {
		super(pdbName, tableOwner, tableName, config.schemaType(), config.processLobs(),
				config.transformLobsImpl(), config, rdbmsInfo);
		this.conId = conId;
		this.rowLevelScn = rowLevelScnDependency;
		this.version = version;
		final boolean isCdb = rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed();
		try {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Preparing column list and mining SQL statements for table {}.", tableFqn);
			}
			if (rdbmsInfo.isCheckSupplementalLogData4Table()) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Need to check supplemental logging settings for table {}.", tableFqn);
				}
				if (!OraRdbmsInfo.supplementalLoggingSet(connection,
						isCdb ? conId : -1, this.tableOwner, this.tableName)) {
					LOGGER.error(
							"""
							
							=====================
							Supplemental logging for table '{}' is not configured correctly!
							Please set it according to the oracdc documentation
							=====================
							
							""",
								tableFqn);
					flags &= (~FLG_CHECK_SUPPLEMENTAL);
				}
			}

			readAndParseOraColumns(connection, isCdb);
		} catch (SQLException sqle) {
			LOGGER.error(
					"""
					
					=====================
					Unable to get information about table {}.{}
					'{}', errorCode = {}, SQLState = '{}'
					=====================
					
					""",
					tableOwner, tableName, sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
			throw new ConnectException(sqle);
		}
	}

	@Override
	void addToIdMap(final OraColumn column) {
		pureIdMap.put(column.getColumnId(), column);
	}

	@Override
	void clearIdMap() {
		pureIdMap.clear();
	}

	@Override
	void removeUnusedColumn(final OraColumn unusedColumn) {
		final int unusedColId = unusedColumn.getColumnId();
		pureIdMap.remove(unusedColId);
		if ((flags & FLG_WITH_LOBS) > 0 && lobColumnIds != null)
			lobColumnIds.remove(unusedColId);
	}

	@Override
	void shiftColumnId(final OraColumn column) {
		final int oldColumnId = column.getColumnId();
		pureIdMap.put(oldColumnId - 1, column);
		if ((flags & FLG_WITH_LOBS) > 0 && lobColumnIds != null && lobColumnIds.contains(oldColumnId)) {
			lobColumnIds.remove(oldColumnId);
			lobColumnIds.add(oldColumnId - 1);
		}
	}

	@Override
	void clearLobHolders() {
		super.clearLobHolders();
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

	SourceRecord parseRedoRecord(
			final OraCdcRedoMinerStatement stmt,
			final OraCdcTransaction transaction,
			final Set<LobId> lobIds,
			final Map<String, Object> offset,
			final Connection connection) throws SQLException {
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
		if ((flags & FLG_ONLY_VALUE) > 0) {
			keyStruct = null;
		} else {
			keyStruct = new Struct(keySchema);
		}
		valueStruct = new Struct(valueSchema);
		boolean skipRedoRecord = false;
		List<OraColumn> missedColumns = null;

		if (LOGGER.isTraceEnabled()) {
			printErrorMessage(
					Level.TRACE,
					"Parsing REDO record for table {}\nRedo record information:\n",
					stmt, transaction);
		}
		if ((flags & FLG_TABLE_WITH_PK) == 0 && keyStruct != null) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Do primary key substitution for table {}", tableFqn);
			}
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("{} is used as primary key for table {}", stmt.getRowId(), tableFqn);
			}
			try {
				keyStruct.put(OraColumn.ROWID_KEY, stmt.getRowId().toString());
				if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
					valueStruct.put(OraColumn.ROWID_KEY, stmt.getRowId().toString());
				}
			} catch (DataException de) {
				StringBuilder sb = new StringBuilder(0x400);
				sb.append("keyFields:");
				keySchema.fields().forEach(f -> sb
						.append("\n\t").append(f.name()).append("\t").append(f.schema().name()));
				if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
					sb.append("\nvalueFields:");
					valueSchema.fields().forEach(f -> sb
							.append("\n\t").append(f.name()).append("\t").append(f.schema().name()));
				}
				LOGGER.error(
						"""
						
						=====================
						Unable to set pseudo key for table {} with schemaType = {}
						Schema details:
						{}
						=====================
						
						""",
							tableFqn, schemaType, sb.toString());
				throw de;
			}
		}
		mandatoryColumnsProcessed = 0;
		final char opType;
		final byte[] redoData = stmt.redoData();
		if (stmt.getOperation() == INSERT) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing INSERT");
			}
			opType = 'c';
			final int colCount = (redoData[0] << 8) | (redoData[1] & 0xFF);
			final int[][] colDefs = new int[colCount][3];
			stmt.readColDefs(colDefs, Short.BYTES);
			for (int i = 0; i < colCount; i++) {
				final OraColumn oraColumn = pureIdMap.get(colDefs[i][0]);
				final int colSize = colDefs[i][1];
				if (oraColumn != null) {
					if (colSize < 0) {
						try {
							valueStruct.put(oraColumn.getColumnName(), null);
						} catch (DataException de) {
							if (Strings.CI.contains(de.getMessage(), "null used for required field")) {
								if (incompleteDataTolerance == INCOMPLETE_REDO_INT_ERROR) {
									printInvalidFieldValue(oraColumn, stmt, transaction);
									throw de;
								} else if (incompleteDataTolerance == INCOMPLETE_REDO_INT_SKIP) {
									printSkippingRedoRecordMessage(stmt, transaction);
									return null;
								} else {
									//INCOMPLETE_REDO_INT_RESTORE
									if (missedColumns == null) {
										missedColumns = new ArrayList<>();
									}
									missedColumns.add(oraColumn);
								}
							} else {
								printInvalidFieldValue(oraColumn, stmt, transaction);
								throw de;
							}
						}
					} else {
						try {
							parseRedoRecordValues(oraColumn, redoData,
									colDefs[i][2], colSize, transaction, lobIds);
							if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
									mandatoryColumnsProcessed++;
							}
						} catch (DataException de) {
							LOGGER.error("Invalid value {} for column {} in table {}",
									rawToHex(Arrays.copyOfRange(redoData, colDefs[i][2], colDefs[i][2] + colSize)),
									oraColumn.getColumnName(), tableFqn);
							printInvalidFieldValue(oraColumn, stmt, transaction);
							throw new DataException(de);
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
								throw new SQLException(sqle);
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
			opType = 'd';
			final int colCount = (redoData[0] << 8) | (redoData[1] & 0xFF);
			final int[][] colDefs = new int[colCount][3];
			stmt.readColDefs(colDefs, Short.BYTES);
			if ((flags & FLG_TABLE_WITH_PK) > 0 || (flags & FLG_PSEUDO_KEY) > 0) {
				for (int i = 0; i < colCount; i++) {
					final int colSize = colDefs[i][1];
					if ((flags & FLG_ALL_COLS_ON_DELETE) > 0) {
						final OraColumn oraColumn = pureIdMap.get(colDefs[i][0]);
						if (oraColumn != null) {
							if (colSize < 0) {
								try {
									valueStruct.put(oraColumn.getColumnName(), null);
								} catch (DataException de) {
									printInvalidFieldValue(oraColumn, stmt, transaction);
									throw de;
								}
							} else {
								try {
									parseRedoRecordValues(oraColumn, redoData,
											colDefs[i][2], colSize, transaction, lobIds);
									if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
										mandatoryColumnsProcessed++;
									}
								} catch (DataException de) {
									LOGGER.error("Invalid value {} for column {} in table {}",
											rawToHex(Arrays.copyOfRange(redoData, colDefs[i][2], colDefs[i][2] + colSize)),
											oraColumn.getColumnName(), tableFqn);
									printInvalidFieldValue(oraColumn, stmt, transaction);
									throw new DataException(de);
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
										throw new SQLException(sqle);
									}
								}
							}
						} else
							printUnableToMapColIdWarning(colDefs[i][0], transaction, stmt);

					} else {
						if (colSize > -1) {
							// PK can't be null!!!
							final OraColumn oraColumn = pureIdMap.get(colDefs[i][0]);
							if (oraColumn != null) {
								if (oraColumn.isPartOfPk()) {
									parseRedoRecordValues(oraColumn, redoData,
											colDefs[i][2], colSize, transaction, lobIds);
									mandatoryColumnsProcessed++;
								}
							} else
								printUnableToMapColIdWarning(colDefs[i][0], transaction, stmt);
						}
					}
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
			opType = 'u';
			setColumns.clear();
			final int setColCount = redoData[0] << 8 | (redoData[1] & 0xFF);
			final int[][] setColDefs = new int[setColCount][3];
			int pos = stmt.readColDefs(setColDefs, Short.BYTES);
			for (int i = 0; i < setColCount; i++) {
				final int colSize = setColDefs[i][1];
				final OraColumn oraColumn = pureIdMap.get(setColDefs[i][0]);
				if (oraColumn != null) {
					if (colSize < 0) {
						try {
							valueStruct.put(oraColumn.getColumnName(), null);
							setColumns.add(setColDefs[i][0]);
						} catch (DataException de) {
							if (!oraColumn.isDefaultValuePresent()) {
								// throw error only if we don't expect to get value from WHERE clause
								printInvalidFieldValue(oraColumn, stmt, transaction);
								throw new DataException(de);
							} else {
								valueStruct.put(oraColumn.getColumnName(), oraColumn.getTypedDefaultValue());
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug("Value of column {} in table {} is set to default value of '{}'",
											oraColumn.getColumnName(), tableFqn, oraColumn.getDefaultValue());
								}
							}
						}
					} else {
						try {
							parseRedoRecordValues(oraColumn, redoData,
									setColDefs[i][2], colSize, transaction, lobIds);
							if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
								mandatoryColumnsProcessed++;
							}
							setColumns.add(setColDefs[i][0]);
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
								throw new SQLException(sqle);
							}
						}
					}
				} else
					printUnableToMapColIdWarning(setColDefs[i][0], transaction, stmt);
			}
			int origValLen = getU24BE(redoData, pos);
			pos = pos + 3 + origValLen;
			//BEGIN: where clause processing...
			final int whereColCount = redoData[pos++] << 8 | (redoData[pos++] & 0xFF);
			if (whereColCount > 0) {
				final int[][] whereColDefs = new int[whereColCount][3];
				stmt.readColDefs(whereColDefs, pos);
				for (int i = 0; i < whereColCount; i++) {
					if (!setColumns.contains(whereColDefs[i][0])) {
						final OraColumn oraColumn = pureIdMap.get(whereColDefs[i][0]);
						if (oraColumn != null) {
							final int colSize = whereColDefs[i][1];
							if (colSize < 0) {
								// Column can be excluded
								try {
									valueStruct.put(oraColumn.getColumnName(), null);
								} catch (DataException de) {
									// Check again for column default value...
									// This is due "SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
									boolean throwDataException = true;
									if (oraColumn.isDefaultValuePresent()) {
										final Object columnDefaultValue = oraColumn.getTypedDefaultValue();
										if (columnDefaultValue != null) {
											printSubstDefaultValueWarning(oraColumn, stmt);
											valueStruct.put(oraColumn.getColumnName(), columnDefaultValue);
											if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
												mandatoryColumnsProcessed++;
											}
											throwDataException = false;
										}
									} else {
										printNullValueError(oraColumn, stmt, de);
									}
									if (throwDataException) {
										if (incompleteDataTolerance == INCOMPLETE_REDO_INT_ERROR) {
											printInvalidFieldValue(oraColumn, stmt, transaction);
											throw de;
										} else if (incompleteDataTolerance == INCOMPLETE_REDO_INT_SKIP) {
											printSkippingRedoRecordMessage(stmt, transaction);
											return null;
										} else {
											//INCOMPLETE_REDO_INT_RESTORE
											if (missedColumns == null) {
												missedColumns = new ArrayList<>();
											}
											missedColumns.add(oraColumn);
										}
									}
								}
							} else {
								try {
									parseRedoRecordValues(
											oraColumn, redoData,
											whereColDefs[i][2], colSize, transaction, lobIds);
									if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
										mandatoryColumnsProcessed++;
									}
								} catch (DataException de) {
									LOGGER.error("Invalid value {} for column {} in table {}",
											rawToHex(Arrays.copyOfRange(redoData, whereColDefs[i][2], whereColDefs[i][2] + colSize)),
											oraColumn.getColumnName(), tableFqn);
									printInvalidFieldValue(oraColumn, stmt, transaction);
									throw new DataException(de);
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
										throw new SQLException(sqle);
									}
								}
							}
						} else
							printUnableToMapColIdWarning(whereColDefs[i][0], transaction, stmt);
					}
				}
			} else {
				if (incompleteDataTolerance == INCOMPLETE_REDO_INT_ERROR) {
					final String message = 
							"Missed where clause in UPDATE record for the table {}.\n";
					printErrorMessage(Level.ERROR,  message + "Exiting!\n", stmt, transaction);
					throw new ConnectException("Incomplete redo record!");
				} else if (incompleteDataTolerance == INCOMPLETE_REDO_INT_SKIP) {
					final String message = 
							"Missed where clause in UPDATE record for the table {}.\n";
					printErrorMessage(Level.WARN,  message, stmt, transaction);
					skipRedoRecord = true;
				} else {
					// OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_RESTORE
					getMissedColumnValues(connection, stmt);
				}
			}
			//END: where clause processing...
		} else {
			// We expect here only 1,2,3 as valid values for OPERATION_CODE (and 68 for special cases)
			printErrorMessage(
					Level.ERROR,
					"Corrupted record for table {} found!!!\nPlease send e-mail to oracle@a2.solutions with record details below:\n",
					stmt, transaction);
			throw new SQLException("Unknown OPERATION_CODE while parsing redo record!");
		}

		return createSourceRecord(stmt, transaction, offset, opType,
				skipRedoRecord, connection, missedColumns);
	}

	
	private void parseRedoRecordValues(
			final OraColumn oraColumn, final byte[] data, final int offset, final int length,
			final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
		final String columnName = oraColumn.getColumnName();
		final Object columnValue;
		final int columnType = oraColumn.getJdbcType();
		if (oraColumn.isEncrypted()) {
			final byte[] plaintext = decrypter.decrypt(Arrays.copyOfRange(data, offset, offset + length), oraColumn.isSalted());
			switch (columnType) {
			case DATE:
			case TIMESTAMP:				
				columnValue = OraDumpDecoder.toTimestamp(plaintext, 0, plaintext.length);
				break;
			case TIMESTAMP_WITH_TIMEZONE:
				columnValue = OraTimestamp.fromLogical(
						plaintext, 0, plaintext.length, oraColumn.isLocalTimeZone(), rdbmsInfo.getDbTimeZone());
				break;
			case TINYINT:
				columnValue = toByte(plaintext, 0, plaintext.length);
				break;
			case SMALLINT:
				columnValue = toShort(plaintext, 0, plaintext.length);
				break;
			case INTEGER:
				columnValue = toInt(plaintext, 0, plaintext.length);
				break;
			case BIGINT:
				columnValue = toLong(plaintext, 0, plaintext.length);
				break;
			case FLOAT:
				if (oraColumn.isBinaryFloatDouble()) {
					columnValue = OraDumpDecoder.fromBinaryFloat(plaintext);
				} else {
					columnValue = toFloat(plaintext, 0, plaintext.length);
				}
				break;
			case DOUBLE:
				if (oraColumn.isBinaryFloatDouble()) {
					columnValue = OraDumpDecoder.fromBinaryDouble(plaintext);
				} else {
					columnValue = toDouble(plaintext, 0, plaintext.length);
				}
				break;
			case DECIMAL:
				BigDecimal bdValue = OraDumpDecoder.toBigDecimal(plaintext);
				if (bdValue.scale() > oraColumn.getDataScale()) {
					LOGGER.warn(
								"Different data scale for column {} in table {}! Current value={}. Data scale from redo={}, data scale in current dictionary={}",
								columnName, this.tableFqn, bdValue, bdValue.scale(), oraColumn.getDataScale());
					columnValue = bdValue.setScale(oraColumn.getDataScale(), RoundingMode.HALF_UP);
				} else if (bdValue.scale() !=  oraColumn.getDataScale()) {
					columnValue = bdValue.setScale(oraColumn.getDataScale());
				} else {
					columnValue = bdValue;
				}
				break;
			case BINARY:
				if (oraColumn.getSecureFile()) {
					columnValue = extendedSizeValue(BINARY, transaction, lobIds, plaintext, 0, plaintext.length);
				} else
					columnValue = plaintext;
				break;
			case NUMERIC:
			case OracleTypes.INTERVALYM:
			case OracleTypes.INTERVALDS:
				// do not need to perform data type conversion here!
				columnValue = plaintext;
				break;
			case CHAR:
			case VARCHAR:
				if (oraColumn.getSecureFile()) {
					columnValue = extendedSizeValue(VARCHAR, transaction, lobIds, plaintext, 0, plaintext.length);
				} else
					
					columnValue = odd.fromVarchar2(plaintext, 0, plaintext.length);
				break;
			case NCHAR:
			case NVARCHAR:
				if (oraColumn.getSecureFile()) {
					columnValue = extendedSizeValue(NVARCHAR, transaction, lobIds, plaintext, 0, plaintext.length);
				} else
					columnValue = odd.fromNvarchar2(plaintext, 0, plaintext.length);
				break;
			case CLOB:
			case NCLOB:
			case BLOB:
			case SQLXML:
			case JSON:
			case VECTOR:
				final OraCdcTransactionChronicleQueue cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final LobLocator ll = new LobLocator(plaintext, 0, plaintext.length);
				if (LOGGER.isTraceEnabled())
					LOGGER.trace("Processing column {}, LID={}, DATALENGTH={}, EXTERNAL={}, LOB CONTENT=>{}",
							columnName, ll.lid(), ll.dataLength(), lobIds.contains(ll.lid()), rawToHex(Arrays.copyOfRange(data, offset, offset + length)));
				if (lobIds.contains(ll.lid())) {
					final byte[] externalPlaintext =
							decrypter.decrypt(cqTrans.getLob(ll), oraColumn.isSalted());
					if (columnType == BLOB)
						columnValue = odd.toOraBlob(externalPlaintext);
					else if (oraColumn.getJdbcType() == JSON)
						columnValue = odd.toOraJson(externalPlaintext);
					else if (columnType == CLOB || columnType == NCLOB)
						columnValue = odd.toOraClob(externalPlaintext, columnType == CLOB); 
					else if (oraColumn.getJdbcType() == VECTOR)
						columnValue = odd.toOraVector(externalPlaintext);
					else
						//SQLXML
						columnValue = odd.toOraXml(externalPlaintext, ll.type() == LobLocator.CLOB);
				} else if (ll.dataLength() >= 0 && ll.dataInRow()) {
					if (columnType == BLOB)
						columnValue = odd.toOraBlob(
								Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
					else if (oraColumn.getJdbcType() == JSON)
						columnValue = odd.toOraJson(
								Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
					else if (columnType == CLOB || columnType == NCLOB)
						columnValue = odd.toOraClob(
								Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length), columnType == CLOB);
					else if (oraColumn.getJdbcType() == VECTOR)
						columnValue = odd.toOraVector(
								Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
					else {
						//SQLXML
						if (ll.type() == LobLocator.CLOB) {
							columnValue = odd.toOraXml(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length), true);
						} else {
							LOGGER.warn("No data for binary IN-ROW SYS.XMLTYPE with lid {} in transaction {}!",
									ll.lid(), transaction.getXid());
							columnValue = null;
						}
					}
				} else if (ll.dataLength() == 0){
					if (LOGGER.isDebugEnabled())
						LOGGER.debug("No data for LOB with zero length, type {} with lid {}  in transaction {}!",
								getTypeName(columnType), ll.lid(), transaction.getXid());
						columnValue = null;
				} else {
					LOGGER.warn("No data for LOB type {} with lid {}, length {} in transaction {}!",
							getTypeName(columnType), ll.lid(), ll.dataLength(), transaction.getXid());
					columnValue = null;
				}
				break;
			case OraColumn.JAVA_SQL_TYPE_INTERVALYM_STRING:
			case OraColumn.JAVA_SQL_TYPE_INTERVALDS_STRING:
				columnValue = OraInterval.fromLogical(plaintext);
				break;
			case BOOLEAN:
				columnValue = OraDumpDecoder.toBoolean(plaintext, 0);
				break;
			default:
				columnValue = oraColumn.unsupportedTypeValue();
				break;
			}
		} else {
			switch (columnType) {
			case DATE:
			case TIMESTAMP:				
				columnValue = OraDumpDecoder.toTimestamp(data, offset, length);
				break;
			case TIMESTAMP_WITH_TIMEZONE:
				columnValue = OraTimestamp.fromLogical(
						data, offset, length, oraColumn.isLocalTimeZone(), rdbmsInfo.getDbTimeZone());
				break;
			case TINYINT:
				columnValue = toByte(data, offset, length);
				break;
			case SMALLINT:
				columnValue = toShort(data, offset, length);
				break;
			case INTEGER:
				columnValue = toInt(data, offset, length);
				break;
			case BIGINT:
				columnValue = toLong(data, offset, length);
				break;
			case FLOAT:
				if (oraColumn.isBinaryFloatDouble()) {
					columnValue = OraDumpDecoder.fromBinaryFloat(Arrays.copyOfRange(data, offset, offset + length));
				} else {
					columnValue = toFloat(data, offset, length);
				}
				break;
			case DOUBLE:
				if (oraColumn.isBinaryFloatDouble()) {
					columnValue = OraDumpDecoder.fromBinaryDouble(Arrays.copyOfRange(data, offset, offset + length));
				} else {
					columnValue = toDouble(data, offset, length);
				}
				break;
			case DECIMAL:
				BigDecimal bdValue = OraDumpDecoder.toBigDecimal(Arrays.copyOfRange(data, offset, offset + length));
				if (bdValue.scale() > oraColumn.getDataScale()) {
					LOGGER.warn(
								"Different data scale for column {} in table {}! Current value={}. Data scale from redo={}, data scale in current dictionary={}",
								columnName, this.tableFqn, bdValue, bdValue.scale(), oraColumn.getDataScale());
					columnValue = bdValue.setScale(oraColumn.getDataScale(), RoundingMode.HALF_UP);
				} else if (bdValue.scale() !=  oraColumn.getDataScale()) {
					columnValue = bdValue.setScale(oraColumn.getDataScale());
				} else {
					columnValue = bdValue;
				}
				break;
			case BINARY:
				if (oraColumn.getSecureFile()) {
					columnValue = extendedSizeValue(BINARY, transaction, lobIds, data, offset, length);
				} else
					columnValue = Arrays.copyOfRange(data, offset, offset + length);
				break;
			case NUMERIC:
			case OracleTypes.INTERVALYM:
			case OracleTypes.INTERVALDS:
				// do not need to perform data type conversion here!
				columnValue = Arrays.copyOfRange(data, offset, offset + length);
				break;
			case CHAR:
			case VARCHAR:
				if (oraColumn.getSecureFile()) {
					columnValue = extendedSizeValue(VARCHAR, transaction, lobIds, data, offset, length);
				} else
					
					columnValue = odd.fromVarchar2(data, offset, length);
				break;
			case NCHAR:
			case NVARCHAR:
				if (oraColumn.getSecureFile()) {
					columnValue = extendedSizeValue(NVARCHAR, transaction, lobIds, data, offset, length);
				} else
					columnValue = odd.fromNvarchar2(data, offset, length);
				break;
			case CLOB:
			case NCLOB:
			case BLOB:
			case SQLXML:
			case JSON:
			case VECTOR:
				final OraCdcTransactionChronicleQueue cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final LobLocator ll = new LobLocator(data, offset, length);
				if (LOGGER.isTraceEnabled())
					LOGGER.trace("Processing column {}, LID={}, DATALENGTH={}, EXTERNAL={}, LOB CONTENT=>{}",
							columnName, ll.lid(), ll.dataLength(), lobIds.contains(ll.lid()), rawToHex(Arrays.copyOfRange(data, offset, offset + length)));
				if (lobIds.contains(ll.lid())) {
					if (columnType == BLOB)
						columnValue = odd.toOraBlob(cqTrans.getLob(ll));
					else if (oraColumn.getJdbcType() == JSON)
						columnValue = odd.toOraJson(cqTrans.getLob(ll));
					else if (columnType == CLOB || columnType == NCLOB)
						columnValue = odd.toOraClob(cqTrans.getLob(ll), columnType == CLOB); 
					else if (oraColumn.getJdbcType() == VECTOR)
						columnValue = odd.toOraVector(cqTrans.getLob(ll));
					else
						//SQLXML
						columnValue = odd.toOraXml(cqTrans.getLob(ll), ll.type() == LobLocator.CLOB);
				} else if (ll.dataLength() >= 0 && ll.dataInRow()) {
					if (columnType == BLOB)
						columnValue = odd.toOraBlob(
								Arrays.copyOfRange(data, offset + length - ll.dataLength(), offset + length));
					else if (oraColumn.getJdbcType() == JSON)
						columnValue = odd.toOraJson(
								Arrays.copyOfRange(data, offset + length - ll.dataLength(), offset + length));
					else if (columnType == CLOB || columnType == NCLOB)
						columnValue = odd.toOraClob(
								Arrays.copyOfRange(data, offset + length - ll.dataLength(), offset + length), columnType == CLOB);
					else if (oraColumn.getJdbcType() == VECTOR)
						columnValue = odd.toOraVector(
								Arrays.copyOfRange(data, offset + length - ll.dataLength(), offset + length));
					else {
						//SQLXML
						if (ll.type() == LobLocator.CLOB) {
							columnValue = odd.toOraXml(Arrays.copyOfRange(data, offset + length - ll.dataLength(), offset + length), true);
						} else {
							LOGGER.warn("No data for binary IN-ROW SYS.XMLTYPE with lid {} in transaction {}!",
									ll.lid(), transaction.getXid());
							columnValue = null;
						}
					}
				} else if (ll.dataLength() == 0){
					if (LOGGER.isDebugEnabled())
						LOGGER.debug("No data for LOB with zero length, type {} with lid {}  in transaction {}!",
								getTypeName(columnType), ll.lid(), transaction.getXid());
						columnValue = null;
				} else {
					LOGGER.warn("No data for LOB type {} with lid {}, length {} in transaction {}!",
							getTypeName(columnType), ll.lid(), ll.dataLength(), transaction.getXid());
					columnValue = null;
				}
				break;
			case OraColumn.JAVA_SQL_TYPE_INTERVALYM_STRING:
			case OraColumn.JAVA_SQL_TYPE_INTERVALDS_STRING:
				columnValue = OraInterval.fromLogical(Arrays.copyOfRange(data, offset, offset + length));
				break;
			case BOOLEAN:
				columnValue = OraDumpDecoder.toBoolean(data, offset);
				break;
			default:
				columnValue = oraColumn.unsupportedTypeValue();
				break;
			}
		}
		if ((flags & FLG_ONLY_VALUE) > 0) {
			valueStruct.put(columnName, columnValue);
		} else {
			if (pkColumns.containsKey(columnName)) {
				keyStruct.put(columnName, columnValue);
				if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
						valueStruct.put(columnName, columnValue);
				}
			} else {
				if ((columnType == BLOB || columnType == CLOB ||
					columnType == NCLOB || columnType == SQLXML) &&
								(lobColumnSchemas != null &&
								lobColumnSchemas.containsKey(columnName))) {
					// Data are overloaded
					LOGGER.warn("LOB transformation not implemented yet!");
//					valueStruct.put(columnName,
//								transformLobs.transformData(
//										pdbName, tableOwner, tableName, oraColumn,
//										(byte[]) columnValue, keyStruct,
//										lobColumnSchemas.get(columnName)));
				} else {
					valueStruct.put(columnName, columnValue);
				}
			}
		}
	}

	private Object extendedSizeValue(
			final int jdbcType, final OraCdcTransaction transaction, final Set<LobId> lobIds,
			final byte[] data, final int offset, final int length) throws SQLException {
		final OraCdcTransactionChronicleQueue cqTrans = (OraCdcTransactionChronicleQueue) transaction;
		final LobLocator ll = new LobLocator(data, offset, length);
		if (lobIds.contains(ll.lid())) {
			if (jdbcType == VARCHAR)
				return odd.fromVarchar2(cqTrans.getLob(ll));
			if (jdbcType == NVARCHAR)
				return odd.fromNvarchar2(cqTrans.getLob(ll));
			else	// BINARY
				return cqTrans.getLob(ll);
		} else {
			if (jdbcType == VARCHAR)
				return odd.fromVarchar2(data, offset + length - ll.dataLength(), ll.dataLength());
			if (jdbcType == NVARCHAR)
				return odd.fromNvarchar2(data, offset + length - ll.dataLength(), ll.dataLength());
			else	// BINARY
				return Arrays.copyOfRange(data, offset + length - ll.dataLength(), offset + length);
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
