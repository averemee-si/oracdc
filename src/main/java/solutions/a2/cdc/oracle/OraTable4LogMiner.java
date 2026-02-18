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

import static java.nio.charset.StandardCharsets.UTF_16;
import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.SQLXML;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.XML_DOC_BEGIN;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.INCOMPLETE_REDO_INT_ERROR;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.INCOMPLETE_REDO_INT_SKIP;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraTable4LogMiner extends OraTable {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraTable4LogMiner.class);

	private static final String SQL_REDO_WHERE = " where ";
	private static final String SQL_REDO_SET = " set ";
	private static final String SQL_REDO_AND = " and ";
	private static final String SQL_REDO_IS = " IS";
	private static final String SQL_REDO_VALUES = " values ";

	private static final int LOB_BASICFILES_DATA_BEGINS = 72;
	private static final int LOB_SECUREFILES_DATA_BEGINS = 60;

	private final Map<String, OraColumn> idToNameMap = new HashMap<>();
	private final Set<String> setColumns = new HashSet<>();
	private Map<Long, OraColumn> lobColumnsObjectIds;
	private Map<String, OraColumn> lobColumnsNames;

	/**
	 * 
	 * For LogMiner worker thread
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
	public OraTable4LogMiner(
			final String pdbName, final short conId, final String tableOwner,
			final String tableName, final boolean rowLevelScn,
			final OraCdcSourceConnectorConfig config,
			final OraRdbmsInfo rdbmsInfo, final Connection connection, final int version) {
		super(pdbName, tableOwner, tableName, rowLevelScn, conId, config, rdbmsInfo, connection, version);
		try {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Preparing column list and mining SQL statements for table {}.", tableFqn);
			}
			readAndParseOraColumns(connection);
		} catch (SQLException sqle) {
			throw sqlExceptionOnInit(sqle);
		}
	}

	@Override
	void addToIdMap(final OraColumn column) {
		idToNameMap.put(column.getNameFromId(), column);
		if ((flags & FLG_WITH_LOBS) > 0)
			lobColumnsNames.put(column.getColumnName(), column);
	}

	@Override
	void clearIdMap() {
		idToNameMap.clear();
	}

	@Override
	void removeUnusedColumn(final OraColumn unusedColumn) {
		idToNameMap.remove(unusedColumn.getColumnName());
	}

	@Override
	void shiftColumnId(final OraColumn column) {}

	@Override
	void removeUnusedLobColumn(final String unusedColName) {
		super.removeUnusedLobColumn(unusedColName);
		if (lobColumnSchemas != null)
			lobColumnSchemas.remove(unusedColName);
	}

	@Override
	void clearLobHolders() {
		super.clearLobHolders();
		if (lobColumnsNames != null)
			lobColumnsNames.clear();
		if (lobColumnsObjectIds != null)
			lobColumnsObjectIds.clear();
	}

	@Override
	void createLobHolders() {
		lobColumnsNames = new HashMap<>();
		lobColumnsObjectIds = new HashMap<>();
	}

	@Override
	void addLobColumnId(final int columnId) {
	}

	SourceRecord parseRedoRecord(
			final OraCdcLogMinerStatement stmt,
			final List<OraCdcLargeObjectHolder> lobs,
			final OraCdcTransaction transaction,
			final Map<String, Object> offset,
			final Connection connection) throws SQLException {
		structWriter.init(stmt);
		boolean skipRedoRecord = false;

		if (LOGGER.isTraceEnabled()) {
			printErrorMessage(
					Level.TRACE,
					"Parsing REDO record for table {}\nRedo record information:\n",
					stmt, transaction);
		}
		if ((flags & FLG_PSEUDO_KEY) > 0)
			structWriter.addRowId(stmt);

		final char opType;
		if (stmt.getOperation() == INSERT) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing INSERT");
			}
			opType = 'c';
			int valuedClauseStart = Strings.CS.indexOf(stmt.getSqlRedo(), SQL_REDO_VALUES);
			String[] columnsList = StringUtils.split(StringUtils.substringBetween(
					StringUtils.substring(stmt.getSqlRedo(), 0, valuedClauseStart), "(", ")"), ",");
			String[] valuesList = StringUtils.split(StringUtils.substringBetween(
					StringUtils.substring(stmt.getSqlRedo(), valuedClauseStart + 8), "(", ")"), ",");
			for (int i = 0; i < columnsList.length; i++) {
				final String columnName = StringUtils.trim(columnsList[i]);
				final String columnValue = StringUtils.trim(valuesList[i]);
				final OraColumn oraColumn = idToNameMap.get(columnName);
				if (oraColumn != null) {
					if (Strings.CS.startsWith(columnValue, "N")) {
						if (oraColumn.mandatory()) {
							if (incompleteDataTolerance == INCOMPLETE_REDO_INT_ERROR) {
								printInvalidFieldValue(oraColumn, stmt, transaction);
								throw new DataException("Mandatory field " + oraColumn.getColumnName() + " is NULL!");
							} else if (incompleteDataTolerance == INCOMPLETE_REDO_INT_SKIP) {
								printSkippingRedoRecordMessage(stmt, transaction);
								return null;
							} else {
								//INCOMPLETE_REDO_INT_RESTORE
								missedColumns.add(oraColumn);
							}
						}
					} else if (Strings.CS.equals("''", columnValue) && oraColumn.largeObject()) {
						// EMPTY_BLOB()/EMPTY_CLOB() passed as ''
						structWriter.insert(oraColumn, new byte[0]);
						continue;
					} else {
						// Handle LOB inline value!
						try {
							//We don't have inline values for XMLTYPE
							if (oraColumn.getJdbcType() != SQLXML) {
								if (columnValue != null && columnValue.length() > 0) {
									try {
										structWriter.insert(oraColumn, parseRedoRecordValues(oraColumn, columnValue));
									} catch (SQLException sqle) {
										if (oraColumn.isNullable()) {
											printToLogInvalidHexValueWarning(
													columnValue, oraColumn.getColumnName(), stmt);
										} else {
											LOGGER.error("Invalid value {} for column {} in table {}",
													columnValue, oraColumn.getColumnName(), tableFqn);
											printInvalidFieldValue(oraColumn, stmt, transaction);
											throw new SQLException(sqle);
										}
									}
								} else {
									LOGGER.warn(
											"""
											
											=====================
											Null or zero length data for overload for LOB column {} with inline value in table {}.
											=====================
											
											""", oraColumn.getColumnName(), tableFqn);
								}
							}
						} catch (DataException de) {
							LOGGER.error("Invalid value {} for column {} in table {}",
									columnValue, oraColumn.getColumnName(), tableFqn);
							printInvalidFieldValue(oraColumn, stmt, transaction);
							throw new DataException(de);
						}
					}
				}
			}
		} else if (stmt.getOperation() == DELETE) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing DELETE");
			}
			opType = 'd';
			if ((flags & FLG_TABLE_WITH_PK) > 0 || (flags & FLG_PSEUDO_KEY) > 0) {
				final int whereClauseStart = Strings.CS.indexOf(stmt.getSqlRedo(), SQL_REDO_WHERE);
				String[] whereClause = StringUtils.splitByWholeSeparator(
						StringUtils.substring(stmt.getSqlRedo(), whereClauseStart + 7), SQL_REDO_AND);
				for (int i = 0; i < whereClause.length; i++) {
					final String currentExpr = StringUtils.trim(whereClause[i]);
					if ((flags & FLG_ALL_COLS_ON_DELETE) > 0) {
						final String columnName;
						if (Strings.CS.endsWith(currentExpr, "L")) {
							columnName = StringUtils.substringBefore(currentExpr, SQL_REDO_IS);
							final OraColumn oraColumn = idToNameMap.get(columnName);
							if (oraColumn != null && oraColumn.mandatory()) {
								printInvalidFieldValue(oraColumn, stmt, transaction);
								throw new DataException("Mandatory field " + oraColumn.getColumnName() + " is NULL!");
							}
						} else {
							columnName = StringUtils.trim(StringUtils.substringBefore(currentExpr, "="));
							final OraColumn oraColumn = idToNameMap.get(columnName);
							if (oraColumn != null) {
								final String columnValue = StringUtils.trim(StringUtils.substringAfter(currentExpr, "="));
								try {
									structWriter.delete(oraColumn, parseRedoRecordValues(oraColumn, columnValue));
								} catch (DataException de) {
									LOGGER.error("Invalid value {} for column {} in table {}",
											columnValue, oraColumn.getColumnName(), tableFqn);
									printInvalidFieldValue(oraColumn, stmt, transaction);
									throw new DataException(de);
								} catch (SQLException sqle) {
									if (oraColumn.isNullable()) {
										printToLogInvalidHexValueWarning(
												columnValue, oraColumn.getColumnName(), stmt);
									} else {
										LOGGER.error("Invalid value {} for column {} in table {}",
												columnValue, oraColumn.getColumnName(), tableFqn);
										printInvalidFieldValue(oraColumn, stmt, transaction);
										throw new SQLException(sqle);
									}
								}
							}
						}
					} else {
						if (!Strings.CS.endsWith(currentExpr, "L")) {
							// PK can't be null!!!
							final String columnName = StringUtils.trim(StringUtils.substringBefore(currentExpr, "="));
							final OraColumn oraColumn = idToNameMap.get(columnName);
							if (oraColumn != null && oraColumn.isPartOfPk()) {
								structWriter.delete(oraColumn,
										parseRedoRecordValues(idToNameMap.get(columnName),
												StringUtils.trim(StringUtils.substringAfter(currentExpr, "="))));
							} else {
								// Handle ORA-1 in Source DB.....
								if (Strings.CI.equals("ROWID", columnName) &&
										whereClause.length == 1) {
									printErrorMessage(
											Level.ERROR,
											"Unable to parse delete record for table {} after INSERT with ORA-1 error.\nRedo record information:\n",
											stmt, transaction);
									skipRedoRecord = true;
								} else if (whereClause.length == 1) {
									printErrorMessage(
											Level.ERROR,
											"Unknown error while parsing delete record for table {}.\nRedo record information:\n",
											stmt, transaction);
									skipRedoRecord = true;
								}
							}
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
			opType = 'u';
			setColumns.clear();
			final int whereClauseStart = Strings.CS.indexOf(stmt.getSqlRedo(), SQL_REDO_WHERE);
			final int setClauseStart = Strings.CS.indexOf(stmt.getSqlRedo(), SQL_REDO_SET);
			final String[] setClause;
			final boolean processWhereFromRow;
			if (whereClauseStart > 0) {
				setClause = StringUtils.split(
						StringUtils.substring(stmt.getSqlRedo(), setClauseStart + 5, whereClauseStart), ",");
				processWhereFromRow = false;
			} else {
				setClause = StringUtils.split(
						StringUtils.substring(stmt.getSqlRedo(), setClauseStart + 5), ",");
				processWhereFromRow = true;
			}
			for (int i = 0; i < setClause.length; i++) {
				final String currentExpr = StringUtils.trim(setClause[i]);
				final String columnName;
				columnName = StringUtils.trim(StringUtils.substringBefore(currentExpr, "="));
				final OraColumn oraColumn = idToNameMap.get(columnName);
				if (oraColumn != null) {
					// Column can be excluded
					if (Strings.CS.endsWith(currentExpr, "L")) {
						if (oraColumn.largeObject()) {
							// Explicit NULL for LOB!
							structWriter.update(oraColumn, new byte[0], true);
						} else {
							if (oraColumn.mandatory()) {
								if (oraColumn.defaultValuePresent()) {
									structWriter.update(oraColumn, oraColumn.typedDefaultValue(), true);
									if (LOGGER.isDebugEnabled()) {
										LOGGER.debug("Value of column {} in table {} is set to default value of '{}'",
												oraColumn.getColumnName(), tableFqn, oraColumn.defaultValue());
									}
								} else {
									// throw error only if we don't expect to get value from WHERE clause
									printInvalidFieldValue(oraColumn, stmt, transaction);
									throw new DataException("Mandatory field " + oraColumn.getColumnName() + " is NULL!");
								}
							}
						}
						setColumns.add(columnName);
					} else {
						final String columnValue = StringUtils.substringAfter(currentExpr, "=");
						if ("''".equals(columnValue) &&
								(oraColumn.largeObject())) {
							structWriter.update(oraColumn, new byte[0], true);
							continue;
						} else {
							try {
								structWriter.update(oraColumn,
										parseRedoRecordValues(oraColumn, columnValue),
										true);
								setColumns.add(columnName);
							} catch (SQLException sqle ) {
								if (oraColumn.isNullable()) {
									printToLogInvalidHexValueWarning(
											columnValue, oraColumn.getColumnName(), stmt);
								} else {
									LOGGER.error("Invalid value {} for column {} in table {}",
											columnValue, oraColumn.getColumnName(), tableFqn);
									printInvalidFieldValue(oraColumn, stmt, transaction);
									throw new SQLException(sqle);
								}
							}
						}
					}
				}
			}
			//BEGIN: where clause processing...
			if (processWhereFromRow) {
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
			} else {
				String[] whereClause = StringUtils.splitByWholeSeparator(
						StringUtils.substring(stmt.getSqlRedo(), whereClauseStart + 7), SQL_REDO_AND);
				for (int i = 0; i < whereClause.length; i++) {
					final String currentExpr = StringUtils.trim(whereClause[i]);
					final String columnName;
					if (Strings.CS.endsWith(currentExpr, "L")) {
						columnName = StringUtils.substringBefore(currentExpr, SQL_REDO_IS);
						if (!setColumns.contains(columnName)) {
							final OraColumn oraColumn = idToNameMap.get(columnName);
							if (oraColumn != null) {
								if (oraColumn.mandatory()) {
									// Check again for column default value...
									// This is due "SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
									boolean throwDataException = true;
									if (oraColumn.defaultValuePresent()) {
										final Object columnDefaultValue = oraColumn.typedDefaultValue();
										if (columnDefaultValue != null) {
											printSubstDefaultValueWarning(oraColumn, stmt);
											structWriter.update(oraColumn, columnDefaultValue, true);
											throwDataException = false;
										}
									} else {
										printNullValueError(oraColumn, stmt);
									}
									if (throwDataException) {
										if (incompleteDataTolerance == INCOMPLETE_REDO_INT_ERROR) {
											printInvalidFieldValue(oraColumn, stmt, transaction);
											throw new DataException("Mandatory field " + oraColumn.getColumnName() + " is NULL!");
										} else if (incompleteDataTolerance == INCOMPLETE_REDO_INT_SKIP) {
											printSkippingRedoRecordMessage(stmt, transaction);
											return null;
										} else {
											//INCOMPLETE_REDO_INT_RESTORE
											missedColumns.add(oraColumn);
										}
									}
								}
							}
						}
					} else {
						columnName = StringUtils.trim(StringUtils.substringBefore(currentExpr, "="));
						if (!setColumns.contains(columnName)) {
							final OraColumn oraColumn = idToNameMap.get(columnName);
							if (oraColumn != null) {
								// Column can be excluded
								final String columnValue = StringUtils.trim(StringUtils.substringAfter(currentExpr, "="));
								try {
									structWriter.update(oraColumn, 
											parseRedoRecordValues(oraColumn, columnValue),
											true);
								} catch (DataException de) {
									LOGGER.error("Invalid value {} for column {} in table {}",
											columnValue, oraColumn.getColumnName(), tableFqn);
									printInvalidFieldValue(oraColumn, stmt, transaction);
									throw new DataException(de);
								} catch (SQLException sqle) {
									if (oraColumn.isNullable()) {
										printToLogInvalidHexValueWarning(
												columnValue, oraColumn.getColumnName(), stmt);
									} else {
										LOGGER.error("Invalid value {} for column {} in table {}",
												columnValue, oraColumn.getColumnName(), tableFqn);
										printInvalidFieldValue(oraColumn, stmt, transaction);
										throw new SQLException(sqle);
									}
								}
							}
						}
					}
				}
			}
			//END: where clause processing...
		} else if (stmt.getOperation() == XML_DOC_BEGIN) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing XML_DOC_BEGIN (for XMLTYPE update)");
			}
			opType = 'u';
			final int whereClauseStart = Strings.CS.indexOf(stmt.getSqlRedo(), SQL_REDO_WHERE);
			String[] whereClause = StringUtils.splitByWholeSeparator(
					StringUtils.substring(stmt.getSqlRedo(), whereClauseStart + 7), SQL_REDO_AND);
			for (int i = 0; i < whereClause.length; i++) {
				final String currentExpr = StringUtils.trim(whereClause[i]);
				final String columnName;
				if (Strings.CS.endsWith(currentExpr, "L")) {
					columnName = StringUtils.trim(StringUtils.substringBefore(currentExpr, SQL_REDO_IS));
				} else {
					columnName = StringUtils.trim(StringUtils.substringBefore(currentExpr, "="));
				}
				final OraColumn oraColumn = idToNameMap.get(columnName);
				if (oraColumn != null) {
					if (!Strings.CS.endsWith(currentExpr, "L")) {
						structWriter.update(oraColumn,parseRedoRecordValues(oraColumn,
								StringUtils.trim(StringUtils.substringAfter(currentExpr, "="))), true);
					}
				} else {
					printErrorMessage(
							Level.ERROR,
							"Can't detect column with name '{}' during parsing!\nRedo record information for table:\n",
							columnName, stmt, transaction);
					throw new DataException(
							"Can't detect column with name " + columnName + " during parsing!");
				}
			}
		} else {
			// We expect here only 1,2,3 as valid values for OPERATION_CODE (and 68 for special cases)
			printErrorMessage(
					Level.ERROR,
					"Corrupted record for table {} found!!!\nPlease send e-mail to oracle@a2.solutions with record details below:\n",
					stmt, transaction);
			throw new SQLException("Unknown OPERATION_CODE while parsing redo record!");
		}

		if ((flags & FLG_PROCESS_LOBS) > 0 &&
				(stmt.getOperation() == UPDATE ||
				stmt.getOperation() == INSERT) ||
				stmt.getOperation() == XML_DOC_BEGIN) {
			if (lobs != null) {
				for (int i = 0; i < lobs.size(); i++) {
					final OraCdcLargeObjectHolder lob = lobs.get(i);
					final String lobColumnName;
					final OraColumn lobColumn;
					if (lob.getLobId() > 0) {
						lobColumn = lobColumnsObjectIds.get(lob.getLobId());
						lobColumnName = lobColumn.getColumnName();
					} else {
						// lob.getLobId() == 0
						lobColumn = idToNameMap.get(lob.getColumnId());
						lobColumnName = lobColumn.getColumnName();
					}
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("{}: setting value for BLOB/C column {}, value length={}.",
								tableFqn, lobColumnName, lob.getContent().length);
					}
					if (lobColumnSchemas != null &&
							lobColumnSchemas.containsKey(lobColumnName)) {
						
						structWriter.update(lobColumn,
								transformLobs.transformData(
										pdbName, tableOwner, tableName,
										lobColumn, lob.getContent(),
										keyStruct, lobColumnSchemas.get(lobColumnName)),
								true);
					} else {
						structWriter.update(lobColumn, lob.getContent(lobColumn.getJdbcType()), true);
					}
				}
			}
		}

		return createSourceRecord(stmt, transaction, offset, opType,
				skipRedoRecord, connection, missedColumns);
	}

	private Object parseRedoRecordValues(final OraColumn oraColumn, final String hexValue) throws SQLException {
		//final String hex = StringUtils.substring(hexValue, 1, hexValue.length() - 1);
		final String hex = StringUtils.substringBetween(hexValue, "'");
		final Object columnValue;
		if (!oraColumn.largeObject()) {
			columnValue = oraColumn.decoder().decode(hex);
		} else {
			switch (oraColumn.getJdbcType()) {
				case CLOB, NCLOB -> {
					final String clobValue;
					if (oraColumn.getSecureFile()) {
						if (hex.length() == LOB_SECUREFILES_DATA_BEGINS || hex.length() == 0) {
							clobValue = "";
						} else {
							clobValue = new String(hexToRaw(StringUtils.substring(
									hex,
									LOB_SECUREFILES_DATA_BEGINS  + (extraSecureFileLengthByte(hex) ? 2 : 0))),
									UTF_16);
						}
					} else {
						clobValue = new String(hexToRaw(StringUtils.substring(hex, LOB_BASICFILES_DATA_BEGINS)),
									UTF_16);
					}
					if (clobValue.length() == 0) {
						columnValue = new byte[0];
					} else {
						columnValue = clobValue;
					}
				}
				case BLOB -> {
					if (oraColumn.getSecureFile()) {
						if (hex.length() == LOB_SECUREFILES_DATA_BEGINS || hex.length() == 0) {
							columnValue = new byte[0];
						} else {
							columnValue = hexToRaw(StringUtils.substring(hex,
									LOB_SECUREFILES_DATA_BEGINS  + (extraSecureFileLengthByte(hex) ? 2 : 0)));
						}
					} else {
						columnValue = hexToRaw(
									StringUtils.substring(hex, LOB_BASICFILES_DATA_BEGINS));
					}
				}
				case SQLXML -> {
					// We not expect SYS.XMLTYPE data here!!!
					// Set it to 'Not touch at Sink!!!'
					columnValue = null;
				}
				default -> columnValue = oraColumn.unsupportedTypeValue();
			}
		}
		return columnValue;
	}

	@Override
	public String toString() {
		return tableFqn;
	}

	OraColumn getLobColumn(final long lobObjectId, final PreparedStatement psCheckLob) throws SQLException {
		if (lobColumnsObjectIds.containsKey(lobObjectId)) {
			return lobColumnsObjectIds.get(lobObjectId);
		} else {
			// Perform mapping of DATA_OBJ# to table column
			psCheckLob.setLong(1, lobObjectId);
			ResultSet rsCheckLob = psCheckLob.executeQuery();
			if (rsCheckLob.next()) {
				final String columnName = rsCheckLob.getString("COLUMN_NAME");
				if (lobColumnsNames.containsKey(columnName)) {
					final OraColumn column = lobColumnsNames.get(columnName);
					column.setSecureFile(Strings.CS.equals("YES", rsCheckLob.getString("SECUREFILE")));
					lobColumnsObjectIds.put(lobObjectId, column);
					return column;
				} else {
					LOGGER.error("Column for LOB with object Id {} not found in oracdc cache!", lobObjectId);
					throw new SQLException("Column for LOB with object Id " + lobObjectId + " not found in oracdc cache!");
				}
			} else {
				LOGGER.error("Column for LOB with object Id {} not found in database!", lobObjectId);
				throw new SQLException("Column for LOB with object Id " + lobObjectId + " not found in database!");
			}
		}
	}

	boolean isWithLobs() {
		return (flags & FLG_WITH_LOBS) > 0;
	}

	private boolean extraSecureFileLengthByte(String hex) throws SQLException {
		final String startPosFlag = StringUtils.substring(hex, 52, 54);
		if (Strings.CS.equals("00", startPosFlag)) {
			return false;
		} else if (Strings.CS.equals("01", startPosFlag)) {
			return true;
		} else {
			LOGGER.error("Invalid SECUREFILE additional length byte value '{}' for hex LOB '{}'",
					startPosFlag, hex);
			throw new SQLException("Invalid SECUREFILE additional length byte value!");
		}
	}

	boolean isCheckSupplementalLogData() {
		return (flags & FLG_CHECK_SUPPLEMENTAL) > 0; 
	}


}
