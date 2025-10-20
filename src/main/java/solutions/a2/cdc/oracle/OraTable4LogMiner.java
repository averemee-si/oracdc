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
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.XML_DOC_BEGIN;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toByte;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toDouble;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toFloat;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toInt;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toLong;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toShort;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import oracle.jdbc.OracleTypes;
import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import solutions.a2.cdc.oracle.data.OraInterval;
import solutions.a2.cdc.oracle.data.OraTimestamp;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.oracle.jdbc.types.OracleDate;
import solutions.a2.oracle.jdbc.types.OracleTimestamp;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
@JsonInclude(Include.NON_EMPTY)
public class OraTable4LogMiner extends OraTable {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraTable4LogMiner.class);

	private static final String SQL_REDO_WHERE = " where ";
	private static final String SQL_REDO_SET = " set ";
	private static final String SQL_REDO_AND = " and ";
	private static final String SQL_REDO_IS = " IS";
	private static final String SQL_REDO_VALUES = " values ";

	private static final int LOB_BASICFILES_DATA_BEGINS = 72;
	private static final int LOB_SECUREFILES_DATA_BEGINS = 60;

	private static final int DATE_DATA_LENGTH = OracleDate.DATA_LENGTH * 2;
	private static final int TS_DATA_LENGTH = OracleTimestamp.DATA_LENGTH * 2;

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
	 * @param rowLevelScnDependency
	 * @param config
	 * @param rdbmsInfo
	 * @param connection
	 */
	public OraTable4LogMiner(
			final String pdbName, final short conId, final String tableOwner,
			final String tableName, final boolean rowLevelScnDependency,
			final OraCdcSourceConnectorConfig config,
			final OraRdbmsInfo rdbmsInfo, final Connection connection, final int version) {
		super(pdbName, tableOwner, tableName, config.schemaType(),
				config.processLobs(), config.transformLobsImpl(), config, rdbmsInfo);
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

	/**
	 * 
	 * Restore OraTable from JSON
	 * 
	 * @param tableData
	 * @param schemaType
	 * @param transformLobs
	 * @param rdbmsInfo
	 */
	public OraTable4LogMiner(Map<String, Object> tableData, final int schemaType,
			final OraCdcLobTransformationsIntf transformLobs,
			final OraRdbmsInfo rdbmsInfo) {
		super((String) tableData.get("pdbName"),
				(String) tableData.get("tableOwner"),
				(String) tableData.get("tableName"),
				schemaType, (boolean) tableData.get("processLobs"),
				transformLobs, null, rdbmsInfo);
		if ((boolean) tableData.get("tableWithPk"))
			flags |= FLG_TABLE_WITH_PK;
		else
			flags &= (~FLG_TABLE_WITH_PK);
		final Boolean rowLevelScnDependency = (Boolean) tableData.get("rowLevelScn");
		if (rowLevelScnDependency == null || !rowLevelScnDependency)
			rowLevelScn = false;
		else
			rowLevelScn = true;
		if (LOGGER.isDebugEnabled()) {
			if (pdbName == null) {
				LOGGER.debug("Deserializing {}.{} from JSON", tableOwner, tableName);
			} else {
				LOGGER.debug("Deserializing {}:{}.{} from JSON", pdbName, tableOwner, tableName);
			}
		}

		// Schema init
		final SchemaBuilder keySchemaBuilder = schemaBuilder(true, 1);
		final SchemaBuilder valueSchemaBuilder = schemaBuilder(false, version);

		try {
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> colDataList = (List<Map<String, Object>>) tableData.get("columns");
			allColumns = new ArrayList<>();
			for (Map<String, Object> colData : colDataList) {
				final OraColumn column = new OraColumn(colData, keySchemaBuilder, valueSchemaBuilder, schemaType);
				allColumns.add(column);
				idToNameMap.put(column.getNameFromId(), column);
				if (column.isPartOfPk()) {
					final String pkColumnName = column.getColumnName();
					pkColumns.put(pkColumnName, column);
				}
				LOGGER.debug("\t Adding {} column.", column.getColumnName());
				//TODO
				//TODO Do we need special processing for LOB's here?
				//TODO
			}
			schemaEiplogue(tableFqn, keySchemaBuilder, valueSchemaBuilder);
		} catch (SQLException sqle) {
			throw new ConnectException(sqle);
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
			keyStruct.put(OraColumn.ROWID_KEY, stmt.getRowId().toString());
			if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
				valueStruct.put(OraColumn.ROWID_KEY, stmt.getRowId().toString());
			}
		}
		mandatoryColumnsProcessed = 0;
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
					// Column can be excluded
					if (Strings.CS.startsWith(columnValue, "N")) {
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
					} else if ("''".equals(columnValue) &&
							(oraColumn.getJdbcType() == BLOB ||
							oraColumn.getJdbcType() == CLOB ||
							oraColumn.getJdbcType() == NCLOB ||
							oraColumn.getJdbcType() == JSON ||
							oraColumn.getJdbcType() == VECTOR)) {
						// EMPTY_BLOB()/EMPTY_CLOB() passed as ''
						valueStruct.put(oraColumn.getColumnName(), new byte[0]);
						continue;
					} else {
						// Handle LOB inline value!
						try {
							//We don't have inline values for XMLTYPE
							if (oraColumn.getJdbcType() != SQLXML) {
								if (columnValue != null && columnValue.length() > 0) {
									try {
										parseRedoRecordValues(oraColumn, columnValue);
										if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
											mandatoryColumnsProcessed++;
										}
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
											"\n" +
											"=====================\n" +
											"Null or zero length data for overload for LOB column {} with inline value in table {}.\n" +
											"=====================\n",
											oraColumn.getColumnName(), tableFqn);
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
							try {
								valueStruct.put(oraColumn.getColumnName(), null);
							} catch (DataException de) {
								//TODO
								//TODO
								//TODO
								//TODO
								printInvalidFieldValue(oraColumn, stmt, transaction);
								throw de;
							}
						} else {
							columnName = StringUtils.trim(StringUtils.substringBefore(currentExpr, "="));
							final OraColumn oraColumn = idToNameMap.get(columnName);
							if (oraColumn != null) {
								// Column can be excluded
								final String columnValue = StringUtils.trim(StringUtils.substringAfter(currentExpr, "="));
								try {
									parseRedoRecordValues(oraColumn, columnValue);
									if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
										mandatoryColumnsProcessed++;
									}
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
								parseRedoRecordValues(idToNameMap.get(columnName),
										StringUtils.trim(StringUtils.substringAfter(currentExpr, "=")));
								if (oraColumn.isPartOfPk()) {
									mandatoryColumnsProcessed++;
								}
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
						try {
							if (oraColumn.getJdbcType() == BLOB ||
									oraColumn.getJdbcType() == CLOB ||
									oraColumn.getJdbcType() == NCLOB) {
								// Explicit NULL for LOB!
								valueStruct.put(oraColumn.getColumnName(), new byte[0]);
							} else {
								valueStruct.put(oraColumn.getColumnName(), null);
							}
							setColumns.add(columnName);
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
						final String columnValue = StringUtils.substringAfter(currentExpr, "=");
						if ("''".equals(columnValue) &&
								(oraColumn.getJdbcType() == BLOB ||
								oraColumn.getJdbcType() == CLOB ||
								oraColumn.getJdbcType() == NCLOB)) {
							valueStruct.put(oraColumn.getColumnName(), new byte[0]);
							continue;
						} else {
							try {
								parseRedoRecordValues(
										oraColumn, columnValue);
								if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
									mandatoryColumnsProcessed++;
								}
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
									parseRedoRecordValues(oraColumn, columnValue);
									if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
										mandatoryColumnsProcessed++;
									}
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
						parseRedoRecordValues(idToNameMap.get(columnName),
								StringUtils.trim(StringUtils.substringAfter(currentExpr, "=")));
						if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
							mandatoryColumnsProcessed++;
						}
					} else {
						// We assume EXPLICIT null here
						valueStruct.put(oraColumn.getColumnName(), null);					}
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
						valueStruct.put(lobColumnName,
								transformLobs.transformData(
										pdbName, tableOwner, tableName,
										lobColumn, lob.getContent(),
										keyStruct, lobColumnSchemas.get(lobColumnName)));
					} else {
						valueStruct.put(lobColumnName, lob.getContent(lobColumn.getJdbcType()));
					}
				}
			}
		}

		return createSourceRecord(stmt, transaction, offset, opType,
				skipRedoRecord, connection, missedColumns);
	}

	private void parseRedoRecordValues(final OraColumn oraColumn, final String hexValue) throws SQLException {
		final String columnName = oraColumn.getColumnName();
		//final String hex = StringUtils.substring(hexValue, 1, hexValue.length() - 1);
		final String hex = StringUtils.substringBetween(hexValue, "'");
		final Object columnValue;
		switch (oraColumn.getJdbcType()) {
			case DATE:
			case TIMESTAMP:
				if (hex.length() == DATE_DATA_LENGTH || hex.length() == TS_DATA_LENGTH) {
					columnValue = OraDumpDecoder.toTimestamp(hex);
				} else {
					throw new SQLException("Invalid DATE (Typ=12) or TIMESTAMP (Typ=180)");
				}
				break;
			case TIMESTAMP_WITH_TIMEZONE:
				columnValue = OraTimestamp.fromLogical(
					hexToRaw(hex), oraColumn.isLocalTimeZone(), rdbmsInfo.getDbTimeZone());
				break;
			case TINYINT:
				columnValue = toByte(hexToRaw(hex));
				break;
			case SMALLINT:
				columnValue = toShort(hexToRaw(hex));
				break;
			case INTEGER:
				columnValue = toInt(hexToRaw(hex));
				break;
			case BIGINT:
				columnValue = toLong(hexToRaw(hex));
				break;
			case FLOAT:
				if (oraColumn.isBinaryFloatDouble()) {
					columnValue = OraDumpDecoder.fromBinaryFloat(hex);
				} else {
					columnValue = toFloat(hexToRaw(hex));
				}
				break;
			case DOUBLE:
				if (oraColumn.isBinaryFloatDouble()) {
					columnValue = OraDumpDecoder.fromBinaryDouble(hex);
				} else {
					columnValue = toDouble(hexToRaw(hex));
				}
				break;
			case DECIMAL:
				BigDecimal bdValue = OraDumpDecoder.toBigDecimal(hex);
				if (bdValue.scale() > oraColumn.getDataScale()) {
					LOGGER.warn(
								"Different data scale for column {} in table {}! Current value={}. Data scale from redo={}, data scale in current dictionary={}",
								columnName, tableFqn, bdValue, bdValue.scale(), oraColumn.getDataScale());
					columnValue = bdValue.setScale(oraColumn.getDataScale(), RoundingMode.HALF_UP);
				} else if (bdValue.scale() !=  oraColumn.getDataScale()) {
					columnValue = bdValue.setScale(oraColumn.getDataScale());
				} else {
					columnValue = bdValue;
				}
				break;
			case BINARY:
			case NUMERIC:
			case OracleTypes.INTERVALYM:
			case OracleTypes.INTERVALDS:
				// do not need to perform data type conversion here!
				columnValue = hexToRaw(hex);
				break;
			case CHAR:
			case VARCHAR:
				columnValue = odd.fromVarchar2(hex);
				break;
			case NCHAR:
			case NVARCHAR:
				columnValue = odd.fromNvarchar2(hex);
				break;
			case CLOB:
			case NCLOB:
				final String clobValue;
				if (oraColumn.getSecureFile()) {
					if (hex.length() == LOB_SECUREFILES_DATA_BEGINS || hex.length() == 0) {
						clobValue = "";
					} else {
						clobValue = OraDumpDecoder.fromClobNclob(StringUtils.substring(hex,
							LOB_SECUREFILES_DATA_BEGINS  + (extraSecureFileLengthByte(hex) ? 2 : 0)));
					}
				} else {
					clobValue = OraDumpDecoder.fromClobNclob(
								StringUtils.substring(hex, LOB_BASICFILES_DATA_BEGINS));
				}
				if (clobValue.length() == 0) {
					columnValue = new byte[0];
				} else {
					columnValue = clobValue;
				}
				break;
			case BLOB:
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
				break;
			case SQLXML:
				// We not expect SYS.XMLTYPE data here!!!
				// Set it to 'Not touch at Sink!!!'
				columnValue = null;
				break;
			case OraColumn.JAVA_SQL_TYPE_INTERVALYM_STRING:
			case OraColumn.JAVA_SQL_TYPE_INTERVALDS_STRING:
				columnValue = OraInterval.fromLogical(hexToRaw(hex));
				break;
			default:
				columnValue = oraColumn.unsupportedTypeValue();
				break;
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
				if ((oraColumn.getJdbcType() == BLOB ||
							oraColumn.getJdbcType() == CLOB ||
							oraColumn.getJdbcType() == NCLOB ||
							oraColumn.getJdbcType() == SQLXML ||
							oraColumn.getJdbcType() == JSON ||
							oraColumn.getJdbcType() == VECTOR) &&
								(lobColumnSchemas != null &&
								lobColumnSchemas.containsKey(columnName))) {
					// Data are overloaded
					valueStruct.put(columnName,
								transformLobs.transformData(
										pdbName, tableOwner, tableName, oraColumn,
										(byte[]) columnValue, keyStruct,
										lobColumnSchemas.get(columnName)));
				} else {
					valueStruct.put(columnName, columnValue);
				}
			}
		}
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
