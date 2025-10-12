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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.event.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import oracle.jdbc.OracleTypes;
import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import solutions.a2.cdc.oracle.data.OraInterval;
import solutions.a2.cdc.oracle.data.OraTimestamp;
import solutions.a2.cdc.oracle.internals.OraCdcTdeColumnDecrypter;
import solutions.a2.cdc.oracle.internals.OraCdcTdeWallet;
import solutions.a2.cdc.oracle.utils.OraSqlUtils;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.oracle.internals.LobId;
import solutions.a2.oracle.internals.LobLocator;
import solutions.a2.oracle.internals.RowId;
import solutions.a2.oracle.jdbc.types.OracleDate;
import solutions.a2.oracle.jdbc.types.OracleTimestamp;

import static java.sql.Types.CHAR;
import static java.sql.Types.VARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.TINYINT;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.BIGINT;
import static java.sql.Types.FLOAT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.DATE;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.BINARY;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.SQLXML;
import static oracle.jdbc.OracleTypes.JSON;
import static oracle.jdbc.OracleTypes.VECTOR;
import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_ERROR;
import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_SKIP;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.XML_DOC_BEGIN;
import static solutions.a2.cdc.oracle.OraColumn.GUARD_COLUMN;
import static solutions.a2.cdc.oracle.OraColumn.UNUSED_COLUMN;
import static solutions.a2.cdc.oracle.schema.JdbcTypes.getTypeName;
import static solutions.a2.cdc.oracle.utils.OraSqlUtils.alterTablePreProcessor;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toByte;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toShort;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toInt;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toLong;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toFloat;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toDouble;
import static solutions.a2.oracle.utils.BinaryUtils.getU24BE;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
@JsonInclude(Include.NON_EMPTY)
public class OraTable4LogMiner extends OraTable4SourceConnector {

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

	private final Map<String, OraColumn> idToNameMap;
	private final Map<Integer, OraColumn> pureIdMap;
	private String pdbName;
	private String kafkaTopic;
	private OraDumpDecoder odd;
	private boolean tableWithPk;
	private boolean processLobs;
	private final OraCdcLobTransformationsIntf transformLobs;
	private final String tableFqn;
	private Map<Long, OraColumn> lobColumnsObjectIds;
	private Map<String, OraColumn> lobColumnsNames;
	private Map<String, Schema> lobColumnSchemas;
	private boolean withLobs = false;
	private int maxColumnId;
	private int topicPartition;
	private boolean checkSupplementalLogData = true;
	private String sqlGetKeysUsingRowId = null;
	private boolean printSqlForMissedWhereInUpdate = true;
	private boolean printInvalidHexValueWarning = false;
	private int incompleteDataTolerance = INCOMPLETE_REDO_INT_ERROR;
	private boolean onlyValue = false;
	private boolean useAllColsOnDelete = false;
	private int mandatoryColumnsCount = 0;
	private int mandatoryColumnsProcessed = 0;
	private boolean pseudoKey = false;
	private boolean printUnableToDeleteWarning;
	private boolean useOracdcSchemas = false;
	private OraCdcPseudoColumnsProcessor pseudoColumns;
	private final Set<Integer> setColumns = new HashSet<>();
	private Set<Integer> lobColumnIds;
	private boolean hasEncryptedColumns = false;
	private OraCdcTdeColumnDecrypter decrypter;
	private boolean allUpdates = true;
	private OraCdcSourceConnectorConfig config;
	private SchemaNameMapper snm;
	private Set<String> pkColsSet;
	private boolean printUnableToMapColIdWarning;
	private short conId;


	/**
	 * 
	 * @param pdbName      PDB name
	 * @param tableOwner   owner
	 * @param tableName    name
	 * @param schemaType   type of schema
	 * @param processLobs  true for LOB support
	 * @param transformLobs
	 */
	private OraTable4LogMiner(
			final String pdbName, final String tableOwner, final String tableName,
			final int schemaType, final boolean processLobs,
			final OraCdcLobTransformationsIntf transformLobs, final boolean logMiner) {
		super(tableOwner, tableName, schemaType);
		if (logMiner) {
			this.idToNameMap = new HashMap<>();
			this.pureIdMap = null;
		} else {
			this.idToNameMap = null;
			this.pureIdMap = new HashMap<>();
		}
		this.pdbName = pdbName;
		this.tableFqn = ((pdbName == null) ? "" : pdbName + ":") +
				this.tableOwner + "." + this.tableName;
		this.processLobs = processLobs;
		this.transformLobs = transformLobs;
	}

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
		this(pdbName, tableOwner, tableName, config.schemaType(),
				config.processLobs(), config.transformLobsImpl(), config.logMiner());
		setTopicDecoderPartition(config, rdbmsInfo.odd(), rdbmsInfo.partition());
		this.conId = conId;
		this.tableWithPk = true;
		this.setRowLevelScn(rowLevelScnDependency);
		this.rdbmsInfo = rdbmsInfo;
		this.topicPartition = config.topicPartition();
		this.printInvalidHexValueWarning = config.isPrintInvalidHexValueWarning();
		this.incompleteDataTolerance = config.getIncompleteDataTolerance();
		this.useAllColsOnDelete = config.useAllColsOnDelete();
		this.printUnableToDeleteWarning = config.printUnableToDeleteWarning();
		this.useOracdcSchemas = config.useOracdcSchemas();
		this.pseudoColumns = config.pseudoColumnsProcessor();
		this.allUpdates = config.allUpdates();
		this.config = config;
		this.version = version;
		this.printUnableToMapColIdWarning = config.printUnable2MapColIdWarning();
		final boolean isCdb = rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed();
		snm = config.getSchemaNameMapper();
		snm.configure(config);
		try {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Preparing column list and mining SQL statements for table {}.", tableFqn);
			}
			if (rdbmsInfo.isCheckSupplementalLogData4Table()) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Need to check supplemental logging settings for table {}.", tableFqn);
				}
				checkSupplementalLogData = OraRdbmsInfo.supplementalLoggingSet(connection,
						isCdb ? conId : -1, this.tableOwner, this.tableName);
				if (!checkSupplementalLogData) {
					LOGGER.error(
							"""
							
							=====================
							Supplemental logging for table '{}' is not configured correctly!
							Please set it according to the oracdc documentation
							=====================
							
							""",
								tableFqn);
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

	private void printUndroppedColumnsMessage(
			final List<Pair<Integer, String>> undroppedColumns,
			final int minUndroppedId) {
		final StringBuilder sb = new StringBuilder(0x800);
		sb
			.append("\n=====================\n")
			.append("The ")
			.append(tableFqn)
			.append(" table contains columns that are not dropped completely:");
		for (final Pair<Integer, String> pair : undroppedColumns) {
			sb
				.append("\n\t")
				.append(pair.getRight())
				.append(", INTERNAL_COLUMN_ID=")
				.append(pair.getLeft());
		}
		List<String> affected = new ArrayList<>();
		for (final OraColumn oraColumn : allColumns) {
			if (oraColumn.getColumnId() >= minUndroppedId) {
				affected.add(oraColumn.getColumnName());
			}
		}
		final boolean warning = affected.size() == 0;
		if (!warning) {
			sb.append("\n\nThe presence of these, not completely dropped, columns may lead to incorrect results for the columns:");
			for (final String columnName : affected) {
				sb
					.append("\n\t")
					.append(columnName);
			}
		}
		affected.clear();
		affected = null;
		sb
			.append("\n\nTo resolve this issue we recommend the following steps")
			.append("\n1) Stop the connector")
			.append("\n2) Run as table owner or DBA")
			.append("\n\t ALTER TABLE ")
			.append(tableOwner)
			.append(".")
			.append(tableName)
			.append(" DROP UNUSED COLUMNS;")
			.append("\n3) Restart the connector")
			.append("\n=====================\n");		
		if (warning)
			LOGGER.warn(sb.toString());
		else
			LOGGER.error(sb.toString());
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
		this((String) tableData.get("pdbName"),
				(String) tableData.get("tableOwner"),
				(String) tableData.get("tableName"),
				schemaType, (boolean) tableData.get("processLobs"),
				transformLobs, true);
		tableWithPk = (boolean) tableData.get("tableWithPk");
		this.rdbmsInfo = rdbmsInfo;
		final Boolean rowLevelScnDependency = (Boolean) tableData.get("rowLevelScn");
		if (rowLevelScnDependency == null || !rowLevelScnDependency) {
			this.setRowLevelScn(false);
		} else {
			this.setRowLevelScn(true);
		}
		if (LOGGER.isDebugEnabled()) {
			if (pdbName == null) {
				LOGGER.debug("Deserializing {}.{} from JSON", tableOwner, tableName);
			} else {
				LOGGER.debug("Deserializing {}:{}.{} from JSON", pdbName, tableOwner, tableName);
			}
		}

		// Schema init
		final SchemaBuilder keySchemaBuilder = keySchemaBuilder();
		final SchemaBuilder valueSchemaBuilder = valueSchemaBuilder(version);

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

	SourceRecord parseRedoRecord(
			final OraCdcLogMinerStatement stmt,
			final List<OraCdcLargeObjectHolder> lobs,
			final OraCdcTransaction transaction,
			final Map<String, Object> offset,
			final Connection connection) throws SQLException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("BEGIN: parseRedoRecord()");
		}
		final String xid = transaction.getXid();
		final long commitScn = transaction.getCommitScn();
		final Struct keyStruct;
		if (onlyValue) {
			keyStruct = null;
		} else {
			keyStruct = new Struct(keySchema);
		}
		final Struct valueStruct = new Struct(valueSchema);
		boolean skipRedoRecord = false;
		List<OraColumn> missedColumns = null;

		if (LOGGER.isTraceEnabled()) {
			printErrorMessage(
					Level.TRACE,
					"Parsing REDO record for table {}\nRedo record information:\n",
					stmt, xid, commitScn);
		}
		if (!tableWithPk && keyStruct != null) {
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
									printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
									throw de;
								} else if (incompleteDataTolerance == INCOMPLETE_REDO_INT_SKIP) {
									printSkippingRedoRecordMessage(stmt, xid, commitScn);
									return null;
								} else {
									//INCOMPLETE_REDO_INT_RESTORE
									if (missedColumns == null) {
										missedColumns = new ArrayList<>();
									}
									missedColumns.add(oraColumn);
								}
							} else {
								printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
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
										parseRedoRecordValues(
												oraColumn, columnValue,
												keyStruct, valueStruct);
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
											printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
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
							printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
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
			if (tableWithPk || pseudoKey) {
				final int whereClauseStart = Strings.CS.indexOf(stmt.getSqlRedo(), SQL_REDO_WHERE);
				String[] whereClause = StringUtils.splitByWholeSeparator(
						StringUtils.substring(stmt.getSqlRedo(), whereClauseStart + 7), SQL_REDO_AND);
				for (int i = 0; i < whereClause.length; i++) {
					final String currentExpr = StringUtils.trim(whereClause[i]);
					if (useAllColsOnDelete) {
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
								printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
								throw de;
							}
						} else {
							columnName = StringUtils.trim(StringUtils.substringBefore(currentExpr, "="));
							final OraColumn oraColumn = idToNameMap.get(columnName);
							if (oraColumn != null) {
								// Column can be excluded
								final String columnValue = StringUtils.trim(StringUtils.substringAfter(currentExpr, "="));
								try {
									parseRedoRecordValues(oraColumn,
											columnValue, keyStruct, valueStruct);
									if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
										mandatoryColumnsProcessed++;
									}
								} catch (DataException de) {
									LOGGER.error("Invalid value {} for column {} in table {}",
											columnValue, oraColumn.getColumnName(), tableFqn);
									printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
									throw new DataException(de);
								} catch (SQLException sqle) {
									if (oraColumn.isNullable()) {
										printToLogInvalidHexValueWarning(
												columnValue, oraColumn.getColumnName(), stmt);
									} else {
										LOGGER.error("Invalid value {} for column {} in table {}",
												columnValue, oraColumn.getColumnName(), tableFqn);
										printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
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
								parseRedoRecordValues(
										idToNameMap.get(columnName),
										StringUtils.trim(StringUtils.substringAfter(currentExpr, "=")),
										keyStruct, valueStruct);
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
											stmt, xid, commitScn);
									skipRedoRecord = true;
								} else if (whereClause.length == 1) {
									printErrorMessage(
											Level.ERROR,
											"Unknown error while parsing delete record for table {}.\nRedo record information:\n",
											stmt, xid, commitScn);
									skipRedoRecord = true;
								}
							}
						}
					}
				}
			} else if (onlyValue) {
				// skip delete operation only when schema don't have key
				skipRedoRecord = true;
				if (printUnableToDeleteWarning) {
					LOGGER.warn(
							"\n" +
							"=====================\n" +
							"Unable to perform delete operation on table {}, SCN={}, RBA='{}', ROWID='{}' without primary key!\n" +
							"SQL_REDO:\n\t{}\n" +
							"=====================\n",
							tableFqn, stmt.getScn(), stmt.getRba(), stmt.getRowId(), stmt.getSqlRedo());
				}
			}
		} else if (stmt.getOperation() == UPDATE) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing UPDATE");
			}
			opType = 'u';
			final Set<String> setColumns = new HashSet<>();
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
								printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
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
										oraColumn, columnValue,
										keyStruct, valueStruct);
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
									printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
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
					printErrorMessage(Level.ERROR,  message + "Exiting!\n", stmt, xid, commitScn);
					throw new ConnectException("Incomplete redo record!");
				} else if (incompleteDataTolerance == INCOMPLETE_REDO_INT_SKIP) {
					final String message = 
							"Missed where clause in UPDATE record for the table {}.\n";
					printErrorMessage(Level.WARN,  message, stmt, xid, commitScn);
					skipRedoRecord = true;
				} else {
					// OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_RESTORE
					if (printSqlForMissedWhereInUpdate) {
						LOGGER.info(
								"\n" +
								"=====================\n" +
								"{}\n" +
								"Will be used to handle UPDATE statements without WHERE clause for table {}.\n" +
								"=====================\n",
								sqlGetKeysUsingRowId, tableFqn);
						printSqlForMissedWhereInUpdate = false;
					}
					LOGGER.warn(
							"\n" +
							"=====================\n" +
							"UPDATE statement without WHERE clause for table {} at SCN='{}', RS_ID='{}', ROLLBACK='{}' for ROWID='{}'.\n" +
							"We will try to get primary key values from table {} at ROWID='{}'.\n" +
							"=====================\n",
							tableFqn, stmt.getScn(), stmt.getRba(), stmt.isRollback(), stmt.getRowId(), tableFqn, stmt.getRowId());
					getMissedColumnValues(connection, stmt.getRowId(), keyStruct);
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
											LOGGER.warn(
													"\n=====================\n" +
													"Substituting NULL value for column {}, table {} with DEFAULT value {}\n" +
													"SCN={}, RBA='{}', SQL_REDO:\n\t{}\n" +
													"=====================\n",
													oraColumn.getColumnName(), this.tableFqn, columnDefaultValue,
													stmt.getScn(), stmt.getRba(), stmt.getSqlRedo());
											valueStruct.put(oraColumn.getColumnName(), columnDefaultValue);
											if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
												mandatoryColumnsProcessed++;
											}
											throwDataException = false;
										}
									} else {
										LOGGER.error(
												"\n=====================\n" +
												"'{}' while setting '{}' to NULL in valueStruct in table {}.\n" +
												"Please check that '{}' is NULLABLE and not member of keyStruct.\n" +
												"=====================\n",
												de.getMessage(), oraColumn.getColumnName(),
												this.tableFqn, oraColumn.getColumnName());
									}
									if (throwDataException) {
										if (incompleteDataTolerance == INCOMPLETE_REDO_INT_ERROR) {
											printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
											throw de;
										} else if (incompleteDataTolerance == INCOMPLETE_REDO_INT_SKIP) {
											printSkippingRedoRecordMessage(stmt, xid, commitScn);
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
									parseRedoRecordValues(
										oraColumn, columnValue,
										keyStruct, valueStruct);
									if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
										mandatoryColumnsProcessed++;
									}
								} catch (DataException de) {
									LOGGER.error("Invalid value {} for column {} in table {}",
											columnValue, oraColumn.getColumnName(), tableFqn);
									printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
									throw new DataException(de);
								} catch (SQLException sqle) {
									if (oraColumn.isNullable()) {
										printToLogInvalidHexValueWarning(
												columnValue, oraColumn.getColumnName(), stmt);
									} else {
										LOGGER.error("Invalid value {} for column {} in table {}",
												columnValue, oraColumn.getColumnName(), tableFqn);
										printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
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
						parseRedoRecordValues(
								idToNameMap.get(columnName),
								StringUtils.trim(StringUtils.substringAfter(currentExpr, "=")),
								keyStruct, valueStruct);
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
							columnName, stmt, xid, commitScn);
					throw new DataException(
							"Can't detect column with name " + columnName + " during parsing!");
				}
			}
		} else {
			// We expect here only 1,2,3 as valid values for OPERATION_CODE (and 68 for special cases)
			printErrorMessage(
					Level.ERROR,
					"Corrupted record for table {} found!!!\nPlease send e-mail to oracle@a2.solutions with record details below:\n",
					stmt, xid, commitScn);
			throw new SQLException("Unknown OPERATION_CODE while parsing redo record!");
		}

		if (processLobs &&
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

		if (incompleteDataTolerance == OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_RESTORE &&
				missedColumns != null) {
			if (getMissedColumnValues(connection, stmt, missedColumns, keyStruct, valueStruct, xid, commitScn)) {
				printRedoRecordRecoveredMessage(stmt, xid, commitScn);
			} else {
				printSkippingRedoRecordMessage(stmt, xid, commitScn);
			}
		}

		SourceRecord sourceRecord = null;
		if (!skipRedoRecord) {
			if (mandatoryColumnsProcessed < mandatoryColumnsCount) {
				if (opType != 'd') {
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug(
								"Mandatory columns count for table {} is {}, but only {} mandatory columns are returned from redo record!",
								tableFqn, mandatoryColumnsCount, mandatoryColumnsProcessed);
					}
					final String message = 
							"Mandatory columns count for table {} is " +
							mandatoryColumnsCount +
							" but only " +
							mandatoryColumnsProcessed +
							" mandatory columns are returned from the redo record!\n" +
							"Please check supplemental logging settings!\n";
					if (incompleteDataTolerance == INCOMPLETE_REDO_INT_ERROR) {
						printErrorMessage(Level.ERROR,  message + "Exiting!\n", stmt, xid, commitScn);
						throw new ConnectException("Incomplete redo record!");
					} else {
						printErrorMessage(Level.ERROR,  message + "Skipping!\n", stmt, xid, commitScn);
						return null;
					}
				} else if (!pseudoKey) {
					// With ROWID we does not need more checks...
					//TODO - logic for delete only with primary columns!
					//TODO
					//TODO - logic for delete with all columns
				}
			}

			if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
				final Struct struct = new Struct(schema);
				final Struct source = rdbmsInfo.getStruct(
						stmt.getSqlRedo(),
						pdbName, tableOwner, tableName,
						stmt.getScn(), stmt.getTs(),
						xid, commitScn, stmt.getRowId().toString());
				struct.put("source", source);
				struct.put("before", keyStruct);
				if (stmt.getOperation() != DELETE ||
						((stmt.getOperation() == DELETE) && useAllColsOnDelete)) {
					struct.put("after", valueStruct);
				}
				struct.put("op", String.valueOf(opType));
				struct.put("ts_ms", System.currentTimeMillis());
				sourceRecord = new SourceRecord(
						sourcePartition,
						offset,
						kafkaTopic,
						schema,
						struct);
			} else {
				if (!(opType == 'd' && (!useAllColsOnDelete))) {
					//TODO
					//TODO Beter handling for 'debezium'-like schemas are required for this case...
					//TODO
					pseudoColumns.addToStruct(valueStruct, stmt, transaction);
				}
				if (onlyValue) {
					sourceRecord = new SourceRecord(
							sourcePartition,
							offset,
							kafkaTopic,
							topicPartition,
							valueSchema,
							valueStruct);
				} else {
					if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_KAFKA_STD) {
						if (stmt.getOperation() == DELETE &&
								(!useAllColsOnDelete)) {
							sourceRecord = new SourceRecord(
									sourcePartition,
									offset,
									kafkaTopic,
									topicPartition,
									keySchema,
									keyStruct,
									null,
									null);
						} else {
							sourceRecord = new SourceRecord(
								sourcePartition,
								offset,
								kafkaTopic,
								topicPartition,
								keySchema,
								keyStruct,
								valueSchema,
								valueStruct);
						}
					}
				}
				if (sourceRecord != null) {
					sourceRecord.headers().addString("op", String.valueOf(opType));
				}
			}
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("END: parseRedoRecord()");
		}
		return sourceRecord;
	}

	private void parseRedoRecordValues(
			final OraColumn oraColumn, final String hexValue,
			final Struct keyStruct, final Struct valueStruct) throws SQLException {
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
		if (onlyValue) {
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

	public String getPdbName() {
		return pdbName;
	}

	public void setPdbName(String pdbName) {
		this.pdbName = pdbName;
	}

	public boolean isTableWithPk() {
		return tableWithPk;
	}

	public void setTableWithPk(boolean tableWithPk) {
		this.tableWithPk = tableWithPk;
	}

	public boolean isProcessLobs() {
		return processLobs;
	}

	public void setProcessLobs(boolean processLobs) {
		this.processLobs = processLobs;
	}

	public int getMaxColumnId() {
		return maxColumnId;
	}

	public void setMaxColumnId(int maxColumnId) {
		this.maxColumnId = maxColumnId;
	}

	public OraColumn getLobColumn(final long lobObjectId, final PreparedStatement psCheckLob) throws SQLException {
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

	public void setTopicDecoderPartition(final OraCdcSourceConnectorConfig config,
			final OraDumpDecoder odd, final Map<String, String> sourcePartition) {
		final TopicNameMapper topicNameMapper = config.getTopicNameMapper();
		topicNameMapper.configure(config);
		this.kafkaTopic = topicNameMapper.getTopicName(pdbName, tableOwner, tableName);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(
					"Kafka topic for table {} set to {}.",
					tableFqn, this.kafkaTopic);
		}
		this.odd = odd;
		this.sourcePartition = sourcePartition;
	}

	public String fqn() {
		return tableFqn;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public boolean isWithLobs() {
		return withLobs;
	}

	public int processDdl(final Connection connection,
			final OraCdcStatementBase stmt,
			final String xid,
			final long commitScn) throws SQLException {
		final String preprocessedDdl;
		final boolean logMiner;
		if (stmt instanceof OraCdcLogMinerStatement) {
			preprocessedDdl = ((OraCdcLogMinerStatement) stmt).getSqlRedo();
			logMiner = true;
		} else {
			preprocessedDdl = alterTablePreProcessor(new String(((OraCdcRedoMinerStatement) stmt).redoData()));
			logMiner = false;
		}
		int updatedColumnCount = 0;
		final String[] ddlDataArray = StringUtils.split(preprocessedDdl, OraSqlUtils.DELIMITER);
		final String operation = ddlDataArray[0];
		final String preProcessed = ddlDataArray[1];
		final String originalDdl;
		if (ddlDataArray.length > 2)
			originalDdl = ddlDataArray[2];
		else
			originalDdl = "N/A";
		boolean rebuildSchemaFromDb = false;
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("BEGIN: Processing DDL for table {}:\n\t'{}'\n\t'{}'",
					tableFqn, originalDdl, preProcessed);
		}
		switch (operation) {
		case OraSqlUtils.ALTER_TABLE_COLUMN_ADD:
			for (String columnDefinition : StringUtils.split(preProcessed, ";")) {
				String newColumnName = StringUtils.split(columnDefinition)[0];
				newColumnName = OraColumn.canonicalColumnName(newColumnName);
				boolean alreadyExist = false;
				for (OraColumn column : allColumns) {
					if (Strings.CS.equals(newColumnName, column.getColumnName())) {
						alreadyExist = true;
						break;
					}
				}
				if (alreadyExist) {
					LOGGER.warn(
							"Ignoring DDL statement\n\t'{}'\n for adding column {} to table {} since this column already present in table definition",
							originalDdl, newColumnName, tableFqn);
				} else {
					rebuildSchemaFromDb = true;
				}
			}
			break;
		case OraSqlUtils.ALTER_TABLE_COLUMN_DROP:
			final String[] columnNamesToDrop = StringUtils.split(preProcessed, ";");
			final List<OraColumn> unusedColumns = new ArrayList<>();
			final Set<Integer> unusedColumnIndexes = new HashSet<>();
			for (int i = 0; i < columnNamesToDrop.length; i++) {
				unusedColumnIndexes.add(i);
				final String columnToDrop = OraColumn.canonicalColumnName(columnNamesToDrop[i]);
				if (pkColumns.containsKey(columnToDrop)) {
					LOGGER.error(
							"\n=====================\n" +
							"Automatic DROP of a column '{}' included in the key for table '{}' is not supported!\n" +
							"Please do this manually. For further support, please contact us at oracle@a2.solutions." +
							"\n=====================\n",
							columnToDrop, tableFqn);
					throw new ConnectException("Automatic DROP of a column included in the key for table is not supported.");
				}
				for (OraColumn column : allColumns)
					if (Strings.CS.equals(columnToDrop, column.getColumnName())) {
						unusedColumns.add(column);
						unusedColumnIndexes.remove(i);
						break;
					}
			}
			if (!unusedColumnIndexes.isEmpty()) {
				boolean first = true;
				final StringBuilder sb = new StringBuilder(0x40);
				for (final int index : unusedColumnIndexes) {
					if (first)
						first = false;
					else
						sb.append(", ");
					sb.append(columnNamesToDrop[index]);
				}
				LOGGER.error(
						"\n=====================\n" +
						"Cannot drop column(s) '{}' because this column(s) {} not present in the oracdc dictionary for table {}.\n" +
						"Original DDL text: '{}'" +
						"\n=====================\n",
				sb.toString(), unusedColumnIndexes.size() == 1 ? "is" : "are", tableFqn, originalDdl);
			}
			if (!unusedColumns.isEmpty()) {
				final Comparator<OraColumn> comparator = new Comparator<OraColumn>() {
					public int compare(OraColumn col1, OraColumn col2){
						return col1.getColumnId() - col2.getColumnId();
					}
				};
				Collections.sort(unusedColumns, comparator);
				Collections.sort(allColumns, comparator);
				for (final OraColumn unusedColumn : unusedColumns) {
					int indexToRemove = -1;
					final String unusedColName = unusedColumn.getColumnName();
					final int unusedColId = unusedColumn.getColumnId();
					for (int i = 0; i < allColumns.size(); i++) {
						final OraColumn column = allColumns.get(i);
						if (column.getColumnId() == unusedColumn.getColumnId()) {
							indexToRemove = i;
							if (withLobs) {
								if (lobColumnsNames != null)
									lobColumnsNames.remove(unusedColName);
								if (lobColumnSchemas != null)
									lobColumnSchemas.remove(unusedColName);
							}
							if (logMiner) {
								idToNameMap.remove(unusedColName);
							} else {
								pureIdMap.remove(unusedColId);
								if (withLobs && lobColumnIds != null)
									lobColumnIds.remove(unusedColId);
							}
						} else if (column.getColumnId() > unusedColumn.getColumnId()) {
							if (!logMiner) {
								final int oldColumnId = column.getColumnId();
								pureIdMap.put(oldColumnId - 1, column);
								if (withLobs && lobColumnIds != null && lobColumnIds.contains(oldColumnId)) {
									lobColumnIds.remove(oldColumnId);
									lobColumnIds.add(oldColumnId - 1);
								}
							}
							column.setColumnId(column.getColumnId() - 1);
						}
					}
					OraColumn columnToRemove = allColumns.get(indexToRemove);
					if (!columnToRemove.isNullable())
						mandatoryColumnsProcessed--;
					allColumns.remove(indexToRemove);
					columnToRemove = null;
				}
				final SchemaBuilder valueSchemaBuilder = valueSchemaBuilder(++version);
				for (final Field field : valueSchema.fields()) {
					final boolean present = allColumns
								.stream()
								.filter(c -> Strings.CS.equals(c.getColumnName(), field.name()))
								.findFirst()
								.isPresent();
					if (present)
						valueSchemaBuilder.field(field.name(), field.schema());
				}
				addPseudoColumns(valueSchemaBuilder);
				schemaEiplogue(tableFqn, valueSchemaBuilder);
				maxColumnId = allColumns.size();
				updatedColumnCount = unusedColumns.size();
			}
			break;
		case OraSqlUtils.ALTER_TABLE_COLUMN_MODIFY:
			for (String columnDefinition : StringUtils.split(preProcessed, ";")) {
				String changedColumnName = StringUtils.split(columnDefinition)[0];
				changedColumnName = OraColumn.canonicalColumnName(changedColumnName);
				int columnIndex = -1;
				for (int i = 0; i < allColumns.size(); i++) {
					if (Strings.CS.equals(changedColumnName, allColumns.get(i).getColumnName())) {
						columnIndex = i;
						break;
					}
				}
				if (columnIndex < 0) {
					LOGGER.warn(
							"Ignoring DDL statement\n\t'{}'\n for modifying column {} in table {} since this column not exists in table definition",
							originalDdl, changedColumnName, tableFqn);
				} else {
					rebuildSchemaFromDb = true;
				}
			}
			break;
		case OraSqlUtils.ALTER_TABLE_COLUMN_RENAME:
			final String[] namesArray = StringUtils.split(preProcessed, ";");
			final String oldName = OraColumn.canonicalColumnName(namesArray[0]);
			final String newName = OraColumn.canonicalColumnName(namesArray[1]);
			boolean newNamePresent = false;
			int columnIndex = -1;
			for (int i = 0; i < allColumns.size(); i++) {
				final String columnName = allColumns.get(i).getColumnName();
				if ((columnIndex < 0) && Strings.CS.equals(oldName, columnName)) {
					columnIndex = i;
				}
				if (!newNamePresent && Strings.CS.equals(newName, columnName)) {
					newNamePresent = true;
				}
			}
			if (newNamePresent) {
				LOGGER.error(
						"\n=====================\n" +
						"Unable to parse\n'{}'\nColumn {} already exists in {}!" +
						"\n=====================\n",
						originalDdl, newName, tableFqn);
			} else {
				if (columnIndex < 0) {
					LOGGER.error(
						"\n=====================\n" +
						"Unable to parse\n'{}'\nColumn {} not exists in {}!" +
						"\n=====================\n",
						originalDdl, oldName, tableFqn);
				} else {
					allColumns.get(columnIndex).setColumnName(newName);
					final SchemaBuilder valueSchemaBuilder = valueSchemaBuilder(++version);
					for (final Field field : valueSchema.fields()) {
						if (Strings.CS.equals(oldName, field.name()))
							valueSchemaBuilder.field(newName, field.schema());
						else
							valueSchemaBuilder.field(field.name(), field.schema());
					}
					addPseudoColumns(valueSchemaBuilder);
					schemaEiplogue(tableFqn, valueSchemaBuilder);
					updatedColumnCount = 1;
				}
			}
			break;
		}

		if (rebuildSchemaFromDb) {
			if (allColumns != null)
				allColumns.clear();
			if (pkColumns != null)
				pkColumns.clear();
			if (idToNameMap != null)
				idToNameMap.clear();
			if (pureIdMap != null)
				pureIdMap.clear();
			if (withLobs) {
				if (lobColumnsNames != null)
					lobColumnsNames.clear();
				if (lobColumnsObjectIds != null)
					lobColumnsObjectIds.clear();
				if (lobColumnSchemas != null)
					lobColumnSchemas.clear();
				if (lobColumnIds != null)
					lobColumnIds.clear();
			}

			mandatoryColumnsCount = 0;
			updatedColumnCount = 1;
			version++;
			readAndParseOraColumns(connection, rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed());

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("END: Processing DDL for OraTable {} using dictionary  data...", tableFqn);
			}
		}
		return updatedColumnCount;
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

	private void printSkippingRedoRecordMessage(
			final OraCdcStatementBase stmt,final String xid, final long commitScn) {
		printErrorMessage(Level.WARN, "Skipping incomplete redo record for table {}!", stmt, xid, commitScn);
	}

	private void printRedoRecordRecoveredMessage(
			final OraCdcStatementBase stmt,final String xid, final long commitScn) {
		printErrorMessage(Level.INFO, "Incomplete redo record restored from latest incarnation for table {}.", stmt, xid, commitScn);
	}

	private void printInvalidFieldValue(final OraColumn oraColumn,
			final OraCdcStatementBase stmt,final String xid, final long commitScn) {
		if (oraColumn.isNullable()) {
			printErrorMessage(Level.ERROR,
					"Redo record information for table {}:\n",
					stmt, xid, commitScn);
		} else {
			printErrorMessage(Level.ERROR,
					"NULL value for NON NULL column {}!\nRedo record information for table {}:\n",
					oraColumn.getColumnName(), stmt, xid, commitScn);
		}
	}

	private void printErrorMessage(final Level level, final String message,
			final OraCdcStatementBase stmt, final String xid, final long commitScn) {
		printErrorMessage(level, message, null, stmt, xid, commitScn);
	}

	private void printErrorMessage(final Level level, final String message, final String columnName,
			final OraCdcStatementBase stmt, final String xid, final long commitScn) {
		final StringBuilder sb = new StringBuilder(256);
		sb
			.append("\n=====================\n")
			.append(message)
			.append("\n\tCOMMIT_SCN = {}\n")
			.append("\tXID = {}\n")
			.append(stmt.toStringBuilder())
			.append("=====================\n");
		if (level == Level.ERROR) {
			if (columnName == null) {
				LOGGER.error(sb.toString(), tableFqn, commitScn, xid);
			} else {
				LOGGER.error(sb.toString(), columnName, tableFqn, commitScn, xid);
				
			}
		} else if (level == Level.WARN) {
			LOGGER.warn(sb.toString(), tableFqn, commitScn, xid);
		} else if (level == Level.INFO) {
			LOGGER.info(sb.toString(), tableFqn, commitScn, xid);
		} else {
			LOGGER.trace(sb.toString(), tableFqn, commitScn, xid);
		}
	}

	public boolean isCheckSupplementalLogData() {
		return checkSupplementalLogData;
	}

	private void getMissedColumnValues(final Connection connection, final RowId rowId, final Struct keyStruct) throws SQLException {
		try {
			if (rdbmsInfo.isCdb() && rdbmsInfo.isCdbRoot()) {
				alterSessionSetContainer(connection, pdbName);
			}
			final PreparedStatement statement = connection .prepareStatement(sqlGetKeysUsingRowId,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.setString(1, rowId.toString());
			final ResultSet resultSet = statement.executeQuery();
			if (resultSet.next()) {
				for (final Map.Entry<String, OraColumn> entry : pkColumns.entrySet()) {
					final OraColumn oraColumn = entry.getValue();
					oraColumn.setValueFromResultSet(keyStruct, resultSet);
				}
			} else {
				LOGGER.error(
						"\n" +
						"=====================\n" +
						"Unable to find row in table {} with ROWID '{}'!\n" +
						"=====================\n",
						tableFqn, rowId);
			}
		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == OraRdbmsInfo.ORA_942) {
				// ORA-00942: table or view does not exist
				LOGGER.error(
						"\n" +
						"=====================\n" +
						"Please run as SYSDBA:\n" +
						"\tgrant select on {} to {};\n" +
						"And restart connector!\n" +
						"=====================\n",
						tableFqn, connection.getSchema());
			}
			throw sqle;
		} finally {
			if (rdbmsInfo.isCdb() && rdbmsInfo.isCdbRoot()) {
				alterSessionSetContainer(connection, OraRdbmsInfo.CDB_ROOT);
			}
		}
	}

	private boolean getMissedColumnValues(
			final Connection connection, final OraCdcStatementBase stmt,
			final List<OraColumn> missedColumns, final Struct keyStruct, final Struct valueStruct,
			String xid, long commitScn) throws SQLException {
		boolean result = false;
		if (stmt.getOperation() == UPDATE ||
				stmt.getOperation() == INSERT) {
			final StringBuilder readData = new StringBuilder(128);
			readData.append("select ");
			boolean firstColumn = true;
			for (final OraColumn oraColumn : missedColumns) {
				if (firstColumn) {
					firstColumn = false;
				} else {
					readData.append(", ");
				}
				readData
					.append("\"")
					.append(oraColumn.getOracleName())
					.append("\"");
			}
			// Special processing based on case with
			// <a href="https://etrm.live/etrm-12.2.2/etrm.oracle.com/pls/trm1222/etrm_pnav57bb.html?c_name=HR_ALL_ORGANIZATION_UNITS&c_owner=HR&c_type=TABLE">HR.HR_ALL_ORGANIZATION_UNITS</a>
			// and 19.13 LogMiner - we need to restore latest incarnation of text data too...
			if (stmt.getOperation() == UPDATE) {
				for (final OraColumn oraColumn : allColumns) {
					if (!pkColumns.containsKey(oraColumn.getColumnName()) &&
							!oraColumn.isNullable() &&
							oraColumn.getJdbcType() == VARCHAR) {
						readData
							.append(", \"")
							.append(oraColumn.getOracleName())
							.append("\"");
					}
				}
			}
			readData
				.append("\nfrom ")
				.append(tableOwner)
				.append(".")
				.append(tableName)
				.append("\nwhere ");
			if (stmt.getOperation() == UPDATE) {
				readData.append("ROWID = CHARTOROWID(?)");
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(
							"The following statement will be used to get missed data from {} using ROWID {}:\n{}\n",
							tableFqn, stmt.getRowId(), readData.toString());
				}
			} else {
				//OraCdcV$LogmnrContents.INSERT
				firstColumn = true;
				for (final String pkColumnName : pkColumns.keySet()) {
					if (firstColumn) {
						firstColumn = false;
					} else {
						readData.append(", ");
					}
					readData
						.append(pkColumnName)
						.append(" = ?");
				}
				LOGGER.error(
						"\n" +
						"=====================\n" +
						"Missed non-null values for columns in table {} at SCN={}, RS_ID()RBA=' {} '!\n" +
						"Please check supplemental logging settings for table {}!\n" +
						"=====================\n",
						tableFqn, stmt.getScn(), stmt.getRba(), tableFqn);
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(
							"The following statement will be used to get missed data from {} using PK values:\n{}\n",
							tableFqn, readData.toString());
				}
			}

			if (rdbmsInfo.isCdb() && rdbmsInfo.isCdbRoot()) {
				alterSessionSetContainer(connection, pdbName);
			}
			try {
				final PreparedStatement statement = connection .prepareStatement(readData.toString(),
						ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				if (stmt.getOperation() == UPDATE) {
					statement.setString(1, stmt.getRowId().toString());
				} else {
					//OraCdcV$LogmnrContents.INSERT
					int bindNo = 0;
					for (final String pkColumnName : pkColumns.keySet()) {
						pkColumns.get(pkColumnName)
							.bindWithPrepStmt(statement, ++bindNo, keyStruct.get(pkColumnName));
					}
				}
				final ResultSet resultSet = statement.executeQuery();
				if (resultSet.next()) {
					for (OraColumn oraColumn : missedColumns) {
						if (oraColumn.setValueFromResultSet(valueStruct, resultSet)) {
							if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
								mandatoryColumnsProcessed++;
							}
						}
					}
					result = true;
				} else {
					LOGGER.error(
							"\n" +
							"=====================\n" +
							"Unable to find row in table {} with ROWID '{}' using SQL statement\n{}\n" +
							"=====================\n",
							tableFqn, stmt.getRowId(), readData.toString());
				}
			} catch (SQLException sqle) {
				if (sqle.getErrorCode() == OraRdbmsInfo.ORA_942) {
					// ORA-00942: table or view does not exist
					LOGGER.error(
							"\n" +
							"=====================\n" +
							"Please run as SYSDBA:\n" +
							"\tgrant select on {} to {};\n" +
							"And restart connector!\n" +
							"=====================\n",
							tableFqn, connection.getSchema());
				} else {
					printErrorMessage(Level.ERROR,
							"Unable to restore row! Redo record information for table {}:\n",
							stmt, xid, commitScn);
				}
				throw sqle;
			}
			if (rdbmsInfo.isCdb() && rdbmsInfo.isCdbRoot()) {
				alterSessionSetContainer(connection, OraRdbmsInfo.CDB_ROOT);
			}
		} else {
			LOGGER.error(
					"\n" +
					"=====================\n" +
					"Unable to restore redo record for operation {} on table {} at SCN = {}, RS_ID(RBA) = '{}'!\n" +
					"Only UPDATE (OPERATION_CODE=3) and INSERT (OPERATION_CODE=1) are supported!\n" +
					"=====================\n",
					stmt.getOperation(), tableFqn, stmt.getScn(), stmt.getRba());
		}
		return result;
	}

	private void printToLogInvalidHexValueWarning(
			final String columnValue,
			final String columnName,
			final OraCdcStatementBase stmt) {
		if (printInvalidHexValueWarning) {
			LOGGER.warn(
					"\n" +
					"=====================\n" +
					"Invalid HEX value \"{}\" for column {} in table {} at SCN={}, RBA='{}' is set to NULL\n" +
					"=====================\n",
					columnValue, columnName, tableFqn, stmt.getScn(), stmt.getRba());
		}
	}

	private void alterSessionSetContainer(final Connection connection, final String container)throws SQLException {
		Statement alterSession = connection.createStatement();
		alterSession.execute("alter session set CONTAINER=" + container);
		alterSession.close();
		alterSession = null;
	}

	SourceRecord parseRedoRecord(
			final OraCdcRedoMinerStatement stmt,
			final OraCdcTransaction transaction,
			final Set<LobId> lobIds,
			final Map<String, Object> offset,
			final Connection connection) throws SQLException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("BEGIN: parseRedoRecord()");
		}
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
		final String xid = transaction.getXid();
		final long commitScn = transaction.getCommitScn();
		final Struct keyStruct;
		if (onlyValue) {
			keyStruct = null;
		} else {
			keyStruct = new Struct(keySchema);
		}
		final Struct valueStruct = new Struct(valueSchema);
		boolean skipRedoRecord = false;
		List<OraColumn> missedColumns = null;

		if (LOGGER.isTraceEnabled()) {
			printErrorMessage(
					Level.TRACE,
					"Parsing REDO record for table {}\nRedo record information:\n",
					stmt, xid, commitScn);
		}
		if (!tableWithPk && keyStruct != null) {
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
									printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
									throw de;
								} else if (incompleteDataTolerance == INCOMPLETE_REDO_INT_SKIP) {
									printSkippingRedoRecordMessage(stmt, xid, commitScn);
									return null;
								} else {
									//INCOMPLETE_REDO_INT_RESTORE
									if (missedColumns == null) {
										missedColumns = new ArrayList<>();
									}
									missedColumns.add(oraColumn);
								}
							} else {
								printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
								throw de;
							}
						}
					} else {
						try {
							parseRedoRecordValues(
									oraColumn, redoData, colDefs[i][2], colSize,
									keyStruct, valueStruct, transaction, lobIds);
							if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
									mandatoryColumnsProcessed++;
							}
						} catch (DataException de) {
							LOGGER.error("Invalid value {} for column {} in table {}",
									rawToHex(Arrays.copyOfRange(redoData, colDefs[i][2], colDefs[i][2] + colSize)),
									oraColumn.getColumnName(), tableFqn);
							printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
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
								printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
								throw new SQLException(sqle);
							}
						}
					}
				} else if (!processLobs && lobColumnIds != null && lobColumnIds.contains(colDefs[i][0])) {
					if (LOGGER.isDebugEnabled())
						LOGGER.debug(
								"\n=====================\n" +
								"Unable to map column with id {} to dictionary for table {} in XID {}!\nDML operation details:\n{}\n" +
								"\n=====================\n",
								colDefs[i][0], tableFqn, xid, stmt.toString());
				} else
					printUnableToMapColIdWarning(colDefs[i][0], xid, stmt);
			}
		} else if (stmt.getOperation() == DELETE) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing DELETE");
			}
			opType = 'd';
			final int colCount = (redoData[0] << 8) | (redoData[1] & 0xFF);
			final int[][] colDefs = new int[colCount][3];
			stmt.readColDefs(colDefs, Short.BYTES);
			if (tableWithPk || pseudoKey) {
				for (int i = 0; i < colCount; i++) {
					final int colSize = colDefs[i][1];
					if (useAllColsOnDelete) {
						final OraColumn oraColumn = pureIdMap.get(colDefs[i][0]);
						if (oraColumn != null) {
							if (colSize < 0) {
								try {
									valueStruct.put(oraColumn.getColumnName(), null);
								} catch (DataException de) {
									printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
									throw de;
								}
							} else {
								try {
									parseRedoRecordValues(oraColumn, redoData, colDefs[i][2], colSize,
											keyStruct, valueStruct, transaction, lobIds);
									if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
										mandatoryColumnsProcessed++;
									}
								} catch (DataException de) {
									LOGGER.error("Invalid value {} for column {} in table {}",
											rawToHex(Arrays.copyOfRange(redoData, colDefs[i][2], colDefs[i][2] + colSize)),
											oraColumn.getColumnName(), tableFqn);
									printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
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
										printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
										throw new SQLException(sqle);
									}
								}
							}
						} else
							printUnableToMapColIdWarning(colDefs[i][0], xid, stmt);

					} else {
						if (colSize > -1) {
							// PK can't be null!!!
							final OraColumn oraColumn = pureIdMap.get(colDefs[i][0]);
							if (oraColumn != null) {
								if (oraColumn.isPartOfPk()) {
									parseRedoRecordValues(oraColumn, redoData, colDefs[i][2], colSize,
											keyStruct, valueStruct, transaction, lobIds);
									mandatoryColumnsProcessed++;
								}
							} else
								printUnableToMapColIdWarning(colDefs[i][0], xid, stmt);
						}
					}
				}
			} else if (onlyValue) {
				// skip delete operation only when schema don't have key
				skipRedoRecord = true;
				if (printUnableToDeleteWarning) {
					LOGGER.warn(
							"\n" +
							"=====================\n" +
							"Unable to perform delete operation on table {}, SCN={}, RBA='{}', ROWID='{}' without primary key!\n" +
							"SQL_REDO:\n\t{}\n" +
							"=====================\n",
							tableFqn, stmt.getScn(), stmt.getRba(), stmt.getRowId(), stmt.getSqlRedo());
				}
			}
		} else if (stmt.getOperation() == UPDATE) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing UPDATE");
			}
			if (!allUpdates && stmt.updateWithoutChanges()) {
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
								printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
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
							parseRedoRecordValues(
									oraColumn, redoData, setColDefs[i][2], colSize,
									keyStruct, valueStruct, transaction, lobIds);
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
								printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
								throw new SQLException(sqle);
							}
						}
					}
				} else
					printUnableToMapColIdWarning(setColDefs[i][0], xid, stmt);
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
											LOGGER.warn(
													"\n=====================\n" +
													"Substituting NULL value for column {}, table {} with DEFAULT value {}\n" +
													"SCN={}, RBA='{}', SQL_REDO:\n\t{}\n" +
													"=====================\n",
													oraColumn.getColumnName(), this.tableFqn, columnDefaultValue,
													stmt.getScn(), stmt.getRba(), stmt.getSqlRedo());
											valueStruct.put(oraColumn.getColumnName(), columnDefaultValue);
											if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
												mandatoryColumnsProcessed++;
											}
											throwDataException = false;
										}
									} else {
										LOGGER.error(
												"\n=====================\n" +
												"'{}' while setting '{}' to NULL in valueStruct in table {}.\n" +
												"Please check that '{}' is NULLABLE and not member of keyStruct.\n" +
												"=====================\n",
												de.getMessage(), oraColumn.getColumnName(),
												this.tableFqn, oraColumn.getColumnName());
									}
									if (throwDataException) {
										if (incompleteDataTolerance == INCOMPLETE_REDO_INT_ERROR) {
											printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
											throw de;
										} else if (incompleteDataTolerance == INCOMPLETE_REDO_INT_SKIP) {
											printSkippingRedoRecordMessage(stmt, xid, commitScn);
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
									parseRedoRecordValues(oraColumn, redoData, whereColDefs[i][2], colSize,
										keyStruct, valueStruct, transaction, lobIds);
									if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
										mandatoryColumnsProcessed++;
									}
								} catch (DataException de) {
									LOGGER.error("Invalid value {} for column {} in table {}",
											rawToHex(Arrays.copyOfRange(redoData, whereColDefs[i][2], whereColDefs[i][2] + colSize)),
											oraColumn.getColumnName(), tableFqn);
									printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
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
										printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
										throw new SQLException(sqle);
									}
								}
							}
						} else
							printUnableToMapColIdWarning(whereColDefs[i][0], xid, stmt);
					}
				}
			} else {
				if (incompleteDataTolerance == INCOMPLETE_REDO_INT_ERROR) {
					final String message = 
							"Missed where clause in UPDATE record for the table {}.\n";
					printErrorMessage(Level.ERROR,  message + "Exiting!\n", stmt, xid, commitScn);
					throw new ConnectException("Incomplete redo record!");
				} else if (incompleteDataTolerance == INCOMPLETE_REDO_INT_SKIP) {
					final String message = 
							"Missed where clause in UPDATE record for the table {}.\n";
					printErrorMessage(Level.WARN,  message, stmt, xid, commitScn);
					skipRedoRecord = true;
				} else {
					// OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_RESTORE
					if (printSqlForMissedWhereInUpdate) {
						LOGGER.info(
								"\n" +
								"=====================\n" +
								"{}\n" +
								"Will be used to handle UPDATE statements without WHERE clause for table {}.\n" +
								"=====================\n",
								sqlGetKeysUsingRowId, tableFqn);
						printSqlForMissedWhereInUpdate = false;
					}
					LOGGER.warn(
							"\n" +
							"=====================\n" +
							"UPDATE statement without WHERE clause for table {} at SCN='{}', RS_ID='{}', ROLLBACK='{}' for ROWID='{}'.\n" +
							"We will try to get primary key values from table {} at ROWID='{}'.\n" +
							"=====================\n",
							tableFqn, stmt.getScn(), stmt.getRba(), stmt.isRollback(), stmt.getRowId(), tableFqn, stmt.getRowId());
					getMissedColumnValues(connection, stmt.getRowId(), keyStruct);
				}
			}
			//END: where clause processing...
		} else {
			// We expect here only 1,2,3 as valid values for OPERATION_CODE (and 68 for special cases)
			printErrorMessage(
					Level.ERROR,
					"Corrupted record for table {} found!!!\nPlease send e-mail to oracle@a2.solutions with record details below:\n",
					stmt, xid, commitScn);
			throw new SQLException("Unknown OPERATION_CODE while parsing redo record!");
		}

		if (incompleteDataTolerance == OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_RESTORE &&
				missedColumns != null) {
			if (getMissedColumnValues(connection, stmt, missedColumns, keyStruct, valueStruct, xid, commitScn)) {
				printRedoRecordRecoveredMessage(stmt, xid, commitScn);
			} else {
				printSkippingRedoRecordMessage(stmt, xid, commitScn);
			}
		}

		SourceRecord sourceRecord = null;
		if (!skipRedoRecord) {
			if (mandatoryColumnsProcessed < mandatoryColumnsCount) {
				if (opType != 'd') {
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug(
								"Mandatory columns count for table {} is {}, but only {} mandatory columns are returned from redo record!",
								tableFqn, mandatoryColumnsCount, mandatoryColumnsProcessed);
					}
					final String message = 
							"Mandatory columns count for table {} is " +
							mandatoryColumnsCount +
							" but only " +
							mandatoryColumnsProcessed +
							" mandatory columns are returned from the redo record!\n" +
							"Please check supplemental logging settings!\n";
					if (incompleteDataTolerance == INCOMPLETE_REDO_INT_ERROR) {
						printErrorMessage(Level.ERROR,  message + "Exiting!\n", stmt, xid, commitScn);
						throw new ConnectException("Incomplete redo record!");
					} else {
						printErrorMessage(Level.ERROR,  message + "Skipping!\n", stmt, xid, commitScn);
						return null;
					}
				} else if (!pseudoKey) {
					// With ROWID we does not need more checks...
					//TODO - logic for delete only with primary columns!
					//TODO
					//TODO - logic for delete with all columns
				}
			}

			if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
				final Struct struct = new Struct(schema);
				final Struct source = rdbmsInfo.getStruct(
						stmt.getSqlRedo(),
						pdbName, tableOwner, tableName,
						stmt.getScn(), stmt.getTs(),
						xid, commitScn, stmt.getRowId().toString());
				struct.put("source", source);
				struct.put("before", keyStruct);
				if (stmt.getOperation() != DELETE ||
						((stmt.getOperation() == DELETE) && useAllColsOnDelete)) {
					struct.put("after", valueStruct);
				}
				struct.put("op", String.valueOf(opType));
				struct.put("ts_ms", System.currentTimeMillis());
				sourceRecord = new SourceRecord(
						sourcePartition,
						offset,
						kafkaTopic,
						schema,
						struct);
			} else {
				if (!(opType == 'd' && (!useAllColsOnDelete))) {
					//TODO
					//TODO Beter handling for 'debezium'-like schemas are required for this case...
					//TODO
					pseudoColumns.addToStruct(valueStruct, stmt, transaction);
				}
				if (onlyValue) {
					sourceRecord = new SourceRecord(
							sourcePartition,
							offset,
							kafkaTopic,
							topicPartition,
							valueSchema,
							valueStruct);
				} else {
					if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_KAFKA_STD) {
						if (stmt.getOperation() == DELETE &&
								(!useAllColsOnDelete)) {
							sourceRecord = new SourceRecord(
									sourcePartition,
									offset,
									kafkaTopic,
									topicPartition,
									keySchema,
									keyStruct,
									null,
									null);
						} else {
							sourceRecord = new SourceRecord(
								sourcePartition,
								offset,
								kafkaTopic,
								topicPartition,
								keySchema,
								keyStruct,
								valueSchema,
								valueStruct);
						}
					}
				}
				if (sourceRecord != null) {
					sourceRecord.headers().addString("op", String.valueOf(opType));
				}
			}
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("END: parseRedoRecord()");
		}
		return sourceRecord;
	}

	
	private void parseRedoRecordValues(
			final OraColumn oraColumn, final byte[] data, final int offset, final int length,
			final Struct keyStruct, final Struct valueStruct, final OraCdcTransaction transaction,
			final Set<LobId> lobIds) throws SQLException {
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
		if (onlyValue) {
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

	private void printUnableToMapColIdWarning(final int colId, final String xid, final OraCdcRedoMinerStatement stmt) {
		if (printUnableToMapColIdWarning)
			LOGGER.warn(
					"\n=====================\n" +
					"Unable to map column with id {} to dictionary for table {} in XID {}!\nDML operation details:\n{}\n" +
					"\n=====================\n",
					colId, tableFqn, xid, stmt.toString());
	}

	private SchemaBuilder keySchemaBuilder(final boolean useRowIdAsKey) {
		if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_SINGLE ||
				(pkColsSet == null && !useRowIdAsKey)) {
			onlyValue = true;
			return null;
		} else {
			return keySchemaBuilder();
		}
	}

	private SchemaBuilder keySchemaBuilder() {
		return schemaBuilder(true, 1);
	}

	private SchemaBuilder valueSchemaBuilder(final int valueSchemaVersion) {
		return schemaBuilder(false, valueSchemaVersion);
	}

	private SchemaBuilder schemaBuilder(final boolean isKey, final int version) {
		return SchemaBuilder
				.struct()
				.optional()
				.name(isKey
						? snm.getKeySchemaName(pdbName, tableOwner, tableName)
						: snm.getValueSchemaName(pdbName, tableOwner, tableName))
				.version(version);
	}

	private void readAndParseOraColumns(final Connection connection, final boolean isCdb) throws SQLException {
		final Entry<OraCdcKeyOverrideTypes, String> keyOverrideType = config.getKeyOverrideType(this.tableOwner + "." + this.tableName);
		final boolean useRowIdAsKey;
		switch (keyOverrideType.getKey()) {
			case NONE -> {
				pkColsSet = OraRdbmsInfo.getPkColumnsFromDict(connection,
					isCdb ? conId : -1, this.tableOwner, this.tableName, config.getPkType());
				useRowIdAsKey = config.useRowidAsKey();
			}
			case ROWID -> {
				pkColsSet = null;
				useRowIdAsKey = true;
			}
			case NOKEY -> {
				pkColsSet = null;
				useRowIdAsKey = false;
			}
			default -> {
				//INDEX
				pkColsSet = OraRdbmsInfo.getPkColumnsFromDict(connection,
					isCdb ? conId : -1, this.tableOwner, this.tableName, keyOverrideType.getValue());
				useRowIdAsKey = config.useRowidAsKey();
			}
		}

		final SchemaBuilder keySchemaBuilder = keySchemaBuilder(config.useRowidAsKey());
		final SchemaBuilder valueSchemaBuilder = valueSchemaBuilder(version);
		
		if (pkColsSet == null) {
			tableWithPk = false;
			if (!onlyValue && useRowIdAsKey) {
				addPseudoKey(keySchemaBuilder, valueSchemaBuilder);
				pseudoKey = true;
			}
			LOGGER.warn(
					"""
					
					=====================
					No primary key detected for table {}. {}
					=====================
					
					""",
						tableFqn,
						onlyValue ? "" : " ROWID will be used as primary key.");
		}


		final List<Triple<List<Pair<String, OraColumn>>, Map<String, OraColumn>, List<Pair<String, OraColumn>>>> numberRemap;
		if (isCdb) {
			alterSessionSetContainer(connection, pdbName);
			numberRemap = config.tableNumberMapping(pdbName, tableOwner, tableName);
		} else {
			numberRemap = config.tableNumberMapping(tableOwner, tableName);
		}
		PreparedStatement statement = connection.prepareStatement(
				OraDictSqlTexts.COLUMN_LIST_PLAIN,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.setString(1, this.tableOwner);
		statement.setString(2, this.tableName);

		ResultSet rsColumns = statement.executeQuery();

		maxColumnId = 0;
		final boolean logMiner = config.logMiner();
		boolean undroppedPresent = false;
		List<Pair<Integer, String>> undroppedColumns = null;
		int minUndroppedId = Integer.MAX_VALUE;
		while (rsColumns.next()) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace(
						"\tColumn {}.{} information:\n" +
						"\t\tDATA_TYPE={}, DATA_LENGTH={}, DATA_PRECISION={}, DATA_SCALE={}, NULLABLE={},\n" +
						"\t\tCOLUMN_ID={}, HIDDEN_COLUMN={}, INTERNAL_COLUMN_ID={}",
						tableFqn, rsColumns.getString("COLUMN_NAME"),
						rsColumns.getString("DATA_TYPE"), rsColumns.getInt("DATA_LENGTH"), rsColumns.getInt("DATA_PRECISION"),
								rsColumns.getInt("DATA_SCALE"), rsColumns.getString("NULLABLE"),
						rsColumns.getInt("COLUMN_ID"), rsColumns.getString("HIDDEN_COLUMN"), rsColumns.getInt("INTERNAL_COLUMN_ID"));
			}
			boolean columnAdded = false;
			OraColumn column = null;
			if (Strings.CI.equals(rsColumns.getString("HIDDEN_COLUMN"), "NO")) {
				try {
					column = new OraColumn(false, useOracdcSchemas, processLobs, rsColumns, pkColsSet);
					if (column.isNumber() && numberRemap != null) {
						final OraColumn newDefinition = config.columnNumberMapping(numberRemap, column.getColumnName());
						if (newDefinition != null) {
							column.remap(newDefinition);
						}
					}
					columnAdded = true;
				} catch (UnsupportedColumnDataTypeException ucdte) {
					LOGGER.warn("Column {} not added to definition of table {}.{}",
							ucdte.getColumnName(), this.tableOwner, this.tableName);
				}
			} else if (rsColumns.getInt("COLUMN_ID") > 0) {
				if (LOGGER.isDebugEnabled())
					LOGGER.debug("Skipping shadow BLOB column {} in table {}",
							rsColumns.getString("COLUMN_NAME"), tableFqn);
			} else {
				final String columnName = rsColumns.getString("COLUMN_NAME");
				final Matcher unusedMatcher = UNUSED_COLUMN.matcher(columnName);
				if (unusedMatcher.matches()) {
					if (!undroppedPresent) {
						undroppedColumns = new ArrayList<>();
						undroppedPresent = true;
					}
					final int internalId = rsColumns.getInt("INTERNAL_COLUMN_ID");
					undroppedColumns.add(Pair.of(internalId, columnName));
					if (internalId < minUndroppedId) {
						minUndroppedId = internalId;
					}
				} else {
					final Matcher guardMatcher = GUARD_COLUMN.matcher(columnName);
					if (guardMatcher.matches()) {
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("Skipping guard column {} in tgable {}.{}",
									columnName, this.tableOwner, this.tableName);
						}
					} else {
						LOGGER.warn(
								"""
								
								=====================
								Table {} contains hidden column '{}' of unknown purpose.
								This column will be excluded from processing.
								For more information, please email us with the DDL for the table at oracle@a2.solutions
								=====================
								
								""",
									tableFqn, columnName);
					}
				}
			}

			if (columnAdded) {
				if (!logMiner && !hasEncryptedColumns && column.isEncrypted()) {
					hasEncryptedColumns = true;
				}
				// For archived redo more logic required
				if (column.getJdbcType() == BLOB ||
					column.getJdbcType() == CLOB ||
					column.getJdbcType() == NCLOB ||
					column.getJdbcType() == SQLXML ||
					column.getJdbcType() == JSON ||
					column.getJdbcType() == VECTOR) {
					if (processLobs) {
						if (!withLobs) {
							withLobs = true;
							lobColumnsNames = new HashMap<>();
						}
						if (withLobs && lobColumnsObjectIds == null) {
							lobColumnsObjectIds = new HashMap<>();
						}
						allColumns.add(column);
						if (logMiner)
							idToNameMap.put(column.getNameFromId(), column);
						else
							pureIdMap.put(column.getColumnId(), column);
						final String lobColumnName = column.getColumnName();
						lobColumnsNames.put(lobColumnName, column);
						final Schema lobSchema = transformLobs.transformSchema(pdbName, tableOwner, tableName, column, valueSchemaBuilder);
						if (lobSchema != null) {
							// BLOB/CLOB/NCLOB/XMLTYPE is transformed
							if (lobColumnSchemas == null) {
								lobColumnSchemas = new HashMap<>();
							}
							lobColumnSchemas.put(lobColumnName, lobSchema);
						}
					} else {
						if (lobColumnIds == null)
							lobColumnIds = new HashSet<>();
						lobColumnIds.add(column.getColumnId());
						columnAdded = false;
					}
				} else {
					allColumns.add(column);
					if (logMiner)
						idToNameMap.put(column.getNameFromId(), column);
					else
						pureIdMap.put(column.getColumnId(), column);
					// Just add to value schema
					if (!column.isPartOfPk() || 
							schemaType == ConnectorParams.SCHEMA_TYPE_INT_SINGLE) {
						valueSchemaBuilder.field(column.getColumnName(), column.getSchema());
					}
				}

				if (column.getColumnId() > maxColumnId) {
					maxColumnId = column.getColumnId();
				}
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("New{}column {}({}) with ID={} added to table definition {}.",
							column.isPartOfPk() ? " PK " : (column.isNullable() ? " " : " mandatory "),
							column.getColumnName(), getTypeName(column.getJdbcType()),
							column.getColumnId(), tableFqn);
					if (column.isDefaultValuePresent()) {
						LOGGER.debug("\tDefault value is set to \"{}\"", column.getDefaultValue());
					}
				}
				if (column.isPartOfPk()) {
					pkColumns.put(column.getColumnName(), column);
					// Schema addition
					if (schemaType != ConnectorParams.SCHEMA_TYPE_INT_SINGLE) {
						keySchemaBuilder.field(column.getColumnName(), column.getSchema());
					}
					if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
						valueSchemaBuilder.field(column.getColumnName(), column.getSchema());
					}
				}

				if (column.isPartOfPk() || (!column.isNullable() && !column.isDefaultValuePresent())) {
					mandatoryColumnsCount++;
				}
			}
		}

		rsColumns.close();
		rsColumns = null;
		statement.close();
		statement = null;

		if (undroppedPresent) {
			printUndroppedColumnsMessage(undroppedColumns, minUndroppedId);
			undroppedColumns = null;
		}

		if (hasEncryptedColumns) {
			final OraCdcTdeWallet tw = rdbmsInfo.tdeWallet();
			if (tw == null) {
				LOGGER.error(
						"""
						
						=====================
						Table {} contains encrypted columns!
						To continue, You must set the parameters a2.tde.wallet.path and a2.tde.wallet.password.
						=====================
						
						""",
							tableFqn);
				throw new ConnectException("Unable to process encrypted columns without configured Oracle Wallet!");
			} else {
				decrypter = OraCdcTdeColumnDecrypter.get(connection, tw, tableOwner, tableName);
			}
		}

		// Handle empty WHERE for update
		if (tableWithPk) {
			final StringBuilder sb = new StringBuilder(128);
			sb.append("select ");
			boolean firstColumn = true;
			for (final Map.Entry<String, OraColumn> entry : pkColumns.entrySet()) {
				if (firstColumn) {
					firstColumn = false;
				} else {
					sb.append(", ");
				}
				sb.append(entry.getKey());
			}
			sb
				.append("\nfrom ")
				.append(tableOwner)
				.append(".")
				.append(tableName)
				.append("\nwhere ROWID = CHARTOROWID(?)");
			sqlGetKeysUsingRowId = sb.toString();
		}

		// Schema
		addPseudoColumns(valueSchemaBuilder);
		schemaEiplogue(tableFqn, keySchemaBuilder, valueSchemaBuilder);
		if (LOGGER.isDebugEnabled()) {
			if (mandatoryColumnsCount > 0) {
				LOGGER.debug("Table {} has {} mandatory columns.", tableFqn, mandatoryColumnsCount);
			}
			if (keySchema != null &&
					keySchema.fields() != null &&
					keySchema.fields().size() > 0) {
				LOGGER.debug("Key fields for table {}.", tableFqn);
				keySchema.fields().forEach(f ->
					LOGGER.debug(
							"\t{} with schema {}",
							f.name(), f.schema().name() != null ? f.schema().name() : f.schema().toString()));
			}
			if (valueSchema != null &&
					valueSchema.fields() != null &&
					valueSchema.fields().size() > 0) {
				LOGGER.debug("Value fields for table {}.", tableFqn);
				valueSchema.fields().forEach(f ->
				LOGGER.debug(
						"\t{} with schema {}",
						f.name(), f.schema().name() != null ? f.schema().name() : f.schema().toString()));
			}
		}
		if (isCdb) {
		// Restore container in session
			alterSessionSetContainer(connection, rdbmsInfo.getPdbName());
		}
	}


	private void addPseudoColumns(final SchemaBuilder valueSchemaBuilder) {
		if (schemaType != ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
			//TODO
			//TODO Beter handling for 'debezium'-like schemas are required for this case...
			//TODO
			pseudoColumns.addToSchema(valueSchemaBuilder);
		}
	}
}
