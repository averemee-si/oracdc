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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
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

import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import solutions.a2.cdc.oracle.data.OraInterval;
import solutions.a2.cdc.oracle.data.OraTimestamp;
import solutions.a2.cdc.oracle.schema.JdbcTypes;
import solutions.a2.cdc.oracle.utils.Lz4Util;
import solutions.a2.cdc.oracle.utils.OraSqlUtils;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.oracle.jdbc.types.OracleDate;
import solutions.a2.oracle.jdbc.types.OracleTimestamp;
import solutions.a2.utils.ExceptionUtils;

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
	private int incompleteDataTolerance = OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_ERROR;
	private boolean onlyValue = false;
	private boolean useAllColsOnDelete = false;
	private int mandatoryColumnsCount = 0;
	private int mandatoryColumnsProcessed = 0;
	private boolean pseudoKey = false;
	private boolean printUnableToDeleteWarning;

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
			final OraCdcLobTransformationsIntf transformLobs) {
		super(tableOwner, tableName, schemaType);
		this.idToNameMap = new HashMap<>();
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
	 * @param processLobs
	 * @param transformLobs
	 * @param isCdb
	 * @param topicPartition
	 * @param odd
	 * @param sourcePartition
	 * @param rdbmsInfo
	 * @param connection
	 */
	public OraTable4LogMiner(
			final String pdbName, final short conId, final String tableOwner,
			final String tableName, final boolean rowLevelScnDependency,
			final OraCdcSourceConnectorConfig config,
			final boolean processLobs, final OraCdcLobTransformationsIntf transformLobs,
			final boolean isCdb, final int topicPartition, final OraDumpDecoder odd,
			final Map<String, String> sourcePartition,
			final OraRdbmsInfo rdbmsInfo, final Connection connection) {
		this(pdbName, tableOwner, tableName, config.getSchemaType(), processLobs, transformLobs);
		LOGGER.trace("BEGIN: Creating OraTable object from LogMiner data...");
		setTopicDecoderPartition(config, odd, sourcePartition);
		this.tableWithPk = true;
		this.setRowLevelScn(rowLevelScnDependency);
		this.rdbmsInfo = rdbmsInfo;
		this.topicPartition = topicPartition;
		this.printInvalidHexValueWarning = config.isPrintInvalidHexValueWarning();
		this.incompleteDataTolerance = config.getIncompleteDataTolerance();
		this.useAllColsOnDelete = config.useAllColsOnDelete();
		this.printUnableToDeleteWarning = config.printUnableToDeleteWarning();
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
							"\n" +
							"=====================\n" +
							"Supplemental logging for table '{}' is not configured correctly!\n" +
							"Please set it according to the oracdc documentation!\n" +
							"=====================\n", tableFqn);
				}
			}
			// Detect PK column list...
			final Set<String> pkColsSet = OraRdbmsInfo.getPkColumnsFromDict(connection,
					isCdb ? conId : -1, this.tableOwner, this.tableName, config.getPkType());

			// Schema init - keySchema is immutable and always 1
			final SchemaBuilder keySchemaBuilder;
			if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_SINGLE ||
					(pkColsSet == null && !config.useRowidAsKey())) {
				keySchemaBuilder = null;
				onlyValue = true;
			} else {
				keySchemaBuilder = SchemaBuilder
						.struct()
						.required()
						.name(config.useProtobufSchemaNaming() ?
								(pdbName == null ? "" : pdbName + "_") + tableOwner + "_" + tableName + "_Key" :
								tableFqn + ".Key")
						.version(1);
			}
			final SchemaBuilder valueSchemaBuilder = SchemaBuilder
						.struct()
						.optional()
						.name(config.useProtobufSchemaNaming() ?
								(pdbName == null ? "" : pdbName + "_") + tableOwner + "_" + tableName + 
										(schemaType == ConnectorParams.SCHEMA_TYPE_INT_SINGLE ? "" : "_Value") :
								tableFqn + (schemaType == ConnectorParams.SCHEMA_TYPE_INT_SINGLE ? "" : ".Value"))
						.version(version);

			if (pkColsSet == null) {
				tableWithPk = false;
				if (!onlyValue) {
					addPseudoKey(keySchemaBuilder, valueSchemaBuilder);
					pseudoKey = true;
				}
				LOGGER.warn("No primary key detected for table {}.{}",
						tableFqn, 
						onlyValue ? "" : " ROWID will be used as primary key.");
			}

			if (isCdb) {
				alterSessionSetContainer(connection, pdbName);
			}
			PreparedStatement statement = connection.prepareStatement(
					OraDictSqlTexts.COLUMN_LIST_PLAIN,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.setString(1, this.tableOwner);
			statement.setString(2, this.tableName);

			ResultSet rsColumns = statement.executeQuery();

			maxColumnId = 0;
			while (rsColumns.next()) {
				boolean columnAdded = false;
				OraColumn column = null;
				try {
					column = new OraColumn(false, config.useOracdcSchemas(), processLobs, rsColumns, pkColsSet);
					columnAdded = true;
				} catch (UnsupportedColumnDataTypeException ucdte) {
					LOGGER.warn("Column {} not added to definition of table {}.{}",
							ucdte.getColumnName(), this.tableOwner, this.tableName);
				}

				if (columnAdded) {
					// For archived redo more logic required
					if (column.getJdbcType() == Types.BLOB ||
						column.getJdbcType() == Types.CLOB ||
						column.getJdbcType() == Types.NCLOB ||
						column.getJdbcType() == Types.SQLXML) {
						if (processLobs) {
							if (!withLobs) {
								withLobs = true;
							}
							if (withLobs && lobColumnsObjectIds == null) {
								lobColumnsObjectIds = new HashMap<>();
								lobColumnsNames = new HashMap<>();
							}
							allColumns.add(column);
							idToNameMap.put(column.getNameFromId(), column);

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
							columnAdded = false;
						}
					} else {
						allColumns.add(column);
						idToNameMap.put(column.getNameFromId(), column);
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
						LOGGER.debug("New {} column {}({}) added to table definition {}.",
								column.isPartOfPk() ? " PK " : " ", column.getColumnName(), 
								JdbcTypes.getTypeName(column.getJdbcType()), tableFqn);
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

				if (column.isPartOfPk() || (!column.isNullable())) {
					mandatoryColumnsCount++;
				}
			}

			rsColumns.close();
			rsColumns = null;
			statement.close();
			statement = null;

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
			schemaEiplogue(tableFqn, keySchemaBuilder, valueSchemaBuilder);

			if (isCdb) {
				// Restore container in session
				if (isCdb) {
					alterSessionSetContainer(connection, rdbmsInfo.getPdbName());
				}
			}


		} catch (SQLException sqle) {
			LOGGER.error("Unable to get table information.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			throw new ConnectException(sqle);
		}
		LOGGER.trace("END: Creating OraTable object from LogMiner data...");
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
				transformLobs);
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
		final SchemaBuilder keySchemaBuilder = SchemaBuilder
					.struct()
					.required()
					.name(tableFqn + ".Key")
					.version(1);
		//TODO
		//TODO version in JSON dictionary?
		//TODO
		final SchemaBuilder valueSchemaBuilder = SchemaBuilder
					.struct()
					.optional()
					.name(tableFqn + ".Value")
					.version(version);

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

	public SourceRecord parseRedoRecord(
			final OraCdcLogMinerStatement stmt,
			final List<OraCdcLargeObjectHolder> lobs,
			final String xid,
			final long commitScn,
			final Map<String, Object> offset,
			final Connection connection) throws SQLException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("BEGIN: parseRedoRecord()");
		}
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
			keyStruct.put(OraColumn.ROWID_KEY, stmt.getRowId());
			if (schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
				valueStruct.put(OraColumn.ROWID_KEY, stmt.getRowId());
			}
		}
		mandatoryColumnsProcessed = 0;
		final char opType;
		if (stmt.getOperation() == OraCdcV$LogmnrContents.INSERT) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing INSERT");
			}
			opType = 'c';
			int valuedClauseStart = StringUtils.indexOf(stmt.getSqlRedo(), SQL_REDO_VALUES);
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
					if (StringUtils.startsWith(columnValue, "N")) {
						try {
							valueStruct.put(oraColumn.getColumnName(), null);
						} catch (DataException de) {
							if (StringUtils.containsIgnoreCase(de.getMessage(), "null used for required field")) {
								if (incompleteDataTolerance == OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_ERROR) {
									printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
									throw de;
								} else if (incompleteDataTolerance == OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_SKIP) {
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
							(oraColumn.getJdbcType() == Types.BLOB ||
							oraColumn.getJdbcType() == Types.CLOB ||
							oraColumn.getJdbcType() == Types.NCLOB)) {
						// EMPTY_BLOB()/EMPTY_CLOB() passed as ''
						valueStruct.put(oraColumn.getColumnName(), new byte[0]);
						continue;
					} else {
						// Handle LOB inline value!
						try {
							//We don't have inline values for XMLTYPE
							if (oraColumn.getJdbcType() != Types.SQLXML) {
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
											oraColumn.getColumnName(), this.fqn());
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
		} else if (stmt.getOperation() == OraCdcV$LogmnrContents.DELETE) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing DELETE");
			}
			opType = 'd';
			if (tableWithPk || pseudoKey) {
				final int whereClauseStart = StringUtils.indexOf(stmt.getSqlRedo(), SQL_REDO_WHERE);
				String[] whereClause = StringUtils.splitByWholeSeparator(
						StringUtils.substring(stmt.getSqlRedo(), whereClauseStart + 7), SQL_REDO_AND);
				for (int i = 0; i < whereClause.length; i++) {
					final String currentExpr = StringUtils.trim(whereClause[i]);
					if (useAllColsOnDelete) {
						final String columnName;
						if (StringUtils.endsWith(currentExpr, "L")) {
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
						if (!StringUtils.endsWith(currentExpr, "L")) {
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
								if (StringUtils.equalsIgnoreCase("ROWID", columnName) &&
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
							this.fqn(), stmt.getScn(), stmt.getRsId(), stmt.getRowId(), stmt.getSqlRedo());
				}
			}
		} else if (stmt.getOperation() == OraCdcV$LogmnrContents.UPDATE) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing UPDATE");
			}
			opType = 'u';
			final Set<String> setColumns = new HashSet<>();
			final int whereClauseStart = StringUtils.indexOf(stmt.getSqlRedo(), SQL_REDO_WHERE);
			final int setClauseStart = StringUtils.indexOf(stmt.getSqlRedo(), SQL_REDO_SET);
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
					if (StringUtils.endsWith(currentExpr, "L")) {
						try {
							if (oraColumn.getJdbcType() == Types.BLOB ||
									oraColumn.getJdbcType() == Types.CLOB ||
									oraColumn.getJdbcType() == Types.NCLOB) {
								// Explicit NULL for LOB!
								valueStruct.put(oraColumn.getColumnName(), new byte[0]);
							} else {
								valueStruct.put(oraColumn.getColumnName(), null);
							}
							setColumns.add(columnName);
						} catch (DataException de) {
							//TODO
							//TODO Check for column value in WHERE clause
							//TODO
							if (!oraColumn.isDefaultValuePresent()) {
								// throw error only if we don't expect to get value from WHERE clause
								printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
								throw new DataException(de);
							}
						}
					} else {
						final String columnValue = StringUtils.substringAfter(currentExpr, "=");
						if ("''".equals(columnValue) &&
								(oraColumn.getJdbcType() == Types.BLOB ||
								oraColumn.getJdbcType() == Types.CLOB ||
								oraColumn.getJdbcType() == Types.NCLOB)) {
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
				if (incompleteDataTolerance == OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_ERROR) {
					final String message = 
							"Missed where clause in UPDATE record for the table {}.\n";
					printErrorMessage(Level.ERROR,  message + "Exiting!\n", stmt, xid, commitScn);
					throw new ConnectException("Incomplete redo record!");
				} else if (incompleteDataTolerance == OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_SKIP) {
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
								sqlGetKeysUsingRowId, fqn());
						printSqlForMissedWhereInUpdate = false;
					}
					LOGGER.warn(
							"\n" +
							"=====================\n" +
							"UPDATE statement without WHERE clause for table {} at SCN='{}', RS_ID='{}', ROLLBACK='{}' for ROWID='{}'.\n" +
							"We will try to get primary key values from table {} at ROWID='{}'.\n" +
							"=====================\n",
							fqn(), stmt.getScn(), stmt.getRsId(), stmt.isRollback(), stmt.getRowId(), fqn(), stmt.getRowId());
					getMissedColumnValues(connection, stmt.getRowId(), keyStruct);
				}
			} else {
				String[] whereClause = StringUtils.splitByWholeSeparator(
						StringUtils.substring(stmt.getSqlRedo(), whereClauseStart + 7), SQL_REDO_AND);
				for (int i = 0; i < whereClause.length; i++) {
					final String currentExpr = StringUtils.trim(whereClause[i]);
					final String columnName;
					if (StringUtils.endsWith(currentExpr, "L")) {
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
													"\n" +
													"=====================\n" +
													"Substituting NULL value for column {}, table {} with DEFAULT value {}\n" +
													"SCN={}, RBA='{}', SQL_REDO:\n\t{}\n" +
													"=====================\n",
													oraColumn.getColumnName(), this.tableFqn, columnDefaultValue,
													stmt.getScn(), stmt.getRsId(), stmt.getSqlRedo());
											valueStruct.put(oraColumn.getColumnName(), columnDefaultValue);
											if (oraColumn.isPartOfPk() || (!oraColumn.isNullable())) {
												mandatoryColumnsProcessed++;
											}
											throwDataException = false;
										}
									}
									if (throwDataException) {
										if (incompleteDataTolerance == OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_ERROR) {
											printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
											throw de;
										} else if (incompleteDataTolerance == OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_SKIP) {
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
		} else if (stmt.getOperation() == OraCdcV$LogmnrContents.XML_DOC_BEGIN) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("parseRedoRecord() processing XML_DOC_BEGIN (for XMLTYPE update)");
			}
			opType = 'u';
			final int whereClauseStart = StringUtils.indexOf(stmt.getSqlRedo(), SQL_REDO_WHERE);
			String[] whereClause = StringUtils.splitByWholeSeparator(
					StringUtils.substring(stmt.getSqlRedo(), whereClauseStart + 7), SQL_REDO_AND);
			for (int i = 0; i < whereClause.length; i++) {
				final String currentExpr = StringUtils.trim(whereClause[i]);
				final String columnName;
				if (StringUtils.endsWith(currentExpr, "L")) {
					columnName = StringUtils.trim(StringUtils.substringBefore(currentExpr, SQL_REDO_IS));
				} else {
					columnName = StringUtils.trim(StringUtils.substringBefore(currentExpr, "="));
				}
				final OraColumn oraColumn = idToNameMap.get(columnName);
				if (oraColumn != null) {
					if (!StringUtils.endsWith(currentExpr, "L")) {
						parseRedoRecordValues(
								idToNameMap.get(columnName),
								StringUtils.trim(StringUtils.substringAfter(currentExpr, "=")),
								keyStruct, valueStruct);
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
					"Corrupted record for table {} found!!!\nPlease send e-mail to oracle@a2-solutions.eu with record details below:\n",
					stmt, xid, commitScn);
			throw new SQLException("Unknown OPERATION_CODE while parsing redo record!");
		}

		if (processLobs &&
				(stmt.getOperation() == OraCdcV$LogmnrContents.UPDATE ||
				stmt.getOperation() == OraCdcV$LogmnrContents.INSERT) ||
				stmt.getOperation() == OraCdcV$LogmnrContents.XML_DOC_BEGIN) {
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
							fqn(), lobColumnName, lob.getContent().length);
					}
					if (lobColumnSchemas != null &&
							lobColumnSchemas.containsKey(lobColumnName)) {
						valueStruct.put(lobColumnName,
								transformLobs.transformData(
										pdbName, tableOwner, tableName,
										lobColumn, lob.getContent(),
										keyStruct, lobColumnSchemas.get(lobColumnName)));
					} else {
						valueStruct.put(lobColumnName, lob.getContent());
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
								fqn(), mandatoryColumnsCount, mandatoryColumnsProcessed);
					}
					final String message = 
							"Mandatory columns count for table {} is " +
							mandatoryColumnsCount +
							" but only " +
							mandatoryColumnsProcessed +
							" mandatory columns are returned from the redo record!\n" +
							"Please check supplemental logging settings!\n";
					if (incompleteDataTolerance == OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_ERROR) {
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
						xid, commitScn, stmt.getRowId());
				struct.put("source", source);
				struct.put("before", keyStruct);
				if (stmt.getOperation() != OraCdcV$LogmnrContents.DELETE ||
						((stmt.getOperation() == OraCdcV$LogmnrContents.DELETE) && useAllColsOnDelete)) {
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
						if (stmt.getOperation() == OraCdcV$LogmnrContents.DELETE &&
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
			case Types.DATE:
			case Types.TIMESTAMP:
				if (hex.length() == DATE_DATA_LENGTH || hex.length() == TS_DATA_LENGTH) {
					columnValue = OraDumpDecoder.toTimestamp(hex);
				} else {
					throw new SQLException("Invalid DATE (Typ=12) or TIMESTAMP (Typ=180)");
				}
				break;
			case Types.TIMESTAMP_WITH_TIMEZONE:
				columnValue = OraTimestamp.fromLogical(
					OraDumpDecoder.toByteArray(hex), oraColumn.isLocalTimeZone(), rdbmsInfo.getDbTimeZone());
				break;
			case Types.TINYINT:
				columnValue = OraDumpDecoder.toByte(hex);
				break;
			case Types.SMALLINT:
				columnValue = OraDumpDecoder.toShort(hex);
				break;
			case Types.INTEGER:
				columnValue = OraDumpDecoder.toInt(hex);
				break;
			case Types.BIGINT:
				columnValue = OraDumpDecoder.toLong(hex);
				break;
			case Types.FLOAT:
				if (oraColumn.isBinaryFloatDouble()) {
					columnValue = OraDumpDecoder.fromBinaryFloat(hex);
				} else {
					columnValue = OraDumpDecoder.toFloat(hex);
				}
				break;
			case Types.DOUBLE:
				if (oraColumn.isBinaryFloatDouble()) {
					columnValue = OraDumpDecoder.fromBinaryDouble(hex);
				} else {
					columnValue = OraDumpDecoder.toDouble(hex);
				}
				break;
			case Types.DECIMAL:
				BigDecimal bdValue = OraDumpDecoder.toBigDecimal(hex);
				if (bdValue.scale() > oraColumn.getDataScale()) {
					LOGGER.warn(
								"Different data scale for column {} in table {}! Current value={}. Data scale from redo={}, data scale in current dictionary={}",
								columnName, this.fqn(), bdValue, bdValue.scale(), oraColumn.getDataScale());
					columnValue = bdValue.setScale(oraColumn.getDataScale(), RoundingMode.HALF_UP);
				} else if (bdValue.scale() !=  oraColumn.getDataScale()) {
					columnValue = bdValue.setScale(oraColumn.getDataScale());
				} else {
					columnValue = bdValue;
				}
				break;
			case Types.BINARY:
			case Types.NUMERIC:
			case OraColumn.JAVA_SQL_TYPE_INTERVALYM_BINARY:
			case OraColumn.JAVA_SQL_TYPE_INTERVALDS_BINARY:
				// do not need to perform data type conversion here!
				columnValue = OraDumpDecoder.toByteArray(hex);
				break;
			case Types.CHAR:
			case Types.VARCHAR:
				columnValue = odd.fromVarchar2(hex);
				break;
			case Types.NCHAR:
			case Types.NVARCHAR:
				columnValue = odd.fromNvarchar2(hex);
				break;
			case Types.CLOB:
			case Types.NCLOB:
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
					columnValue = Lz4Util.compress(clobValue);
				}
				break;
			case Types.BLOB:
				if (oraColumn.getSecureFile()) {
					if (hex.length() == LOB_SECUREFILES_DATA_BEGINS || hex.length() == 0) {
						columnValue = new byte[0];
					} else {
						columnValue = OraDumpDecoder.toByteArray(StringUtils.substring(hex,
								LOB_SECUREFILES_DATA_BEGINS  + (extraSecureFileLengthByte(hex) ? 2 : 0)));
					}
				} else {
					columnValue = OraDumpDecoder.toByteArray(
								StringUtils.substring(hex, LOB_BASICFILES_DATA_BEGINS));
				}
				break;
			case Types.SQLXML:
				// We not expect SYS.XMLTYPE data here!!!
				// Set it to 'Not touch at Sink!!!'
				columnValue = null;
				break;
			case OraColumn.JAVA_SQL_TYPE_INTERVALYM_STRING:
			case OraColumn.JAVA_SQL_TYPE_INTERVALDS_STRING:
				columnValue = OraInterval.fromLogical(OraDumpDecoder.toByteArray(hex));
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
				if ((oraColumn.getJdbcType() == Types.BLOB ||
							oraColumn.getJdbcType() == Types.CLOB ||
							oraColumn.getJdbcType() == Types.NCLOB ||
							oraColumn.getJdbcType() == Types.SQLXML) &&
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
					column.setSecureFile(StringUtils.equals("YES", rsCheckLob.getString("SECUREFILE")));
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
					this.fqn(), this.kafkaTopic);
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

	public int processDdl(final boolean useOracdcSchemas,
			final OraCdcLogMinerStatement stmt,
			final String xid,
			final long commitScn) throws SQLException {
		int updatedColumnCount = 0;
		final String[] ddlDataArray = StringUtils.split(stmt.getSqlRedo(), "\n");
		final String operation = ddlDataArray[0];
		final String preProcessed = ddlDataArray[1];
		final String originalDdl = ddlDataArray[2];
		boolean rebuildSchema = false;
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("BEGIN: Processing DDL for table {}:\n\t'{}'\n\t'{}'",
					tableFqn, originalDdl, preProcessed);
		}
		switch (operation) {
		case OraSqlUtils.ALTER_TABLE_COLUMN_ADD:
			for (String columnDefinition : StringUtils.split(preProcessed, ";")) {
				String newColumnName = StringUtils.split(columnDefinition)[0];
				final String columnAttributes = StringUtils.trim( 
						StringUtils.substring(columnDefinition, newColumnName.length()));
				newColumnName = OraColumn.canonicalColumnName(newColumnName);
				boolean alreadyExist = false;
				for (OraColumn column : allColumns) {
					if (StringUtils.equals(newColumnName, column.getColumnName())) {
						alreadyExist = true;
						break;
					}
				}
				if (alreadyExist) {
					LOGGER.warn(
							"Ignoring DDL statement\n\t'{}'\n for adding column {} to table {} since this column already present in table definition",
							originalDdl, newColumnName, this.fqn());
				} else {
					try {
						OraColumn newColumn = new OraColumn(
								useOracdcSchemas,
								newColumnName, columnAttributes, originalDdl,
								maxColumnId + 1);
						allColumns.add(newColumn);
						maxColumnId++;
						rebuildSchema = true;
						updatedColumnCount++;
					} catch (UnsupportedColumnDataTypeException ucte) {
						LOGGER.error("Unable to perform DDL statement\n'{}'\nfor column {} table {}",
								originalDdl, newColumnName, this.fqn());
					}
				}
			}
			break;
		case OraSqlUtils.ALTER_TABLE_COLUMN_DROP:
			for (String columnName : StringUtils.split(preProcessed, ";")) {
				final String columnToDrop = OraColumn.canonicalColumnName(columnName); 
				int columnIndex = -1;
				for (int i = 0; i < allColumns.size(); i++) {
					if (StringUtils.equals(columnToDrop, allColumns.get(i).getColumnName())) {
						columnIndex = i;
						break;
					}
				}
				if (columnIndex > -1) {
					rebuildSchema = true;
					final int columnId = allColumns.get(columnIndex).getColumnId();
					allColumns.remove(columnIndex);
					for (OraColumn column : allColumns) {
						if (column.getColumnId() > columnId) {
							column.setColumnId(column.getColumnId() - 1);
						}
					}
					maxColumnId--;
					updatedColumnCount++;
				} else {
					LOGGER.error("Unable to perform\n'{}'\nColumn {} not exist in {}!",
							originalDdl, columnToDrop, fqn());
				}
			}
			break;
		case OraSqlUtils.ALTER_TABLE_COLUMN_MODIFY:
			for (String columnDefinition : StringUtils.split(preProcessed, ";")) {
				String changedColumnName = StringUtils.split(columnDefinition)[0];
				final String columnAttributes = StringUtils.trim( 
						StringUtils.substring(columnDefinition, changedColumnName.length()));
				changedColumnName = OraColumn.canonicalColumnName(changedColumnName);
				int columnIndex = -1;
				for (int i = 0; i < allColumns.size(); i++) {
					if (StringUtils.equals(changedColumnName, allColumns.get(i).getColumnName())) {
						columnIndex = i;
						break;
					}
				}
				if (columnIndex < 0) {
					LOGGER.warn(
							"Ignoring DDL statement\n\t'{}'\n for modifying column {} in table {} since this column not exists in table definition",
							originalDdl, changedColumnName, this.fqn());
				} else {
					try {
						OraColumn changedColumn = new OraColumn(
								useOracdcSchemas,
								changedColumnName, columnAttributes, originalDdl,
								allColumns.get(columnIndex).getColumnId());
						if (changedColumn.equals(allColumns.get(columnIndex))) {
							LOGGER.warn(
									"Ignoring DDL statement\n\t'{}'\n for modifying column {} in table {} since this column not changed",
									originalDdl, changedColumnName, this.fqn());
						} else {
							allColumns.set(columnIndex, changedColumn);
							if (!rebuildSchema) {
								rebuildSchema = true;
							}
							updatedColumnCount++;
						}
					} catch (UnsupportedColumnDataTypeException ucte) {
						LOGGER.error("Unable to perform DDL statement\n'{}'\nfor column {} table {}",
								originalDdl, changedColumnName, this.fqn());
					}
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
				if ((columnIndex < 0) && StringUtils.equals(oldName, allColumns.get(i).getColumnName())) {
					columnIndex = i;
				}
				if (!newNamePresent && StringUtils.equals(newName, allColumns.get(i).getColumnName())) {
					newNamePresent = true;
				}
			}
			if (newNamePresent) {
				LOGGER.error("Unable to perform\n'{}'\nColumn {} already exist in {}!",
						originalDdl, newName, fqn());
			} else if (columnIndex < 0) {
				LOGGER.error("Unable to perform\n'{}'\nColumn {} not exist in {}!",
						originalDdl, oldName, fqn());
			} else {
				rebuildSchema = true;
				allColumns.get(columnIndex).setColumnName(newName);
				updatedColumnCount++;
			}
			break;
		}

		if (rebuildSchema) {
			// Change version!!!
			final SchemaBuilder valueSchemaBuilder = SchemaBuilder
					.struct()
					.optional()
					.name(tableFqn + ".Value")
					.version(++version);
			
			// Clear column mappings
			idToNameMap.clear();
			if (withLobs) {
				// Do not need to clear lobColumnsObjectIds - 
				lobColumnsNames.clear();
			}
		
			for (OraColumn column : allColumns) {
				idToNameMap.put(column.getNameFromId(), column);
				if (processLobs && 
						(column.getJdbcType() == Types.BLOB ||
						column.getJdbcType() == Types.CLOB ||
						column.getJdbcType() == Types.NCLOB ||
						column.getJdbcType() == Types.SQLXML)) {
					if (!withLobs) {
						withLobs = true;
					}
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
					if (!column.isPartOfPk() ||
							schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
						valueSchemaBuilder.field(column.getColumnName(), column.getSchema());
					}
				}
			}
			schemaEiplogue(tableFqn, valueSchemaBuilder);
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.trace("END: Processing DDL for OraTable {} from LogMiner data...", tableFqn);
		}
		return updatedColumnCount;
	}


	private boolean extraSecureFileLengthByte(String hex) throws SQLException {
		final String startPosFlag = StringUtils.substring(hex, 52, 54);
		if (StringUtils.equals("00", startPosFlag)) {
			return false;
		} else if (StringUtils.equals("01", startPosFlag)) {
			return true;
		} else {
			LOGGER.error("Invalid SECUREFILE additional length byte value '{}' for hex LOB '{}'",
					startPosFlag, hex);
			throw new SQLException("Invalid SECUREFILE additional length byte value!");
		}
	}

	private void printSkippingRedoRecordMessage(
			final OraCdcLogMinerStatement stmt,final String xid, final long commitScn) {
		printErrorMessage(Level.WARN, "Skipping incomplete redo record for table {}!", stmt, xid, commitScn);
	}

	private void printRedoRecordRecoveredMessage(
			final OraCdcLogMinerStatement stmt,final String xid, final long commitScn) {
		printErrorMessage(Level.INFO, "Incomplete redo record restored from latest incarnation for table {}.", stmt, xid, commitScn);
	}

	private void printInvalidFieldValue(final OraColumn oraColumn,
			final OraCdcLogMinerStatement stmt,final String xid, final long commitScn) {
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
			final OraCdcLogMinerStatement stmt,final String xid, final long commitScn) {
		printErrorMessage(level, message, null, stmt, xid, commitScn);
	}

	private void printErrorMessage(final Level level, final String message, final String columnName,
			final OraCdcLogMinerStatement stmt, final String xid, final long commitScn) {
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

	private void getMissedColumnValues(final Connection connection, final String rowId, final Struct keyStruct) throws SQLException {
		try {
			if (rdbmsInfo.isCdb() && rdbmsInfo.isCdbRoot()) {
				alterSessionSetContainer(connection, pdbName);
			}
			final PreparedStatement statement = connection .prepareStatement(sqlGetKeysUsingRowId,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.setString(1, rowId);
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
						fqn(), rowId);
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
						fqn(), connection.getSchema());
			}
			throw sqle;
		} finally {
			if (rdbmsInfo.isCdb() && rdbmsInfo.isCdbRoot()) {
				alterSessionSetContainer(connection, OraRdbmsInfo.CDB_ROOT);
			}
		}
	}

	private boolean getMissedColumnValues(
			final Connection connection, final OraCdcLogMinerStatement stmt,
			final List<OraColumn> missedColumns, final Struct keyStruct, final Struct valueStruct,
			String xid, long commitScn) throws SQLException {
		boolean result = false;
		if (stmt.getOperation() == OraCdcV$LogmnrContents.UPDATE ||
				stmt.getOperation() == OraCdcV$LogmnrContents.INSERT) {
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
			if (stmt.getOperation() == OraCdcV$LogmnrContents.UPDATE) {
				for (final OraColumn oraColumn : allColumns) {
					if (!pkColumns.containsKey(oraColumn.getColumnName()) &&
							!oraColumn.isNullable() &&
							oraColumn.getJdbcType() == Types.VARCHAR) {
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
			if (stmt.getOperation() == OraCdcV$LogmnrContents.UPDATE) {
				readData.append("ROWID = CHARTOROWID(?)");
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(
							"The following statement will be used to get missed data from {} using ROWID {}:\n{}\n",
							fqn(), stmt.getRowId(), readData.toString());
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
						tableFqn, stmt.getScn(), stmt.getRsId(), tableFqn);
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(
							"The following statement will be used to get missed data from {} using PK values:\n{}\n",
							fqn(), readData.toString());
				}
			}

			if (rdbmsInfo.isCdb() && rdbmsInfo.isCdbRoot()) {
				alterSessionSetContainer(connection, pdbName);
			}
			try {
				final PreparedStatement statement = connection .prepareStatement(readData.toString(),
						ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				if (stmt.getOperation() == OraCdcV$LogmnrContents.UPDATE) {
					statement.setString(1, stmt.getRowId());
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
							fqn(), stmt.getRowId(), readData.toString());
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
							fqn(), connection.getSchema());
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
					stmt.getOperation(), tableFqn, stmt.getScn(), stmt.getRsId());
		}
		return result;
	}

	private void printToLogInvalidHexValueWarning(
			final String columnValue,
			final String columnName,
			final OraCdcLogMinerStatement stmt) {
		if (printInvalidHexValueWarning) {
			LOGGER.warn(
					"\n" +
					"=====================\n" +
					"Invalid HEX value \"{}\" for column {} in table {} at SCN={}, RBA='{}' is set to NULL\n" +
					"=====================\n",
					columnValue, columnName, tableFqn, stmt.getScn(), stmt.getRsId());
		}
	}

	private void alterSessionSetContainer(final Connection connection, final String container)throws SQLException {
		Statement alterSession = connection.createStatement();
		alterSession.execute("alter session set CONTAINER=" + container);
		alterSession.close();
		alterSession = null;
	}

}
