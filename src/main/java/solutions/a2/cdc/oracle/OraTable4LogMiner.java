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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import solutions.a2.cdc.oracle.data.OraTimestamp;
import solutions.a2.cdc.oracle.schema.JdbcTypes;
import solutions.a2.cdc.oracle.utils.ExceptionUtils;
import solutions.a2.cdc.oracle.utils.KafkaUtils;
import solutions.a2.cdc.oracle.utils.Lz4Util;
import solutions.a2.cdc.oracle.utils.OraSqlUtils;

/**
 * 
 * @author averemee
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
	 * @param schemaType
	 * @param useOracdcSchemas
	 * @param processLobs
	 * @param transformLobs
	 * @param isCdb
	 * @param odd
	 * @param sourcePartition
	 * @param topicParam
	 * @param topicNameStyle
	 * @param topicNameDelimiter
	 * @param rdbmsInfo
	 * @param connection
	 */
	public OraTable4LogMiner(
			final String pdbName, final short conId, final String tableOwner,
			final String tableName, final boolean rowLevelScnDependency,
			final int schemaType, final boolean useOracdcSchemas,
			final boolean processLobs, final OraCdcLobTransformationsIntf transformLobs,
			final boolean isCdb, final OraDumpDecoder odd,
			final Map<String, String> sourcePartition, final String topicParam,
			final int topicNameStyle, final String topicNameDelimiter,
			final OraRdbmsInfo rdbmsInfo, final Connection connection) {
		this(pdbName, tableOwner, tableName, schemaType, processLobs, transformLobs);
		LOGGER.trace("BEGIN: Creating OraTable object from LogMiner data...");
		setTopicDecoderPartition(topicParam, topicNameStyle, topicNameDelimiter, odd, sourcePartition);
		this.tableWithPk = true;
		this.setRowLevelScn(rowLevelScnDependency);
		this.rdbmsInfo = rdbmsInfo;
		try {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("Preparing column list and mining SQL statements for table {}.", tableFqn);
			}
			// Detect PK column list...
			Set<String> pkColsSet = OraRdbmsInfo.getPkColumnsFromDict(connection,
					isCdb ? conId : -1, this.tableOwner, this.tableName);
			if (pkColsSet == null) {
				this.tableWithPk = false;
			}

			if (isCdb) {
				Statement alterSession = connection.createStatement();
				alterSession.execute("alter session set CONTAINER=" + pdbName);
				alterSession.close();
				alterSession = null;
			}
			PreparedStatement statement = connection.prepareStatement(
					OraDictSqlTexts.COLUMN_LIST_PLAIN,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.setString(1, this.tableOwner);
			statement.setString(2, this.tableName);

			ResultSet rsColumns = statement.executeQuery();

			// Schema init - keySchema is immutable and always 1
			final SchemaBuilder keySchemaBuilder = SchemaBuilder
						.struct()
						.required()
						.name(tableFqn + ".Key")
						.version(1);
			final SchemaBuilder valueSchemaBuilder = SchemaBuilder
						.struct()
						.optional()
						.name(tableFqn + ".Value")
						.version(version);

			if (!tableWithPk) {
				addPseudoKey(keySchemaBuilder, valueSchemaBuilder);
			}
			maxColumnId = 0;
			while (rsColumns.next()) {
				boolean columnAdded = false;
				OraColumn column = null;
				try {
					column = new OraColumn(false, useOracdcSchemas, processLobs, rsColumns, pkColsSet);
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
						if (!column.isPartOfPk()) {
							valueSchemaBuilder.field(column.getColumnName(), column.getSchema());
						}
					}
				}
				if (columnAdded) {
					if (column.getColumnId() > maxColumnId) {
						maxColumnId = column.getColumnId();
					}
					LOGGER.debug("New column {} added to table definition {}.", column.getColumnName(), tableFqn);
				}

				if (column.isPartOfPk()) {
					pkColumns.put(column.getColumnName(), column);
					// Schema addition
					keySchemaBuilder.field(column.getColumnName(), column.getSchema());
					if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
						valueSchemaBuilder.field(column.getColumnName(), column.getSchema());
					}
				}
			}

			rsColumns.close();
			rsColumns = null;
			statement.close();
			statement = null;

			// Schema
			schemaEiplogue(tableFqn, keySchemaBuilder, valueSchemaBuilder);

			if (isCdb) {
				// Restore container in session
				Statement alterSession = connection.createStatement();
				alterSession.execute("alter session set CONTAINER=" + rdbmsInfo.getPdbName());
				alterSession.close();
				alterSession = null;
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
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("BEGIN: parseRedoRecord()");
		}
		final Struct keyStruct = new Struct(keySchema);
		final Struct valueStruct = new Struct(valueSchema);

		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("Parsing REDO record for table {}", tableFqn);
			LOGGER.trace("Redo record information:");
			LOGGER.trace("\tSCN = {}", stmt.getScn());
			LOGGER.trace("\tCOMMIT_SCN = {}", commitScn);
			LOGGER.trace("\tXID = {}", xid);
			LOGGER.trace("\tTIMESTAMP = {}", stmt.getTs());
			LOGGER.trace("\tRS_ID = {}", stmt.getRsId());
			LOGGER.trace("\tSSN = {}", stmt.getSsn());
			LOGGER.trace("\tROW_ID = {}", stmt.getRowId());
			LOGGER.trace("\tOPERATION_CODE = {}", stmt.getOperation());
			LOGGER.trace("\tSQL_REDO = {}", stmt.getSqlRedo());
		}
		if (!tableWithPk) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("Do primary key substitution for table {}", tableFqn);
			}
			keyStruct.put(OraColumn.ROWID_KEY, stmt.getRowId());
			if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
				valueStruct.put(OraColumn.ROWID_KEY, stmt.getRowId());
			}
		}
		String opType = null;
		if (stmt.getOperation() == OraCdcV$LogmnrContents.INSERT) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("parseRedoRecord() processing INSERT");
			}
			opType = "c";
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
						valueStruct.put(oraColumn.getColumnName(), null);
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
										parseRedoRecordValues(oraColumn, columnValue, keyStruct, valueStruct, connection);
									} catch (SQLException sqle) {
										LOGGER.error("Invalid value {} for column {} in table {}",
												columnValue, oraColumn.getColumnName(), tableFqn);
										printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
										if (!oraColumn.isNullable()) {
											throw new SQLException(sqle);
										} else {
											LOGGER.error("Value of column {} in table is set to NULL.",
													oraColumn.getColumnName(), this.fqn());
										}
									}
								} else {
									LOGGER.warn("Null or zero length data for overload for column {} in table {}.",
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
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("parseRedoRecord() processing DELETE");
			}
			opType = "d";
			if (tableWithPk) {
				final int whereClauseStart = StringUtils.indexOf(stmt.getSqlRedo(), SQL_REDO_WHERE);
				String[] whereClause = StringUtils.splitByWholeSeparator(
						StringUtils.substring(stmt.getSqlRedo(), whereClauseStart + 7), SQL_REDO_AND);
				for (int i = 0; i < whereClause.length; i++) {
					final String currentExpr = StringUtils.trim(whereClause[i]);
					if (!StringUtils.endsWith(currentExpr, "L")) {
						// PK can't be null!!!
						final String columnName = StringUtils.trim(StringUtils.substringBefore(currentExpr, "="));
						final OraColumn oraColumn = idToNameMap.get(columnName);
						if (oraColumn != null && oraColumn.isPartOfPk()) {
							parseRedoRecordValues(
									idToNameMap.get(columnName),
									StringUtils.trim(StringUtils.substringAfter(currentExpr, "=")),
									keyStruct, valueStruct, connection);
						}
					}
				}
			}
		} else if (stmt.getOperation() == OraCdcV$LogmnrContents.UPDATE) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("parseRedoRecord() processing UPDATE");
			}
			opType = "u";
			final Set<String> setColumns = new HashSet<>();
			final int whereClauseStart = StringUtils.indexOf(stmt.getSqlRedo(), SQL_REDO_WHERE);
			final int setClauseStart = StringUtils.indexOf(stmt.getSqlRedo(), SQL_REDO_SET);
			String[] setClause = StringUtils.split(
					StringUtils.substring(stmt.getSqlRedo(), setClauseStart + 5, whereClauseStart), ",");
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
							if (!oraColumn.getDefaultValuePresent()) {
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
							parseRedoRecordValues(oraColumn, columnValue,
									keyStruct, valueStruct, connection);
							setColumns.add(columnName);
						}
					}
				}
			}
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
								if (oraColumn.getDefaultValuePresent()) {
									final Object columnDefaultValue = oraColumn.getTypedDefaultValue();
									if (columnDefaultValue != null) {
										LOGGER.warn("Substituting NULL value for column {}, table {} with DEFAULT value {}",
												oraColumn.getColumnName(), this.tableFqn, columnDefaultValue);
										LOGGER.warn("\tRedo record information:");
										LOGGER.warn("\t\tSCN = {},\tRS_ID = {},\tSSN = {}",
												stmt.getScn(), stmt.getRsId(), stmt.getSsn());
										valueStruct.put(oraColumn.getColumnName(), columnDefaultValue);
										throwDataException = false;
									}
								}
								if (throwDataException) {
									printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
									throw new DataException(de);
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
									oraColumn,
									columnValue,
									keyStruct, valueStruct, connection);
							} catch (DataException de) {
								LOGGER.error("Invalid value {} for column {} in table {}",
										columnValue, oraColumn.getColumnName(), tableFqn);
								printInvalidFieldValue(oraColumn, stmt, xid, commitScn);
								throw new DataException(de);
							}
						}
					}
				}
			}
		} else if (stmt.getOperation() == OraCdcV$LogmnrContents.XML_DOC_BEGIN) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("parseRedoRecord() processing XML_DOC_BEGIN (for XMLTYPE update)");
			}
			opType = "u";
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
								keyStruct, valueStruct, connection);
					} else {
						// We assume EXPLICIT null here
						valueStruct.put(oraColumn.getColumnName(), null);					}
				} else {
					LOGGER.error("Can't detect column with name '{}' during parsing!", columnName);
					printInvalidFieldValue(false, columnName, stmt, xid, commitScn);
					throw new DataException(
							"Can't detect column with name " + columnName + " during parsing!");
				}
			}
		} else {
			// We expect here only 1,2,3 as valid values for OPERATION_CODE (and 68 for special cases)
			LOGGER.error("Corrupted record for table {} found!!!\nPlease send e-mail to oracle@a2-solutions.eu with record details below:",
					tableFqn);
			LOGGER.error("\tSCN = {}", stmt.getScn());
			LOGGER.error("\tCOMMIT_SCN = {}", commitScn);
			LOGGER.error("\tXID = {}", xid);
			LOGGER.error("\tTIMESTAMP = {}", stmt.getTs());
			LOGGER.error("\tRS_ID = {}", stmt.getRsId());
			LOGGER.error("\tSSN = {}", stmt.getSsn());
			LOGGER.error("\tROW_ID = {}", stmt.getRowId());
			LOGGER.error("\tOPERATION_CODE = {}", stmt.getOperation());
			LOGGER.error("\tSQL_REDO = {}", stmt.getSqlRedo());
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

		SourceRecord sourceRecord = null;
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
			final Struct struct = new Struct(schema);
			final Struct source = rdbmsInfo.getStruct(
					stmt.getSqlRedo(),
					pdbName, tableOwner, tableName,
					stmt.getScn(), stmt.getTs(),
					xid, commitScn, stmt.getRowId());
			struct.put("source", source);
			struct.put("before", keyStruct);
			if (stmt.getOperation() != OraCdcV$LogmnrContents.DELETE) {
				struct.put("after", valueStruct);
			}
			struct.put("op", opType);
			struct.put("ts_ms", System.currentTimeMillis());
			sourceRecord = new SourceRecord(
					sourcePartition,
					offset,
					kafkaTopic,
					schema,
					struct);
		} else if (schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			if (stmt.getOperation() == OraCdcV$LogmnrContents.DELETE) {
				sourceRecord = new SourceRecord(
						sourcePartition,
						offset,
						kafkaTopic,
						keySchema,
						keyStruct,
						null,
						null);
			} else {
				sourceRecord = new SourceRecord(
					sourcePartition,
					offset,
					kafkaTopic,
					keySchema,
					keyStruct,
					valueSchema,
					valueStruct);
			}
			sourceRecord.headers().addString("op", opType);
		}
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("END: parseRedoRecord()");
		}
		return sourceRecord;
	}

	private void parseRedoRecordValues(
			final OraColumn oraColumn, final String hexValue,
			final Struct keyStruct, final Struct valueStruct,
			final Connection connection) throws SQLException {
		final String columnName = oraColumn.getColumnName();
		//final String hex = StringUtils.substring(hexValue, 1, hexValue.length() - 1);
		final String hex = StringUtils.substringBetween(hexValue, "'");
		final Object columnValue;
		try {
			switch (oraColumn.getJdbcType()) {
				case Types.DATE:
				case Types.TIMESTAMP:
					columnValue = OraDumpDecoder.toTimestamp(hex);
					break;
				case Types.TIMESTAMP_WITH_TIMEZONE:
					columnValue = OraTimestamp.fromLogical(
						OraDumpDecoder.toByteArray(hex), oraColumn.isLocalTimeZone(), connection);
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
					if (bdValue.scale() != oraColumn.getDataScale()) {
						LOGGER.error("Different data scale for column {} in table {}!",
								columnName, this.fqn());
						LOGGER.error("Current value={}. Data scale from redo={}, data scale in current dictionary={}",
								bdValue, bdValue.scale(), oraColumn.getDataScale());
						columnValue = bdValue.setScale(oraColumn.getDataScale(), RoundingMode.HALF_UP);
					} else {
						columnValue = bdValue.setScale(oraColumn.getDataScale());
					}
					break;
				case Types.NUMERIC:
					// do not need to call OraNumber.fromLogical()
					columnValue = OraDumpDecoder.toByteArray(hex);
					break;
				case Types.BINARY:
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
				default:
					columnValue = oraColumn.unsupportedTypeValue();
					break;
			}
			if (pkColumns.containsKey(columnName)) {
				keyStruct.put(columnName, columnValue);
			}
			if ((schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD && !pkColumns.containsKey(columnName)) ||
				schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
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
		} catch (SQLException sqle) {
			LOGGER.error(
					"{}! While decoding redo values for table {}\n\t\tcolumn {}\n\t\tJDBC Type {}\n\t\tdump value (hex) '{}'",
					sqle.getMessage(), this.tableFqn, columnName, JdbcTypes.getTypeName(oraColumn.getJdbcType()), hex);
			throw new SQLException(sqle);
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

	public void setTopicDecoderPartition(final String topicParam,
			final int topicNameStyle, final String topicNameDelimiter,
			final OraDumpDecoder odd, final Map<String, String> sourcePartition) {
		if (this.schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD) {
			if (topicNameStyle == ParamConstants.TOPIC_NAME_STYLE_INT_TABLE) {
				this.kafkaTopic = this.tableName;
			} else if (topicNameStyle == ParamConstants.TOPIC_NAME_STYLE_INT_SCHEMA_TABLE) {
				this.kafkaTopic = this.tableOwner + topicNameDelimiter + this.tableName;
			} else {
				// topicNameStyle == ParamConstants.TOPIC_NAME_STYLE_INT_PDB_SCHEMA_TABLE
				if (this.pdbName == null) {
					LOGGER.warn("Unable to use a2.topic.name.style=PDB_SCHEMA_TABLE in non-CDB database for table {}!",
							this.fqn());
					this.kafkaTopic = this.tableOwner + topicNameDelimiter + this.tableName;
				} else {
					this.kafkaTopic = this.pdbName + topicNameDelimiter +
										this.tableOwner + topicNameDelimiter + this.tableName;
				}
			}
			if (!StringUtils.isEmpty(topicParam)) {
				this.kafkaTopic = topicParam + topicNameDelimiter + this.kafkaTopic;
			}
			if (!KafkaUtils.validTopicName(this.kafkaTopic)) {
				this.kafkaTopic = KafkaUtils.fixTopicName(this.kafkaTopic, "zZ");
			}
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Kafka topic for table {} set to {}.",
						this.fqn(), this.kafkaTopic);
			}
		} else {
			// ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM
			this.kafkaTopic = topicParam;
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
								useOracdcSchemas, processLobs,
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
								useOracdcSchemas, processLobs,
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
							schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
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

	private void printInvalidFieldValue(final OraColumn oraColumn,
			final OraCdcLogMinerStatement stmt,final String xid, final long commitScn) {
		printInvalidFieldValue(
				!oraColumn.isNullable(), oraColumn.getColumnName(), stmt, xid, commitScn);
	}

	private void printInvalidFieldValue(final boolean printNullMessage, final String columnName,
			final OraCdcLogMinerStatement stmt,final String xid, final long commitScn) {
		if (printNullMessage) {
			LOGGER.error("NULL value for NON NULL column {}, table {}",
				columnName, tableFqn);
		}
		LOGGER.error("Redo record information for table {}:", tableFqn);
		LOGGER.error("\tSCN = {}", stmt.getScn());
		LOGGER.error("\tCOMMIT_SCN = {}", commitScn);
		LOGGER.error("\tXID = {}", xid);
		LOGGER.error("\tTIMESTAMP = {}", stmt.getTs());
		LOGGER.error("\tRS_ID = {}", stmt.getRsId());
		LOGGER.error("\tSSN = {}", stmt.getSsn());
		LOGGER.error("\tROW_ID = {}", stmt.getRowId());
		LOGGER.error("\tOPERATION_CODE = {}", stmt.getOperation());
		LOGGER.error("\tSQL_REDO = {}", stmt.getSqlRedo());
	}

}
