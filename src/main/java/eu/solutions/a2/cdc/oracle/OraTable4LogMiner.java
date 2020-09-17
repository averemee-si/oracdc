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

package eu.solutions.a2.cdc.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import eu.solutions.a2.cdc.oracle.data.OraTimestamp;
import eu.solutions.a2.cdc.oracle.schema.JdbcTypes;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;
import eu.solutions.a2.cdc.oracle.utils.KafkaUtils;

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

	private final Map<String, OraColumn> idToNameMap;
	private String pdbName;
	private String kafkaTopic;
	private OraDumpDecoder odd;
	private boolean tableWithPk;
	private boolean processLobs;
	private final String tableFqn;
	private boolean withLobs = false;
	private Map<Integer, OraColumn> lobColumns;

	/**
	 * 
	 * @param pdbName      PDB name
	 * @param tableOwner   owner
	 * @param tableName    name
	 * @param schemaType   type of schema
	 * @param processLobs  true for LOB support
	 */
	private OraTable4LogMiner(
			final String pdbName, final String tableOwner, final String tableName,
			final int schemaType, final boolean processLobs) {
		super(tableOwner, tableName, schemaType);
		this.idToNameMap = new HashMap<>();
		this.pdbName = pdbName;
		this.tableFqn = ((pdbName == null) ? "" : pdbName + ":") +
				this.tableOwner + "." + this.tableName;
		this.processLobs = processLobs;
		if (this.processLobs) {
			lobColumns = new HashMap<>();
		}
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
	 * @param isCdb
	 * @param odd
	 * @param sourcePartition
	 * @param topicParam
	 * @param topicNameStyle
	 * @param topicNameDelimiter
	 */
	public OraTable4LogMiner(
			final String pdbName, final Short conId, final String tableOwner,
			final String tableName, final boolean rowLevelScnDependency,
			final int schemaType, final boolean useOracdcSchemas, final boolean processLobs,
			final boolean isCdb, final OraDumpDecoder odd,
			final Map<String, String> sourcePartition, final String topicParam,
			final int topicNameStyle, final String topicNameDelimiter) {
		this(pdbName, tableOwner, tableName, schemaType, processLobs);
		LOGGER.trace("BEGIN: Creating OraTable object from LogMiner data...");
		setTopicDecoderPartition(topicParam, topicNameStyle, topicNameDelimiter, odd, sourcePartition);
		this.tableWithPk = true;
		this.setRowLevelScn(rowLevelScnDependency);
		try (Connection connection = OraPoolConnectionFactory.getConnection()) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("Preparing column list and mining SQL statements for table {}.", tableFqn);
			}
			// Detect PK column list...
			Set<String> pkColumns = OraRdbmsInfo.getPkColumnsFromDict(connection,
					isCdb ? conId : null, this.tableOwner, this.tableName);
			if (pkColumns == null) {
				this.tableWithPk = false;
			}
			PreparedStatement statement = connection.prepareStatement(
					isCdb ? OraDictSqlTexts.COLUMN_LIST_CDB : OraDictSqlTexts.COLUMN_LIST_PLAIN,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.setString(1, this.tableOwner);
			statement.setString(2, this.tableName);
			if (isCdb) {
				statement.setShort(3, conId);
			}

			ResultSet rsColumns = statement.executeQuery();
			
			buildColumnList(
					false, useOracdcSchemas, processLobs, pdbName, rsColumns, null, pkColumns,
					idToNameMap, lobColumns,
					null, null, null, null, false, false, false);
			if (lobColumns.size() > 0) {
				this.withLobs = true;
			}
			rsColumns.close();
			rsColumns = null;
			statement.close();
			statement = null;
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
	 */
	public OraTable4LogMiner(Map<String, Object> tableData, final int schemaType) {
		this((String) tableData.get("pdbName"),
				(String) tableData.get("tableOwner"),
				(String) tableData.get("tableName"),
				schemaType, (boolean) tableData.get("processLobs"));
		tableWithPk = (boolean) tableData.get("tableWithPk");
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
		final SchemaBuilder valueSchemaBuilder = SchemaBuilder
					.struct()
					.optional()
					.name(tableFqn + ".Value")
					.version(1);

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
				if (column.getJdbcType() == Types.BLOB || column.getJdbcType() == Types.CLOB) {
					lobColumns.put(column.getLobObjectId(), column);
				}
				LOGGER.debug("\t Adding {} column.", column.getColumnName());
			}
			schemaEiplogue(tableFqn, keySchemaBuilder, valueSchemaBuilder);
		} catch (SQLException sqle) {
			throw new ConnectException(sqle);
		}
	}

	public SourceRecord parseRedoRecord(OraCdcLogMinerStatement stmt) throws SQLException {
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("BEGIN: parseRedoRecord()");
		}
		final Struct keyStruct = new Struct(keySchema);
		final Struct valueStruct = new Struct(valueSchema);

		Map<String, Object> offset = new HashMap<>(3);
		offset.put("SCN", stmt.getScn());
		offset.put("RS_ID", stmt.getRsId());
		offset.put("SSN", stmt.getSsn());

		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("Parsing REDO record for {}", tableFqn);
			LOGGER.trace("Redo record information:");
			LOGGER.trace("\tSCN = {}", stmt.getScn());
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
		if (stmt.getOperation() == OraLogMiner.V$LOGMNR_CONTENTS_INSERT) {
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
							(oraColumn.getJdbcType() == Types.BLOB || oraColumn.getJdbcType() == Types.CLOB)) {
						// EMPTY_BLOB()/EMPTY_CLOB() passed as ''
						break;
					} else {
						parseRedoRecordValues(oraColumn, columnValue, keyStruct, valueStruct);
					}
				}
			}
		} else if (stmt.getOperation() == OraLogMiner.V$LOGMNR_CONTENTS_DELETE) {
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
									keyStruct, valueStruct);
						}
					}
				}
			}
		} else if (stmt.getOperation() == OraLogMiner.V$LOGMNR_CONTENTS_UPDATE) {
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
				setColumns.add(columnName);
				final OraColumn oraColumn = idToNameMap.get(columnName);
				if (oraColumn != null) {
					// Column can be excluded
					if (StringUtils.endsWith(currentExpr, "L")) {
						valueStruct.put(oraColumn.getColumnName(), null);
					} else if (oraColumn.getJdbcType() == Types.BLOB || oraColumn.getJdbcType() == Types.CLOB) {
						//TODO
						//TODO EMPTY_BLOB()/EMPTY_CLOB() handling!
						//TODO LOB_WRITE results!!!
						//TODO
						break;
					} else {
						parseRedoRecordValues(oraColumn,
								StringUtils.trim(StringUtils.substringAfter(currentExpr, "=")),
								keyStruct, valueStruct);
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
							valueStruct.put(oraColumn.getColumnName(), null);
						}
					}
				} else {
					columnName = StringUtils.trim(StringUtils.substringBefore(currentExpr, "="));
					if (!setColumns.contains(columnName)) {
						final OraColumn oraColumn = idToNameMap.get(columnName);
						if (oraColumn != null) {
							// Column can be excluded
							parseRedoRecordValues(
									oraColumn,
									StringUtils.trim(StringUtils.substringAfter(currentExpr, "=")),
									keyStruct, valueStruct);
						}
					}
				}
			}
		}
		SourceRecord sourceRecord = null;
		if (schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
			final Struct struct = new Struct(schema);
			final Struct source = OraRdbmsInfo.getInstance().getStruct(
					stmt.getSqlRedo(),
					pdbName,
					tableOwner,
					tableName,
					stmt.getScn(),
					stmt.getTs());
			struct.put("source", source);
			struct.put("before", keyStruct);
			if (stmt.getOperation() != OraLogMiner.V$LOGMNR_CONTENTS_DELETE) {
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
			if (stmt.getOperation() == OraLogMiner.V$LOGMNR_CONTENTS_DELETE) {
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
			final Struct keyStruct, final Struct valueStruct) throws SQLException {
		final String columnName = oraColumn.getColumnName();
		final String hex = StringUtils.substring(hexValue, 1, hexValue.length() - 1);
		final Object columnValue;
		try {
			switch (oraColumn.getJdbcType()) {
				case Types.DATE:
				case Types.TIMESTAMP:
					columnValue = OraDumpDecoder.toTimestamp(hex);
					break;
				case Types.TIMESTAMP_WITH_TIMEZONE:
					columnValue = OraTimestamp.fromLogical(
						OraDumpDecoder.toByteArray(hex), oraColumn.isLocalTimeZone());
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
					columnValue = OraDumpDecoder.toBigDecimal(hex).setScale(oraColumn.getDataScale());
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
				default:
					columnValue = oraColumn.unsupportedTypeValue();
					break;
			}
			if (pkColumns.containsKey(columnName)) {
				keyStruct.put(columnName, columnValue);
			}
			if ((schemaType == ParamConstants.SCHEMA_TYPE_INT_KAFKA_STD && !pkColumns.containsKey(columnName)) ||
				schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
				valueStruct.put(columnName, columnValue);
			}
		} catch (SQLException sqle) {
			LOGGER.error(
					"{}! While decoding redo values for table {}\n\t\tcolumn {}\n\t\tJDBC Type {}\n\t\tdump value (hex) {}",
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

	public boolean isWithLobs() {
		return withLobs;
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

}
