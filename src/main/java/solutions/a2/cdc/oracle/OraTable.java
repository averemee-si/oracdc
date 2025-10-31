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

import static java.sql.Types.VARCHAR;
import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_ERROR;
import static solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig.INCOMPLETE_REDO_INT_RESTORE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;
import static solutions.a2.cdc.oracle.OraColumn.GUARD_COLUMN;
import static solutions.a2.cdc.oracle.OraColumn.ROWID_KEY;
import static solutions.a2.cdc.oracle.OraColumn.UNUSED_COLUMN;
import static solutions.a2.cdc.oracle.data.JdbcTypes.getTypeName;
import static solutions.a2.cdc.oracle.utils.OraSqlUtils.alterTablePreProcessor;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_INT_KAFKA_STD;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_INT_SINGLE;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import solutions.a2.cdc.oracle.internals.OraCdcTdeColumnDecrypter;
import solutions.a2.cdc.oracle.internals.OraCdcTdeWallet;
import solutions.a2.cdc.oracle.utils.OraSqlUtils;
import solutions.a2.kafka.ConnectorParams;
import solutions.a2.oracle.internals.RowId;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public abstract class OraTable extends OraTable4SourceConnector {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraTable.class);

	private int topicPartition;
	final String pdbName;
	final String tableFqn;
	final OraCdcLobTransformationsIntf transformLobs;
	private final OraCdcSourceConnectorConfig config;
	private String sqlGetKeysUsingRowId = null;
	int incompleteDataTolerance = INCOMPLETE_REDO_INT_ERROR;
	private int mandatoryColumnsCount = 0;
	private int maxColumnId;
	private int mandatoryColumnsProcessed = 0;
	private final OraCdcPseudoColumnsProcessor pseudoColumns;
	private final SchemaNameMapper snm;
	private String kafkaTopic;
	private final short conId;
	Struct keyStruct;
	private Struct valueStruct;
	private Struct struct;
	private OraCdcTdeColumnDecrypter decrypter;
	Map<String, Schema> lobColumnSchemas;
	StructWriter structWriter;
	final List<OraColumn> missedColumns = new ArrayList<>();

	static final short FLG_TABLE_WITH_PK               = (short)0x0001; 
	static final short FLG_PROCESS_LOBS                = (short)0x0002;
	static final short FLG_WITH_LOBS                   = (short)0x0004;
	static final short FLG_ONLY_VALUE                  = (short)0x0008;
	static final short FLG_ALL_COLS_ON_DELETE          = (short)0x0010;
	static final short FLG_PSEUDO_KEY                  = (short)0x0020;
	static final short FLG_ORACDC_SCHEMAS              = (short)0x0040;
	static final short FLG_ALL_UPDATES                 = (short)0x0080;
	static final short FLG_CHECK_SUPPLEMENTAL          = (short)0x0100;
	static final short FLG_PRINT_SQL_FOR_MISSED_WHERE  = (short)0x0200;
	static final short FLG_PRINT_INVALID_HEX_WARNING   = (short)0x0400;
	static final short FLG_PRINT_UNABLE_DELETE_WARNING = (short)0x0800;
	static final short FLG_PRINT_UNABLE_MAP_COL_ID     = (short)0x1000;
	static final short FLG_SUPPLEMENTAL_LOG_ALL        = (short)0x2000;
	short flags = FLG_TABLE_WITH_PK | FLG_ALL_UPDATES | FLG_CHECK_SUPPLEMENTAL | FLG_PRINT_SQL_FOR_MISSED_WHERE;

	/**
	 * 
	 * @param pdbName      PDB name
	 * @param tableOwner   owner
	 * @param tableName    name
	 * @param rowLevelScn
	 * @param conId
	 * @param config
	 * @param rdbmsInfo
	 */
	OraTable(final String pdbName, final String tableOwner, final String tableName,
			final boolean rowLevelScn, final short conId, final OraCdcSourceConnectorConfig config,
			final OraRdbmsInfo rdbmsInfo, final Connection connection, final int version) {
		super(tableOwner, tableName, config.schemaType());
		this.pdbName = pdbName;
		this.conId = conId;
		this.tableFqn = ((pdbName == null) ? "" : pdbName + ":") +
				this.tableOwner + "." + this.tableName;
		this.rdbmsInfo = rdbmsInfo;
		this.config = config;
		this.rowLevelScn = rowLevelScn;
		this.version = version;
		final TopicNameMapper topicNameMapper = config.getTopicNameMapper();
		topicNameMapper.configure(config);
		this.kafkaTopic = topicNameMapper.getTopicName(pdbName, tableOwner, tableName);
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Kafka topic for table {} set to {}.", tableFqn, kafkaTopic);
		this.sourcePartition = rdbmsInfo.partition();
		incompleteDataTolerance = config.getIncompleteDataTolerance();
		topicPartition = config.topicPartition();
		pseudoColumns = config.pseudoColumnsProcessor();
		snm = config.getSchemaNameMapper();
		snm.configure(config);
		if (config.isPrintInvalidHexValueWarning()) flags |= FLG_PRINT_INVALID_HEX_WARNING;
		if (config.useAllColsOnDelete()) flags |=FLG_ALL_COLS_ON_DELETE;
		if (config.printUnableToDeleteWarning()) flags |= FLG_PRINT_UNABLE_DELETE_WARNING;
		if (config.useOracdcSchemas()) flags |= FLG_ORACDC_SCHEMAS;
		if (config.allUpdates()) flags |= FLG_ALL_UPDATES;
		if (config.printUnable2MapColIdWarning()) flags |= FLG_PRINT_UNABLE_MAP_COL_ID;
		if (config.supplementalLogAll()) flags |= FLG_SUPPLEMENTAL_LOG_ALL;
		else {
			flags |=FLG_ALL_COLS_ON_DELETE;
			flags &= (~FLG_ORACDC_SCHEMAS);
		}
		if (config.processLobs()) {
			flags |= FLG_PROCESS_LOBS;
			transformLobs = config.transformLobsImpl();
		} else
			transformLobs = null;
		switch (schemaType) {
			case SCHEMA_TYPE_INT_KAFKA_STD -> structWriter = new KafkaStructWriter(this);
			case SCHEMA_TYPE_INT_SINGLE    -> structWriter = new SingleStructWriter(this); 
			case SCHEMA_TYPE_INT_DEBEZIUM  -> structWriter = new DebeziumStructWriter(this); 
		}
		try {
			if ((flags & FLG_SUPPLEMENTAL_LOG_ALL) > 0) {
				if (rdbmsInfo.isCheckSupplementalLogData4Table()) {
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Need to check supplemental logging settings for table {}.", tableFqn);
					}
					if (!OraRdbmsInfo.supplementalLoggingSet(connection,
							(rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed()) ? conId : -1,
									this.tableOwner, this.tableName)) {
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
			}
		} catch (SQLException sqle) {
			throw sqlExceptionOnInit(sqle);
		}
	}

	abstract void addToIdMap(final OraColumn column);
	abstract void clearIdMap();
	abstract void removeUnusedColumn(final OraColumn unusedColumn);
	abstract void shiftColumnId(final OraColumn column);
	void removeUnusedLobColumn(final String unusedColName) {
		if (lobColumnSchemas != null)
			lobColumnSchemas.remove(unusedColName);
	}
	void clearLobHolders() {
		if (lobColumnSchemas != null)
			lobColumnSchemas.clear();
	}
	abstract void createLobHolders();
	abstract void addLobColumnId(final int columnId);

	void readAndParseOraColumns(final Connection connection) throws SQLException {
		final var isCdb = rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed();
		final Entry<OraCdcKeyOverrideTypes, String> keyOverrideType = config.getKeyOverrideType(this.tableOwner + "." + this.tableName);
		final boolean useRowIdAsKey;
		final Set<String> pkColsSet;
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

		if (pkColsSet == null) {
			flags &= (~FLG_TABLE_WITH_PK);
			if ((flags & FLG_ONLY_VALUE) == 0 && useRowIdAsKey) {
				flags |= FLG_PSEUDO_KEY;
				final OraColumn rowIdColumn = OraColumn.getRowIdKey();
				allColumns.add(rowIdColumn);
				pkColumns.put(rowIdColumn.getColumnName(), rowIdColumn);
			}
			LOGGER.warn(
					"""
					
					=====================
					No primary key detected for table {}. {}
					=====================
					
					""",
						tableFqn,
						(flags & FLG_ONLY_VALUE) > 0 ? "" : " ROWID will be used as primary key.");
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
		boolean undroppedPresent = false;
		List<Pair<Integer, String>> undroppedColumns = null;
		int minUndroppedId = Integer.MAX_VALUE;
		boolean hasEncryptedColumns = false;
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
					column = new OraColumn(
							false, (flags & FLG_ORACDC_SCHEMAS) > 0, (flags & FLG_PROCESS_LOBS) > 0,
							rsColumns, pkColsSet, decrypter, rdbmsInfo, (flags & FLG_SUPPLEMENTAL_LOG_ALL) > 0);
					if (column.isNumber() && numberRemap != null) {
						final OraColumn newDefinition = config.columnNumberMapping(numberRemap, column.getColumnName());
						if (newDefinition != null) {
							column.remap(newDefinition, decrypter, (flags & FLG_SUPPLEMENTAL_LOG_ALL) > 0);
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

			if (columnAdded && column.largeObject()) {
				if ((flags & FLG_PROCESS_LOBS) == 0) {
					addLobColumnId(column.getColumnId());
					columnAdded = false;
				} else {
					if ((flags & FLG_WITH_LOBS) == 0) {
						flags |= FLG_WITH_LOBS;
						createLobHolders();
					}
				}
			}

			if (columnAdded) {
				if (!hasEncryptedColumns && column.isEncrypted()) {
					hasEncryptedColumns = true;
				}
				allColumns.add(column);
				addToIdMap(column);

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
		if ((flags & FLG_TABLE_WITH_PK) > 0) {
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

		buildSchema(true);

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

	private void buildSchema(final boolean initial) throws SQLException {
		final SchemaBuilder keySchemaBuilder;
		if (initial) {
			if (schemaType == SCHEMA_TYPE_INT_KAFKA_STD ||
					schemaType == SCHEMA_TYPE_INT_DEBEZIUM) {
				if ((flags & FLG_TABLE_WITH_PK) > 0 || (flags & FLG_PSEUDO_KEY) > 0)
					keySchemaBuilder = schemaBuilder(true, 1);
				else
					keySchemaBuilder = null;
			} else {
				keySchemaBuilder = null;
			}
		} else {
			keySchemaBuilder = null;
			if (lobColumnSchemas != null)
				lobColumnSchemas.clear();
		}
		final SchemaBuilder valueSchemaBuilder = schemaBuilder(false, version);
		for (final OraColumn column : allColumns) {
			if (column.largeObject()) {
				final String lobColumnName = column.getColumnName();
				final Schema lobSchema = transformLobs.transformSchema(pdbName, tableOwner, tableName, column, valueSchemaBuilder);
				if (lobSchema != null) {
					// BLOB/CLOB/NCLOB/XMLTYPE is transformed
					if (lobColumnSchemas == null) {
						lobColumnSchemas = new HashMap<>();
					}
					lobColumnSchemas.put(lobColumnName, lobSchema);
					column.transformLob(true);
				}
			}
			if (schemaType == SCHEMA_TYPE_INT_KAFKA_STD) {
				if (column.isPartOfPk()) {
					if (initial)
						keySchemaBuilder.field(column.getColumnName(), column.getSchema());
				} else {
					if (!column.largeObject())
						valueSchemaBuilder.field(column.getColumnName(), column.getSchema());
				}
			} else {
				if (schemaType == SCHEMA_TYPE_INT_DEBEZIUM) {
					if (column.isPartOfPk()) {
						if (initial)
							keySchemaBuilder.field(column.getColumnName(), column.getSchema());
					}
				}
				if (!column.largeObject())
					valueSchemaBuilder.field(column.getColumnName(), column.getSchema());
			}
		}
		if (schemaType != SCHEMA_TYPE_INT_DEBEZIUM) {
			//TODO
			//TODO Beter handling for 'debezium'-like schemas are required for this case...
			//TODO
			pseudoColumns.addToSchema(valueSchemaBuilder);
		}
		// Epilogue
		if (keySchemaBuilder == null) {
			if (initial)
				keySchema = null;
		} else {
			keySchema = keySchemaBuilder.build();
		}
		if (this.schemaType == ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
			valueSchema = valueSchemaBuilder.build();
			schema = SchemaBuilder
					.struct()
					.name(snm.getEnvelopeSchemaName(pdbName, tableOwner, tableName))
					.version(version)
					.field("before", valueSchema)
					.field("after", valueSchema)
					.field("source", rdbmsInfo.getSchema())
					.field("op", Schema.STRING_SCHEMA)
					.field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
					.field("ts_ns", Schema.OPTIONAL_INT64_SCHEMA)
					.build();
		} else {
			valueSchema = valueSchemaBuilder.build();
		}
	}



	SourceRecord createSourceRecord(
			final OraCdcStatementBase stmt, final OraCdcTransaction transaction,
			final Map<String, Object> offset, final char opType, final boolean skipRedoRecord,
			final Connection connection, final List<OraColumn> missedColumns) throws SQLException {

		if (incompleteDataTolerance == INCOMPLETE_REDO_INT_RESTORE && missedColumns != null) {
			if (getMissedColumnValues(connection, stmt, missedColumns, transaction)) {
				printErrorMessage(Level.INFO, "Incomplete redo record restored from latest incarnation for table {}.", stmt, transaction);
			} else {
				printSkippingRedoRecordMessage(stmt, transaction);
			}
		}

		if (skipRedoRecord)
			return null;
		else {
			SourceRecord sourceRecord = null;
			if ((flags & FLG_SUPPLEMENTAL_LOG_ALL) > 0 && mandatoryColumnsProcessed < mandatoryColumnsCount) {
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
						printErrorMessage(Level.ERROR,  message + "Exiting!\n", stmt, transaction);
						throw new ConnectException("Incomplete redo record!");
					} else {
						printErrorMessage(Level.ERROR,  message + "Skipping!\n", stmt, transaction);
						return null;
					}
				} else if ((flags & FLG_PSEUDO_KEY) == 0) {
					// With ROWID we does not need more checks...
					//TODO - logic for delete only with primary columns!
					//TODO
					//TODO - logic for delete with all columns
				}
			}

			if (schemaType == SCHEMA_TYPE_INT_DEBEZIUM) {
				switch (opType) {
					case 'c' -> struct.put("after", valueStruct);
					case 'd' -> struct.put("before", valueStruct);
					case 'u' -> struct.put("before", valueStruct);
				}
				struct.put("source", rdbmsInfo.getStruct(
						stmt.getSqlRedo(), pdbName, tableOwner, tableName,
						stmt.getScn(), stmt.getTs(), transaction.getXid(),
						transaction.getCommitScn(), stmt.getRowId().toString()));
				struct.put("op", String.valueOf(opType));
				struct.put("ts_ms", System.currentTimeMillis());
				struct.put("ts_ns", System.nanoTime());

				sourceRecord = new SourceRecord(
						sourcePartition,
						offset,
						kafkaTopic,
						topicPartition,
						keySchema,
						keyStruct,
						schema,
						struct);
			} else {
				if (!(opType == 'd' && (flags & FLG_ALL_COLS_ON_DELETE) == 0)) {
					//TODO
					//TODO Beter handling for 'debezium'-like schemas are required for this case...
					//TODO
					pseudoColumns.addToStruct(valueStruct, stmt, transaction);
				}
				if ((flags & FLG_ONLY_VALUE) > 0) {
					sourceRecord = new SourceRecord(
							sourcePartition,
							offset,
							kafkaTopic,
							topicPartition,
							valueSchema,
							valueStruct);
				} else {
					if (schemaType == SCHEMA_TYPE_INT_KAFKA_STD) {
						if (stmt.getOperation() == DELETE &&
								(flags & FLG_ALL_COLS_ON_DELETE) == 0) {
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
			return sourceRecord;
		}
	}

	int processDdl(final Connection connection,
			final OraCdcStatementBase stmt,
			final String xid,
			final long commitScn) throws SQLException {
		final String preprocessedDdl = stmt instanceof OraCdcLogMinerStatement
							? ((OraCdcLogMinerStatement) stmt).getSqlRedo()
							: alterTablePreProcessor(new String(((OraCdcRedoMinerStatement) stmt).redoData()));
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
							"""
							
							=====================
							Automatic DROP of a column '{}' included in the key for table '{}' is not supported!
							Please do this manually. For further support, please contact us at oracle@a2.solutions.
							=====================
							
							""", columnToDrop, tableFqn);
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
						"""
						
						=====================
						Cannot drop column(s) '{}' because this column(s) {} not present in the oracdc dictionary for table {}.
						Original DDL text: '{}'
						=====================
						
						""",  sb.toString(),
								unusedColumnIndexes.size() == 1 ? "is" : "are",
								tableFqn, originalDdl);
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
						if (column.getColumnId() == unusedColId) {
							indexToRemove = i;
							if ((flags & FLG_WITH_LOBS) > 0) {
								removeUnusedLobColumn(unusedColName);
							}
							removeUnusedColumn(unusedColumn);
						} else if (column.getColumnId() > unusedColumn.getColumnId()) {
							shiftColumnId(column);
							column.setColumnId(column.getColumnId() - 1);
						}
					}
					OraColumn columnToRemove = allColumns.get(indexToRemove);
					if (!columnToRemove.isNullable())
						mandatoryColumnsProcessed--;
					allColumns.remove(indexToRemove);
					columnToRemove = null;
				}
				maxColumnId = allColumns.size();
				updatedColumnCount = unusedColumns.size();

				version++;
				buildSchema(false);
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
							"""
							
							=====================
							Ignoring DDL statement\n\t'{}'
							for modifying column {} in table {} since this column not exists in table definition
							=====================
							
							""",  originalDdl, changedColumnName, tableFqn);
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
						"""
						
						=====================
						Unable to parse\n'{}'
						Column {} already exists in {}!
						=====================
						
						""", originalDdl, newName, tableFqn);
			} else {
				if (columnIndex < 0) {
					LOGGER.error(
							"""
							
							=====================
							Unable to parse\n'{}'
							Column {} not exists in {}!
							=====================
							
							""", originalDdl, oldName, tableFqn);
				} else {
					allColumns.get(columnIndex).setColumnName(newName);
					updatedColumnCount = 1;

					version++;
					buildSchema(false);
				}
			}
			break;
		}

		if (rebuildSchemaFromDb) {
			if (allColumns != null)
				allColumns.clear();
			if (pkColumns != null)
				pkColumns.clear();
			clearIdMap();
			if ((flags & FLG_WITH_LOBS) > 0)
				clearLobHolders();

			mandatoryColumnsCount = 0;
			updatedColumnCount = 1;
			version++;
			readAndParseOraColumns(connection);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("END: Processing DDL for OraTable {} using dictionary  data...", tableFqn);
			}
		}
		return updatedColumnCount;
	}

	private void alterSessionSetContainer(final Connection connection, final String container) throws SQLException {
		Statement alterSession = connection.createStatement();
		alterSession.execute("alter session set CONTAINER=" + container);
		alterSession.close();
		alterSession = null;
	}

	SchemaBuilder schemaBuilder(final boolean isKey, final int version) {
		if (isKey)
			return SchemaBuilder
				.struct()
				.name(snm.getKeySchemaName(pdbName, tableOwner, tableName))
				.version(version);
		else
			return SchemaBuilder
				.struct()
				.optional()
				.name(snm.getValueSchemaName(pdbName, tableOwner, tableName))
				.version(version);
	}

	void getMissedColumnValues(final Connection connection,  final OraCdcStatementBase stmt) throws SQLException {
		final RowId rowId = stmt.getRowId();
		if ((flags & FLG_PRINT_SQL_FOR_MISSED_WHERE) > 0) {
			LOGGER.info(
					"""
					
					=====================
					'{}'
					Will be used to handle UPDATE statements without WHERE clause for table {}.
					=====================
					
					""", sqlGetKeysUsingRowId, tableFqn);
			flags &= (~FLG_PRINT_SQL_FOR_MISSED_WHERE);
		}
		LOGGER.warn(
				"""
				
				=====================
				UPDATE statement without WHERE clause for table {} at SCN='{}', RS_ID='{}', ROLLBACK='{}' for ROWID='{}'.
				We will try to get primary key values from table {} at ROWID='{}'.
				=====================
				
				""",
					tableFqn, stmt.getScn(), stmt.getRba(), stmt.isRollback(), rowId, tableFqn, rowId);
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
						"""
						
						=====================
						Unable to find row in table {} with ROWID '{}'!
						=====================
						
						""", tableFqn, rowId);
			}
		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == OraRdbmsInfo.ORA_942) {
				// ORA-00942: table or view does not exist
				LOGGER.error(
						"""
						
						=====================
						Please run as SYSDBA:
							grant select on {} to {};
						And restart connector!
						=====================
						
						""", tableFqn, connection.getSchema());
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
			final List<OraColumn> missedColumns, final OraCdcTransaction transaction) throws SQLException {
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
						"""
						
						=====================
						Missed non-null values for columns in table {} at SCN={}, RBA='{}'!
						Please check supplemental logging settings for table {}!
						=====================
						
						""", tableFqn, stmt.getScn(), stmt.getRba(), tableFqn);
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
							"""
							
							=====================
							Unable to find row in table {} with ROWID '{}' using SQL statement
							{}
							=====================
							
							""", tableFqn, stmt.getRowId(), readData.toString());
				}
			} catch (SQLException sqle) {
				if (sqle.getErrorCode() == OraRdbmsInfo.ORA_942) {
					// ORA-00942: table or view does not exist
					LOGGER.error(
							"""
							
							=====================
							Please run as SYSDBA:
								grant select on {} to {};
							And restart connector!
							=====================
							
							""", tableFqn, connection.getSchema());
				} else {
					printErrorMessage(Level.ERROR,
							"Unable to restore row! Redo record information for table {}:\n",
							stmt, transaction);
				}
				throw sqle;
			}
			if (rdbmsInfo.isCdb() && rdbmsInfo.isCdbRoot()) {
				alterSessionSetContainer(connection, OraRdbmsInfo.CDB_ROOT);
			}
		} else {
			LOGGER.error(
					"""
					
					=====================
					Unable to restore redo record for operation {} on table {} at SCN = {}, RS_ID(RBA) = '{}'!
					Only UPDATE (OPERATION_CODE=3) and INSERT (OPERATION_CODE=1) are supported!
					=====================
					
					""", stmt.getOperation(), tableFqn, stmt.getScn(), stmt.getRba());
		}
		return result;
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

	void printSkippingRedoRecordMessage(
			final OraCdcStatementBase stmt, final OraCdcTransaction transaction) {
		printErrorMessage(Level.WARN, "Skipping incomplete redo record for table {}!", stmt, transaction);
	}

	void printErrorMessage(final Level level, final String message,
			final OraCdcStatementBase stmt, final OraCdcTransaction transaction) {
		printErrorMessage(level, message, null, stmt, transaction);
	}

	void printErrorMessage(final Level level, final String message, final String columnName,
			final OraCdcStatementBase stmt, final OraCdcTransaction transaction) {
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
				LOGGER.error(sb.toString(), tableFqn, transaction.getCommitScn(), transaction.getXid());
			} else {
				LOGGER.error(sb.toString(), columnName, tableFqn, transaction.getCommitScn(), transaction.getXid());
				
			}
		} else if (level == Level.WARN) {
			LOGGER.warn(sb.toString(), tableFqn, transaction.getCommitScn(), transaction.getXid());
		} else if (level == Level.INFO) {
			LOGGER.info(sb.toString(), tableFqn, transaction.getCommitScn(), transaction.getXid());
		} else {
			LOGGER.trace(sb.toString(), tableFqn, transaction.getCommitScn(), transaction.getXid());
		}
	}

	void printInvalidFieldValue(final OraColumn oraColumn,
			final OraCdcStatementBase stmt, final OraCdcTransaction transaction) {
		if (oraColumn.isNullable()) {
			printErrorMessage(Level.ERROR,
					"Redo record information for table {}:\n",
					stmt, transaction);
		} else {
			printErrorMessage(Level.ERROR,
					"NULL value for NON NULL column {}!\nRedo record information for table {}:\n",
					oraColumn.getColumnName(), stmt, transaction);
		}
	}

	void printToLogInvalidHexValueWarning(
			final String columnValue,
			final String columnName,
			final OraCdcStatementBase stmt) {
		if ((flags & FLG_PRINT_INVALID_HEX_WARNING) > 0) {
			LOGGER.warn(
					"""
					
					=====================
					Invalid HEX value "{}" for column {} in table {} at SCN={}, RBA='{}' is set to NULL
					=====================
					
					""", columnValue, columnName, tableFqn, stmt.getScn(), stmt.getRba());
		}
	}

	void printUnableToDeleteWarning(final OraCdcStatementBase stmt) {
		if ((flags & FLG_PRINT_UNABLE_DELETE_WARNING) > 0)
			LOGGER.warn(
					"""
					
					=====================
					Unable to perform delete operation on table {}, SCN={}, RBA='{}', ROWID='{}' without primary key!
					SQL_REDO:
						{}
					=====================
					
					""",
					tableFqn, stmt.getScn(), stmt.getRba(), stmt.getRowId(), stmt.getSqlRedo());
	}

	void printSubstDefaultValueWarning(final OraColumn column, final OraCdcStatementBase stmt) {
		LOGGER.warn(
				"""
				
				=====================
				Substituting NULL value for column {}, table {} with DEFAULT value {}
				SCN={}, RBA='{}', SQL_REDO:
					{}
				=====================
				
				""",
				column.getColumnName(), tableFqn, column.getTypedDefaultValue(),
				stmt.getScn(), stmt.getRba(), stmt.getSqlRedo());
	}

	void printNullValueError(final OraColumn column, final OraCdcStatementBase stmt) {
		LOGGER.error(
				"""
				
				=====================
				Field '{}' is NULL in valueStruct in table {}.
				SCN={}, RBA={}, SQL_REDO='{}'
				Please check that '{}' is NULLABLE and not member of keyStruct.
				=====================
				
				""",
				column.getColumnName(), tableFqn,
				stmt.getScn(), stmt.getRba(), stmt.getSqlRedo(), column.getColumnName());
	}

	ConnectException sqlExceptionOnInit(final SQLException sqle) {
		LOGGER.error(
				"""
				
				=====================
				Unable to get information about table {}.{}
				'{}', errorCode = {}, SQLState = '{}'
				=====================
				
				""",
				tableOwner, tableName, sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
		return new ConnectException(sqle);
	}

	public String fqn() {
		return tableFqn;
	}

	String kafkaTopic() {
		return kafkaTopic;
	}

	String pdbName() {
		return pdbName;
	}
	static abstract class StructWriter {
		final OraTable table;
		StructWriter(OraTable table) {
			this.table = table;
		}
		public void init(OraCdcStatementBase stmt) {
			if ((table.flags & FLG_ONLY_VALUE) > 0) {
				table.keyStruct = null;
			} else {
				table.keyStruct = new Struct(table.keySchema);
			}
			table.valueStruct = new Struct(table.valueSchema);
			table.mandatoryColumnsProcessed = 0;
			table.missedColumns.clear();
		}
		abstract void insert(OraColumn column, Object value);
		abstract void delete(OraColumn column, Object value);
		abstract void update(OraColumn column, Object value, boolean after);
		void addRowId(OraCdcStatementBase stmt) {
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Do primary key substitution for table {}", table.tableFqn);
			if (LOGGER.isTraceEnabled())
				LOGGER.trace("{} is used as primary key for table {}", stmt.getRowId(), table.tableFqn);
		}
		void afterBefore() {}
	}

	private static class KafkaStructWriter extends StructWriter {
		KafkaStructWriter(OraTable table) {
			super(table);
		}
		@Override
		public void insert(OraColumn column, Object value) {
			if (column.isPartOfPk()) {
				table.keyStruct.put(column.getColumnName(), value);
				table.mandatoryColumnsProcessed++;
			} else {
				table.valueStruct.put(column.getColumnName(), value);
				if (!column.isNullable())
					table.mandatoryColumnsProcessed++;
			}
		}
		@Override
		public void delete(OraColumn column, Object value) {
			if (column.isPartOfPk()) {
				table.keyStruct.put(column.getColumnName(), value);
				table.mandatoryColumnsProcessed++;
			} else {
				table.valueStruct.put(column.getColumnName(), value);
				if (!column.isNullable())
					table.mandatoryColumnsProcessed++;
			}
		}
		@Override
		public void update(OraColumn column, Object value, boolean after) {
			if (column.isPartOfPk()) {
				table.keyStruct.put(column.getColumnName(), value);
				table.mandatoryColumnsProcessed++;
			} else {
				table.valueStruct.put(column.getColumnName(), value);
				if (!column.isNullable())
					table.mandatoryColumnsProcessed++;
			}
		}
		@Override
		public void addRowId(OraCdcStatementBase stmt) {
			super.addRowId(stmt);
			table.keyStruct.put(ROWID_KEY, stmt.getRowId().toString());
		}
	}

	private static class SingleStructWriter extends StructWriter {
		SingleStructWriter(OraTable table) {
			super(table);
		}
		@Override
		public void insert(OraColumn column, Object value) {
			table.valueStruct.put(column.getColumnName(), value);
			if (column.mandatory())
				table.mandatoryColumnsProcessed++;
		}
		@Override
		public void delete(OraColumn column, Object value) {
			table.valueStruct.put(column.getColumnName(), value);
			if (column.mandatory())
				table.mandatoryColumnsProcessed++;
		}
		@Override
		public void update(OraColumn column, Object value, boolean after) {
			table.valueStruct.put(column.getColumnName(), value);
			if (column.mandatory())
				table.mandatoryColumnsProcessed++;
		}
		@Override
		public void addRowId(OraCdcStatementBase stmt) {
			super.addRowId(stmt);
			table.valueStruct.put(ROWID_KEY, stmt.getRowId().toString());
		}
	}

	private static class DebeziumStructWriter extends StructWriter {
		DebeziumStructWriter(OraTable table) {
			super(table);
		}
		@Override
		public void init(OraCdcStatementBase stmt) {
			super.init(stmt);
			table.struct = new Struct(table.schema);
		}
		@Override
		public void insert(OraColumn column, Object value) {
			if (column.isPartOfPk()) {
				table.keyStruct.put(column.getColumnName(), value);
				table.valueStruct.put(column.getColumnName(), value);
				table.mandatoryColumnsProcessed++;
			} else {
				table.valueStruct.put(column.getColumnName(), value);
				if (!column.isNullable())
					table.mandatoryColumnsProcessed++;
			}
		}
		@Override
		public void delete(OraColumn column, Object value) {
			if (column.isPartOfPk()) {
				table.keyStruct.put(column.getColumnName(), value);
				table.valueStruct.put(column.getColumnName(), value);
				table.mandatoryColumnsProcessed++;
			} else {
				table.valueStruct.put(column.getColumnName(), value);
				if (!column.isNullable())
					table.mandatoryColumnsProcessed++;
			}
		}
		@Override
		public void update(OraColumn column, Object value, boolean after) {
			if (after) {
				if (column.isPartOfPk()) {
					table.keyStruct.put(column.getColumnName(), value);
					table.valueStruct.put(column.getColumnName(), value);
					table.mandatoryColumnsProcessed++;
				} else {
					table.valueStruct.put(column.getColumnName(), value);
					if (!column.isNullable())
						table.mandatoryColumnsProcessed++;
				}
			} else
				table.valueStruct.put(column.getColumnName(), value);
		}
		@Override
		public void addRowId(OraCdcStatementBase stmt) {
			super.addRowId(stmt);
			final String rowId = stmt.getRowId().toString();
			table.keyStruct.put(ROWID_KEY, rowId);
			table.valueStruct.put(ROWID_KEY, rowId);
		}
		@Override
		void afterBefore() {
			table.struct.put("after", table.valueStruct);
			final Struct before = new Struct(table.valueSchema);
			table.valueStruct.schema().fields().forEach(f -> 
				before.put(f, table.valueStruct.get(f)));
			table.valueStruct = before;
		}
	}

}
