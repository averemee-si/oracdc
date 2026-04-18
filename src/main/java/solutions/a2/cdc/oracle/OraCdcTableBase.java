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

import static solutions.a2.cdc.oracle.OraCdcColumn.GUARD_COLUMN;
import static solutions.a2.cdc.oracle.OraCdcColumn.UNUSED_COLUMN;
import static solutions.a2.cdc.oracle.OraDictSqlTexts.COLUMN_LIST_PLAIN;
import static solutions.a2.cdc.oracle.OraDictSqlTexts.COLUMN_LIST_PLAIN_CDB;
import static solutions.a2.cdc.oracle.OraDictSqlTexts.COLUMN_LIST_PLAIN_PDB;
import static solutions.a2.cdc.oracle.data.JdbcTypes.getTypeName;
import static solutions.a2.cdc.oracle.utils.OraSqlUtils.alterTablePreProcessor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;

import org.agrona.collections.IntHashSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import solutions.a2.cdc.oracle.internals.OraCdcTdeColumnDecrypter;
import solutions.a2.cdc.oracle.runtime.data.DataBinder;
import solutions.a2.cdc.oracle.utils.OraSqlUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public abstract class OraCdcTableBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTableBase.class);

	private final String pdbName;
	private final String tableOwner;
	private final String tableName;
	private final List<OraCdcColumn> allColumns;
	private final Map<String, OraCdcColumn> pkColumns;
	private final OraRdbmsInfo rdbmsInfo;
	private final OraCdcSourceConnectorConfig config;
	private final short conId;
	private final boolean rowLevelScn;
	private int version;
	private int mandatoryColumnsCount = 0;
	private int maxColumnId;
	final String tableFqn;
	private OraCdcTdeColumnDecrypter decrypter;
	DataBinder dataBinder;
	final IntHashSet hiddenColumns = new IntHashSet();

	public static final short FLG_TABLE_WITH_PK               = (short)0x0001; 
	public static final short FLG_PROCESS_LOBS                = (short)0x0002;
	public static final short FLG_WITH_LOBS                   = (short)0x0004;
	public static final short FLG_ONLY_VALUE                  = (short)0x0008;
	public static final short FLG_ALL_COLS_ON_DELETE          = (short)0x0010;
	public static final short FLG_PSEUDO_KEY                  = (short)0x0020;
	public static final short FLG_ORACDC_SCHEMAS              = (short)0x0040;
	public static final short FLG_ALL_UPDATES                 = (short)0x0080;
	public static final short FLG_CHECK_SUPPLEMENTAL          = (short)0x0100;
	public static final short FLG_PRINT_SQL_FOR_MISSED_WHERE  = (short)0x0200;
	public static final short FLG_PRINT_INVALID_HEX_WARNING   = (short)0x0400;
	public static final short FLG_PRINT_UNABLE_DELETE_WARNING = (short)0x0800;
	public static final short FLG_PRINT_UNABLE_MAP_COL_ID     = (short)0x1000;
	public static final short FLG_SUPPLEMENTAL_LOG_ALL        = (short)0x2000;
	public static final short FLG_TOLERATE_INCOMPLETE_ROW     = (short)0x4000;
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
	OraCdcTableBase(final String pdbName, final String tableOwner, final String tableName,
			final boolean rowLevelScn, final short conId, final OraCdcSourceConnectorConfig config,
			final OraRdbmsInfo rdbmsInfo, final Connection connection, final int version) {
		this.pkColumns = new LinkedHashMap<>();
		this.allColumns = new ArrayList<>();
		this.tableOwner = tableOwner;
		this.tableName = tableName;		
		this.pdbName = pdbName;
		this.conId = conId;
		this.tableFqn = ((pdbName == null) ? "" : pdbName + ":") +
				this.tableOwner + "." + this.tableName;
		this.rdbmsInfo = rdbmsInfo;
		this.config = config;
		this.rowLevelScn = rowLevelScn;
		this.version = version;
		if (config.tolerateIncompleteRow()) flags |= FLG_TOLERATE_INCOMPLETE_ROW;
		if (config.isPrintInvalidHexValueWarning()) flags |= FLG_PRINT_INVALID_HEX_WARNING;
		if (config.useAllColsOnDelete()) flags |=FLG_ALL_COLS_ON_DELETE;
		if (config.printUnableToDeleteWarning()) flags |= FLG_PRINT_UNABLE_DELETE_WARNING;
		if (config.useOracdcSchemas()) flags |= FLG_ORACDC_SCHEMAS;
		if (config.allUpdates()) flags |= FLG_ALL_UPDATES;
		if (config.printUnable2MapColIdWarning()) flags |= FLG_PRINT_UNABLE_MAP_COL_ID;
		if (config.processLobs()) flags |= FLG_PROCESS_LOBS;
		if (config.supplementalLogAll())
			flags |= FLG_SUPPLEMENTAL_LOG_ALL;
		else {
			flags |=FLG_ALL_COLS_ON_DELETE;
			flags &= (~FLG_ORACDC_SCHEMAS);
		}
		dataBinder = config.dataBinder(this, rdbmsInfo);
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

	abstract void addToIdMap(final OraCdcColumn column);
	abstract void clearIdMap();
	abstract void removeUnusedColumn(final OraCdcColumn unusedColumn);
	abstract void shiftColumnId(final OraCdcColumn column);
	void removeUnusedLobColumn(final String unusedColName) {}
	abstract void clearLobHolders();
	abstract void createLobHolders();
	abstract void addLobColumnId(final int columnId);

	void readAndParseOraColumns(final Connection connection, final boolean initial) throws SQLException {
		final var isCdb = rdbmsInfo.isCdb() && !rdbmsInfo.isPdbConnectionAllowed();
		final Entry<OraCdcKeyOverrideTypes, String> keyOverrideType = config.getKeyOverrideType(this.tableOwner + "." + this.tableName);
		final boolean useRowIdAsKey;
		final Set<String> pkColsSet;
		hiddenColumns.clear();
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
				final OraCdcColumn rowIdColumn = OraCdcColumn.getRowIdKey();
				allColumns.add(rowIdColumn);
				pkColumns.put(rowIdColumn.name(), rowIdColumn);
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


		final List<Triple<List<Pair<String, OraCdcColumn>>, Map<String, OraCdcColumn>, List<Pair<String, OraCdcColumn>>>> numberRemap;
		if (isCdb && rdbmsInfo.isCdbRoot()) {
			if (!rdbmsInfo.noLongInDict())
				alterSessionSetContainer(connection, pdbName);
			numberRemap = config.tableNumberMapping(pdbName, tableOwner, tableName);
		} else {
			numberRemap = config.tableNumberMapping(tableOwner, tableName);
		}
		PreparedStatement statement = connection.prepareStatement(
				rdbmsInfo.noLongInDict() 
					? rdbmsInfo.isCdbRoot()
							? COLUMN_LIST_PLAIN_CDB
							: COLUMN_LIST_PLAIN_PDB
					: COLUMN_LIST_PLAIN,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		statement.setString(1, this.tableOwner);
		statement.setString(2, this.tableName);
		if (rdbmsInfo.noLongInDict() && rdbmsInfo.isCdbRoot())
			statement.setInt(3, conId);

		ResultSet rsColumns = statement.executeQuery();

		maxColumnId = 0;
		boolean undroppedPresent = false;
		List<Pair<Integer, String>> undroppedColumns = null;
		int minUndroppedId = Integer.MAX_VALUE;
		var hasEncryptedColumns = false;
		while (rsColumns.next()) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace(
						"""
							Column {}.{} information:
								DATA_TYPE={}, DATA_LENGTH={}, DATA_PRECISION={}, DATA_SCALE={}, NULLABLE={},
								COLUMN_ID={}, HIDDEN_COLUMN={}, INTERNAL_COLUMN_ID={}
						""",
						tableFqn, rsColumns.getString("COLUMN_NAME"),
						rsColumns.getString("DATA_TYPE"), rsColumns.getInt("DATA_LENGTH"), rsColumns.getInt("DATA_PRECISION"),
								rsColumns.getInt("DATA_SCALE"), rsColumns.getString("NULLABLE"),
						rsColumns.getInt("COLUMN_ID"), rsColumns.getString("HIDDEN_COLUMN"), rsColumns.getInt("INTERNAL_COLUMN_ID"));
			}
			var columnAdded = false;
			OraCdcColumn column = null;
			if (Strings.CI.equals(rsColumns.getString("HIDDEN_COLUMN"), "NO")) {
				try {
					if (!hasEncryptedColumns && Strings.CI.equals("YES", rsColumns.getString("ENCRYPTED"))) {
						hasEncryptedColumns = true;
						final var tw = rdbmsInfo.tdeWallet();
						if (tw == null) {
							LOGGER.error(
									"""
									
									=====================
									Table {} contains encrypted columns!
									To continue, You must set the parameters a2.tde.wallet.path and a2.tde.wallet.password.
									=====================
									
									""", tableFqn);
							throw new OraCdcException("Unable to process encrypted columns without configured Oracle Wallet!");
						} else {
							decrypter = OraCdcTdeColumnDecrypter.get(connection, tw, tableOwner, tableName);
						}
					}
					column = new OraCdcColumn(
							false, (flags & FLG_ORACDC_SCHEMAS) > 0, (flags & FLG_PROCESS_LOBS) > 0,
							rsColumns, pkColsSet, decrypter, rdbmsInfo, (flags & FLG_SUPPLEMENTAL_LOG_ALL) > 0);
					if (column.isNumber() && numberRemap != null) {
						final OraCdcColumn newDefinition = config.columnNumberMapping(numberRemap, column.name());
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
						hiddenColumns.add(rsColumns.getInt("INTERNAL_COLUMN_ID"));
						if (LOGGER.isDebugEnabled())
							LOGGER.debug("Skipping guard column {} in tgable {}.{}",
									columnName, this.tableOwner, this.tableName);
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
				allColumns.add(column);
				addToIdMap(column);

				if (column.getColumnId() > maxColumnId) {
					maxColumnId = column.getColumnId();
				}
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("New{}column {}({}) with ID={} added to table definition {}.",
							column.partOfPk() ? " PK " : (column.nullable() ? " " : " mandatory "),
							column.name(), getTypeName(column.jdbcType()),
							column.getColumnId(), tableFqn);
					if (column.defaultValuePresent()) {
						LOGGER.debug("\tDefault value is set to \"{}\"", column.defaultValue());
					}
				}
				if (column.partOfPk()) {
					pkColumns.put(column.name(), column);
				}

				if (column.partOfPk() || (!column.nullable() && !column.defaultValuePresent())) {
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

		dataBinder.buildSchema(initial);

		if (isCdb && rdbmsInfo.isCdbRoot()) {
			if (!rdbmsInfo.noLongInDict())
				alterSessionSetContainer(connection, rdbmsInfo.getPdbName());
		}
	}

	public int processDdl(final Connection connection,
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
				newColumnName = OraCdcColumn.canonicalColumnName(newColumnName);
				boolean alreadyExist = false;
				for (OraCdcColumn column : allColumns) {
					if (Strings.CS.equals(newColumnName, column.name())) {
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
			final List<OraCdcColumn> unusedColumns = new ArrayList<>();
			final IntHashSet unusedColumnIndexes = new IntHashSet();
			for (int i = 0; i < columnNamesToDrop.length; i++) {
				unusedColumnIndexes.add(i);
				final String columnToDrop = OraCdcColumn.canonicalColumnName(columnNamesToDrop[i]);
				if (pkColumns.containsKey(columnToDrop)) {
					LOGGER.error(
							"""
							
							=====================
							Automatic DROP of a column '{}' included in the key for table '{}' is not supported!
							Please do this manually. For further support, please contact us at oracle@a2.solutions.
							=====================
							
							""", columnToDrop, tableFqn);
					throw new OraCdcException("Automatic DROP of a column included in the key for table is not supported.");
				}
				for (OraCdcColumn column : allColumns)
					if (Strings.CS.equals(columnToDrop, column.name())) {
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
				final Comparator<OraCdcColumn> comparator = new Comparator<OraCdcColumn>() {
					public int compare(OraCdcColumn col1, OraCdcColumn col2){
						return col1.getColumnId() - col2.getColumnId();
					}
				};
				Collections.sort(unusedColumns, comparator);
				Collections.sort(allColumns, comparator);
				for (final OraCdcColumn unusedColumn : unusedColumns) {
					int indexToRemove = -1;
					final String unusedColName = unusedColumn.name();
					final int unusedColId = unusedColumn.getColumnId();
					for (int i = 0; i < allColumns.size(); i++) {
						final OraCdcColumn column = allColumns.get(i);
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
					OraCdcColumn columnToRemove = allColumns.get(indexToRemove);
					if (!columnToRemove.nullable())
						mandatoryColumnsCount--;
					allColumns.remove(indexToRemove);
					columnToRemove = null;
				}
				maxColumnId = allColumns.size();
				updatedColumnCount = unusedColumns.size();

				version++;
				dataBinder.buildSchema(false);
			}
			break;
		case OraSqlUtils.ALTER_TABLE_COLUMN_MODIFY:
			for (String columnDefinition : StringUtils.split(preProcessed, ";")) {
				String changedColumnName = StringUtils.split(columnDefinition)[0];
				changedColumnName = OraCdcColumn.canonicalColumnName(changedColumnName);
				int columnIndex = -1;
				for (int i = 0; i < allColumns.size(); i++) {
					if (Strings.CS.equals(changedColumnName, allColumns.get(i).name())) {
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
			final String oldName = OraCdcColumn.canonicalColumnName(namesArray[0]);
			final String newName = OraCdcColumn.canonicalColumnName(namesArray[1]);
			boolean newNamePresent = false;
			int columnIndex = -1;
			for (int i = 0; i < allColumns.size(); i++) {
				final String columnName = allColumns.get(i).name();
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
					allColumns.get(columnIndex).name(newName);
					updatedColumnCount = 1;

					version++;
					dataBinder.buildSchema(false);
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
			readAndParseOraColumns(connection, false);

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("END: Processing DDL for OraTable {} using dictionary  data...", tableFqn);
			}
		}
		return updatedColumnCount;
	}

	private void alterSessionSetContainer(final Connection connection, final String container) throws SQLException {
		var alterSession = connection.createStatement();
		alterSession.execute("alter session set CONTAINER=" + container);
		alterSession.close();
		alterSession = null;
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
		for (final OraCdcColumn oraColumn : allColumns) {
			if (oraColumn.getColumnId() >= minUndroppedId) {
				affected.add(oraColumn.name());
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

	void printInvalidFieldValue(final OraCdcColumn oraColumn,
			final OraCdcStatementBase stmt, final OraCdcTransaction transaction) {
		if (oraColumn.nullable()) {
			printErrorMessage(Level.ERROR,
					"Redo record information for table {}:\n",
					stmt, transaction);
		} else {
			printErrorMessage(Level.ERROR,
					"NULL value for NON NULL column {}!\nRedo record information for table {}:\n",
					oraColumn.name(), stmt, transaction);
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

	void printSubstDefaultValueWarning(final OraCdcColumn column, final OraCdcStatementBase stmt) {
		LOGGER.warn(
				"""
				
				=====================
				Substituting NULL value for column {}, table {} with DEFAULT value {}
				SCN={}, RBA='{}', SQL_REDO:
					{}
				=====================
				
				""",
				column.name(), tableFqn, column.typedDefaultValue(),
				stmt.getScn(), stmt.getRba(), stmt.getSqlRedo());
	}

	void printNullValueError(final OraCdcColumn column, final OraCdcStatementBase stmt) {
		LOGGER.error(
				"""
				
				=====================
				Field '{}' is NULL in valueStruct in table {}.
				SCN={}, RBA={}, SQL_REDO='{}'
				Please check that '{}' is NULLABLE and not member of keyStruct.
				=====================
				
				""",
				column.name(), tableFqn,
				stmt.getScn(), stmt.getRba(), stmt.getSqlRedo(), column.name());
	}

	OraCdcException sqlExceptionOnInit(final SQLException sqle) {
		LOGGER.error(
				"""
				
				=====================
				Unable to get information about table {}.{}
				'{}', errorCode = {}, SQLState = '{}'
				=====================
				
				""",
				tableOwner, tableName, sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
		return new OraCdcException(sqle);
	}

	public int version() {
		return version;
	}

	public String fqn() {
		return tableFqn;
	}

	public DataBinder dataBinder() {
		return dataBinder;
	}

	public String pdb() {
		return pdbName;
	}

	public String owner() {
		return tableOwner;
	}

	public String name() {
		return tableName;
	}

	public short flags() {
		return flags;
	}

	public List<OraCdcColumn> allColumns() {
		return allColumns;
	}

	public Map<String, OraCdcColumn> pkColumns() {
		return pkColumns;
	}

	public int mandatoryColumnsCount() {
		return mandatoryColumnsCount;
	}

	public boolean rowLevelScn() {
		return rowLevelScn;
	}

	public OraCdcTdeColumnDecrypter decrypter() {
		return decrypter;
	}

}
