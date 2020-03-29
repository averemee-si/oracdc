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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author averemee
 *
 */
public abstract class OraTable4SourceConnector extends OraTableDefinition {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraTable4SourceConnector.class);

	protected Map<String, String> sourcePartition;
	protected Schema schema;
	protected Schema keySchema;
	protected Schema valueSchema;

	protected OraTable4SourceConnector(String tableOwner, String tableName, int schemaType) {
		super(tableOwner, tableName, schemaType);
	}

	protected void buildColumnList(
			final boolean mviewSource,
			final boolean useOracdcSchemas,
			final String pdbName,
			final ResultSet rsColumns, 
			final Map<String, Object> sourceOffset,
			final Set<String> pkColsSet,
			final Map<String, OraColumn> idToNameMap,
			final String snapshotLog,
			final StringBuilder mViewSelect,
			final StringBuilder masterSelect,
			final StringBuilder snapshotDelete,
			final boolean logWithRowIds,
			final boolean logWithPrimaryKey,
			final boolean logWithSequence
			) throws SQLException {
		final String snapshotFqn;
		final StringBuilder masterWhere;
		boolean mViewFirstColumn = true;
		boolean masterFirstColumn = true;

		if (mviewSource) {
			snapshotFqn = "\"" + this.tableOwner + "\"" + ".\"" + snapshotLog + "\"";
			snapshotDelete.append("delete from ");
			snapshotDelete.append(snapshotFqn);
			snapshotDelete.append(" where ROWID = ?");
			
			masterSelect.append("select ");
			mViewSelect.append("select ");
			masterWhere = new StringBuilder(256);
			if (logWithRowIds) {
				// ROWID access is always faster that any other
				masterWhere.append("ROWID=?");
				// Add M_ROW$$ column for snapshot logs with ROWID
				LOGGER.trace("Adding {} to column list.", OraColumn.ROWID_KEY);
				mViewFirstColumn = false;
				mViewSelect.append("chartorowid(M_ROW$$) ");
				mViewSelect.append(OraColumn.ROWID_KEY);
			}
		} else {
			masterWhere = null;
			snapshotFqn = null;
		}

		final String tableFqn = ((pdbName == null) ? "" : pdbName + ":") +
				this.tableOwner + "." + this.tableName;
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
		// Substitute missing primary key with ROWID value
		if ((mviewSource && (!logWithPrimaryKey && logWithRowIds)) ||
				(!mviewSource && pkColsSet == null)) {
			// Add ROWID (ORA$ROWID) - this column is not in dictionary!!!
			OraColumn rowIdColumn = OraColumn.getRowIdKey();
			allColumns.add(rowIdColumn);
			pkColumns.put(rowIdColumn.getColumnName(), rowIdColumn);
			keySchemaBuilder.field(rowIdColumn.getColumnName(), Schema.STRING_SCHEMA);
			if (this.schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
				valueSchemaBuilder.field(rowIdColumn.getColumnName(), Schema.STRING_SCHEMA);
			}
		}

		while (rsColumns .next()) {
			final OraColumn column = new OraColumn(
					mviewSource, useOracdcSchemas,
					rsColumns, keySchemaBuilder, valueSchemaBuilder, schemaType, pkColsSet);
			allColumns.add(column);
			LOGGER.debug("New column {} added to table definition {}.", column.getColumnName(), tableFqn);
			if (mviewSource) {
				if (masterFirstColumn) {
					masterFirstColumn = false;
				} else {
					masterSelect.append(", ");
				}
				masterSelect.append("\"");
				masterSelect.append(column.getColumnName());
				masterSelect.append("\"");
			} else {
				idToNameMap.put(column.getNameFromId(), column);
			}

			if (column.isPartOfPk()) {
				pkColumns.put(column.getColumnName(), column);
				if (mviewSource) {
					if (mViewFirstColumn) {
						mViewFirstColumn = false;
					} else {
						mViewSelect.append(", ");
						if (!logWithRowIds)
							// We need this only when snapshot log don't contains M_ROW$$ 
							masterWhere.append(" and ");
					}
					mViewSelect.append("\"");
					mViewSelect.append(column.getColumnName());
					mViewSelect.append("\"");
					if (!logWithRowIds) {
						// We need this only when snapshot log don't contains M_ROW$$ 
						masterWhere.append("\"");
						masterWhere.append(column.getColumnName());
						masterWhere.append("\"=?");
					}
				}
			}
		}
		// Schema
		schemaEiplogue(tableFqn, keySchemaBuilder, valueSchemaBuilder);

		if (mviewSource) {
			masterSelect.append(" from \"");
			masterSelect.append(this.tableOwner);
			masterSelect.append("\".\"");
			masterSelect.append(this.tableName);
			masterSelect.append("\" where ");
			masterSelect.append(masterWhere);

			if (logWithSequence) {
				mViewSelect.append(", ");
				mViewSelect.append(OraColumn.MVLOG_SEQUENCE);
			}
			mViewSelect.append(", case DMLTYPE$$ when 'I' then 'c' when 'U' then 'u' else 'd' end as OPTYPE$$, ORA_ROWSCN, SYSTIMESTAMP at time zone 'GMT' as TIMESTAMP$$, ROWID from ");
			mViewSelect.append(snapshotFqn);
			if (logWithSequence) {
				LOGGER.trace("BEGIN: mvlog with sequence specific.");
				if (sourceOffset != null && sourceOffset.get(OraColumn.MVLOG_SEQUENCE) != null) {
					long lastProcessedSequence = (long) sourceOffset.get(OraColumn.MVLOG_SEQUENCE);
					mViewSelect.append("\nwhere ");
					mViewSelect.append(OraColumn.MVLOG_SEQUENCE);
					mViewSelect.append(" > ");
					mViewSelect.append(lastProcessedSequence);
					mViewSelect.append("\n");
					LOGGER.debug("Will read mvlog with {} greater than {}.",
							OraColumn.MVLOG_SEQUENCE, lastProcessedSequence);
				}
				mViewSelect.append(" order by ");
				mViewSelect.append(OraColumn.MVLOG_SEQUENCE);
				LOGGER.trace("END: mvlog with sequence specific.");
			}
			LOGGER.trace("End of column list and SQL statements preparation for table {}.{}", this.tableOwner, this.tableName);
		}
	}

	protected void schemaEiplogue(final String tableFqn,
			final SchemaBuilder keySchemaBuilder, final SchemaBuilder valueSchemaBuilder) throws SQLException {
		keySchema = keySchemaBuilder.build(); 
		valueSchema = valueSchemaBuilder.build(); 
		if (this.schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM) {
			final SchemaBuilder schemaBuilder = SchemaBuilder
					.struct()
					.name(tableFqn + ".Envelope");
			schemaBuilder.field("op", Schema.STRING_SCHEMA);
			schemaBuilder.field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA);
			schemaBuilder.field("before", keySchema);
			schemaBuilder.field("after", valueSchema);
			schemaBuilder.field("source", OraRdbmsInfo.getInstance().getSchema());
			schema = schemaBuilder.build();
		}
	}
}
