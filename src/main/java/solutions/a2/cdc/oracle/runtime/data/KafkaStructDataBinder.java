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

package solutions.a2.cdc.oracle.runtime.data;

import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;
import static solutions.a2.cdc.oracle.OraCdcTableBase.FLG_ALL_COLS_ON_DELETE;
import static solutions.a2.cdc.oracle.OraCdcTableBase.FLG_ONLY_VALUE;
import static solutions.a2.cdc.oracle.OraCdcTableBase.FLG_PSEUDO_KEY;
import static solutions.a2.cdc.oracle.OraCdcTableBase.FLG_SUPPLEMENTAL_LOG_ALL;
import static solutions.a2.cdc.oracle.OraCdcTableBase.FLG_TABLE_WITH_PK;
import static solutions.a2.cdc.oracle.OraCdcTableBase.FLG_TOLERATE_INCOMPLETE_ROW;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_DEBEZIUM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_KAFKA_STD;

import java.sql.SQLException;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.OraCdcDataException;
import solutions.a2.cdc.oracle.OraCdcPseudoColumnsProcessor;
import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.cdc.oracle.OraCdcStatementBase;
import solutions.a2.cdc.oracle.OraCdcTransaction;
import solutions.a2.cdc.oracle.OraCdcColumn;
import solutions.a2.cdc.oracle.OraRdbmsInfo;
import solutions.a2.cdc.oracle.OraCdcTableBase;
import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import solutions.a2.cdc.oracle.runtime.config.KafkaSchemaNameMapper;
import solutions.a2.cdc.oracle.runtime.config.KafkaSourceConnectorConfig;
import solutions.a2.cdc.oracle.runtime.config.KafkaTopicNameMapper;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public abstract class KafkaStructDataBinder implements DataBinder {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStructDataBinder.class);

	private final KafkaSchemaNameMapper snm;
	private final OraCdcPseudoColumnsProcessor pseudoColumns;
	private final int topicPartition;
	private OraCdcStatementBase stmt = null;
	private final Map<String, String> sourcePartition;
	private final KafkaConnectSchema kcs;
	int mandatoryColumnsProcessed = 0;
	final int schemaType;
	final OraCdcTableBase table;
	final OraCdcLobTransformationsIntf transformLobs;
	final KafkaRdbmsInfoStruct kris;
	final String kafkaTopic;
	Struct keyStruct;
	Struct valueStruct;
	Struct struct;
	Schema schema;
	Schema keySchema;
	Schema valueSchema;

	KafkaStructDataBinder(final OraCdcSourceConnectorConfig config, final OraRdbmsInfo rdbmsInfo, final OraCdcTableBase table) {
		this.table = table;
		kris = new KafkaRdbmsInfoStruct(rdbmsInfo);
		kcs = new KafkaConnectSchema(rdbmsInfo, table);
		snm = ((KafkaSourceConnectorConfig) config).getSchemaNameMapper();
		snm.configure(config);
		schemaType = config.schemaType();
		topicPartition = ((KafkaSourceConnectorConfig) config).topicPartition();
		sourcePartition = rdbmsInfo.partition();
		final KafkaTopicNameMapper topicNameMapper = ((KafkaSourceConnectorConfig) config).getTopicNameMapper();
		topicNameMapper.configure(config);
		kafkaTopic = topicNameMapper.getTopicName(table.pdb(), table.owner(), table.name());
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Kafka topic for table {} set to {}.", table.fqn(), kafkaTopic);
		pseudoColumns = config.pseudoColumnsProcessor();
		if (config.processLobs())
			transformLobs = config.transformLobsImpl();
		else
			transformLobs = null;

	}

	public void init(OraCdcStatementBase stmt) {
		this.stmt = stmt;
		keyStruct = (table.flags() & FLG_ONLY_VALUE) > 0 ? null : new Struct(keySchema);
		valueStruct = new Struct(valueSchema);
		mandatoryColumnsProcessed = 0;
	}

	public abstract void insert(OraCdcColumn column, Object value);
	public abstract void delete(OraCdcColumn column, Object value);
	public abstract void update(OraCdcColumn column, Object value, boolean after);
	public void addRowId(OraCdcStatementBase stmt) {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Do primary key substitution for table {}", table.fqn());
		if (LOGGER.isTraceEnabled())
			LOGGER.trace("{} is used as primary key for table {}", stmt.getRowId(), table.fqn());
	}
	public abstract void afterBefore();

	public void buildSchema(final boolean initial) throws SQLException {
		final SchemaBuilder keySchemaBuilder;
		if (initial) {
			if (schemaType == SCHEMA_TYPE_INT_KAFKA_STD ||
					schemaType == SCHEMA_TYPE_INT_DEBEZIUM) {
				if ((table.flags() & FLG_TABLE_WITH_PK) > 0 || (table.flags() & FLG_PSEUDO_KEY) > 0)
					keySchemaBuilder = schemaBuilder(true, 1);
				else
					keySchemaBuilder = null;
			} else {
				keySchemaBuilder = null;
			}
			kcs.decrypter(table.decrypter());
		} else {
			keySchemaBuilder = null;
		}
		final SchemaBuilder valueSchemaBuilder = schemaBuilder(false, table.version());
		for (final OraCdcColumn column : table.allColumns()) {
			if (column.largeObject()) {
				final Schema lobSchema = transformLobs.transformSchema(table.pdb(), table.owner(), table.name(), column, valueSchemaBuilder);
				if (lobSchema != null) {
					// BLOB/CLOB/NCLOB/XMLTYPE is transformed
					column.transformLob(true);
				}
			}
			var schema = kcs.get(column);
			if (schemaType == SCHEMA_TYPE_INT_KAFKA_STD) {
				if (column.partOfPk()) {
					if (initial)
						keySchemaBuilder.field(column.name(), schema);
				} else {
					if (!column.largeObject())
						valueSchemaBuilder.field(column.name(), schema);
				}
			} else {
				if (schemaType == SCHEMA_TYPE_INT_DEBEZIUM) {
					if (column.partOfPk()) {
						if (initial)
							keySchemaBuilder.field(column.name(), schema);
					}
				}
				if (!column.largeObject())
					valueSchemaBuilder.field(column.name(), schema);
			}
		}
		if (schemaType != SCHEMA_TYPE_INT_DEBEZIUM) {
			//TODO
			//TODO Beter handling for 'debezium'-like schemas are required for this case...
			//TODO
			((KafkaPseudoColumnsProcessor) pseudoColumns).addToSchema(valueSchemaBuilder);
		}
		// Epilogue
		if (keySchemaBuilder == null) {
			if (initial)
				keySchema = null;
		} else {
			keySchema = keySchemaBuilder.build();
		}
		if (this.schemaType == SCHEMA_TYPE_INT_DEBEZIUM) {
			valueSchema = valueSchemaBuilder.build();
			schema = SchemaBuilder
					.struct()
					.name(snm.getEnvelopeSchemaName(table.pdb(), table.owner(), table.name()))
					.version(table.version())
					.field("before", valueSchema)
					.field("after", valueSchema)
					.field("source", kris.schema())
					.field("op", Schema.STRING_SCHEMA)
					.field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
					.field("ts_ns", Schema.OPTIONAL_INT64_SCHEMA)
					.build();
		} else {
			valueSchema = valueSchemaBuilder.build();
		}

		if (LOGGER.isDebugEnabled()) {
			if (table.mandatoryColumnsCount() > 0) {
				LOGGER.debug("Table {} has {} mandatory columns.", table.fqn(), table.mandatoryColumnsCount());
			}
			if (keySchema != null &&
					keySchema.fields() != null &&
					keySchema.fields().size() > 0) {
				LOGGER.debug("Key fields for table {}.", table.fqn());
				keySchema.fields().forEach(f ->
					LOGGER.debug(
							"\t{} with schema {}",
							f.name(), f.schema().name() != null ? f.schema().name() : f.schema().toString()));
			}
			if (valueSchema != null &&
					valueSchema.fields() != null &&
					valueSchema.fields().size() > 0) {
				LOGGER.debug("Value fields for table {}.", table.fqn());
				valueSchema.fields().forEach(f ->
				LOGGER.debug(
						"\t{} with schema {}",
						f.name(), f.schema().name() != null ? f.schema().name() : f.schema().toString()));
			}
		}
	}

	private SchemaBuilder schemaBuilder(final boolean isKey, final int version) {
		if (isKey)
			return SchemaBuilder
				.struct()
				.name(snm.getKeySchemaName(table.pdb(), table.owner(), table.name()))
				.version(1);
		else {
			if ((table.flags() & FLG_ALL_COLS_ON_DELETE) > 0)
				return SchemaBuilder
						.struct()
						.name(snm.getValueSchemaName(table.pdb(), table.owner(), table.name()))
						.version(version);
			else
				return SchemaBuilder
						.struct()
						.optional()
						.name(snm.getValueSchemaName(table.pdb(), table.owner(), table.name()))
						.version(version);
		}
	}

	@Override
	public SourceRecord changeVector(OraCdcTransaction transaction, Map<String, Object> offset, boolean skipRedoRecord) throws SQLException {
		if (skipRedoRecord)
			return null;
		else {
			SourceRecord sourceRecord = null;
			if ((table.flags() & FLG_SUPPLEMENTAL_LOG_ALL) > 0 && mandatoryColumnsProcessed < table.mandatoryColumnsCount()) {
				if (stmt.getOperation() != DELETE) {
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug(
								"Mandatory columns count for table {} is {}, but only {} mandatory columns are returned from redo record!",
								table.fqn(), table.mandatoryColumnsCount(), mandatoryColumnsProcessed);
					}

					if ((table.flags() & FLG_TOLERATE_INCOMPLETE_ROW) > 0) {
						LOGGER.error(TOLERANCE_ERR_MSG, table.fqn(), table.mandatoryColumnsCount(),
								mandatoryColumnsProcessed,
								Long.toUnsignedString(stmt.getScn()), stmt.getRba(),
								Long.toUnsignedString(transaction.getCommitScn()), transaction.getXid(),
								stmt.getSqlRedo(), "Skipping!");
						return null;
					} else {
						LOGGER.error(TOLERANCE_ERR_MSG, table.fqn(), table.mandatoryColumnsCount(),
								mandatoryColumnsProcessed,
								Long.toUnsignedString(stmt.getScn()), stmt.getRba(),
								Long.toUnsignedString(transaction.getCommitScn()), transaction.getXid(),
								stmt.getSqlRedo(), "Exiting!");
						throw new OraCdcDataException("Incomplete redo record!");
					}
				} else if ((table.flags() & FLG_PSEUDO_KEY) == 0) {
					// With ROWID we does not need more checks...
					//TODO - logic for delete only with primary columns!
					//TODO
					//TODO - logic for delete with all columns
				}
			}

			if (schemaType == SCHEMA_TYPE_INT_DEBEZIUM) {
				switch (stmt.getOperation()) {
					case INSERT -> {
						struct.put("after", valueStruct);
						struct.put("op", "c");
					}
					case DELETE -> {
						struct.put("before", valueStruct);
						struct.put("op", "d");
					}
					case UPDATE -> {
						struct.put("before", valueStruct);
						struct.put("op", "u");
					}
				}
				struct.put("source", kris.getStruct(table, stmt, transaction));
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
				if (!(stmt.getOperation() == DELETE && (table.flags() & FLG_ALL_COLS_ON_DELETE) == 0)) {
					//TODO
					//TODO Beter handling for 'debezium'-like schemas are required for this case...
					//TODO
					((KafkaPseudoColumnsProcessor) pseudoColumns).addToStruct(valueStruct, stmt, transaction);
				}
				if ((table.flags() & FLG_ONLY_VALUE) > 0) {
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
								(table.flags() & FLG_ALL_COLS_ON_DELETE) == 0) {
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
					sourceRecord.headers().addString("op",
							stmt.getOperation() == INSERT ? "c" : stmt.getOperation() == UPDATE ? "u" : "d");
				}
			}
			return sourceRecord;
		}
	}


}
