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

package solutions.a2.kafka.transforms;

import static solutions.a2.kafka.transforms.SchemaAndStructUtils.copySchemaBasics;
import static solutions.a2.kafka.transforms.SchemaAndStructUtils.requireMap;
import static solutions.a2.kafka.transforms.SchemaAndStructUtils.requireStructOrNull;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.data.OraInterval;
import solutions.a2.cdc.oracle.data.OraIntervalDS;
import solutions.a2.cdc.oracle.data.OraIntervalYM;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public abstract class OraIntervalConverter <R extends ConnectRecord<R>> implements Transformation<R>  {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraIntervalConverter.class);
	private static final String PURPOSE = "convert solutions.a2.cdc.oracle.data.OraIntervalDS and solutions.a2.cdc.oracle.data.OraIntervalYM into ISO-8601 String";

	private static final String FIELD_PARAM = "field";

	private static final String REPLACE_NULL_WITH_DEFAULT_PARAM = "replace.null.with.default";

	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(
					FIELD_PARAM, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
					"The field containing solutions.a2.cdc.oracle.data.OraIntervalDS or solutions.a2.cdc.oracle.data.OraIntervalYM.\n" +
					"When set to empty value converter processes all fields with type solutions.a2.cdc.oracle.data.OraIntervalDS and solutions.a2.cdc.oracle.data.OraIntervalYM.\n" +
					"Default - ''")
			.define(
					REPLACE_NULL_WITH_DEFAULT_PARAM, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
					"Whether to replace fields that have a default value and that are null to the default value.\n" +
					"When set to true, the default value is used, otherwise null is used.\nDefault - 'true'");

	private String fieldName = null;
	private Cache<Schema, Schema> schemaUpdateCache;
	private boolean replaceNullWithDefault;

	@Override
	public void configure(Map<String, ?> configs) {
		final SimpleConfig simpleConfig = new SimpleConfig(CONFIG_DEF, configs);
		fieldName = simpleConfig.getString(FIELD_PARAM);
		replaceNullWithDefault = simpleConfig.getBoolean(REPLACE_NULL_WITH_DEFAULT_PARAM);
		schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
	}

	@Override
	public R apply(R record) {
		if (operatingSchema(record) == null) {
			return applySchemaless(record);
		} else {
			return applyWithSchema(record);
		}
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	@Override
	public void close() {
		schemaUpdateCache = null;
	}

	protected abstract Schema operatingSchema(R record);
	protected abstract Object operatingValue(R record);
	protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

	public static class Key<R extends ConnectRecord<R>> extends OraIntervalConverter<R> {
		@Override
		protected Schema operatingSchema(R record) {
			return record.keySchema();
		}

		@Override
		protected Object operatingValue(R record) {
			return record.key();
		}

		@Override
		protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
			return record.newRecord(
					record.topic(), record.kafkaPartition(), updatedSchema, updatedValue,
					record.valueSchema(), record.value(), record.timestamp());
		}
	}

	public static class Value<R extends ConnectRecord<R>> extends OraIntervalConverter<R> {
		@Override
		protected Schema operatingSchema(R record) {
			return record.valueSchema();
		}

		@Override
		protected Object operatingValue(R record) {
			return record.value();
		}

		@Override
		protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
			return record.newRecord(
					record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
					updatedSchema, updatedValue, record.timestamp());
		}
	}

	private R applyWithSchema(R record) {
		final Schema schema = operatingSchema(record);
		final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
		Schema updatedSchema = schemaUpdateCache.get(schema);
		if (StringUtils.isBlank(fieldName)) {
			if (updatedSchema == null) {
				final SchemaBuilder builder = copySchemaBasics(schema, SchemaBuilder.struct());
				for (Field field: schema.fields()) {
					if (field.schema().type() == Schema.Type.BYTES && (
							Strings.CS.equals(field.schema().name(), OraIntervalYM.LOGICAL_NAME) ||
							Strings.CS.equals(field.schema().name(), OraIntervalDS.LOGICAL_NAME))) {
						builder.field(field.name(), field.schema().isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
					} else {
						builder.field(field.name(), field.schema());
					}					
				}
				if (schema.isOptional()) {
					builder.optional();
				}
				if (schema.defaultValue() != null) {
					Struct updatedDefaultValue = applyValueWithSchema((Struct) schema.defaultValue(), builder);
					builder.defaultValue(updatedDefaultValue);
				}

				updatedSchema = builder.build();
				schemaUpdateCache.put(schema, updatedSchema);
			}
		} else {
			if (updatedSchema == null) {
				final SchemaBuilder builder = copySchemaBasics(schema, SchemaBuilder.struct());
				for (Field field : schema.fields()) {
					if (Strings.CS.equals(field.name(), fieldName)) {
						if (Strings.CS.equals(field.schema().name(), OraIntervalYM.LOGICAL_NAME) ||
								Strings.CS.equals(field.schema().name(), OraIntervalDS.LOGICAL_NAME)) {
							builder.field(field.name(), field.schema().isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
						} else {
							LOGGER.warn("The field {} is not of the correct type for {} converter!",
									fieldName, this.getClass().getName());
							builder.field(field.name(), field.schema());
						}
					} else {
						builder.field(field.name(), field.schema());
					}
				}
				if (schema.isOptional()) {
					builder.optional();
				}
				if (schema.defaultValue() != null) {
					Struct updatedDefaultValue = applyValueWithSchema((Struct) schema.defaultValue(), builder);
					builder.defaultValue(updatedDefaultValue);
				}

				updatedSchema = builder.build();
				schemaUpdateCache.put(schema, updatedSchema);
			}
        }
		final Struct updatedValue = applyValueWithSchema(value, updatedSchema);
		return newRecord(record, updatedSchema, updatedValue);
	}

	private Struct applyValueWithSchema(Struct value, Schema updatedSchema) {
		if (value == null) {
			return null;
		} else {
			Struct updatedValue = new Struct(updatedSchema);
			final boolean processAll = StringUtils.isBlank(fieldName);
			for (Field field : value.schema().fields()) {
				final Object updatedFieldValue;
				if (processAll && (
						field.schema().type() == Schema.Type.BYTES && (
								Strings.CS.equals(field.schema().name(), OraIntervalYM.LOGICAL_NAME) ||
								Strings.CS.equals(field.schema().name(), OraIntervalDS.LOGICAL_NAME)))) {
					final Object fieldValue = getFieldValue(value, field);
					if (fieldValue instanceof byte[]) {
						updatedFieldValue = convertOraInterval((byte[]) fieldValue);
					} else if (fieldValue instanceof ByteBuffer) {
						updatedFieldValue = convertOraInterval(((ByteBuffer) fieldValue).array());
					} else {
						throw new ConnectException("Unsupported source type for conversion: " + fieldValue.getClass().getName());
					}
				} else  if (!processAll &&
						Strings.CS.equals(field.name(), fieldName)) {
					final Object fieldValue = getFieldValue(value, field);
					if (fieldValue instanceof byte[]) {
						updatedFieldValue = convertOraInterval((byte[]) fieldValue);
					} else if (fieldValue instanceof ByteBuffer) {
						updatedFieldValue = convertOraInterval(((ByteBuffer) fieldValue).array());
					} else {
						throw new ConnectException("Unsupported source type for conversion: " + fieldValue.getClass().getName());
					}
				} else {
					updatedFieldValue = getFieldValue(value, field);
				}
				updatedValue.put(field.name(), updatedFieldValue);
			}
			return updatedValue;
		}
	}

	private R applySchemaless(R record) {
		Object rawValue = operatingValue(record);
		if (rawValue == null || StringUtils.isBlank(fieldName)) {
			return newRecord(record, null, convertOraInterval((byte[]) rawValue));
		} else {
			final Map<String, Object> value = requireMap(rawValue, PURPOSE);
			final HashMap<String, Object> updatedValue = new HashMap<>(value);
			updatedValue.put(fieldName, convertOraInterval((byte[]) value.get(fieldName)));
			return newRecord(record, null, updatedValue);
		}
	}

	private Object getFieldValue(final Struct value, final Field field) {
		if (replaceNullWithDefault) {
			return value.get(field);
		} else {
			return value.getWithoutDefault(field.name());
		}
	}

	private String convertOraInterval(final byte[] ba) {
		if (ba == null) {
			return null;
		} else {
			return OraInterval.fromLogical(ba);
		}
	}

}
