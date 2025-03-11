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

import static solutions.a2.kafka.transforms.ConversionType.ALL;
import static solutions.a2.kafka.transforms.ConversionType.STARTS_WITH;
import static solutions.a2.kafka.transforms.ConversionType.ENDS_WITH;
import static solutions.a2.kafka.transforms.ConversionType.SPECIFIC;
import static solutions.a2.kafka.transforms.SchemaAndStructUtils.copySchemaBasics;
import static solutions.a2.kafka.transforms.SchemaAndStructUtils.requireMap;
import static solutions.a2.kafka.transforms.SchemaAndStructUtils.requireStructOrNull;


import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.sql.NUMBER;
import solutions.a2.cdc.oracle.data.OraNumber;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public abstract class OraNumberConverter <R extends ConnectRecord<R>> implements Transformation<R>  {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraNumberConverter.class);
	private static final String PURPOSE = "convert solutions.a2.cdc.oracle.data.OraNumber into String/FLOAT32/FLOAT64/org.apache.kafka.connect.data.Decimal";

	private static final String FIELD_PARAM = "field";

	private static final String TARGET_TYPE_PARAM = "target.type";
	private static final String TARGET_TYPE_STRING = "string";
	private static final String TARGET_TYPE_DOUBLE = "double";
	private static final String TARGET_TYPE_FLOAT = "float";
	private static final String TARGET_TYPE_LONG = "long";
	private static final String TARGET_TYPE_INT = "int";
	private static final String TARGET_TYPE_SHORT = "short";
	private static final String TARGET_TYPE_BYTE = "byte";
	private static final String TARGET_TYPE_DECIMAL = "decimal";

	private static final String DECIMAL_SCALE_PARAM = "decimal.scale";
	private static final int DECIMAL_SCALE_DEFAULT = 2;

	private static final String REPLACE_NULL_WITH_DEFAULT_PARAM = "replace.null.with.default";

	private static final String CONV_ERROR_MSG =
			"\n=====================\n" +
			"Unable to convert oracle.sql.NUMBER to {}! Exception:\n\t{}" +
			"\n=====================\n";


	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(
					FIELD_PARAM, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
					"The field containing solutions.a2.cdc.oracle.data.OraNumber.\n" +
					"When set to empty value converter processes all fields with type solutions.a2.cdc.oracle.data.OraNumber.\n" +
					"Default - ''")
			.define(
					TARGET_TYPE_PARAM, ConfigDef.Type.STRING, TARGET_TYPE_STRING,
					ConfigDef.ValidString.in(TARGET_TYPE_STRING, TARGET_TYPE_DECIMAL, TARGET_TYPE_DOUBLE, TARGET_TYPE_FLOAT, TARGET_TYPE_LONG, TARGET_TYPE_INT, TARGET_TYPE_SHORT),
					ConfigDef.Importance.HIGH,
					"The type to which the value of solutions.a2.cdc.oracle.data.OraNumber will be converted. Default - 'string'")
			.define(
					DECIMAL_SCALE_PARAM, ConfigDef.Type.INT, DECIMAL_SCALE_DEFAULT, ConfigDef.Importance.HIGH,
					"Decimal scale of org.apache.kafka.connect.data.Decimal")
			.define(
					REPLACE_NULL_WITH_DEFAULT_PARAM, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
					"Whether to replace fields that have a default value and that are null to the default value.\n" +
					"When set to true, the default value is used, otherwise null is used.\nDefault - 'true'");

	private static Map<String, OraNumberTranslator> CONVERTERS = new HashMap<>();
	static {
		CONVERTERS.put(TARGET_TYPE_STRING, new OraNumberTranslator() {
			@Override
			public Schema typeSchema(final boolean isOptional, final ParamHolder params) {
				return isOptional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
			}
			@Override
			public String toType(final ParamHolder params, final NUMBER number) {
				return number.stringValue();
			}
		});
		CONVERTERS.put(TARGET_TYPE_DECIMAL, new OraNumberTranslator() {
			@Override
			public Schema typeSchema(final boolean isOptional, final ParamHolder params) {
				final SchemaBuilder builder = Decimal.builder(params.scale);
				return isOptional ? builder.optional().build() : builder.build();
			}
			@Override
			public BigDecimal toType(final ParamHolder params, final NUMBER number) {
				try {
					return number.bigDecimalValue().setScale(params.scale);
				} catch (SQLException sqle) {
					LOGGER.error(CONV_ERROR_MSG, TARGET_TYPE_DECIMAL, sqle.getMessage());
					return null;
				}
			}
		});
		CONVERTERS.put(TARGET_TYPE_DOUBLE, new OraNumberTranslator() {
			@Override
			public Schema typeSchema(final boolean isOptional, final ParamHolder params) {
				return isOptional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
			}
			@Override
			public Double toType(final ParamHolder params, final NUMBER number) {
				return number.doubleValue();
			}
		});
		CONVERTERS.put(TARGET_TYPE_FLOAT, new OraNumberTranslator() {
			@Override
			public Schema typeSchema(final boolean isOptional, final ParamHolder params) {
				return isOptional ? Schema.OPTIONAL_FLOAT32_SCHEMA : Schema.FLOAT32_SCHEMA;
			}
			@Override
			public Float toType(final ParamHolder params, final NUMBER number) {
				return number.floatValue();
			}
		});
		CONVERTERS.put(TARGET_TYPE_LONG, new OraNumberTranslator() {
			@Override
			public Schema typeSchema(final boolean isOptional, final ParamHolder params) {
				return isOptional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
			}
			@Override
			public Long toType(final ParamHolder params, final NUMBER number) {
				try {
					return number.longValue();
				} catch (SQLException sqle) {
					LOGGER.error(CONV_ERROR_MSG, TARGET_TYPE_LONG, sqle.getMessage());
					return null;
				}
			}
		});
		CONVERTERS.put(TARGET_TYPE_INT, new OraNumberTranslator() {
			@Override
			public Schema typeSchema(final boolean isOptional, final ParamHolder params) {
				return isOptional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA;
			}
			@Override
			public Integer toType(final ParamHolder params, final NUMBER number) {
				try {
					return number.intValue();
				} catch (SQLException sqle) {
					LOGGER.error(CONV_ERROR_MSG, TARGET_TYPE_INT, sqle.getMessage());
					return null;
				}
			}
		});
		CONVERTERS.put(TARGET_TYPE_SHORT, new OraNumberTranslator() {
			@Override
			public Schema typeSchema(final boolean isOptional, final ParamHolder params) {
				return isOptional ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA;
			}
			@Override
			public Short toType(final ParamHolder params, final NUMBER number) {
				try {
					return number.shortValue();
				} catch (SQLException sqle) {
					LOGGER.error(CONV_ERROR_MSG, TARGET_TYPE_SHORT, sqle.getMessage());
					return null;
				}
			}
		});
		CONVERTERS.put(TARGET_TYPE_BYTE, new OraNumberTranslator() {
			@Override
			public Schema typeSchema(final boolean isOptional, final ParamHolder params) {
				return isOptional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
			}
			@Override
			public Byte toType(final ParamHolder params, final NUMBER number) {
				try {
					return number.byteValue();
				} catch (SQLException sqle) {
					LOGGER.error(CONV_ERROR_MSG, TARGET_TYPE_SHORT, sqle.getMessage());
					return null;
				}
			}
		});
	}

	private final ParamHolder params = new ParamHolder();
	private Cache<Schema, Schema> schemaUpdateCache;
	private boolean replaceNullWithDefault;

	@Override
	public void configure(Map<String, ?> configs) {
		final SimpleConfig simpleConfig = new SimpleConfig(CONFIG_DEF, configs);
		params.field = simpleConfig.getString(FIELD_PARAM);
		if (StringUtils.isBlank(params.field)) {
			params.convType = ALL;
		}
		else if (StringUtils.startsWith(params.field, "%")) {
			params.convType = ENDS_WITH;
			params.field = StringUtils.substring(params.field, 1);
		} else if (StringUtils.endsWith(params.field, "%")) {
			params.convType = STARTS_WITH;
			params.field = StringUtils.substring(params.field, 0, params.field.length() - 1);
		} else {
			params.convType = SPECIFIC;
		}
		params.targetType = simpleConfig.getString(TARGET_TYPE_PARAM);
		params.scale = simpleConfig.getInt(DECIMAL_SCALE_PARAM);
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
	}

	protected abstract Schema operatingSchema(R record);
	protected abstract Object operatingValue(R record);
	protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

	public static class Key<R extends ConnectRecord<R>> extends OraNumberConverter<R> {
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

	public static class Value<R extends ConnectRecord<R>> extends OraNumberConverter<R> {
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
		if (updatedSchema == null) {
			final SchemaBuilder builder = copySchemaBasics(schema, SchemaBuilder.struct());
			if (params.convType == ALL) {
				for (Field field: schema.fields()) {
					if (field.schema().type() == Schema.Type.BYTES &&
							StringUtils.equals(field.schema().name(), OraNumber.LOGICAL_NAME)) {
						builder.field(field.name(), CONVERTERS.get(params.targetType).typeSchema(field.schema().isOptional(), params));
					} else {
						builder.field(field.name(), field.schema());
					}					
				}
			} else if (params.convType == STARTS_WITH ||
					params.convType == ENDS_WITH) {
				for (Field field: schema.fields()) {
					if (field.schema().type() == Schema.Type.BYTES &&
							StringUtils.equals(field.schema().name(), OraNumber.LOGICAL_NAME) && (
							(params.convType == STARTS_WITH && field.name().startsWith(params.field)) ||
							(params.convType == ENDS_WITH && field.name().endsWith(params.field)))) {
						builder.field(field.name(), CONVERTERS.get(params.targetType).typeSchema(field.schema().isOptional(), params));
					} else {
						builder.field(field.name(), field.schema());
					}					
				}				
			} else {
				for (Field field : schema.fields()) {
					if (StringUtils.equals(field.name(), params.field)) {
						final OraNumberTranslator translator = CONVERTERS.get(params.targetType);
						if (translator == null) {
							LOGGER.error(
									"\n=====================\n" +
									"Unable to find mapping for field '{}' and target type '{}'!\n" +
									"Original data are passed back to Sink connector!" +
									"\n=====================\n",
									params.field, params.targetType);
							builder.field(field.name(), field.schema());
						} else {
							builder.field(field.name(), translator.typeSchema(field.schema().isOptional(), params));
						}
					} else {
						builder.field(field.name(), field.schema());
					}
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

		final Struct updatedValue = applyValueWithSchema(value, updatedSchema);
		return newRecord(record, updatedSchema, updatedValue);
	}

	private Struct applyValueWithSchema(Struct value, Schema updatedSchema) {
		if (value == null) {
			return null;
		} else {
			Struct updatedValue = new Struct(updatedSchema);
			for (Field field : value.schema().fields()) {
				final Object updatedFieldValue;
				if (params.convType == ALL && (
						field.schema().type() == Schema.Type.BYTES &&
						StringUtils.equals(field.schema().name(), OraNumber.LOGICAL_NAME))) {
					updatedFieldValue = convertOraNumber(getFieldValue(value, field));
				} else  if (params.convType == SPECIFIC &&
						StringUtils.equals(field.name(), params.field)) {
						updatedFieldValue = convertOraNumber(getFieldValue(value, field));
				} else 	if (field.schema().type() == Schema.Type.BYTES &&
						StringUtils.equals(field.schema().name(), OraNumber.LOGICAL_NAME) && (
						(params.convType == STARTS_WITH && field.name().startsWith(params.field)) ||
						(params.convType == ENDS_WITH && field.name().endsWith(params.field)))) {
					updatedFieldValue = convertOraNumber(getFieldValue(value, field));
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
		if (rawValue == null || StringUtils.isBlank(params.field)) {
			return newRecord(record, null, convertOraNumber(rawValue));
		} else {
			final Map<String, Object> value = requireMap(rawValue, PURPOSE);
			final HashMap<String, Object> updatedValue = new HashMap<>(value);
			updatedValue.put(params.field, convertOraNumber(value.get(params.field)));
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

	private Object convertOraNumber(final Object ba) {
		if (ba == null) {
			return null;
		}
		final OraNumberTranslator translator = CONVERTERS.get(params.targetType);
		if (translator == null) {
			throw new ConnectException("Unsupported type for conversion: " + params.targetType);
		} else if (ba instanceof byte[]) {
			return translator.toType(params, new NUMBER((byte[]) ba));
		} else if (ba instanceof ByteBuffer) {
			return translator.toType(params, new NUMBER(((ByteBuffer) ba).array()));
		} else {
			throw new ConnectException("Unsupported source type for conversion: " + ba.getClass().getName());
		}
	}

	private interface OraNumberTranslator {
		/**
		 * Get the schema for this format.
		 */
		Schema typeSchema(final boolean isOptional, final ParamHolder params);

		/**
		 * Convert from the oracle.sql.NUMBER format to the type-specific format
		 */
		Object toType(final ParamHolder params, final NUMBER number);
	}

	private static class ParamHolder {
		String field;
		String targetType;
		int scale;
		ConversionType convType;
	}

}
