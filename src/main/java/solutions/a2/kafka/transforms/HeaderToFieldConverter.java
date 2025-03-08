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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public abstract class HeaderToFieldConverter <R extends ConnectRecord<R>> implements Transformation<R>  {

	private static final Logger LOGGER = LoggerFactory.getLogger(HeaderToFieldConverter.class);
	private static final String PURPOSE = "copies the header field into the key or value structure and replaces values of this field ​​if necessary";

	private static final String HEADER_PARAM = "header";
	private static final String FIELD_PARAM = "field";
	private static final String MAP_PARAM = "map";

	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(
					HEADER_PARAM, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
					"Header field")
			.define(
					FIELD_PARAM, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
					"Value field")
			.define(
					MAP_PARAM, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
					"Mapping in in h1:v1[,h2:v2] format");


	private final ParamHolder params = new ParamHolder();
	private Cache<Schema, Schema> schemaUpdateCache;
	private boolean readyToConvert;
	private boolean mapValue;

	@Override
	public void configure(Map<String, ?> configs) {
		final SimpleConfig simpleConfig = new SimpleConfig(CONFIG_DEF, configs);
		params.header = simpleConfig.getString(HEADER_PARAM);
		params.field = simpleConfig.getString(FIELD_PARAM);
		if (StringUtils.isAllBlank(params.header, params.field)) {
			readyToConvert = false;
			LOGGER.error("");
		} else {
			readyToConvert = true;
			final String valueMapping = simpleConfig.getString(MAP_PARAM);
			if (StringUtils.isNotBlank(valueMapping) && StringUtils.contains(valueMapping, ':')) {
				mapValue = true;
				params.createMap(valueMapping);
			} else {
				params.map = null;
				mapValue = false;
			}
				
			schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(0x10));
		}
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

	public static class Key<R extends ConnectRecord<R>> extends HeaderToFieldConverter<R> {
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

	public static class Value<R extends ConnectRecord<R>> extends HeaderToFieldConverter<R> {
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

	private R applySchemaless(R record) {
		if (readyToConvert) {
			String headerValue = record.headers().lastWithName(params.header).value().toString();
			if (mapValue && params.map.containsKey(headerValue))
				headerValue = params.map.get(headerValue);
			final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
	        final Map<String, Object> updatedValue = new HashMap<>(value);
	        updatedValue.put(params.field, headerValue);
			return newRecord(record, null, updatedValue);
		} else
			return record;
	}

	private R applyWithSchema(R record) {
		if (readyToConvert) {
			final Schema schema = operatingSchema(record);
			final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
			Schema updatedSchema = schemaUpdateCache.get(schema);
			if (updatedSchema == null) {
				final SchemaBuilder builder = copySchemaBasics(schema, SchemaBuilder.struct());
				for (Field field: schema.fields())
					builder.field(field.name(), field.schema());
				builder.field(params.field, Schema.OPTIONAL_STRING_SCHEMA);
				updatedSchema = builder.build();
				schemaUpdateCache.put(schema, updatedSchema);
			}
			final Struct updatedValue = new Struct(updatedSchema);
			for (Field field: schema.fields())
				updatedValue.put(field.name(), value.get(field));
			String headerValue = record.headers().lastWithName(params.header).value().toString();
			if (mapValue && params.map.containsKey(headerValue))
				headerValue = params.map.get(headerValue);
	        updatedValue.put(params.field, headerValue);
			return newRecord(record, updatedSchema, updatedValue);
		} else
			return record;

	}

	private static class ParamHolder {
		String header;
		String field;
		Map<String, String> map;

		void createMap(final String value) {
			map = new HashMap<>();
			final String[] pairs = StringUtils.split(value, ',');
			for (final String mapping : pairs) {
				final String[] pair = StringUtils.split(StringUtils.trim(mapping), ':');
				if (pair.length == 2)
					map.put(pair[0], pair[1]);
				else
					LOGGER.error(
							"\n=====================\n" +
							"Unable to parse mapping '{}' from header '{}' to  field '{}'!" +
							"\n=====================\n",
							mapping, header, field);
			}

		}
	}

}
