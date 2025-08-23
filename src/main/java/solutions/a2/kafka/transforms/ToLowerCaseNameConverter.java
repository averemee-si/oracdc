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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public abstract class ToLowerCaseNameConverter <R extends ConnectRecord<R>> implements Transformation<R>  {

	private static final Logger LOGGER = LoggerFactory.getLogger(ToLowerCaseNameConverter.class);
	private static final String PURPOSE = "сhanges field name to lower case (Oracle Database to Apache Iceberg table case)";

	private static final String FIELD_PARAM = "field";

	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(
					FIELD_PARAM, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH,
					"Field name (or % for all or list of field names)");

	private Set<String> fields;
	private Cache<Schema, Schema> schemaUpdateCache;
	private boolean allFields;

	@Override
	public void configure(Map<String, ?> configs) {
		final List<String> fieldList = (new SimpleConfig(CONFIG_DEF, configs)).getList(FIELD_PARAM);
		if (fieldList.size() == 0 ||
				(fieldList.size() == 1 &&  Strings.CS.equals(fieldList.get(0), "%"))) {
			allFields = true;
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("{} will change all field names to lower case.", this.getClass().getCanonicalName());
		} else {
			allFields = false;
			fields = new HashSet<>(fieldList);
		}
		schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(0x10));
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

	public static class Key<R extends ConnectRecord<R>> extends ToLowerCaseNameConverter<R> {
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

	public static class Value<R extends ConnectRecord<R>> extends ToLowerCaseNameConverter<R> {
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
		final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value.size());
        for (Map.Entry<String, Object> e : value.entrySet()) {
        	final String fieldName = e.getKey();
        	if (allFields || fields.contains(fieldName))
        		updatedValue.put(StringUtils.lowerCase(fieldName), e.getValue());
        	else
        		updatedValue.put(fieldName, e.getValue());
        }
		return newRecord(record, null, updatedValue);
	}

	private R applyWithSchema(R record) {
		final Schema schema = operatingSchema(record);
		final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
		Schema updatedSchema = schemaUpdateCache.get(schema);
		if (updatedSchema == null) {
			final SchemaBuilder builder = copySchemaBasics(schema, SchemaBuilder.struct());
			for (Field field: schema.fields()) {
				final String fieldName = field.name();
				if (allFields || fields.contains(fieldName))
					builder.field(StringUtils.lowerCase(fieldName), field.schema());
				else
					builder.field(fieldName, field.schema());
			}
			updatedSchema = builder.build();
			schemaUpdateCache.put(schema, updatedSchema);
		}
		final Struct updatedValue = new Struct(updatedSchema);
		for (Field field: schema.fields()) {
			final String fieldName = field.name();
			if (allFields || fields.contains(fieldName))
				updatedValue.put(StringUtils.lowerCase(fieldName), value.get(field));
			else
				updatedValue.put(fieldName, value.get(field));
		}
		return newRecord(record, updatedSchema, updatedValue);
	}

}
