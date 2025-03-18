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
import static solutions.a2.kafka.transforms.SchemaAndStructUtils.requireStruct;

import java.util.List;
import java.util.Map;

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

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class KeyToValueConverter <R extends ConnectRecord<R>> implements Transformation<R>  {

	private static final String PURPOSE = "copies the key fields into the value structure";

	private static final String FIELDS_PARAM = "fields";

	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(
					FIELDS_PARAM, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
					"Header field");


	private List<String> fields;
	private Cache<Schema, Schema> schemaUpdateCache;

	@Override
	public void configure(Map<String, ?> configs) {
		final SimpleConfig simpleConfig = new SimpleConfig(CONFIG_DEF, configs);
		fields = simpleConfig.getList(FIELDS_PARAM);
		schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(0x10));
	}

	@Override
	public R apply(R record) {
		if (record.keySchema() == null) {
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

	private R applySchemaless(R record) {
        final Map<String, Object> key = requireMap(record.key(), PURPOSE);
        final Map<String, Object> value = requireMap(record.value(), PURPOSE);;
        for (String field : fields) {
            value.put(field, key.get(field));
        }
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), key, null, record.value(), record.timestamp());
	}

	private R applyWithSchema(R record) {
		final Struct key = requireStruct(record.key(), PURPOSE);
		final Struct value = requireStruct(record.value(), PURPOSE);
		Schema valueSchema = schemaUpdateCache.get(key.schema());
		if (valueSchema == null) {
			final SchemaBuilder builder = copySchemaBasics(value.schema(), SchemaBuilder.struct());
			for (final Field field : value.schema().fields())
				builder.field(field.name(), field.schema());
			for (final Field field : key.schema().fields())
				if (fields.contains(field.name()))
					builder.field(field.name(), field.schema());
			valueSchema = builder.build();
			schemaUpdateCache.put(key.schema(), valueSchema);
		}
		final Struct valueStruct = new Struct(valueSchema);
		for (Field field: value.schema().fields())
			valueStruct.put(field.name(), value.get(field));
		for (final Field field : key.schema().fields())
			if (fields.contains(field.name()))
				valueStruct.put(field.name(), key.get(field));
		return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), key, valueSchema, valueStruct, record.timestamp()); 
	}

}
