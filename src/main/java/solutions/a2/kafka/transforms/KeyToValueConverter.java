/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.

 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
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
