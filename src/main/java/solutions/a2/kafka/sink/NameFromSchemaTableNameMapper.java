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

package solutions.a2.kafka.sink;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.runtime.config.Parameters;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class NameFromSchemaTableNameMapper implements TableNameMapper {

	private static final Logger LOGGER = LoggerFactory.getLogger(NameFromSchemaTableNameMapper.class);

	private final Map<String, String> preparedNames = new HashMap<>();
	private List<String> schemaPrefixes;
	private String prefix;
	private String suffix;
	private int schemaType;

	@Override
	public void configure(final JdbcSinkConnectorConfig config) {
		schemaPrefixes = config.schemaPrefix();
		prefix = StringUtils.trim(config.getTableNamePrefix());
		suffix = StringUtils.trim(config.getTableNameSuffix());
		schemaType = config.getSchemaType();
	}

	@Override
	public String getTableName(final SinkRecord record) {
		String tableName;
		if (schemaType == Parameters.SCHEMA_TYPE_INT_KAFKA_STD ||
				schemaType == Parameters.SCHEMA_TYPE_INT_SINGLE) {
			tableName = preparedNames.get(record.valueSchema().name());
			if (tableName == null) {
				var schemaName = StringUtils.substring(record.valueSchema().name(), 0, Strings.CS.lastIndexOf(record.valueSchema().name(), "Value") - 1);
				var need2Build = false;
				String schemaPrefix = null;
				if (schemaPrefixes != null && schemaPrefixes.size() > 0) {
					for (var prfx : schemaPrefixes)
						if (Strings.CS.startsWith(schemaName, prfx)) {
							need2Build = true;
							schemaPrefix = prfx;
							break;
						}
				}
				if (need2Build) {
					tableName = prefix + StringUtils.substring(schemaName, schemaPrefix.length()) + suffix;
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Table name '{}' is set using the Kafka schema name {} and parameter '{}' with value {}.",
							tableName, schemaName, schemaPrefix);
					}
				} else {
					tableName = schemaName;
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Table name is set to the Kafka schema name '{}'.", tableName);
					}
				}
				preparedNames.put(record.valueSchema().name(), tableName);
			}
		} else { //schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM
			tableName = ((Struct) record.value()).getStruct("source").getString("table");
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Table name '{}' is set using the 'source' field in SinkRecord.", tableName);
			}
		}
		return tableName;
	}

}
