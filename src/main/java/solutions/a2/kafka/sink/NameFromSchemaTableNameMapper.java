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

package solutions.a2.kafka.sink;

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

	private String schemaPrefix;
	private String prefix;
	private String suffix;
	private int schemaType;

	@Override
	public void configure(final JdbcSinkConnectorConfig config) {
		schemaPrefix = config.schemaPrefix();
		prefix = StringUtils.trim(config.getTableNamePrefix());
		suffix = StringUtils.trim(config.getTableNameSuffix());
		schemaType = config.getSchemaType();
	}

	@Override
	public String getTableName(final SinkRecord record) {
		final String tableName;
		if (schemaType == Parameters.SCHEMA_TYPE_INT_KAFKA_STD ||
				schemaType == Parameters.SCHEMA_TYPE_INT_SINGLE) {
			var schemaName = StringUtils.substring(record.valueSchema().name(), 0, Strings.CS.lastIndexOf(record.valueSchema().name(), "Value") - 1);
			if (StringUtils.isNotBlank(schemaPrefix) &&
					Strings.CS.startsWith(schemaName, schemaPrefix)) {
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
		} else { //schemaType == ParamConstants.SCHEMA_TYPE_INT_DEBEZIUM
			tableName = ((Struct) record.value()).getStruct("source").getString("table");
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Table name '{}' is set using the 'source' field in SinkRecord.", tableName);
			}
		}
		return tableName;
	}

}
