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
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.kafka.sink;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import solutions.a2.cdc.oracle.runtime.config.Parameters;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class DefaultTableNameMapper implements TableNameMapper {

	private static final Logger LOGGER = LogManager.getLogger(DefaultTableNameMapper.class);

	private String topicPrefix;
	private String prefix;
	private String suffix;
	private int schemaType;

	@Override
	public void configure(final KafkaSinkConfig config) {
		topicPrefix = config.topicPrefix();
		prefix = StringUtils.trim(config.tableNamePrefix());
		suffix = StringUtils.trim(config.tableNameSuffix());
		schemaType = config.schemaType();
	}

	@Override
	public String getTableName(final SinkRecord record) {
		final String tableName;
		if (schemaType == Parameters.SCHEMA_TYPE_INT_KAFKA_STD ||
				schemaType == Parameters.SCHEMA_TYPE_INT_SINGLE) {
			if (StringUtils.isNotBlank(topicPrefix) &&
					Strings.CS.startsWith(record.topic(), topicPrefix)) {
				tableName = prefix + StringUtils.substring(record.topic(), topicPrefix.length()) + suffix;
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Table name '{}' is set using the Kafka topic name {} and parameter '{}' with value {}.",
						tableName, record.topic(), topicPrefix);
				}
			} else {
				tableName = record.topic();
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Table name is set to the Kafka topic name '{}'.", tableName);
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
