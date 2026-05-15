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

package solutions.a2.cdc.oracle.runtime.config;

import static solutions.a2.cdc.oracle.runtime.config.Parameters.TOPIC_NAME_STYLE_INT_SCHEMA_TABLE;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.TOPIC_NAME_STYLE_INT_TABLE;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.cdc.oracle.utils.KafkaUtils;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class KafkaDefaultTopicNameMapper implements KafkaTopicNameMapper {

	private static final Logger LOGGER = LogManager.getLogger(KafkaDefaultTopicNameMapper.class);

	private String topicParam;
	private int topicNameStyle;
	private String delimiter;

	@Override
	public void configure(OraCdcSourceConnectorConfig config) {
		topicParam = config.topicOrPrefix();
		topicNameStyle = config.getTopicNameStyle();
		delimiter = config.getTopicNameDelimiter();
	}

	@Override
	public String getTopicName(
			final String pdbName, final String tableOwner, final String tableName) {
		final String kafkaTopic;
		final var sb = new StringBuilder(256);
			// Add prefix
		if (StringUtils.isNotBlank(topicParam))
			sb
				.append(topicParam)
				.append(delimiter);

		if (topicNameStyle == TOPIC_NAME_STYLE_INT_TABLE)
			sb.append(tableName);
		else if (topicNameStyle == TOPIC_NAME_STYLE_INT_SCHEMA_TABLE)
			sb
				.append(tableOwner)
				.append(delimiter)
				.append(tableName);
		else {
			// topicNameStyle == ParamConstants.TOPIC_NAME_STYLE_INT_PDB_SCHEMA_TABLE
			if (StringUtils.isBlank(pdbName)) {
				sb
					.append(tableOwner)
					.append(delimiter)
					.append(tableName);
				LOGGER.warn(
						"""
						
						=====================
						Unable to use a2.topic.name.style=PDB_SCHEMA_TABLE in non-CDB database for table {}.{}!
						Topic name is set to '{}'
						=====================
						
						""", tableOwner, tableName, sb.toString());
			} else
				sb
					.append(pdbName)
					.append(delimiter)
					.append(tableOwner)
					.append(delimiter)
					.append(tableName);
		}
		if (KafkaUtils.validTopicName(sb.toString()))
			kafkaTopic = sb.toString();
		else {
			kafkaTopic = KafkaUtils.fixTopicName(sb.toString(), "zZ");
			LOGGER.warn(
					"""
					
					=====================
					Calculated topic name '{}' contains characters that are not supported ny Apache Kafka!
					Topic name changed to to '{}'
					=====================
					""", sb.toString(), kafkaTopic);
		}
		return kafkaTopic;
	}

}
