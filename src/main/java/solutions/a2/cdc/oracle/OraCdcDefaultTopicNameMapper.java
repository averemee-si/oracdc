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

package solutions.a2.cdc.oracle;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.utils.KafkaUtils;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcDefaultTopicNameMapper implements TopicNameMapper {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcDefaultTopicNameMapper.class);

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

		if (topicNameStyle == OraCdcSourceConnectorConfig.TOPIC_NAME_STYLE_INT_TABLE)
			sb.append(tableName);
		else if (topicNameStyle == OraCdcSourceConnectorConfig.TOPIC_NAME_STYLE_INT_SCHEMA_TABLE)
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
