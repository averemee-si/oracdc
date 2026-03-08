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

package solutions.a2.cdc.oracle.runtime.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class KafkaFlexibleTopicNameMapper extends KafkaDefaultTopicNameMapper {

	private Map<String, String> tableTopicMap = null;

	@Override
	public void configure(OraCdcSourceConnectorConfig config) {
		var topicParams = ((KafkaSourceConnectorConfig)config).topicMapParams();
		if (topicParams != null && topicParams.size() > 0) {
			tableTopicMap = new HashMap<>();
			topicParams.forEach((param, value) -> {
				var tables = StringUtils.split(value, ',');
				for (var tfqn : tables)
					tableTopicMap.put(tfqn, param);
			});
		}
		super.configure(config);
	}

	@Override
	public String getTopicName(
			final String pdbName, final String tableOwner, final String tableName) {
		if (tableTopicMap != null) {
			var sb = new StringBuilder(0x80);
			if (pdbName != null)
				sb
					.append(pdbName)
					.append('.');
			sb
				.append(tableOwner)
				.append('.')
				.append(tableName);
			var topicName = tableTopicMap.get(sb.toString());
			if (topicName != null)
				return topicName;
		}
		return super.getTopicName(pdbName, tableOwner, tableName);
	}

}
