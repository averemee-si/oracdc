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
