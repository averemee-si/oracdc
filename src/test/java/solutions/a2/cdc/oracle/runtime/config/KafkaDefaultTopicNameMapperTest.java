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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;

import org.junit.jupiter.api.Test;
import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;


/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class KafkaDefaultTopicNameMapperTest {

	@Test
	public void testPrefix() {
		var props = new HashMap<String, String>();
		var prefix = "OEBS";
		var tableName = "OE_ORDER_LINES_ALL";
		props.put("a2.topic.mapper", Parameters.TOPIC_MAPPER_DEFAULT);
		props.put("a2.topic.prefix", prefix);
		props.put("a2.topic.name.style", Parameters.TOPIC_NAME_STYLE_TABLE);
		props.put("a2.topic.name.delimiter", Parameters.TOPIC_NAME_DELIMITER_UNDERSCORE);

		final OraCdcSourceConnectorConfig config = new KafkaSourceConnectorConfig(props);
		var tnm = ((KafkaSourceConnectorConfig)config).getTopicNameMapper();
		tnm.configure(config);

		assertEquals(prefix + Parameters.TOPIC_NAME_DELIMITER_UNDERSCORE + tableName, tnm.getTopicName(null, "ONT", tableName));
	}

	@Test
	public void testDelimDefault() {
		var props = new HashMap<String, String>();
		var owner = "ONT";
		var tableName = "OE_ORDER_LINES_ALL";
		props.put("a2.topic.mapper", Parameters.TOPIC_MAPPER_DEFAULT);
		props.put("a2.topic.name.style", Parameters.TOPIC_NAME_STYLE_SCHEMA_TABLE);

		final OraCdcSourceConnectorConfig config = new KafkaSourceConnectorConfig(props);
		var tnm = ((KafkaSourceConnectorConfig)config).getTopicNameMapper();
		tnm.configure(config);

		assertEquals(owner + Parameters.TOPIC_NAME_DELIMITER_UNDERSCORE + tableName, tnm.getTopicName(null, owner, tableName));
	}

	@Test
	public void testDelimUnderscore() {
		var props = new HashMap<String, String>();
		var owner = "ONT";
		var tableName = "OE_ORDER_LINES_ALL";
		props.put("a2.topic.mapper", Parameters.TOPIC_MAPPER_DEFAULT);
		props.put("a2.topic.name.style", Parameters.TOPIC_NAME_STYLE_SCHEMA_TABLE);
		props.put("a2.topic.name.delimiter", Parameters.TOPIC_NAME_DELIMITER_UNDERSCORE);

		final OraCdcSourceConnectorConfig config = new KafkaSourceConnectorConfig(props);
		var tnm = ((KafkaSourceConnectorConfig)config).getTopicNameMapper();
		tnm.configure(config);

		assertEquals(owner + Parameters.TOPIC_NAME_DELIMITER_UNDERSCORE + tableName, tnm.getTopicName(null, owner, tableName));
	}

	@Test
	public void testDelimDash() {
		var props = new HashMap<String, String>();
		var owner = "ONT";
		var tableName = "OE_ORDER_LINES_ALL";
		props.put("a2.topic.mapper", Parameters.TOPIC_MAPPER_DEFAULT);
		props.put("a2.topic.name.style", Parameters.TOPIC_NAME_STYLE_SCHEMA_TABLE);
		props.put("a2.topic.name.delimiter", Parameters.TOPIC_NAME_DELIMITER_DASH);

		final OraCdcSourceConnectorConfig config = new KafkaSourceConnectorConfig(props);
		var tnm = ((KafkaSourceConnectorConfig)config).getTopicNameMapper();
		tnm.configure(config);

		assertEquals(owner + Parameters.TOPIC_NAME_DELIMITER_DASH + tableName, tnm.getTopicName(null, owner, tableName));
	}

	@Test
	public void testDelimDot() {
		var props = new HashMap<String, String>();
		var owner = "ONT";
		var tableName = "OE_ORDER_LINES_ALL";
		props.put("a2.topic.mapper", Parameters.TOPIC_MAPPER_DEFAULT);
		props.put("a2.topic.name.style", Parameters.TOPIC_NAME_STYLE_SCHEMA_TABLE);
		props.put("a2.topic.name.delimiter", Parameters.TOPIC_NAME_DELIMITER_DOT);

		final OraCdcSourceConnectorConfig config = new KafkaSourceConnectorConfig(props);
		var tnm = ((KafkaSourceConnectorConfig)config).getTopicNameMapper();
		tnm.configure(config);

		assertEquals(owner + Parameters.TOPIC_NAME_DELIMITER_DOT + tableName, tnm.getTopicName(null, owner, tableName));
	}

	@Test
	public void testStyleTable() {
		var props = new HashMap<String, String>();
		var pdb = "R12";
		var owner = "ONT";
		var prefix = "OEBS";
		var tableName = "OE_ORDER_LINES_ALL";
		props.put("a2.topic.mapper", Parameters.TOPIC_MAPPER_DEFAULT);
		props.put("a2.topic.name.style", Parameters.TOPIC_NAME_STYLE_TABLE);
		props.put("a2.topic.name.delimiter", Parameters.TOPIC_NAME_DELIMITER_DOT);
		props.put("a2.topic.prefix", prefix);

		final OraCdcSourceConnectorConfig config = new KafkaSourceConnectorConfig(props);
		var tnm = ((KafkaSourceConnectorConfig)config).getTopicNameMapper();
		tnm.configure(config);

		assertEquals(prefix + Parameters.TOPIC_NAME_DELIMITER_DOT + tableName, tnm.getTopicName(pdb, owner, tableName));
	}

	@Test
	public void testStyleSchemaTable() {
		var props = new HashMap<String, String>();
		var pdb = "R12";
		var owner = "ONT";
		var prefix = "OEBS";
		var tableName = "OE_ORDER_LINES_ALL";
		props.put("a2.topic.mapper", Parameters.TOPIC_MAPPER_DEFAULT);
		props.put("a2.topic.name.style", Parameters.TOPIC_NAME_STYLE_SCHEMA_TABLE);
		props.put("a2.topic.name.delimiter", Parameters.TOPIC_NAME_DELIMITER_DOT);
		props.put("a2.topic.prefix", prefix);

		final OraCdcSourceConnectorConfig config = new KafkaSourceConnectorConfig(props);
		var tnm = ((KafkaSourceConnectorConfig)config).getTopicNameMapper();
		tnm.configure(config);

		assertEquals(prefix + Parameters.TOPIC_NAME_DELIMITER_DOT + owner + Parameters.TOPIC_NAME_DELIMITER_DOT + tableName,
				tnm.getTopicName(pdb, owner, tableName));
	}

	@Test
	public void testStylePdbSchemaTable() {
		var props = new HashMap<String, String>();
		var pdb = "R12";
		var owner = "ONT";
		var prefix = "OEBS";
		var tableName = "OE_ORDER_LINES_ALL";
		props.put("a2.topic.mapper", Parameters.TOPIC_MAPPER_DEFAULT);
		props.put("a2.topic.name.style", Parameters.TOPIC_NAME_STYLE_PDB_SCHEMA_TABLE);
		props.put("a2.topic.name.delimiter", Parameters.TOPIC_NAME_DELIMITER_DOT);
		props.put("a2.topic.prefix", prefix);

		final OraCdcSourceConnectorConfig config = new KafkaSourceConnectorConfig(props);
		var tnm = ((KafkaSourceConnectorConfig)config).getTopicNameMapper();
		tnm.configure(config);

		assertEquals(prefix + Parameters.TOPIC_NAME_DELIMITER_DOT + pdb + Parameters.TOPIC_NAME_DELIMITER_DOT + owner + Parameters.TOPIC_NAME_DELIMITER_DOT + tableName,
				tnm.getTopicName(pdb, owner, tableName));
	}

}
