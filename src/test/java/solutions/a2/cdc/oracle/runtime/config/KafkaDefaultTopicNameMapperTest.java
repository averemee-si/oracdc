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
