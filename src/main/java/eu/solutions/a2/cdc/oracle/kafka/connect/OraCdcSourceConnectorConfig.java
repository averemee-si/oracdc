/**
 * Copyright (c) 2018-present, http://a2-solutions.eu
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

package eu.solutions.a2.cdc.oracle.kafka.connect;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class OraCdcSourceConnectorConfig extends AbstractConfig {

	public static final String CONNECTION_WALLET_PARAM = "a2.wallet.location";
	private static final String CONNECTION_WALLET_DOC = "Location of Oracle Wallet. Not required when a2.jdbc.url & a2.jdbc.username & a2.jdbc.password are set";

	public static final String CONNECTION_TNS_ADMIN_PARAM = "a2.tns.admin";
	private static final String CONNECTION_TNS_ADMIN_DOC = "Location of tnsnames.ora file. Not required when a2.jdbc.url & a2.jdbc.username & a2.jdbc.password are set";

	public static final String CONNECTION_TNS_ALIAS_PARAM = "a2.tns.alias";
	private static final String CONNECTION_TNS_ALIAS_DOC = "Connection TNS alias. Not required when a2.jdbc.url & a2.jdbc.username & a2.jdbc.password are set";

	public static final String KAFKA_TOPIC_PARAM = "a2.kafka.topic";
	private static final String KAFKA_TOPIC_PARAM_DOC = "Target topic to send data";
	public static final String KAFKA_TOPIC_PARAM_DEFAULT = "oracdc-topic";

	public static final String POLL_INTERVAL_MS_PARAM = "a2.poll.interval";
	private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in each table";
	public static final int POLL_INTERVAL_MS_DEFAULT = 1000;

	public static final String TOPIC_PREFIX_PARAM = "a2.topic.prefix";
	private static final String TOPIC_PREFIX_DOC = "Prefix to prepend table names to generate name of Kafka topic.";
	public static final String TOPIC_PREFIX_DEFAULT = "";

	public static final String TABLE_EXCLUDE_PARAM = "a2.exclude";
	private static final String TABLE_EXCLUDE_DOC = "List of tables to exclude from processing";
	public static final String TABLE_EXCLUDE_DEFAULT = "";

	public static final String TABLE_INCLUDE_PARAM = "a2.include";
	private static final String TABLE_INCLUDE_DOC = "List of table names to include in processing";
	public static final String TABLE_INCLUDE_DEFAULT = "";

	public static final String TASK_PARAM_MASTER = "master";
	public static final String TASK_PARAM_MV_LOG = "mv.log";
	public static final String TASK_PARAM_OWNER = "owner";
	public static final String TASK_PARAM_SCHEMA_TYPE = "schema.type";
	public static final String TASK_PARAM_MV_ROWID = "mvlog.rowid";
	public static final String TASK_PARAM_MV_PK = "mvlog.pk";
	public static final String TASK_PARAM_MV_SEQUENCE = "mvlog.seq";

	public static ConfigDef config() {
		return new ConfigDef()
				.define(ConnectorConfigConstants.CONNECTION_URL_PARAM, Type.STRING, "",
						Importance.HIGH, ConnectorConfigConstants.CONNECTION_URL_DOC)
				.define(ConnectorConfigConstants.CONNECTION_USER_PARAM, Type.STRING, "",
						Importance.HIGH, ConnectorConfigConstants.CONNECTION_USER_DOC)
				.define(ConnectorConfigConstants.CONNECTION_PASSWORD_PARAM, Type.STRING, "",
						Importance.HIGH, ConnectorConfigConstants.CONNECTION_PASSWORD_DOC)
				.define(CONNECTION_WALLET_PARAM, Type.STRING, "",
						Importance.HIGH, CONNECTION_WALLET_DOC)
				.define(CONNECTION_TNS_ADMIN_PARAM, Type.STRING, "",
						Importance.HIGH, CONNECTION_TNS_ADMIN_DOC)
				.define(CONNECTION_TNS_ALIAS_PARAM, Type.STRING, "",
						Importance.HIGH, CONNECTION_TNS_ALIAS_DOC)
				.define(KAFKA_TOPIC_PARAM, Type.STRING, KAFKA_TOPIC_PARAM_DEFAULT,
						Importance.HIGH, KAFKA_TOPIC_PARAM_DOC)
				.define(POLL_INTERVAL_MS_PARAM, Type.INT, POLL_INTERVAL_MS_DEFAULT,
						Importance.HIGH, POLL_INTERVAL_MS_DOC)
				.define(ConnectorConfigConstants.BATCH_SIZE_PARAM, Type.INT,
						ConnectorConfigConstants.BATCH_SIZE_DEFAULT,
						Importance.HIGH, ConnectorConfigConstants.BATCH_SIZE_DOC)
				.define(ConnectorConfigConstants.SCHEMA_TYPE_PARAM, Type.STRING,
						ConnectorConfigConstants.SCHEMA_TYPE_KAFKA,
						ConfigDef.ValidString.in(ConnectorConfigConstants.SCHEMA_TYPE_KAFKA, ConnectorConfigConstants.SCHEMA_TYPE_STANDALONE),
						Importance.HIGH, ConnectorConfigConstants.SCHEMA_TYPE_DOC)
				.define(TOPIC_PREFIX_PARAM, Type.STRING, TOPIC_PREFIX_DEFAULT,
						Importance.MEDIUM, TOPIC_PREFIX_DOC)
				.define(TABLE_EXCLUDE_PARAM, Type.LIST, TABLE_EXCLUDE_DEFAULT,
						Importance.MEDIUM, TABLE_EXCLUDE_DOC)
				.define(TABLE_INCLUDE_PARAM, Type.LIST, TABLE_INCLUDE_DEFAULT,
						Importance.MEDIUM, TABLE_INCLUDE_DOC);
	}

	public OraCdcSourceConnectorConfig(Map<?, ?> originals) {
		super(config(), originals);
	}

}
