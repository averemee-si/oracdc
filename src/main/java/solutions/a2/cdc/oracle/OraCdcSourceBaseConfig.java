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

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import solutions.a2.kafka.ConnectorParams;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcSourceBaseConfig extends AbstractConfig {

	static final String TABLE_EXCLUDE_PARAM = "a2.exclude";
	private static final String TABLE_EXCLUDE_DOC = "List of tables to exclude from processing";

	static final String TABLE_INCLUDE_PARAM = "a2.include";
	private static final String TABLE_INCLUDE_DOC = "List of table names to include in processing";

	private static final String CONNECTION_WALLET_PARAM = "a2.wallet.location";
	private static final String CONNECTION_WALLET_DOC = "Location of Oracle Wallet. Not required when a2.jdbc.url & a2.jdbc.username & a2.jdbc.password are set";

	private static final String KAFKA_TOPIC_PARAM = "a2.kafka.topic";
	private static final String KAFKA_TOPIC_PARAM_DOC = "Target topic to send data";
	private static final String KAFKA_TOPIC_PARAM_DEFAULT = "oracdc-topic";

	private static final String POLL_INTERVAL_MS_PARAM = "a2.poll.interval";
	private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in each table";
	private static final int POLL_INTERVAL_MS_DEFAULT = 1000;

	private int schemaType = -1;


	public static ConfigDef config() {
		return new ConfigDef()
				.define(ConnectorParams.CONNECTION_URL_PARAM, Type.STRING, "",
						Importance.HIGH, ConnectorParams.CONNECTION_URL_DOC)
				.define(ConnectorParams.CONNECTION_USER_PARAM, Type.STRING, "",
						Importance.HIGH, ConnectorParams.CONNECTION_USER_DOC)
				.define(ConnectorParams.CONNECTION_PASSWORD_PARAM, Type.PASSWORD, "",
						Importance.HIGH, ConnectorParams.CONNECTION_PASSWORD_DOC)
				.define(CONNECTION_WALLET_PARAM, Type.STRING, "",
						Importance.HIGH, CONNECTION_WALLET_DOC)
				.define(ConnectorParams.SCHEMA_TYPE_PARAM, Type.STRING,
						ConnectorParams.SCHEMA_TYPE_KAFKA,
						ConfigDef.ValidString.in(
								ConnectorParams.SCHEMA_TYPE_KAFKA,
								ConnectorParams.SCHEMA_TYPE_SINGLE,
								ConnectorParams.SCHEMA_TYPE_DEBEZIUM),
						Importance.LOW, ConnectorParams.SCHEMA_TYPE_DOC)
				.define(KAFKA_TOPIC_PARAM, Type.STRING, KAFKA_TOPIC_PARAM_DEFAULT,
						Importance.HIGH, KAFKA_TOPIC_PARAM_DOC)
				.define(ConnectorParams.TOPIC_PREFIX_PARAM, Type.STRING, "",
						Importance.MEDIUM, ConnectorParams.TOPIC_PREFIX_DOC)
				.define(TABLE_EXCLUDE_PARAM, Type.LIST, "",
						Importance.MEDIUM, TABLE_EXCLUDE_DOC)
				.define(TABLE_INCLUDE_PARAM, Type.LIST, "",
						Importance.MEDIUM, TABLE_INCLUDE_DOC)
				.define(POLL_INTERVAL_MS_PARAM, Type.INT, POLL_INTERVAL_MS_DEFAULT,
						Importance.LOW, POLL_INTERVAL_MS_DOC)
				.define(ConnectorParams.BATCH_SIZE_PARAM, Type.INT,
						ConnectorParams.BATCH_SIZE_DEFAULT,
						Importance.LOW, ConnectorParams.BATCH_SIZE_DOC)
				;
	}

	public OraCdcSourceBaseConfig(Map<?, ?> originals) {
		super(config(), originals);
	}

	public OraCdcSourceBaseConfig(ConfigDef config, Map<?, ?> originals) {
		super(config, originals);
	}

	public String walletLocation() {
		return getString(CONNECTION_WALLET_PARAM);
	}

	public String kafkaTopic() {
		return getString(KAFKA_TOPIC_PARAM);
	}

	public void kafkaTopic(final Map<String, String> taskParam) {
		taskParam.put(KAFKA_TOPIC_PARAM, kafkaTopic());
	}

	public int schemaType() {
		if (schemaType == -1) {
			switch (getString(ConnectorParams.SCHEMA_TYPE_PARAM)) {
			case ConnectorParams.SCHEMA_TYPE_KAFKA:
				schemaType = ConnectorParams.SCHEMA_TYPE_INT_KAFKA_STD;
				break;
			case ConnectorParams.SCHEMA_TYPE_SINGLE:
				schemaType = ConnectorParams.SCHEMA_TYPE_INT_SINGLE;
				break;
			case ConnectorParams.SCHEMA_TYPE_DEBEZIUM:
				schemaType = ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM;
				break;
			}
		}
		return schemaType;
	}

	public void schemaType(final Map<String, String> taskParam) {
		taskParam.put(ConnectorParams.SCHEMA_TYPE_PARAM, getString(ConnectorParams.SCHEMA_TYPE_PARAM));
	}

	public String topicOrPrefix() {
		if (schemaType() != ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM) {
			return getString(ConnectorParams.TOPIC_PREFIX_PARAM);
		} else {
			return getString(KAFKA_TOPIC_PARAM);
		}
	}

	public List<String> includeObj() {
		return getList(TABLE_INCLUDE_PARAM);
	}

	public List<String> excludeObj() {
		return getList(TABLE_EXCLUDE_PARAM);
	}

	public int pollIntervalMs() {
		return getInt(POLL_INTERVAL_MS_PARAM);
	}

	public void pollIntervalMs(final Map<String, String> taskParam) {
		taskParam.put(POLL_INTERVAL_MS_PARAM, getInt(POLL_INTERVAL_MS_PARAM).toString());
	}

}
