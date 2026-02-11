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

package solutions.a2.kafka;

import static solutions.a2.cdc.oracle.OraCdcParameters.BATCH_SIZE_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.BATCH_SIZE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.BATCH_SIZE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONNECTION_PASSWORD_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONNECTION_PASSWORD_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONNECTION_URL_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONNECTION_URL_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONNECTION_USER_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONNECTION_USER_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SCHEMA_TYPE_DEBEZIUM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SCHEMA_TYPE_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.SCHEMA_TYPE_INT_DEBEZIUM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SCHEMA_TYPE_INT_KAFKA_STD;
import static solutions.a2.cdc.oracle.OraCdcParameters.SCHEMA_TYPE_INT_SINGLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.SCHEMA_TYPE_KAFKA;
import static solutions.a2.cdc.oracle.OraCdcParameters.SCHEMA_TYPE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SCHEMA_TYPE_SINGLE;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_PREFIX_DOC;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_PREFIX_PARAM;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import solutions.a2.cdc.oracle.OraCdcSourceBaseConfig;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class KafkaSourceBaseConfig extends AbstractConfig implements OraCdcSourceBaseConfig {

	private int schemaType = -1;

	public static ConfigDef config() {
		return new ConfigDef()
				.define(CONNECTION_URL_PARAM, Type.STRING, "", Importance.HIGH, CONNECTION_URL_DOC)
				.define(CONNECTION_USER_PARAM, Type.STRING, "", Importance.HIGH, CONNECTION_USER_DOC)
				.define(CONNECTION_PASSWORD_PARAM, Type.PASSWORD, "", Importance.HIGH, CONNECTION_PASSWORD_DOC)
				.define(CONNECTION_WALLET_PARAM, Type.STRING, "", Importance.HIGH, CONNECTION_WALLET_DOC)
				.define(
						SCHEMA_TYPE_PARAM, Type.STRING, SCHEMA_TYPE_KAFKA,
						ConfigDef.ValidString.in(
								SCHEMA_TYPE_KAFKA, SCHEMA_TYPE_SINGLE, SCHEMA_TYPE_DEBEZIUM),
						Importance.LOW, SCHEMA_TYPE_DOC)
				.define(KAFKA_TOPIC_PARAM, Type.STRING, KAFKA_TOPIC_PARAM_DEFAULT, Importance.HIGH, KAFKA_TOPIC_PARAM_DOC)
				.define(TOPIC_PREFIX_PARAM, Type.STRING, "", Importance.MEDIUM, TOPIC_PREFIX_DOC)
				.define(TABLE_EXCLUDE_PARAM, Type.LIST, "", Importance.MEDIUM, TABLE_EXCLUDE_DOC)
				.define(TABLE_INCLUDE_PARAM, Type.LIST, "", Importance.MEDIUM, TABLE_INCLUDE_DOC)
				.define(POLL_INTERVAL_MS_PARAM, Type.INT, POLL_INTERVAL_MS_DEFAULT, Importance.LOW, POLL_INTERVAL_MS_DOC)
				.define(BATCH_SIZE_PARAM, Type.INT, BATCH_SIZE_DEFAULT, Importance.LOW, BATCH_SIZE_DOC);
	}

	public KafkaSourceBaseConfig(Map<?, ?> originals) {
		super(config(), originals);
	}

	public KafkaSourceBaseConfig(ConfigDef config, Map<?, ?> originals) {
		super(config, originals);
	}

	@Override
	public String rdbmsUrl() {
		return getString(CONNECTION_URL_PARAM);
	}

	@Override
	public String rdbmsUser() {
		return getString(CONNECTION_USER_PARAM);
	}

	@Override
	public String rdbmsPassword() {
		return getPassword(CONNECTION_PASSWORD_PARAM).value();
	}

	@Override
	public String walletLocation() {
		return getString(CONNECTION_WALLET_PARAM);
	}

	@Override
	public String kafkaTopic() {
		return getString(KAFKA_TOPIC_PARAM);
	}

	@Override
	public void kafkaTopic(final Map<String, String> taskParam) {
		taskParam.put(KAFKA_TOPIC_PARAM, kafkaTopic());
	}

	@Override
	public int schemaType() {
		if (schemaType == -1)
			switch (getString(SCHEMA_TYPE_PARAM)) {
				case SCHEMA_TYPE_KAFKA -> schemaType = SCHEMA_TYPE_INT_KAFKA_STD;
				case SCHEMA_TYPE_SINGLE -> schemaType = SCHEMA_TYPE_INT_SINGLE;
				case SCHEMA_TYPE_DEBEZIUM -> schemaType = SCHEMA_TYPE_INT_DEBEZIUM;
			}
		return schemaType;
	}

	@Override
	public void schemaType(final Map<String, String> taskParam) {
		taskParam.put(SCHEMA_TYPE_PARAM, getString(SCHEMA_TYPE_PARAM));
	}

	@Override
	public String topicOrPrefix() {
		return getString(TOPIC_PREFIX_PARAM);
	}

	@Override
	public List<String> includeObj() {
		return getList(TABLE_INCLUDE_PARAM);
	}

	@Override
	public List<String> excludeObj() {
		return getList(TABLE_EXCLUDE_PARAM);
	}

	@Override
	public int pollIntervalMs() {
		return getInt(POLL_INTERVAL_MS_PARAM);
	}

	@Override
	public void pollIntervalMs(final Map<String, String> taskParam) {
		taskParam.put(POLL_INTERVAL_MS_PARAM, getInt(POLL_INTERVAL_MS_PARAM).toString());
	}

	@Override
	public int batchSize() {
		return getInt(BATCH_SIZE_PARAM);
	}

}
