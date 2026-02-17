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

import static solutions.a2.cdc.ParameterType.INT;
import static solutions.a2.cdc.ParameterType.LIST;
import static solutions.a2.cdc.ParameterType.PASSWORD;
import static solutions.a2.cdc.ParameterType.STRING;
import static solutions.a2.cdc.oracle.OraCdcParameters.BATCH_SIZE_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.BATCH_SIZE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONNECTION_PASSWORD_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONNECTION_URL_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONNECTION_USER_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.CONNECTION_WALLET_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.KAFKA_TOPIC_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.KAFKA_TOPIC_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.POLL_INTERVAL_MS_DEFAULT;
import static solutions.a2.cdc.oracle.OraCdcParameters.POLL_INTERVAL_MS_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.SCHEMA_TYPE_KAFKA;
import static solutions.a2.cdc.oracle.OraCdcParameters.SCHEMA_TYPE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TABLE_EXCLUDE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TABLE_INCLUDE_PARAM;
import static solutions.a2.cdc.oracle.OraCdcParameters.TOPIC_PREFIX_PARAM;

import java.util.List;
import java.util.Map;

import solutions.a2.cdc.AbstractConfiguration;
import solutions.a2.cdc.Configuration;
import solutions.a2.cdc.oracle.OraCdcSourceBaseConfig;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class GenericSourceBaseConfig extends AbstractConfiguration implements OraCdcSourceBaseConfig {

	private final BaseConfig holder;

	static Configuration config() {
		return new Configuration()
				.define(CONNECTION_URL_PARAM, STRING, "")
				.define(CONNECTION_USER_PARAM, STRING, "")
				.define(CONNECTION_PASSWORD_PARAM, PASSWORD, "")
				.define(CONNECTION_WALLET_PARAM, STRING, "")
				.define(SCHEMA_TYPE_PARAM, STRING, SCHEMA_TYPE_KAFKA)
				.define(KAFKA_TOPIC_PARAM, STRING, KAFKA_TOPIC_DEFAULT)
				.define(TOPIC_PREFIX_PARAM, STRING, "")
				.define(TABLE_EXCLUDE_PARAM, LIST, "")
				.define(TABLE_INCLUDE_PARAM, LIST, "")
				.define(POLL_INTERVAL_MS_PARAM, INT, POLL_INTERVAL_MS_DEFAULT)
				.define(BATCH_SIZE_PARAM, INT, BATCH_SIZE_DEFAULT);
	}

	public GenericSourceBaseConfig(Configuration config, Map<?, ?> originals) {
		super(config, originals);
		holder = new BaseConfig(getString(SCHEMA_TYPE_PARAM));
	}

	public GenericSourceBaseConfig(Map<?, ?> originals) {
		super(config(), originals);
		holder = new BaseConfig(getString(SCHEMA_TYPE_PARAM));
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
		return getPassword(CONNECTION_PASSWORD_PARAM);
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
		return holder.schemaType();
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
		taskParam.put(POLL_INTERVAL_MS_PARAM, Integer.toString(getInt(POLL_INTERVAL_MS_PARAM)));
	}

	@Override
	public int batchSize() {
		return getInt(BATCH_SIZE_PARAM);
	}

}
