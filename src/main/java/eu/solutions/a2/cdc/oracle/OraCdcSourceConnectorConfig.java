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

package eu.solutions.a2.cdc.oracle.kafka.connect;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import eu.solutions.a2.cdc.oracle.ParamConstants;

public class OraCdcSourceConnectorConfig extends AbstractConfig {

	public static final String KAFKA_TOPIC_PARAM = "a2.kafka.topic";
	private static final String KAFKA_TOPIC_PARAM_DOC = "Target topic to send data";
	public static final String KAFKA_TOPIC_PARAM_DEFAULT = "oracdc-topic";

	public static final String TOPIC_PREFIX_PARAM = "a2.topic.prefix";
	private static final String TOPIC_PREFIX_DOC = "Prefix to prepend table names to generate name of Kafka topic.";

	public static final String TASK_PARAM_MASTER = "master";
	public static final String TASK_PARAM_MV_LOG = "mv.log";
	public static final String TASK_PARAM_OWNER = "owner";
	public static final String TASK_PARAM_SCHEMA_TYPE = "schema.type";
	public static final String TASK_PARAM_MV_ROWID = "mvlog.rowid";
	public static final String TASK_PARAM_MV_PK = "mvlog.pk";
	public static final String TASK_PARAM_MV_SEQUENCE = "mvlog.seq";

	public static ConfigDef config() {
		return new ConfigDef()
				.define(ParamConstants.ORACDC_MODE_PARAM, Type.STRING,
						ParamConstants.ORACDC_MODE_MVLOG,
						ConfigDef.ValidString.in(ParamConstants.ORACDC_MODE_MVLOG, ParamConstants.ORACDC_MODE_LOGMINER),
						Importance.HIGH, ParamConstants.ORACDC_MODE_DOC)
				.define(ParamConstants.CONNECTION_URL_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.CONNECTION_URL_DOC)
				.define(ParamConstants.CONNECTION_USER_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.CONNECTION_USER_DOC)
				.define(ParamConstants.CONNECTION_PASSWORD_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.CONNECTION_PASSWORD_DOC)
				.define(ParamConstants.CONNECTION_WALLET_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.CONNECTION_WALLET_DOC)
				.define(ParamConstants.CONNECTION_TNS_ADMIN_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.CONNECTION_TNS_ADMIN_DOC)
				.define(ParamConstants.CONNECTION_TNS_ALIAS_PARAM, Type.STRING, "",
						Importance.HIGH, ParamConstants.CONNECTION_TNS_ALIAS_DOC)
				.define(KAFKA_TOPIC_PARAM, Type.STRING, KAFKA_TOPIC_PARAM_DEFAULT,
						Importance.HIGH, KAFKA_TOPIC_PARAM_DOC)
				.define(ParamConstants.POLL_INTERVAL_MS_PARAM, Type.INT, ParamConstants.POLL_INTERVAL_MS_DEFAULT,
						Importance.HIGH, ParamConstants.POLL_INTERVAL_MS_DOC)
				.define(ParamConstants.BATCH_SIZE_PARAM, Type.INT,
						ParamConstants.BATCH_SIZE_DEFAULT,
						Importance.HIGH, ParamConstants.BATCH_SIZE_DOC)
				.define(ParamConstants.SCHEMA_TYPE_PARAM, Type.STRING,
						ParamConstants.SCHEMA_TYPE_KAFKA,
						ConfigDef.ValidString.in(ParamConstants.SCHEMA_TYPE_KAFKA, ParamConstants.SCHEMA_TYPE_DEBEZIUM),
						Importance.HIGH, ParamConstants.SCHEMA_TYPE_DOC)
				.define(TOPIC_PREFIX_PARAM, Type.STRING, "",
						Importance.MEDIUM, TOPIC_PREFIX_DOC)
				.define(ParamConstants.TABLE_EXCLUDE_PARAM, Type.LIST, "",
						Importance.MEDIUM, ParamConstants.TABLE_EXCLUDE_DOC)
				.define(ParamConstants.TABLE_INCLUDE_PARAM, Type.LIST, "",
						Importance.MEDIUM, ParamConstants.TABLE_INCLUDE_DOC);
	}

	public OraCdcSourceConnectorConfig(Map<?, ?> originals) {
		super(config(), originals);
	}

}
