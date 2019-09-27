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
import org.apache.log4j.Logger;

public class OraCdcSourceConnectorConfig extends AbstractConfig {

	private static final Logger LOGGER = Logger.getLogger(OraCdcSourceConnectorConfig.class);

	public static final String CONNECTION_URL_PARAM = "a2.jdbc.url";
	private static final String CONNECTION_URL_DOC = "JDBC connection URL";
	private static final String CONNECTION_URL_DISPLAY = "JDBC URL";

	public static final String CONNECTION_USER_PARAM = "a2.jdbc.username";
	private static final String CONNECTION_USER_DOC = "JDBC connection user";
	private static final String CONNECTION_USER_DISPLAY = "JDBC User";

	public static final String CONNECTION_PASSWORD_PARAM = "a2.jdbc.password";
	private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password";
	private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

	public static final String POLL_INTERVAL_MS_PARAM = "a2.poll.interval";
	private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in each table";
	public static final int POLL_INTERVAL_MS_DEFAULT = 1000;
	private static final String POLL_INTERVAL_MS_DISPLAY = "Poll Interval (ms)";

	public static final String BATCH_SIZE_PARAM = "a2.batch.size";
	private static final String BATCH_SIZE_DOC = "Maximum number of rows to include in a single batch when polling for new data";
	public static final int BATCH_SIZE_DEFAULT = 100;
	private static final String BATCH_SIZE_DISPLAY = "Max Rows Per Batch";

	public static final String TABLE_EXCLUDE_PARAM = "a2.exclude";
	private static final String TABLE_EXCLUDE_DOC = "List of tables to exclude from processing";
	public static final String TABLE_EXCLUDE_DEFAULT = "";
	private static final String TABLE_EXCLUDE_DISPLAY = "Tables to exclude";

	public static ConfigDef config() {
		return new ConfigDef()
				.define(CONNECTION_URL_PARAM, Type.STRING, Importance.HIGH, CONNECTION_URL_DOC)
				.define(CONNECTION_USER_PARAM, Type.STRING, Importance.HIGH, CONNECTION_USER_DOC)
				.define(CONNECTION_PASSWORD_PARAM, Type.STRING, Importance.HIGH, CONNECTION_PASSWORD_DOC)
				.define(POLL_INTERVAL_MS_PARAM, Type.INT, POLL_INTERVAL_MS_DEFAULT, Importance.HIGH, POLL_INTERVAL_MS_DOC)
				.define(BATCH_SIZE_PARAM, Type.INT, BATCH_SIZE_DEFAULT, Importance.HIGH, BATCH_SIZE_DOC)
				.define(TABLE_EXCLUDE_PARAM, Type.LIST, Importance.MEDIUM, TABLE_EXCLUDE_DOC);
	}

	public OraCdcSourceConnectorConfig(Map<?, ?> originals) {
		super(config(), originals);
	}

}
