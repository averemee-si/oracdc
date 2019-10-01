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

public class OraCdcJdbcSinkConnectorConfig extends AbstractConfig {

	public static final String CONNECTION_URL_PARAM = "a2.jdbc.url";
	private static final String CONNECTION_URL_DOC = "JDBC connection URL";

	public static final String CONNECTION_USER_PARAM = "a2.jdbc.username";
	private static final String CONNECTION_USER_DOC = "JDBC connection user";

	public static final String CONNECTION_PASSWORD_PARAM = "a2.jdbc.password";
	private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password";

	public static final String BATCH_SIZE_PARAM = "a2.batch.size";
	private static final String BATCH_SIZE_DOC = "Maximum number of statements to include in a single batch when inserting/updating/deleting data";
	public static final int BATCH_SIZE_DEFAULT = 1000;

	public static final String AUTO_CREATE_PARAM = "a2.autocreate";
	private static final String AUTO_CREATE_DOC = "Automatically create the destination table if missed";
	public static final String AUTO_CREATE_DEFAULT = "false";

	public static ConfigDef config() {
		return new ConfigDef()
				.define(CONNECTION_URL_PARAM, Type.STRING, Importance.HIGH, CONNECTION_URL_DOC)
				.define(CONNECTION_USER_PARAM, Type.STRING, Importance.HIGH, CONNECTION_USER_DOC)
				.define(CONNECTION_PASSWORD_PARAM, Type.STRING, Importance.HIGH, CONNECTION_PASSWORD_DOC)
				.define(BATCH_SIZE_PARAM, Type.INT, BATCH_SIZE_DEFAULT, Importance.HIGH, BATCH_SIZE_DOC)
				.define(AUTO_CREATE_PARAM, Type.BOOLEAN, AUTO_CREATE_DEFAULT, Importance.HIGH, AUTO_CREATE_DOC);
	}

	public OraCdcJdbcSinkConnectorConfig(Map<?, ?> originals) {
		super(config(), originals);
	}

}
