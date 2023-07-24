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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
/**
 * 
 * @author averemee
 *
 */
public class OraCdcJdbcSinkConnectorConfig extends AbstractConfig {

	public static final String AUTO_CREATE_PARAM = "a2.autocreate";
	private static final String AUTO_CREATE_DOC = "Automatically create the destination table if missed";

	public static final String PK_STRING_LENGTH_PARAM = "a2.pk.string.length";
	private static final String PK_STRING_LENGTH_DOC = "The length of the string by default when it is used as a part of primary key. Derfault - 15";
	public static final int PK_STRING_LENGTH_DEFAULT = 15;


	public static ConfigDef config() {
		return new ConfigDef()
				.define(ParamConstants.CONNECTION_URL_PARAM, Type.STRING,
						Importance.HIGH, ParamConstants.CONNECTION_URL_DOC)
				.define(ParamConstants.CONNECTION_USER_PARAM, Type.STRING,
						Importance.HIGH, ParamConstants.CONNECTION_USER_DOC)
				.define(ParamConstants.CONNECTION_PASSWORD_PARAM, Type.PASSWORD,
						Importance.HIGH, ParamConstants.CONNECTION_PASSWORD_DOC)
				.define(ParamConstants.BATCH_SIZE_PARAM, Type.INT,
						ParamConstants.BATCH_SIZE_DEFAULT,
						Importance.HIGH, ParamConstants.BATCH_SIZE_DOC)
				.define(ParamConstants.SCHEMA_TYPE_PARAM, Type.STRING,
						ParamConstants.SCHEMA_TYPE_KAFKA,
						ConfigDef.ValidString.in(ParamConstants.SCHEMA_TYPE_KAFKA, ParamConstants.SCHEMA_TYPE_DEBEZIUM),
						Importance.HIGH, ParamConstants.SCHEMA_TYPE_DOC)
				.define(AUTO_CREATE_PARAM, Type.BOOLEAN, false,
						Importance.HIGH, AUTO_CREATE_DOC)
				.define(PK_STRING_LENGTH_PARAM, Type.INT, PK_STRING_LENGTH_DEFAULT,
						Importance.LOW, PK_STRING_LENGTH_DOC)
				;
	}

	public OraCdcJdbcSinkConnectorConfig(Map<?, ?> originals) {
		super(config(), originals);
	}

}
