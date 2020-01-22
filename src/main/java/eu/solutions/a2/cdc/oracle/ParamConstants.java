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

package eu.solutions.a2.cdc.oracle;

public class ParamConstants {

	public static final String ORACDC_MODE_PARAM = "a2.cdc.mode";
	public static final String ORACDC_MODE_DOC = "Operational mode. When set to mvlog (default) materialized view logs are used as source, when set to logminer - archived redo logs are used as source.";
	public static final String ORACDC_MODE_MVLOG = "mvlog";
	public static final String ORACDC_MODE_LOGMINER = "logminer";

	public static final String CONNECTION_URL_PARAM = "a2.jdbc.url";
	public static final String CONNECTION_URL_DOC = "JDBC connection URL";

	public static final String CONNECTION_USER_PARAM = "a2.jdbc.username";
	public static final String CONNECTION_USER_DOC = "JDBC connection user";

	public static final String CONNECTION_PASSWORD_PARAM = "a2.jdbc.password";
	public static final String CONNECTION_PASSWORD_DOC = "JDBC connection password";

	public static final String CONNECTION_WALLET_PARAM = "a2.wallet.location";
	public static final String CONNECTION_WALLET_DOC = "Location of Oracle Wallet. Not required when a2.jdbc.url & a2.jdbc.username & a2.jdbc.password are set";

	public static final String CONNECTION_TNS_ADMIN_PARAM = "a2.tns.admin";
	public static final String CONNECTION_TNS_ADMIN_DOC = "Location of tnsnames.ora file. Not required when a2.jdbc.url & a2.jdbc.username & a2.jdbc.password are set";

	public static final String CONNECTION_TNS_ALIAS_PARAM = "a2.tns.alias";
	public static final String CONNECTION_TNS_ALIAS_DOC = "Connection TNS alias. Not required when a2.jdbc.url & a2.jdbc.username & a2.jdbc.password are set";

	public static final String POLL_INTERVAL_MS_PARAM = "a2.poll.interval";
	public static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in each table";
	public static final int POLL_INTERVAL_MS_DEFAULT = 1000;

	public static final String BATCH_SIZE_PARAM = "a2.batch.size";
	public static final String BATCH_SIZE_DOC = "Maximum number of statements to include in a single batch when inserting/updating/deleting data";
	public static final int BATCH_SIZE_DEFAULT = 1000;

	public static final String SCHEMA_TYPE_PARAM = "a2.schema.type";
	public static final String SCHEMA_TYPE_DOC = "Type of schema used by oracdc: Kafka Connect (default) or standalone";
	public static final String SCHEMA_TYPE_KAFKA = "kafka";
	public static final String SCHEMA_TYPE_STANDALONE = "standalone";

	public static final String TABLE_EXCLUDE_PARAM = "a2.exclude";
	public static final String TABLE_EXCLUDE_DOC = "List of tables to exclude from processing";

	public static final String TABLE_INCLUDE_PARAM = "a2.include";
	public static final String TABLE_INCLUDE_DOC = "List of table names to include in processing";

}
