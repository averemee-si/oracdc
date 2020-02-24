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

/**
 * 
 * @author averemee
 *
 */
public class ParamConstants {

	public static final int SCHEMA_TYPE_INT_DEBEZIUM = 1;
	public static final int SCHEMA_TYPE_INT_KAFKA_STD = 2;

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
	public static final String SCHEMA_TYPE_DOC = "Type of schema used by oracdc: Kafka Connect JDBC compatible (default) or Debezium";
	public static final String SCHEMA_TYPE_KAFKA = "kafka";
	public static final String SCHEMA_TYPE_DEBEZIUM = "debezium";

	public static final String TABLE_EXCLUDE_PARAM = "a2.exclude";
	public static final String TABLE_EXCLUDE_DOC = "List of tables to exclude from processing";

	public static final String TABLE_INCLUDE_PARAM = "a2.include";
	public static final String TABLE_INCLUDE_DOC = "List of table names to include in processing";

	public static final String REDO_FILES_COUNT_PARAM = "a2.redo.count";
	public static final String REDO_FILES_COUNT_DOC = "Quantity of archived logs to process during each DBMS_LOGMNR.START_LOGMNR call";

	public static final String REDO_FILES_SIZE_PARAM = "a2.redo.size";
	public static final String REDO_FILES_SIZE_DOC = "Minimal size of archived logs to process during each DBMS_LOGMNR.START_LOGMNR call";

	public static final String LGMNR_START_SCN_PARAM = "a2.first.change";
	public static final String LGMNR_START_SCN_DOC = "When set DBMS_LOGMNR.START_LOGMNR will start mining from this SCN. When not set min(FIRST_CHANGE#) from V$ARCHIVED_LOG will used. Overrides SCN value  stored in offset file";

	public static final String TEMP_DIR_PARAM = "a2.tmpdir";
	public static final String TEMP_DIR_DOC = "Temporary directory for non-heap storage. When not set, OS temp directory used"; 

}
