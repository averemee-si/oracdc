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

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcParameters {

	public static final int SCHEMA_TYPE_INT_DEBEZIUM = 1;
	public static final int SCHEMA_TYPE_INT_KAFKA_STD = 2;
	public static final int SCHEMA_TYPE_INT_SINGLE = 3;

	public static final String CONNECTION_URL_PARAM = "a2.jdbc.url";
	public static final String CONNECTION_URL_DOC = "JDBC connection URL";

	public static final String CONNECTION_USER_PARAM = "a2.jdbc.username";
	public static final String CONNECTION_USER_DOC = "JDBC connection user";

	public static final String CONNECTION_PASSWORD_PARAM = "a2.jdbc.password";
	public static final String CONNECTION_PASSWORD_DOC = "JDBC connection password";

	public static final String BATCH_SIZE_PARAM = "a2.batch.size";
	public static final String BATCH_SIZE_DOC = "Maximum number of statements to include in a single batch when inserting/updating/deleting data";
	public static final int BATCH_SIZE_DEFAULT = 1000;

	public static final String SCHEMA_TYPE_PARAM = "a2.schema.type";
	public static final String SCHEMA_TYPE_DOC = "Type of schema used by oracdc: kafka (default) with separate schemas for key and value, single - single schema for all fields or Debezium";
	public static final String SCHEMA_TYPE_KAFKA = "kafka";
	public static final String SCHEMA_TYPE_DEBEZIUM = "debezium";
	public static final String SCHEMA_TYPE_SINGLE = "single";

	public static final String USE_ALL_COLUMNS_ON_DELETE_PARAM = "a2.use.all.columns.on.delete";
	public static final String USE_ALL_COLUMNS_ON_DELETE_DOC =
			"""
			Default - false.
			When set to false (default) oracdc reads and processes only the PK columns from the redo record and sends only the key fields to the Kafka topic.
			When set to true oracdc reads and processes all table columns from the redo record.
			""";
	public static final boolean USE_ALL_COLUMNS_ON_DELETE_DEFAULT = false; 

	public static final String TOPIC_PREFIX_PARAM = "a2.topic.prefix";
	public static final String TOPIC_PREFIX_DOC = "Prefix to prepend table names to generate name of Kafka topic.";

}
