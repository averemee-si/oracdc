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


/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public interface OraCdcSourceBaseConfig {

	static final String TABLE_EXCLUDE_PARAM = "a2.exclude";
	static final String TABLE_EXCLUDE_DOC = "List of tables to exclude from processing";

	static final String TABLE_INCLUDE_PARAM = "a2.include";
	static final String TABLE_INCLUDE_DOC = "List of table names to include in processing";

	static final String CONNECTION_WALLET_PARAM = "a2.wallet.location";
	static final String CONNECTION_WALLET_DOC = "Location of Oracle Wallet. Not required when a2.jdbc.url & a2.jdbc.username & a2.jdbc.password are set";

	static final String KAFKA_TOPIC_PARAM = "a2.kafka.topic";
	static final String KAFKA_TOPIC_PARAM_DOC = "Target topic to send data";
	static final String KAFKA_TOPIC_PARAM_DEFAULT = "oracdc-topic";

	static final String POLL_INTERVAL_MS_PARAM = "a2.poll.interval";
	static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in each table";
	static final int POLL_INTERVAL_MS_DEFAULT = 1000;

	public String rdbmsUrl();
	public String rdbmsUser();
	public String rdbmsPassword();
	public String walletLocation();
	public String kafkaTopic();
	public void kafkaTopic(final Map<String, String> taskParam);
	public int schemaType();
	public void schemaType(final Map<String, String> taskParam);
	public String topicOrPrefix();
	public List<String> includeObj();
	public List<String> excludeObj();
	public int pollIntervalMs();
	public void pollIntervalMs(final Map<String, String> taskParam);
	public int batchSize();

}
