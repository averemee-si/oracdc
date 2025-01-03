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

import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcLogMinerConnector extends OraCdcConnectorBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerConnector.class);

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc '{}' LogMiner source connector", props.get("name"));
		super.start(props);
	}

	@Override
	public void stop() {
		LOGGER.info("Stopping oracdc LogMiner source connector");
	}

	@Override
	public Class<? extends Task> taskClass() {
		return OraCdcLogMinerTask.class;
	}


}
