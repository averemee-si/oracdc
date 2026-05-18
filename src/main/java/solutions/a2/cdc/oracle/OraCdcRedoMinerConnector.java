/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle;

import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import solutions.a2.cdc.oracle.runtime.thread.KafkaSourceRedoMinerTask;
import solutions.a2.cdc.oracle.runtime.thread.KafkaSourceConnectorBase;


/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcRedoMinerConnector extends KafkaSourceConnectorBase {

	private static final Logger LOGGER = LogManager.getLogger(OraCdcRedoMinerConnector.class);

	@Override
	public void start(Map<String, String> props) {
		LOGGER.info("Starting oracdc '{}' Redo Miner source connector", props.get("name"));
		super.start(props);
	}

	@Override
	public void stop() {
		LOGGER.info("Stopping oracdc Redo Miner source connector");
	}

	@Override
	public Class<? extends Task> taskClass() {
		return KafkaSourceRedoMinerTask.class;
	}

}
