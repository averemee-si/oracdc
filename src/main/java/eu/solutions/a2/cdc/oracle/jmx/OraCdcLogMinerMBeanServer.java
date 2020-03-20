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

package eu.solutions.a2.cdc.oracle.jmx;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.OraRdbmsInfo;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcLogMinerMBeanServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcLogMinerMBeanServer.class);

	private final OraCdcLogMinerMgmt mbean;

	public OraCdcLogMinerMBeanServer(final OraRdbmsInfo rdbmsInfo, final String connectorName) {
		mbean = new OraCdcLogMinerMgmt();
		try {
			final StringBuilder sb = new StringBuilder(96);
			sb.append("eu.solutions.a2.oracdc:type=LogMiner-metrics,name=");
			sb.append(connectorName);
			sb.append(",database=");
			sb.append(rdbmsInfo.getInstanceName());
			sb.append("_");
			sb.append(rdbmsInfo.getHostName());
			ObjectName name = new ObjectName(sb.toString());
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
			mbs.registerMBean(mbean, name);
		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
			LOGGER.error("Unable to register MBean - " + e.getMessage() + " !!!!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new ConnectException(e);
		}
	}

	public OraCdcLogMinerMgmt getMbean() {
		return mbean;
	}

}
