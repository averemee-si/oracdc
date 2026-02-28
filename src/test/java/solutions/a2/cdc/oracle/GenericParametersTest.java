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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SMB_DOMAIN_PARAM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SMB_PASSWORD_PARAM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SMB_SERVER_PARAM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SMB_SHARE_ARCHIVE_PARAM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SMB_SHARE_ONLINE_PARAM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SMB_USER_PARAM;

import java.util.HashMap;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import solutions.a2.cdc.oracle.runtime.config.GenericSourceConnectorConfig;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class GenericParametersTest {

	@Test
	public void test() {
		var props = new HashMap<String, String>();
		props.put(SMB_SERVER_PARAM, UUID.randomUUID().toString());
		props.put(SMB_SHARE_ONLINE_PARAM, UUID.randomUUID().toString());
		props.put(SMB_SHARE_ARCHIVE_PARAM, UUID.randomUUID().toString());
		props.put(SMB_USER_PARAM, UUID.randomUUID().toString());
		props.put(SMB_PASSWORD_PARAM, UUID.randomUUID().toString());
		props.put(SMB_DOMAIN_PARAM, UUID.randomUUID().toString());

		var config = new GenericSourceConnectorConfig(props);

		assertEquals(config.smbServer(), props.get(SMB_SERVER_PARAM));
		assertEquals(config.smbShareOnline(), props.get(SMB_SHARE_ONLINE_PARAM));
		assertEquals(config.smbShareArchive(), props.get(SMB_SHARE_ARCHIVE_PARAM));
		assertEquals(config.smbDomain(), props.get(SMB_DOMAIN_PARAM));
		assertEquals(config.smbUser(), props.get(SMB_USER_PARAM));
		assertEquals(config.smbPassword(), props.get(SMB_PASSWORD_PARAM));

	}
}
