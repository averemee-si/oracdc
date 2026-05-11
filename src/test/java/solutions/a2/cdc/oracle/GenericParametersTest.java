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

 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
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
