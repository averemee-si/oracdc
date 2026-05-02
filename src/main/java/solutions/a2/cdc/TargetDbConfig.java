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

package solutions.a2.cdc;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public interface TargetDbConfig {

	boolean autoCreateTable();
	int pkStringLength();
	int getConnectorMode();
	int batchSize();

	public static final String AUTO_CREATE_PARAM = "a2.autocreate";
	public static final String AUTO_CREATE_DOC =
			"""
			Automatically create the destination table if missed.
			""";

	public static final int PK_STRING_LENGTH_DEFAULT = 30;
	public static final String PK_STRING_LENGTH_PARAM = "a2.pk.string.length";
	public static final String PK_STRING_LENGTH_DOC =
			"""
			The length of the string by default when it is used as a part of primary key.
			"Derfault - """ + PK_STRING_LENGTH_DEFAULT;

	public static final int CONNECTOR_REPLICATE = 1;
	public static final int CONNECTOR_AUDIT_TRAIL = 2;
	public static final String CONN_TYPE_PARAM = "a2.sink.connector.mode";
	public static final String CONN_TYPE_REPLICATE = "replicate";
	public static final String CONN_TYPE_AUDIT_TRAIL = "audit_trail";
	public static final String CONN_TYPE_DOC =
			"""
			Connector operating mode - 'replicate' or 'audit_trail'.
			In 'replicate' mode, the connector sends INSERT/UPDATE/DELETE commands to the target database,
			and in 'audit_trail' mode, it only sends INSERT commands to record the change history of the source table.
			Default - """ + CONN_TYPE_REPLICATE;



}
