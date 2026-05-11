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
