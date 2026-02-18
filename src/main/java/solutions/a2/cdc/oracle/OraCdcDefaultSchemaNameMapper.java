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

import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_SINGLE;

import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcDefaultSchemaNameMapper implements SchemaNameMapper {

	private int schemaType;
	private boolean protobufSchemaNaming;

	@Override
	public void configure(OraCdcSourceConnectorConfig config) {
		schemaType = config.schemaType();
		protobufSchemaNaming = config.useProtobufSchemaNaming();
	}

	@Override
	public String getKeySchemaName(
			final String pdbName, final String tableOwner, final String tableName) {
		if (protobufSchemaNaming) {
			return getSchemaName(pdbName, '_', tableOwner, tableName, '_', "Key");
		} else {
			return getSchemaName(pdbName, ':', tableOwner, tableName, '.', "Key");
		}
	}

	@Override
	public String getValueSchemaName(
			final String pdbName, final String tableOwner, final String tableName) {
		if (protobufSchemaNaming) {
			return getSchemaName(pdbName, '_', tableOwner, tableName, '_', "Value");
		} else {
			return getSchemaName(pdbName, ':', tableOwner, tableName, '.', "Value");
		}
	}

	@Override
	public String getEnvelopeSchemaName(
			final String pdbName, final String tableOwner, final String tableName) {
		if (protobufSchemaNaming) {
			return getSchemaName(pdbName, '_', tableOwner, tableName, '_', "Envelope");
		} else {
			return getSchemaName(pdbName, ':', tableOwner, tableName, '.', "Envelope");
		}
	}

	private String getSchemaName(final String pdbName, final char pdbDelimiter,
			final String tableOwner, final String tableName, final char delimiter,
			final String suffix) {
		final StringBuilder sb = new StringBuilder(256);
		if (StringUtils.isNotBlank(pdbName)) {
			sb
				.append(pdbName)
				.append(pdbDelimiter);
		}
		sb
			.append(tableOwner)
			.append(delimiter)
			.append(tableName);
		if (schemaType != SCHEMA_TYPE_INT_SINGLE) {
			sb
				.append(delimiter)
				.append(suffix);
		}
		return sb.toString();
	}

}
