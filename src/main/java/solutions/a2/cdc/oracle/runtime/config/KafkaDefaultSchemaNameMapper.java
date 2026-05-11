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

package solutions.a2.cdc.oracle.runtime.config;

import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_SINGLE;

import org.apache.commons.lang3.StringUtils;

import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class KafkaDefaultSchemaNameMapper implements KafkaSchemaNameMapper {

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
