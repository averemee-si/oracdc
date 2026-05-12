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

package solutions.a2.cdc.oracle.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import solutions.a2.cdc.oracle.OraCdcColumn;

/**
 * 
 * Default (just add column to schema) LOB/XMLTYPE transformation feature
 * BLOV/CLOB/NCLOB/XMLTYPE "as is" 
 * 
 * @author averemee
 *
 */
public class OraCdcDefaultLobTransformationsImpl implements OraCdcLobTransformationsIntf {

	@Override
	public Schema transformSchema(final String pdbName, final String tableOwner,
			final String tableName, final OraCdcColumn lobColumn, final SchemaBuilder valueSchema) {
		return OraCdcLobTransformationsIntf.super.transformSchema(
				pdbName, tableOwner, tableName, lobColumn,valueSchema);
	}

	@Override
	public Struct transformData(final String pdbName, final String tableOwner,
			final String tableName, final OraCdcColumn lobColumn, final byte[] content,
			final Struct keyStruct, final Schema valueSchema) {
		// Default do nothing...
		// Not called if column definition is not overloaded with transformation
		return null;
	}
	
}
