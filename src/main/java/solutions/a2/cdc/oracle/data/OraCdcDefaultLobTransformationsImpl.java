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

package solutions.a2.cdc.oracle.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import solutions.a2.cdc.oracle.OraColumn;

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
			final String tableName, final OraColumn lobColumn, final SchemaBuilder valueSchema) {
		return OraCdcLobTransformationsIntf.super.transformSchema(
				pdbName, tableOwner, tableName, lobColumn,valueSchema);
	}

	@Override
	public Struct transformData(final String pdbName, final String tableOwner,
			final String tableName, final OraColumn lobColumn, final byte[] content,
			final Struct keyStruct, final Schema valueSchema) {
		// Default do nothing...
		// Not called if column definition is not overloaded with transformation
		return null;
	}
	
}
