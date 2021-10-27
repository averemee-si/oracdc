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

package eu.solutions.a2.cdc.oracle.data;

import java.sql.Types;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import eu.solutions.a2.cdc.oracle.OraColumn;

/**
 * 
 * Interface for LOB/XMLTYPE transformations feature 
 * 
 * 
 * @author averemee
 *
 */
public interface OraCdcLobTransformationsIntf {

	public default Schema transformSchema(final String pdbName, final String tableOwner,
			final String tableName, final OraColumn lobColumn, final SchemaBuilder valueSchema) {
		// Default:
		// pass columns AS IS to valueSchema
		final String columnName = lobColumn.getColumnName(); 
		switch (lobColumn.getJdbcType()) {
		case Types.BLOB:
			valueSchema.field(columnName, OraBlob.builder().build());
			break;
		case Types.SQLXML:
			valueSchema.field(columnName, OraXmlBinary.builder().build());
			break;
		case Types.CLOB:
			valueSchema.field(columnName, OraClob.builder().build());
			break;
		case Types.NCLOB:
			valueSchema.field(columnName, OraNClob.builder().build());
			break;
		}
		return null;
	}

	public Struct transformData(final String pdbName, final String tableOwner,
			final String tableName, final OraColumn lobColumn, final byte[] content,
			final Struct keyStruct, final Schema valueSchema);

}
