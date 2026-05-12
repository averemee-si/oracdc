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

import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.SQLXML;
import static oracle.jdbc.OracleTypes.JSON;
import static oracle.jdbc.OracleTypes.VECTOR;


/**
 * 
 * Interface for LOB/XMLTYPE transformations feature 
 * 
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public interface OraCdcLobTransformationsIntf {

	public default Schema transformSchema(final String pdbName, final String tableOwner,
			final String tableName, final OraCdcColumn lobColumn, final SchemaBuilder valueSchema) {
		// Default:
		// pass columns AS IS to valueSchema
		final String columnName = lobColumn.name(); 
		switch (lobColumn.jdbcType()) {
		case BLOB:
			valueSchema.field(columnName, OraBlob.schema());
			break;
		case SQLXML:
			valueSchema.field(columnName, OraXml.schema());
			break;
		case CLOB:
			valueSchema.field(columnName, OraClob.schema());
			break;
		case NCLOB:
			valueSchema.field(columnName, OraNClob.schema());
			break;
		case JSON:
			valueSchema.field(columnName, OraJson.schema());
			break;
		case VECTOR:
			valueSchema.field(columnName, OraVector.schema());
			break;
		}
		return null;
	}

	public Struct transformData(final String pdbName, final String tableOwner,
			final String tableName, final OraCdcColumn lobColumn, final byte[] content,
			final Struct keyStruct, final Schema valueSchema);

}
