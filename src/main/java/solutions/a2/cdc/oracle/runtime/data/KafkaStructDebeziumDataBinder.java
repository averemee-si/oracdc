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

package solutions.a2.cdc.oracle.runtime.data;

import static solutions.a2.cdc.oracle.OraCdcColumn.ROWID_KEY;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import solutions.a2.cdc.oracle.OraCdcDataException;
import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.cdc.oracle.OraCdcStatementBase;
import solutions.a2.cdc.oracle.OraCdcColumn;
import solutions.a2.cdc.oracle.OraRdbmsInfo;
import solutions.a2.cdc.oracle.OraCdcTableBase;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class KafkaStructDebeziumDataBinder extends KafkaStructDataBinder {

	public KafkaStructDebeziumDataBinder(final OraCdcSourceConnectorConfig config, final OraRdbmsInfo rdbmsInfo, final OraCdcTableBase table) {
		super(config, rdbmsInfo, table);
	}

	@Override
	public void init(OraCdcStatementBase stmt) {
		super.init(stmt);
		struct = new Struct(schema);
	}

	@Override
	public void insert(OraCdcColumn column, Object value) {
		try {
			if (column.partOfPk()) {
				keyStruct.put(column.name(), value);
				valueStruct.put(column.name(), value);
				mandatoryColumnsProcessed++;
			} else {
				valueStruct.put(column.name(), value);
				if (!column.nullable())
					mandatoryColumnsProcessed++;
			}
		} catch (DataException de) {
			throw new OraCdcDataException(de);
		}
	}

	@Override
	public void delete(OraCdcColumn column, Object value) {
		try {
			if (column.partOfPk()) {
				keyStruct.put(column.name(), value);
				valueStruct.put(column.name(), value);
				mandatoryColumnsProcessed++;
			} else {
				valueStruct.put(column.name(), value);
				if (!column.nullable())
					mandatoryColumnsProcessed++;
			}
		} catch (DataException de) {
			throw new OraCdcDataException(de);
		}
	}

	@Override
	public void update(OraCdcColumn column, Object value, boolean after) {
		try {
			if (after) {
				if (column.partOfPk()) {
					keyStruct.put(column.name(), value);
					valueStruct.put(column.name(), value);
					mandatoryColumnsProcessed++;
				} else {
					valueStruct.put(column.name(), value);
					if (!column.nullable())
						mandatoryColumnsProcessed++;
				}
			} else
				valueStruct.put(column.name(), value);
		} catch (DataException de) {
			throw new OraCdcDataException(de);
		}
	}

	@Override
	public void addRowId(OraCdcStatementBase stmt) {
		super.addRowId(stmt);
		final String rowId = stmt.getRowId().toString();
		keyStruct.put(ROWID_KEY, rowId);
		valueStruct.put(ROWID_KEY, rowId);
	}

	@Override
	public void afterBefore() {
		struct.put("after", valueStruct);
		final Struct before = new Struct(valueSchema);
		valueStruct.schema().fields().forEach(f -> 
			before.put(f, valueStruct.get(f)));
		valueStruct = before;
	}

}
