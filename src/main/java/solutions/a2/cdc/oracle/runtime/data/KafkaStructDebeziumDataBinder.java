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

package solutions.a2.cdc.oracle.runtime.data;

import static solutions.a2.cdc.oracle.OraColumn.ROWID_KEY;

import org.apache.kafka.connect.data.Struct;

import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.cdc.oracle.OraCdcStatementBase;
import solutions.a2.cdc.oracle.OraColumn;
import solutions.a2.cdc.oracle.OraRdbmsInfo;
import solutions.a2.cdc.oracle.OraTable;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class KafkaStructDebeziumDataBinder extends KafkaStructDataBinder {

	public KafkaStructDebeziumDataBinder(final OraCdcSourceConnectorConfig config, final OraRdbmsInfo rdbmsInfo, final OraTable table) {
		super(config, rdbmsInfo, table);
	}

	@Override
	public void init(OraCdcStatementBase stmt) {
		super.init(stmt);
		struct = new Struct(schema);
	}

	@Override
	public void insert(OraColumn column, Object value) {
		if (column.isPartOfPk()) {
			keyStruct.put(column.getColumnName(), value);
			valueStruct.put(column.getColumnName(), value);
			mandatoryColumnsProcessed++;
		} else {
			valueStruct.put(column.getColumnName(), value);
			if (!column.isNullable())
				mandatoryColumnsProcessed++;
		}
	}

	@Override
	public void delete(OraColumn column, Object value) {
		if (column.isPartOfPk()) {
			keyStruct.put(column.getColumnName(), value);
			valueStruct.put(column.getColumnName(), value);
			mandatoryColumnsProcessed++;
		} else {
			valueStruct.put(column.getColumnName(), value);
			if (!column.isNullable())
				mandatoryColumnsProcessed++;
		}
	}

	@Override
	public void update(OraColumn column, Object value, boolean after) {
		if (after) {
			if (column.isPartOfPk()) {
				keyStruct.put(column.getColumnName(), value);
				valueStruct.put(column.getColumnName(), value);
				mandatoryColumnsProcessed++;
			} else {
				valueStruct.put(column.getColumnName(), value);
				if (!column.isNullable())
					mandatoryColumnsProcessed++;
			}
		} else
			valueStruct.put(column.getColumnName(), value);
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
