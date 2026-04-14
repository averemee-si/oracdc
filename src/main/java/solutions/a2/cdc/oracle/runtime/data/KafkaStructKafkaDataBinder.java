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

import static solutions.a2.cdc.oracle.OraCdcColumn.ROWID_KEY;

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
public class KafkaStructKafkaDataBinder extends KafkaStructDataBinder {

	public KafkaStructKafkaDataBinder(final OraCdcSourceConnectorConfig config, final OraRdbmsInfo rdbmsInfo, final OraCdcTableBase table) {
		super(config, rdbmsInfo, table);
	}

	@Override
	public void insert(OraCdcColumn column, Object value) {
		try {
			if (column.isPartOfPk()) {
				keyStruct.put(column.name(), value);
				mandatoryColumnsProcessed++;
			} else {
				valueStruct.put(column.name(), value);
				if (!column.isNullable())
					mandatoryColumnsProcessed++;
			}
		} catch (DataException de) {
			throw new OraCdcDataException(de);
		}
	}

	@Override
	public void delete(OraCdcColumn column, Object value) {
		try {
			if (column.isPartOfPk()) {
				keyStruct.put(column.name(), value);
				mandatoryColumnsProcessed++;
			} else {
				valueStruct.put(column.name(), value);
				if (!column.isNullable())
					mandatoryColumnsProcessed++;
			}
		} catch (DataException de) {
			throw new OraCdcDataException(de);
		}
	}

	@Override
	public void update(OraCdcColumn column, Object value, boolean after) {
		try {
			if (column.isPartOfPk()) {
				keyStruct.put(column.name(), value);
				mandatoryColumnsProcessed++;
			} else {
				valueStruct.put(column.name(), value);
				if (!column.isNullable())
					mandatoryColumnsProcessed++;
			}
		} catch (DataException de) {
			throw new OraCdcDataException(de);
		}
	}

	@Override
	public void addRowId(OraCdcStatementBase stmt) {
		super.addRowId(stmt);
		keyStruct.put(ROWID_KEY, stmt.getRowId().toString());
	}

	@Override
	public void afterBefore() {};

}
