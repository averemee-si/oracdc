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

package solutions.a2.cdc.oracle.runtime.data;

import static solutions.a2.cdc.oracle.OraCdcTableBase.FLG_ONLY_VALUE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

import org.agrona.collections.Int2NullableObjectHashMap;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import solutions.a2.cdc.oracle.OraCdcColumn;
import solutions.a2.cdc.oracle.OraCdcStatementBase;
import solutions.a2.cdc.oracle.OraCdcTableBase;
import solutions.a2.cdc.oracle.OraCdcTransaction;
import solutions.a2.cdc.oracle.OraRdbmsInfo;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public abstract class GenericAbstractMapDataBinder implements DataBinder {

	private static final Logger LOGGER = LogManager.getLogger(GenericAbstractMapDataBinder.class);

	final OraCdcTableBase table;
	final OraRdbmsInfo rdbmsInfo;
	OraCdcStatementBase stmt = null;
	final Int2ObjectHashMap<Object> keyData;
	final Int2NullableObjectHashMap<Object> valueData;

	GenericAbstractMapDataBinder(final OraRdbmsInfo rdbmsInfo, final OraCdcTableBase table) {
		this.table = table;
		this.rdbmsInfo = rdbmsInfo;
		keyData = (table.flags() & FLG_ONLY_VALUE) > 0
					? null
					: new Int2ObjectHashMap<>((int) 1.2 * table.pkColumns().size(), .9f);
		valueData = new Int2NullableObjectHashMap<>(
				(int) 1.2 * (table.allColumns().size() - ((table.flags() & FLG_ONLY_VALUE) > 0 ? 0 : table.pkColumns().size())),
				.9f);
	}

	@Override
	public void init() {
		if ((table.flags() & FLG_ONLY_VALUE) == 0)
			keyData.clear();
		valueData.clear();
	}

	@Override
	public void init(OraCdcStatementBase stmt) {
		this.stmt = stmt;
		if ((table.flags() & FLG_ONLY_VALUE) == 0)
			keyData.clear();
		valueData.clear();
	}

	@Override
	abstract public void insert(OraCdcColumn column, Object value);

	@Override
	abstract public void delete(OraCdcColumn column, Object value);

	@Override
	abstract public void update(OraCdcColumn column, Object value, boolean after);

	@Override
	public void addRowId(OraCdcStatementBase stmt) {
		if (LOGGER.isDebugEnabled())
			LOGGER.debug("Do primary key substitution for table {}", table.fqn());
		if (LOGGER.isTraceEnabled())
			LOGGER.trace("{} is used as primary key for table {}", stmt.getRowId(), table.fqn());
		keyData.put(0, stmt.getRowId().toString());
	}

	@Override
	public void afterBefore() {};

	@Override
	abstract public void buildSchema(boolean initial) throws SQLException;

	@Override
	abstract public KeyValuePair changeVector(OraCdcTransaction transaction, Map<String, Object> offset, boolean skipRedoRecord) throws SQLException;

	public record KeyValuePair(short operation, Int2ObjectHashMap<Object> key, Int2NullableObjectHashMap<Object> value) {};

	@Override
	public KeyValuePair initialLoadRow() {
		return new KeyValuePair(INSERT, keyData, valueData);
	}

	@Override
	public void initialLoadSetLob(OraCdcColumn column, Object value) {
		valueData.put(column.getColumnId(), Optional.ofNullable(value));
	}


}
