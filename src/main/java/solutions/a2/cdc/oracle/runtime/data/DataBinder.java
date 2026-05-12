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

import java.sql.SQLException;
import java.util.Map;

import solutions.a2.cdc.oracle.OraCdcStatementBase;
import solutions.a2.cdc.oracle.OraCdcTransaction;
import solutions.a2.cdc.oracle.OraCdcColumn;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public interface DataBinder {
	default void init() {};
	void init(OraCdcStatementBase stmt);
	void insert(OraCdcColumn column, Object value);
	void delete(OraCdcColumn column, Object value);
	void update(OraCdcColumn column, Object value, boolean after);
	void addRowId(OraCdcStatementBase stmt);
	void afterBefore();
	void buildSchema(boolean initial) throws SQLException;
	Object changeVector(OraCdcTransaction transaction, Map<String, Object> offset, boolean skipRedoRecord) throws SQLException;
	default void initialLoadSetLob(OraCdcColumn column, Object value) {};
	default Object initialLoadRow() {
		return null;
	};
	default Object newInstance() {
		return null;
	}

	static final String TOLERANCE_ERR_MSG =
			"""
			
			=====================
			The number of required columns for table {} is {},
			but only {} required columns are returned from the redo record!
			Please check the supplemental logging settings!
			SQL statement information:
			SCN/RBA = {}/{}, COMMIT_SCN={}, XID={}
			{}
			
			{}
			
			=====================
			
			""";

}
