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

package solutions.a2.cdc.oracle;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 
 * Interface for LogMiner operations
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public interface OraLogMiner {

	void createStatements(final Connection connLogMiner) throws SQLException;
	boolean next() throws SQLException;
	boolean extend() throws SQLException;
	void stop() throws SQLException;
	boolean isDictionaryAvailable();
	long getDbId();
	String getDbUniqueName();
	void setFirstChange(long firstChange) throws SQLException;
	long getFirstChange();
	long getNextChange();

	/**
	 * Does implementation requires connection to Oracle Database? 
	 *
	 * 
	 * @return true if implementation required connection to Oracle database otherwise - false 
	 */
	default boolean isOracleConnectionRequired() {
		return true;
	}

}
