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

package eu.solutions.a2.cdc.oracle;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 
 * Interface for LogMiner operations
 * 
 * 
 * @author averemee
 */
public interface OraLogMiner {

	void createStatements(final Connection connection) throws SQLException;
	boolean next() throws SQLException;
	boolean extend() throws SQLException;
	void stop() throws SQLException;
	boolean isDictionaryAvailable();
	long getDbId();
	String getDbUniqueName();

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
