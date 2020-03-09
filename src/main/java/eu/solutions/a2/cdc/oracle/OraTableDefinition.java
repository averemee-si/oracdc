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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * 
 * @author averemee
 *
 */
public abstract class OraTableDefinition {

	protected String tableOwner;
	protected String tableName;
	protected final int schemaType;

	protected final List<OraColumn> allColumns;
	protected final Map<String, OraColumn> pkColumns;

	protected OraTableDefinition(final int schemaType) {
		this.schemaType = schemaType;
		this.allColumns = new ArrayList<>();
		this.pkColumns = new LinkedHashMap<>();
	}

	protected OraTableDefinition(final String tableOwner, final String tableName, final int schemaType) {
		this(schemaType);
		this.tableOwner = tableOwner;
		this.tableName = tableName;
	}


}
