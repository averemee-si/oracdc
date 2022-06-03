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

package solutions.a2.cdc.oracle;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;


/**
 * 
 * @author averemee
 *
 */
@JsonInclude(Include.NON_EMPTY)
public abstract class OraTableDefinition {

	protected String tableOwner;
	protected String tableName;
	protected final int schemaType;
	protected int version;

	@JsonProperty("columns")
	protected List<OraColumn> allColumns;
	protected Map<String, OraColumn> pkColumns;

	protected OraTableDefinition(final int schemaType) {
		this.pkColumns = new LinkedHashMap<>();
		this.schemaType = schemaType;
		this.allColumns = new ArrayList<>();
		this.version = 1;
	}

	protected OraTableDefinition(final String tableOwner, final String tableName, final int schemaType) {
		this(schemaType);
		this.tableOwner = tableOwner;
		this.tableName = tableName;
	}

	public String getTableOwner() {
		return tableOwner;
	}

	public void setTableOwner(String tableOwner) {
		this.tableOwner = tableOwner;
	}

	public String getTableName() {
		return tableName;
	}

	public int getSchemaType() {
		return schemaType;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<OraColumn> getAllColumns() {
		return allColumns;
	}

	public void setAllColumns(List<OraColumn> allColumns) {
		this.allColumns = allColumns;
	}

	public Map<String, OraColumn> getPkColumns() {
		return pkColumns;
	}

}
