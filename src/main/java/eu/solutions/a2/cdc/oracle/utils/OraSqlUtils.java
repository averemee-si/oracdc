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

package eu.solutions.a2.cdc.oracle.utils;

import java.util.List;

/**
 * 
 * @author averemee
 *
 */
public class OraSqlUtils {

	private static final String SQL_AND = " and ";

	public static final int MODE_WHERE_ALL_MVIEW_LOGS = 1;
	public static final int MODE_WHERE_ALL_OBJECTS = 2;

	public static String parseTableSchemaList(final boolean exclude, final int mode, final List<String> listSchemaObj) {
		final String schemaNameField;
		final String objNameField;
		if (mode == MODE_WHERE_ALL_MVIEW_LOGS) {
			schemaNameField = "L.LOG_OWNER";
			objNameField = "L.MASTER";
		} else {
			schemaNameField = "OWNER";
			objNameField = "OBJECT_NAME";
		}

		final StringBuilder sb = new StringBuilder(512);
		sb.append(SQL_AND);
		sb.append("(");

		for (int i = 0; i < listSchemaObj.size(); i++) {
			final String schemaObj = listSchemaObj.get(i).trim();
			if (schemaObj.contains(".")) {
				final String[] pairSchemaObj = schemaObj.split("\\.");
				if ("%".equals(pairSchemaObj[1]) || "*".equals(pairSchemaObj[1])) {
					// Only schema name present
					sb.append("(");
					sb.append(schemaNameField);
					sb.append(exclude ? "!='" : "='");
					sb.append(pairSchemaObj[0]);
					sb.append("')");
				} else {
					// Process pair... ... ...
					sb.append("(");
					sb.append(schemaNameField);
					sb.append(exclude ? "!='" : "='");
					sb.append(pairSchemaObj[0]);
					sb.append("'");
					sb.append(SQL_AND);
					sb.append(objNameField);
					sb.append(exclude ? "!='" : "='");
					sb.append(pairSchemaObj[1]);
					sb.append("')");
				}
			} else {
				// Just plain table name without owner
				sb.append("(");
				sb.append(objNameField);
				sb.append(exclude ? "!='" : "='");
				sb.append(schemaObj);
				sb.append("')");
			}
			if (i < listSchemaObj.size() - 1) {
				if (exclude)
					sb.append(SQL_AND);
				else
					sb.append(" or ");
			}
		}

		sb.append(")");
		return sb.toString();
	}

}
