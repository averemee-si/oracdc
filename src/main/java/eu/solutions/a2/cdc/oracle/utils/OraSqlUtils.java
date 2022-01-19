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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 
 * @author averemee
 *
 */
public class OraSqlUtils {

	private static final String SQL_AND = " and ";

	public static final int MODE_WHERE_ALL_MVIEW_LOGS = 1;
	public static final int MODE_WHERE_ALL_OBJECTS = 2;
	public static final String ALTER_TABLE_COLUMN_RENAME = "rename";
	public static final String ALTER_TABLE_COLUMN_ADD = "add";
	public static final String ALTER_TABLE_COLUMN_MODIFY = "modify";
	public static final String ALTER_TABLE_COLUMN_DROP = "drop";
	public static final String ALTER_TABLE_COLUMN_SET = "set";
	public static final String RESERVED_WORD_COLUMN = "column";
	public static final String RESERVED_WORD_UNUSED = "unused";
	public static final String RESERVED_WORD_CONSTRAINT = "constraint";
	public static final String RESERVED_WORD_SUPPLEMENTAL = "supplemental";
	public static final String RESERVED_WORD_TO = "to";

	private static final String COMMA_INSIDE = ",(?=[^()]*\\))";

	public static String parseTableSchemaList(final boolean exclude, final int mode, final List<String> listSchemaObj) {
		final String schemaNameField;
		final String objNameField;
		if (mode == MODE_WHERE_ALL_MVIEW_LOGS) {
			schemaNameField = "L.LOG_OWNER";
			objNameField = "L.MASTER";
		} else {
			schemaNameField = "O.OWNER";
			objNameField = "O.OBJECT_NAME";
		}

		final StringBuilder sb = new StringBuilder(512);
		sb.append(SQL_AND);
		sb.append("(");

		for (int i = 0; i < listSchemaObj.size(); i++) {
			final String schemaObj = StringUtils.trim(listSchemaObj.get(i));
			boolean escaped = StringUtils.contains(schemaObj, "\"");
			if (schemaObj.contains(".")) {
				final String[] pairSchemaObj = schemaObj.split("\\.");
				final String schemaName = StringUtils.trim(pairSchemaObj[0]);
				final String objName = StringUtils.trim(pairSchemaObj[1]);
				if (StringUtils.equals("%", pairSchemaObj[1]) ||
						StringUtils.equals("*", pairSchemaObj[1])) {
					// Only schema name present
					sb.append("(");
					sb.append(schemaNameField);
					sb.append(exclude ? "!='" : "='");
					sb.append(escaped ? 
										StringUtils.remove(schemaName, "\"") :
										StringUtils.upperCase(schemaName));
					sb.append("')");
				} else {
					// Process pair... ... ...
					sb.append("(");
					sb.append(schemaNameField);
					sb.append("='");
					sb.append(escaped ? 
										StringUtils.remove(schemaName, "\"") :
										StringUtils.upperCase(schemaName));
					sb.append("'");
					sb.append(SQL_AND);
					sb.append(objNameField);
					sb.append(exclude ? "!='" : "='");
					sb.append(escaped ? 
										StringUtils.remove(objName, "\"") :
										StringUtils.upperCase(objName));
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

	public static String alterTablePreProcessor(final String originalText) {
		String[] tokens = StringUtils.splitPreserveAllTokens(originalText);
		if (StringUtils.equalsIgnoreCase(tokens[0], "alter") &&
				StringUtils.equalsIgnoreCase(tokens[1], "table")) {
			final int beginIndex;
			if ((StringUtils.endsWith(tokens[2], ".") && tokens[2].length() > 1) ||
					(StringUtils.startsWith(tokens[3], ".") && tokens[3].length() > 1)) {
				// alter table SCOTT. DEPT <REST OF...>
				// alter table SCOTT .DEPT <REST OF...>
				beginIndex = 4;
			} else if (StringUtils.equals(tokens[3], ".")) {
				// alter table SCOTT . DEPT <REST OF...>
				beginIndex = 5;
			} else {
				// alter table SCOTT.DEPT <REST OF...>
				beginIndex = 3;
			}
			switch (StringUtils.lowerCase(tokens[beginIndex])) {
			case ALTER_TABLE_COLUMN_RENAME:
				// Only
				//     alter table rename column <OLD_NAME> to <NEW_NAME>
				// is supported
				if (StringUtils.equalsIgnoreCase(tokens[beginIndex + 1], RESERVED_WORD_COLUMN)) {
					if ((tokens.length - beginIndex) == 5 && 
							StringUtils.equalsIgnoreCase(tokens[beginIndex + 3], RESERVED_WORD_TO)) {
						// tokens[beginIndex + 2] - old name
						// tokens[beginIndex + 4] - new name
						return ALTER_TABLE_COLUMN_RENAME + "\n" +
							tokens[beginIndex + 2] + ";" + tokens[beginIndex + 4];
					} else {
						return null;
					}
				} else {
					return null;
				}
			case ALTER_TABLE_COLUMN_ADD:
				return alterTablePreProcessor(originalText, ALTER_TABLE_COLUMN_ADD, tokens, beginIndex);
			case ALTER_TABLE_COLUMN_MODIFY:
				return alterTablePreProcessor(originalText, ALTER_TABLE_COLUMN_MODIFY, tokens, beginIndex);
			case ALTER_TABLE_COLUMN_DROP:
				if (StringUtils.equalsIgnoreCase(tokens[beginIndex + 1], RESERVED_WORD_UNUSED)) {
					// Ignore
					// alter table drop unused columns;
					return null;
				} else if (StringUtils.equalsIgnoreCase(tokens[beginIndex + 1], RESERVED_WORD_COLUMN)) {
					// alter table table_name drop column column_name;
					return ALTER_TABLE_COLUMN_DROP + "\n" +
						StringUtils.trim(tokens[beginIndex + 2]);
				} else if (StringUtils.startsWith(tokens[beginIndex + 1], "(")) {
					// alter table table_name drop (column_name1, column_name2);
					return ALTER_TABLE_COLUMN_DROP + "\n" +
						Arrays
							.stream(StringUtils.split(
									StringUtils.substringBetween(
											originalText, "(", ")"), ","))
							.map(s -> StringUtils.trim(s))
							.collect(Collectors.joining(";"));
				} else {
					return null;
				}
			case ALTER_TABLE_COLUMN_SET:
				if (StringUtils.equalsIgnoreCase(tokens[beginIndex + 1], RESERVED_WORD_UNUSED)) {
					// alter table table_name set unused (column_name1, column_name2);
					return ALTER_TABLE_COLUMN_DROP + "\n" +
						Arrays
							.stream(StringUtils.split(
										StringUtils.substringBetween(
												originalText, "(", ")"), ","))
							.map(s -> StringUtils.trim(s))
							.collect(Collectors.joining(";"));
				} else {
					return null;
				}
			default:
				return null;
			}		
		} else {
			return null;
		}
	}

	private static String alterTablePreProcessor(
			final String originalText, final String operation, final String[] tokens, final int beginIndex) {
		if (StringUtils.equalsIgnoreCase(tokens[beginIndex + 1], RESERVED_WORD_COLUMN)) {
			// alter table add column <COLUMN_NAME> .......
			return operation + "\n" +
				Arrays
					.stream(tokens, beginIndex + 2, tokens.length)
					.map(s -> StringUtils.trim(s))
					.map(s -> StringUtils.replace(s, ",", "|"))
					.collect(Collectors.joining(" "));
		} else if (StringUtils.startsWith(tokens[beginIndex + 1], "(")) {
			// alter table add (<COLUMN_NAME> .......)
			// For further processing only data between first "(" and last ")"
			// are needed, also replace all commas used in NUMBER precision
			// and finally split by commas
			final String[] columnsToAdd = 
					StringUtils.split(
						RegExUtils.replaceAll(
							StringUtils.substring(originalText, 
								StringUtils.indexOf(originalText, "(") + 1, 
								StringUtils.lastIndexOf(originalText, ")")),
							COMMA_INSIDE, "|"),
						",");
			return operation + "\n" +
				Arrays
					.stream(columnsToAdd)
					.map(s -> StringUtils.trim(s))
					.collect(Collectors.joining(";"));
		} else if (StringUtils.equalsIgnoreCase(tokens[beginIndex + 1], RESERVED_WORD_CONSTRAINT) ||
				StringUtils.equalsIgnoreCase(tokens[beginIndex + 1], RESERVED_WORD_SUPPLEMENTAL)) {
			// Ignore
			// alter table add CoNsTrAiNt ...
			// alter table add SuPpLeMeNtAl LoG DaTa ...
			return null;
		} else {
			return operation + "\n" +
				Arrays
					.stream(tokens, beginIndex + 1, tokens.length)
					.map(s -> StringUtils.trim(s))
					.map(s -> StringUtils.replace(s, ",", "|"))
					.collect(Collectors.joining(" "));
		}
	}

}
