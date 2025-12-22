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

package solutions.a2.cdc.oracle.internals;

import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.DELETE;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.INSERT;
import static solutions.a2.cdc.oracle.OraCdcV$LogmnrContents.UPDATE;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public abstract class TestWithOutput {

	private static final String SQL_REDO_WHERE = " where ";
	private static final String SQL_REDO_SET = " set ";
	private static final String SQL_REDO_AND = " and ";
	private static final String SQL_REDO_IS = " IS";
	private static final String SQL_REDO_VALUES = " values ";

	private static final Logger LOGGER = LoggerFactory.getLogger(TestWithOutput.class);
	private static boolean initLogging = true;

	TestWithOutput() {
		if (initLogging) {
			BasicConfigurator.configure();
			initLogging = false;
		}
	}

	boolean compareLogMinerText(final String logMinerSql, final String redoMinerSql) {
		var squeezedSql = StringUtils.trim(Strings.CS.replace(
				Strings.CS.replace(Strings.CS.replace(logMinerSql, "\n", ""), "HEXTORAW('", "'"),
				"')", "'"));
		final short lmOp;
		if (Strings.CI.startsWith(squeezedSql, "delete"))
			lmOp = DELETE;
		else if (Strings.CI.startsWith(squeezedSql, "insert"))
			lmOp = INSERT;
		else if (Strings.CI.startsWith(squeezedSql, "update"))
			lmOp = UPDATE;
		else
			throw new IllegalArgumentException("Unsupported operation in " + logMinerSql);
		switch (lmOp) {
			case DELETE -> {
				var diffWhere = Maps.difference(
						whereClause(squeezedSql), whereClause(redoMinerSql));
				if (!diffWhere.areEqual()) {
					printOneStmtOnly(diffWhere.entriesOnlyOnLeft(),
							"A list of columns and their values ​​that are only available in LogMiner DELETE stmt WHERE clause:");
					printOneStmtOnly(diffWhere.entriesOnlyOnRight(),
							"A list of columns and their values ​​that are only available in RedoMiner DELETE stmt WHERE clause:");
					printDiffs(diffWhere.entriesDiffering(), "Different values in DELETE stmt WHERE clause:");
				}
				return diffWhere.areEqual();
			}
			case INSERT -> {
				var diffWhere = Maps.difference(
						insertStmt(squeezedSql), whereClause(redoMinerSql));
				if (!diffWhere.areEqual()) {
					printOneStmtOnly(diffWhere.entriesOnlyOnLeft(),
							"A list of columns and their values ​​that are only available in LogMiner INSERT stmt:");
					printOneStmtOnly(diffWhere.entriesOnlyOnRight(),
							"A list of columns and their values ​​that are only available in RedoMiner INSERT stmt:");
					printDiffs(diffWhere.entriesDiffering(), "Different values in INSERT stmt:");
				}
				return diffWhere.areEqual();
			}
			case UPDATE -> {
				var diffSet = Maps.difference(
						setClause(squeezedSql), setClause(redoMinerSql));
				if (!diffSet.areEqual()) {
					printOneStmtOnly(diffSet.entriesOnlyOnLeft(),
							"A list of columns and their values ​​that are only available in LogMiner UPDATE stmt SET clause:");
					printOneStmtOnly(diffSet.entriesOnlyOnRight(),
							"A list of columns and their values ​​that are only available in RedoMiner UPDATE stmt SET clause:");
					printDiffs(diffSet.entriesDiffering(), "Different values in the SET clause:");
				}
				var diffWhere = Maps.difference(
						whereClause(squeezedSql), whereClause(redoMinerSql));
				if (!diffWhere.areEqual()) {
					printOneStmtOnly(diffWhere.entriesOnlyOnLeft(),
							"A list of columns and their values ​​that are only available in LogMiner UPDATE stmt WHERE clause:");
					printOneStmtOnly(diffWhere.entriesOnlyOnRight(),
							"A list of columns and their values ​​that are only available in RedoMiner UPDATE stmt WHERE clause:");
					printDiffs(diffWhere.entriesDiffering(), "Different values in UPDATE stmt WHERE clause:");
				}
				return diffSet.areEqual() && diffWhere.areEqual();
			}
		}
		return false;
	}

	private Map<String, String> setClause(final String sql) {
		var whereClauseStart = Strings.CS.indexOf(sql, SQL_REDO_WHERE);
		var setClauseStart = Strings.CS.indexOf(sql, SQL_REDO_SET);
		var setClausePairs = StringUtils.split(
				StringUtils.substring(sql, setClauseStart + SQL_REDO_SET.length(), whereClauseStart), ",");
		var setMap = new HashMap<String, String>((int) (setClausePairs.length * 1.3), .9f);
		for (var i = 0; i < setClausePairs.length; i++) {
			var currentExpr = StringUtils.trim(setClausePairs[i]);
			var columnName = StringUtils.trim(StringUtils.substringBefore(currentExpr, "="));
			if (Strings.CS.endsWith(currentExpr, "L"))
				setMap.put(columnName, "NULL");
			else
				setMap.put(columnName, StringUtils.trim(StringUtils.substringAfter(currentExpr, "=")));
		}
		return setMap;
	}

	private Map<String, String> whereClause(final String sql) {
		var whereClauseStart = Strings.CS.indexOf(sql, SQL_REDO_WHERE);
		var whereClausePairs = StringUtils.splitByWholeSeparator(
				StringUtils.substring(sql, whereClauseStart + SQL_REDO_WHERE.length()), SQL_REDO_AND);
		var whereMap = new HashMap<String, String>((int) (whereClausePairs.length * 1.3), .9f);
		for (var i = 0; i < whereClausePairs.length; i++) {
			var currentExpr = StringUtils.trim(whereClausePairs[i]);
			if (Strings.CS.endsWith(currentExpr, "L"))
				whereMap.put(
						StringUtils.trim(StringUtils.substringBefore(currentExpr, SQL_REDO_IS)),
						"NULL");
			else
				whereMap.put(
						StringUtils.trim(StringUtils.substringBefore(currentExpr, "=")),
						StringUtils.trim(StringUtils.substringAfter(currentExpr, "=")));
		}
		return whereMap;
	}

	private Map<String, String> insertStmt(final String sql) {
		var valueClauseStart = Strings.CS.indexOf(sql, SQL_REDO_VALUES);
		var columnsList = StringUtils.split(StringUtils.substringBetween(
				StringUtils.substring(sql, 0, valueClauseStart), "(", ")"), ",");
		var valuesList = StringUtils.split(StringUtils.substringBetween(
				StringUtils.substring(sql, valueClauseStart + SQL_REDO_VALUES.length()), "(", ")"), ",");
		var insertMap = new HashMap<String, String>((int) (columnsList.length * 1.3), .9f);
		for (var i = 0; i < columnsList.length; i++) {
			var columnValue = StringUtils.trim(valuesList[i]);
			insertMap.put(
					StringUtils.trim(columnsList[i]),
					Strings.CS.startsWith(columnValue, "N") ? "NULL" : columnValue);
		}
		return insertMap;
	}

	private void printOneStmtOnly(Map<String, String> entries, String message) {
		if (!entries.isEmpty()) {
			var sb = new StringBuilder();
			sb
				.append(message)
				.append("\n");
			for (var columnName : entries.keySet())
				sb
					.append('\t')
					.append(columnName)
					.append(" = ")
					.append(entries.get(columnName))
					.append("\n");
			LOGGER.error(sb.toString());
		}
	}

	private void printDiffs(Map<String, ValueDifference<String>> entries, String message) {
		if (!entries.isEmpty()) {
			var sb = new StringBuilder();
			sb
				.append(message)
				.append("\n");
			for (var columnName : entries.keySet())
				sb
					.append('\t')
					.append(columnName)
					.append(" = ")
					.append(entries.get(columnName).leftValue())
					.append("<>")
					.append(entries.get(columnName).rightValue())
					.append("\n");
			LOGGER.error(sb.toString());
		}
	}

}
