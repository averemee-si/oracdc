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

package solutions.a2.cdc.oracle;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static solutions.a2.cdc.oracle.utils.OraSqlUtils.ALTER_TABLE_COLUMN_ADD;
import static solutions.a2.cdc.oracle.utils.OraSqlUtils.ALTER_TABLE_COLUMN_DROP;
import static solutions.a2.cdc.oracle.utils.OraSqlUtils.ALTER_TABLE_COLUMN_MODIFY;
import static solutions.a2.cdc.oracle.utils.OraSqlUtils.ALTER_TABLE_COLUMN_RENAME;
import static solutions.a2.cdc.oracle.utils.OraSqlUtils.DELIMITER;
import static solutions.a2.cdc.oracle.utils.OraSqlUtils.alterTablePreProcessor;

import org.junit.jupiter.api.Test;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcAlterTablePreProcessorTest {

	@Test
	public void test() {

		String originalText = null;

		originalText = "alter table dept rename column DESCRIPTION to COMMENTARY";
		assertEquals(
				ALTER_TABLE_COLUMN_RENAME + DELIMITER + "DESCRIPTION;COMMENTARY" + DELIMITER + originalText,
				alterTablePreProcessor("alter table dept rename column DESCRIPTION to COMMENTARY"),
				"Unexpected results");
		originalText = "alter table SCOTT.DEPT rename column DESCRIPTION to COMMENTARY";
		assertEquals(
				ALTER_TABLE_COLUMN_RENAME + DELIMITER + "DESCRIPTION;COMMENTARY" + DELIMITER + originalText,
				alterTablePreProcessor(originalText),
				"Unexpected results");
		originalText = "alter table scott. DEPT rename column DESCRIPTION to COMMENTARY";
		assertEquals(
				ALTER_TABLE_COLUMN_RENAME + DELIMITER + "DESCRIPTION;COMMENTARY" + DELIMITER + originalText,
				alterTablePreProcessor(originalText),
				"Unexpected results");
		originalText = "alter table SCOTT .dept rename column DESCRIPTION to COMMENTARY";
		assertEquals(
				ALTER_TABLE_COLUMN_RENAME + DELIMITER + "DESCRIPTION;COMMENTARY" + DELIMITER + originalText,
				alterTablePreProcessor(originalText),
				"Unexpected results");
		originalText = "alter table scott . dept rename column DESCRIPTION to COMMENTARY";
		assertEquals(
				ALTER_TABLE_COLUMN_RENAME + DELIMITER + "DESCRIPTION;COMMENTARY" + DELIMITER + originalText,
				alterTablePreProcessor(originalText),
				"Unexpected results");

		originalText = "alter table SCOTT . EMP add column AMOUNT number(5,2) default 0";
		assertEquals(
				ALTER_TABLE_COLUMN_ADD + DELIMITER + "AMOUNT number(5|2) default 0",
				alterTablePreProcessor(originalText),
				"Unexpected results");
		originalText = "alter table SCOTT.DEPT add DESCRIPTION varchar2(255)";
		assertEquals(
				ALTER_TABLE_COLUMN_ADD + DELIMITER + "DESCRIPTION varchar2(255)",
				alterTablePreProcessor(originalText),
				"Unexpected results");
		originalText = "ALTER TABLE SCOTT . EMP ADD (jcol JSON, AMOUNT number(5,2) default -1)";
		assertEquals(
				ALTER_TABLE_COLUMN_ADD + DELIMITER + "jcol JSON;AMOUNT number(5|2) default -1",
				alterTablePreProcessor(originalText),
				"Unexpected results");
		originalText = "ALTER TABLE SCOTT . EMP ADD AMOUNT number(5,2)";
		assertEquals(
				ALTER_TABLE_COLUMN_ADD + DELIMITER + "AMOUNT number(5|2)",
				alterTablePreProcessor(originalText),
				"Unexpected results");

		originalText = "alter table SCOTT . EMP modify REF_NO number(9) default 0";
		assertEquals(
				ALTER_TABLE_COLUMN_MODIFY + DELIMITER + "REF_NO number(9) default 0",
				alterTablePreProcessor(originalText),
				"Unexpected results");
		originalText = "alter table SCOTT.DEPT modify column DESCRIPTION varchar2(1000)";
		assertEquals(
				ALTER_TABLE_COLUMN_MODIFY + DELIMITER + "DESCRIPTION varchar2(1000)",
				alterTablePreProcessor(originalText),
				"Unexpected results");
		originalText = "ALTER TABLE SCOTT . EMP MODIFY (REF_NO number(9) default 0, AMOUNT number(5,2) default -1)";
		assertEquals(
				ALTER_TABLE_COLUMN_MODIFY + DELIMITER + "REF_NO number(9) default 0;AMOUNT number(5|2) default -1",
				alterTablePreProcessor(originalText),
				"Unexpected results");

		originalText = "alter table SALARY drop column SALGRADE";
		assertEquals(
				ALTER_TABLE_COLUMN_DROP + DELIMITER + "SALGRADE" + DELIMITER + originalText,
				alterTablePreProcessor(originalText),
				"Unexpected results");
		originalText = "alter table SCOTT.SALARY drop (SALGRADE, BONUS)";
		assertEquals(
				ALTER_TABLE_COLUMN_DROP + DELIMITER + "SALGRADE;BONUS" + DELIMITER + originalText,
				alterTablePreProcessor(originalText),
				"Unexpected results");
		originalText = "alter table SCOTT. EMP set unused (SALGRADE)";
		assertEquals(
				ALTER_TABLE_COLUMN_DROP + DELIMITER + "SALGRADE" + DELIMITER + originalText,
				alterTablePreProcessor(originalText),
				"Unexpected results");
		originalText = "alter table SCOTT .EMP set unused (BONUS, SALGRADE)";
		assertEquals(
				ALTER_TABLE_COLUMN_DROP + DELIMITER + "BONUS;SALGRADE" + DELIMITER + originalText,
				alterTablePreProcessor(originalText),
				"Unexpected results");

	}
}
