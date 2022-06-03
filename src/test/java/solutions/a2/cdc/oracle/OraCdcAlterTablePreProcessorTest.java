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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import eu.solutions.a2.cdc.oracle.utils.OraSqlUtils;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcAlterTablePreProcessorTest {

	@Test
	public void test() {

		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_RENAME + "\n" + "DESCRIPTION;COMMENTARY",
				OraSqlUtils.alterTablePreProcessor("alter table dept rename column DESCRIPTION to COMMENTARY"),
				"Unexpected results");
		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_RENAME + "\n" + "DESCRIPTION;COMMENTARY",
				OraSqlUtils.alterTablePreProcessor("alter table SCOTT.DEPT rename column DESCRIPTION to COMMENTARY"),
				"Unexpected results");
		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_RENAME + "\n" + "DESCRIPTION;COMMENTARY",
				OraSqlUtils.alterTablePreProcessor("alter table scott. DEPT rename column DESCRIPTION to COMMENTARY"),
				"Unexpected results");
		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_RENAME + "\n" + "DESCRIPTION;COMMENTARY",
				OraSqlUtils.alterTablePreProcessor("alter table SCOTT .dept rename column DESCRIPTION to COMMENTARY"),
				"Unexpected results");
		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_RENAME + "\n" + "DESCRIPTION;COMMENTARY",
				OraSqlUtils.alterTablePreProcessor("alter table scott . dept rename column DESCRIPTION to COMMENTARY"),
				"Unexpected results");


		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_ADD + "\n" + "AMOUNT number(5|2) default 0",
				OraSqlUtils.alterTablePreProcessor("alter table SCOTT . EMP add column AMOUNT number(5,2) default 0"),
				"Unexpected results");
		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_ADD + "\n" + "DESCRIPTION varchar2(255)",
				OraSqlUtils.alterTablePreProcessor("alter table SCOTT.DEPT add DESCRIPTION varchar2(255)"),
				"Unexpected results");
		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_ADD + "\n" + "jcol JSON;AMOUNT number(5|2) default -1",
				OraSqlUtils.alterTablePreProcessor("ALTER TABLE SCOTT . EMP ADD (jcol JSON, AMOUNT number(5,2) default -1)"),
				"Unexpected results");
		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_ADD + "\n" + "AMOUNT number(5|2)",
				OraSqlUtils.alterTablePreProcessor("ALTER TABLE SCOTT . EMP ADD AMOUNT number(5,2)"),
				"Unexpected results");


		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_MODIFY + "\n" + "REF_NO number(9) default 0",
				OraSqlUtils.alterTablePreProcessor("alter table SCOTT . EMP modify REF_NO number(9) default 0"),
				"Unexpected results");
		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_MODIFY + "\n" + "DESCRIPTION varchar2(1000)",
				OraSqlUtils.alterTablePreProcessor("alter table SCOTT.DEPT modify column DESCRIPTION varchar2(1000)"),
				"Unexpected results");
		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_MODIFY + "\n" + "REF_NO number(9) default 0;AMOUNT number(5|2) default -1",
				OraSqlUtils.alterTablePreProcessor("ALTER TABLE SCOTT . EMP MODIFY (REF_NO number(9) default 0, AMOUNT number(5,2) default -1)"),
				"Unexpected results");


		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_DROP + "\n" + "SALGRADE",
				OraSqlUtils.alterTablePreProcessor("alter table SALARY drop column SALGRADE"),
				"Unexpected results");
		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_DROP + "\n" + "SALGRADE;BONUS",
				OraSqlUtils.alterTablePreProcessor("alter table SCOTT.SALARY drop (SALGRADE, BONUS)"),
				"Unexpected results");
		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_DROP + "\n" + "SALGRADE",
				OraSqlUtils.alterTablePreProcessor("alter table SCOTT. EMP set unused (SALGRADE)"),
				"Unexpected results");
		assertEquals(
				OraSqlUtils.ALTER_TABLE_COLUMN_DROP + "\n" + "BONUS;SALGRADE",
				OraSqlUtils.alterTablePreProcessor("alter table SCOTT .EMP set unused (BONUS, SALGRADE)"),
				"Unexpected results");
	}
}
