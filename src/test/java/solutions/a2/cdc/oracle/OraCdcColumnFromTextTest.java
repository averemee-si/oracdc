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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Types;

import org.junit.jupiter.api.Test;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcColumnFromTextTest {

	@Test
	public void test() throws UnsupportedColumnDataTypeException {
		OraCdcColumn colDname = new OraCdcColumn(true, "DNAME", "varchar2(100) null default 'SALES'",
				"alter table dept add DNAME varchar2(100) null default 'SALES'",
				4, null, null, true);
		assertTrue(colDname.nullable());
		assertEquals(colDname.defaultValue(), "'SALES'");
		assertEquals(colDname.jdbcType(), Types.VARCHAR);

		OraCdcColumn colDnameSimple = new OraCdcColumn(true, "DNAME", "varchar2(100)",
				"alter table dept add DNAME varchar2(100)",
				4, null, null, true);
		assertTrue(colDnameSimple.nullable());
		assertNull(colDnameSimple.defaultValue());
		assertEquals(colDnameSimple.jdbcType(), Types.VARCHAR);

		OraCdcColumn colDloc = new OraCdcColumn(true, "DLOC", "varchar2(10) not null default 'NY'",
				"alter table dept add DLOC varchar2(10) not null default 'NY'",
				5, null, null, true);
		assertFalse(colDloc.nullable());
		assertEquals(colDloc.defaultValue(), "'NY'");
		assertEquals(colDloc.jdbcType(), Types.VARCHAR);

		OraCdcColumn colDeptDate = new OraCdcColumn(true, "DEPT_FOUNDED", "date not null default SYSDATE",
				"alter table dept add DEPT_FOUNDED date not null default SYSDATE",
				6, null, null, true);
		assertFalse(colDeptDate.nullable());
		assertEquals(colDeptDate.defaultValue(), "SYSDATE");
		assertEquals(colDeptDate.jdbcType(), Types.DATE);

		OraCdcColumn colDeptTimestamp = new OraCdcColumn(true, "DEPT_FOUNDED", "TIMESTAMP not null default SYSDATE",
				"alter table dept add DEPT_FOUNDED date not null default SYSDATE",
				6, null, null, true);
		assertFalse(colDeptTimestamp.nullable());
		assertEquals(colDeptTimestamp.defaultValue(), "SYSDATE");
		assertEquals(colDeptTimestamp.jdbcType(), Types.TIMESTAMP);

		OraCdcColumn colDeptDateSimple = new OraCdcColumn(true, "DEPT_FOUNDED", "date",
				"alter table dept add DEPT_FOUNDED date",
				6, null, null, true);
		assertTrue(colDeptDateSimple.nullable());
		assertNull(colDeptDateSimple.defaultValue());
		assertEquals(colDeptDateSimple.jdbcType(), Types.DATE);

		OraCdcColumn colDeptTimestampSimple = new OraCdcColumn(true, "DEPT_FOUNDED", "timestamp",
				"alter table dept add DEPT_FOUNDED timestamp",
				6, null, null, true);
		assertTrue(colDeptTimestampSimple.nullable());
		assertNull(colDeptTimestampSimple.defaultValue());
		assertEquals(colDeptTimestampSimple.jdbcType(), Types.TIMESTAMP);

		OraCdcColumn colDeptNumberTinyInt = new OraCdcColumn(true, "SOME_NUMBER", "NUMBER(2)",
				"alter table dept add SOME_NUMBER number(2)",
				7, null, null, true);
		assertTrue(colDeptNumberTinyInt.nullable());
		assertNull(colDeptNumberTinyInt.defaultValue());
		assertEquals(colDeptNumberTinyInt.jdbcType(), Types.TINYINT);

		OraCdcColumn colDeptNumberSmallInt = new OraCdcColumn(true, "SOME_NUMBER", "NUMBER(4)",
				"alter table dept add SOME_NUMBER NUMBER(4)",
				7, null, null, true);
		assertTrue(colDeptNumberSmallInt.nullable());
		assertNull(colDeptNumberSmallInt.defaultValue());
		assertEquals(colDeptNumberSmallInt.jdbcType(), Types.SMALLINT);

		OraCdcColumn colDeptNumberInteger = new OraCdcColumn(true, "SOME_NUMBER", "NUMBER(8)",
				"alter table dept add SOME_NUMBER NUMBER(8)",
				7, null, null, true);
		assertTrue(colDeptNumberInteger.nullable());
		assertNull(colDeptNumberInteger.defaultValue());
		assertEquals(colDeptNumberInteger.jdbcType(), Types.INTEGER);

		OraCdcColumn colDeptNumberBigInt = new OraCdcColumn(true, "SOME_NUMBER", "NUMBER(18) not null",
				"alter table dept add SOME_NUMBER NUMBER(18) not null",
				7, null, null, true);
		assertFalse(colDeptNumberBigInt.nullable());
		assertNull(colDeptNumberBigInt.defaultValue());
		assertEquals(colDeptNumberBigInt.jdbcType(), Types.BIGINT);

		OraCdcColumn colDeptNumber = new OraCdcColumn(true, "SOME_NUMBER", "NUMBER not null",
				"alter table dept add SOME_NUMBER NUMBER not null",
				7, null, null, true);
		assertFalse(colDeptNumber.nullable());
		assertNull(colDeptNumber.defaultValue());
		assertEquals(colDeptNumber.jdbcType(), Types.NUMERIC);

	}


}
