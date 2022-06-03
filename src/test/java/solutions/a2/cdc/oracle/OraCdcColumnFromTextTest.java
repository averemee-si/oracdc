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

import solutions.a2.cdc.oracle.OraColumn;
import solutions.a2.cdc.oracle.UnsupportedColumnDataTypeException;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcColumnFromTextTest {

	@Test
	public void test() throws UnsupportedColumnDataTypeException {
		OraColumn colDname = new OraColumn(true, true, "DNAME", "varchar2(100) null default 'SALES'",
				"alter table dept add DNAME varchar2(100) null default 'SALES'",
				4);
		assertTrue(colDname.isNullable());
		assertEquals(colDname.getDefaultValue(), "'SALES'");
		assertEquals(colDname.getJdbcType(), Types.VARCHAR);

		OraColumn colDnameSimple = new OraColumn(true, true, "DNAME", "varchar2(100)",
				"alter table dept add DNAME varchar2(100)",
				4);
		assertTrue(colDnameSimple.isNullable());
		assertNull(colDnameSimple.getDefaultValue());
		assertEquals(colDnameSimple.getJdbcType(), Types.VARCHAR);

		OraColumn colDloc = new OraColumn(true, true, "DLOC", "varchar2(10) not null default 'NY'",
				"alter table dept add DLOC varchar2(10) not null default 'NY'",
				5);
		assertFalse(colDloc.isNullable());
		assertEquals(colDloc.getDefaultValue(), "'NY'");
		assertEquals(colDloc.getJdbcType(), Types.VARCHAR);

		OraColumn colDeptDate = new OraColumn(true, true, "DEPT_FOUNDED", "date not null default SYSDATE",
				"alter table dept add DEPT_FOUNDED date not null default SYSDATE",
				6);
		assertFalse(colDeptDate.isNullable());
		assertEquals(colDeptDate.getDefaultValue(), "SYSDATE");
		assertEquals(colDeptDate.getJdbcType(), Types.TIMESTAMP);

		OraColumn colDeptDateSimple = new OraColumn(true, true, "DEPT_FOUNDED", "date",
				"alter table dept add DEPT_FOUNDED date",
				6);
		assertTrue(colDeptDateSimple.isNullable());
		assertNull(colDeptDateSimple.getDefaultValue());
		assertEquals(colDeptDateSimple.getJdbcType(), Types.TIMESTAMP);

		OraColumn colDeptNumberTinyInt = new OraColumn(true, true, "SOME_NUMBER", "NUMBER(2)",
				"alter table dept add SOME_NUMBER number(2)",
				7);
		assertTrue(colDeptNumberTinyInt.isNullable());
		assertNull(colDeptNumberTinyInt.getDefaultValue());
		assertEquals(colDeptNumberTinyInt.getJdbcType(), Types.TINYINT);

		OraColumn colDeptNumberSmallInt = new OraColumn(true, true, "SOME_NUMBER", "NUMBER(4)",
				"alter table dept add SOME_NUMBER NUMBER(4)",
				7);
		assertTrue(colDeptNumberSmallInt.isNullable());
		assertNull(colDeptNumberSmallInt.getDefaultValue());
		assertEquals(colDeptNumberSmallInt.getJdbcType(), Types.SMALLINT);

		OraColumn colDeptNumberInteger = new OraColumn(true, true, "SOME_NUMBER", "NUMBER(8)",
				"alter table dept add SOME_NUMBER NUMBER(8)",
				7);
		assertTrue(colDeptNumberInteger.isNullable());
		assertNull(colDeptNumberInteger.getDefaultValue());
		assertEquals(colDeptNumberInteger.getJdbcType(), Types.INTEGER);

		OraColumn colDeptNumberBigInt = new OraColumn(true, true, "SOME_NUMBER", "NUMBER(18) not null",
				"alter table dept add SOME_NUMBER NUMBER(18) not null",
				7);
		assertFalse(colDeptNumberBigInt.isNullable());
		assertNull(colDeptNumberBigInt.getDefaultValue());
		assertEquals(colDeptNumberBigInt.getJdbcType(), Types.BIGINT);

		OraColumn colDeptNumber = new OraColumn(true, true, "SOME_NUMBER", "NUMBER not null",
				"alter table dept add SOME_NUMBER NUMBER not null",
				7);
		assertFalse(colDeptNumber.isNullable());
		assertNull(colDeptNumber.getDefaultValue());
		assertEquals(colDeptNumber.getJdbcType(), Types.NUMERIC);

	}


}
