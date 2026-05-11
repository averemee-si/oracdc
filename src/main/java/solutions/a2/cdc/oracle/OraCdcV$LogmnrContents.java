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

 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle;

/**
 * 
 * V$LOGMNR_CONTENTS.OPERATION_CODE
 * 
 * For description please see <a href="https://docs.oracle.com/en/database/oracle/oracle-database/23/refrn/V-LOGMNR_CONTENTS.html">V$LOGMNR_CONTENTS</a>
 * 
 * @author averemee
 */
public class OraCdcV$LogmnrContents {

	public static final short INTERNAL = 0;
	public static final short INSERT = 1;
	public static final short DELETE = 2;
	public static final short UPDATE = 3;
	public static final short DDL = 5;
	public static final short START = 6;
	public static final short COMMIT = 7;
	public static final short ROLLBACK = 36;
	public static final short SELECT_LOB_LOCATOR = 9;
	public static final short LOB_WRITE = 10;
	public static final short LOB_TRIM = 11;
	public static final short LOB_ERASE = 29;
	public static final short XML_DOC_BEGIN = 68;
	public static final short XML_DOC_WRITE = 70;
	public static final short XML_DOC_END = 71;
	public static final short UNSUPPORTED = 255;

}
