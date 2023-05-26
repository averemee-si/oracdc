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

}
