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
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public abstract class OraCdcRollbackData {

	final static OraCdcLogMinerStatement updIn1 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='SALES' where DEPTNO=10",
			System.currentTimeMillis(),275168436063l," 0x000098.000001b5.0010 ",
			0, "AAAWbzAAEAAAB6FAAA", false);

	final static OraCdcLogMinerStatement updIn2 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='OPERATIONS' where DEPTNO=20",
			System.currentTimeMillis(),275168436122l," 0x000098.000001b5.0020 ",
			0, "AAAWbzAAEAAAB6FABB", false);

	final static OraCdcLogMinerStatement rb1 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='SALES' where DEPTNO=20",
			System.currentTimeMillis(),275168436122l," 0x000098.000001b5.0020 ",
			0, "AAAWbzAAEAAAB6FABB", true);

	final static OraCdcLogMinerStatement updIn3 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='ACCOUNTING' where DEPTNO=30",
			System.currentTimeMillis(),275168436125l," 0x000098.000001b6.0030 ",
			0, "AAAWbzAAEAAAB6FACC", false);

	final static OraCdcLogMinerStatement updIn4 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='MARKETING' where DEPTNO=40",
			System.currentTimeMillis(),275168436126l," 0x000098.000001b6.0040 ",
			0, "AAAWbzAAEAAAB6FACD", false);

	final static OraCdcLogMinerStatement updIn5 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='SALES' where DEPTNO=50",
			System.currentTimeMillis(),275168436127l," 0x000098.000001b6.0050 ",
			0, "AAAWbzAAEAAAB6FACE", false);

	final static OraCdcLogMinerStatement rb2 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='SALES' where DEPTNO=50",
			System.currentTimeMillis(),275168436127l," 0x000098.000001b6.0052 ",
			0, "AAAWbzAAEAAAB6FACE", true);

	final static OraCdcLogMinerStatement updIn6 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='SALES' where DEPTNO=50",
			System.currentTimeMillis(),275168436127l," 0x000098.000001b6.0054 ",
			0, "AAAWbzAAEAAAB6FACE", false);

	final static OraCdcLogMinerStatement rb3 =  new  OraCdcLogMinerStatement(
			74590, (short)3, "update DEPT set DNAME='SALES' where DEPTNO=50",
			System.currentTimeMillis(),275168436127l," 0x000098.000001b6.0056 ",
			0, "AAAWbzAAEAAAB6FACE", true);

}
