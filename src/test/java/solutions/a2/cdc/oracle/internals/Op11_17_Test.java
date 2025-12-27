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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.LOB_OP_WRITE;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.TYPE_1;
import static solutions.a2.cdc.oracle.internals.OraCdcChangeLlb.TYPE_3;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import org.junit.jupiter.api.Test;

import solutions.a2.oracle.internals.LobId;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class Op11_17_Test extends TestWithOutput {

	@Test
	public void type1_OpWrite_Le() {

		var orl = OraCdcRedoLog.getLinux19c();
		
		var baLlb = hexToRaw("B000000001008905C9C7CEDE2F00000007D7CD51000000000B110000000000000000000000000000000000000006073503000000000000000A00100001005000080013540000000000000000000031010000000001EC45150200000054000C00C98628000000000100002EE38F9A240001000000C41F000000000000E8F40200030000000000000001000000010000000000000000000000000000000000000000000000000000000000000000000000");
		var rrLlb = new OraCdcRedoRecord(orl, 0x00000589decec7c9l, "0x003659.0000042d.00c8", baLlb);

		assertFalse(rrLlb.has5_1());
		assertTrue(rrLlb.hasLlb());
		assertEquals(rrLlb.changeLlb().obj(), 193768);
		assertEquals(rrLlb.changeLlb().conId(), 3);
		assertEquals(rrLlb.changeLlb().lid(), new LobId(hexToRaw("0000000100002ee38f9a")));
		assertEquals(rrLlb.changeLlb().type(), TYPE_1);
		assertEquals(rrLlb.changeLlb().lobOp(), LOB_OP_WRITE);
		assertEquals(rrLlb.changeLlb().lobCol(), 0x24);

		System.out.println(rrLlb);
	}
	
	@Test
	public void type1_OpWrite_Be() {

		var orl = OraCdcRedoLog.getAix19c();
		
		var baLlb = hexToRaw("000000B001400101B491EA0E000100F100000000000080980B11000000000000000000000000000000000000000600000000000000000000000A0010000100500008000000000000000000000000310100000000014C00090200000001060018009886F1000000010000F544708500070000000100001FC400000000002339E9000100000000000001000000000000010000000000000000000000000000000000000000000000000000000000000000");
		var rrLlb = new OraCdcRedoRecord(orl, 0x00000589decec7c9l, "0x053105.00017784.0058", baLlb);

		assertFalse(rrLlb.has5_1());
		assertTrue(rrLlb.hasLlb());
		assertEquals(rrLlb.changeLlb().obj(), 2308585);
		assertEquals(rrLlb.changeLlb().conId(), 0);
		assertEquals(rrLlb.changeLlb().lid(), new LobId(hexToRaw("000000010000f5447085")));
		assertEquals(rrLlb.changeLlb().type(), TYPE_1);
		assertEquals(rrLlb.changeLlb().lobOp(), LOB_OP_WRITE);
		assertEquals(rrLlb.changeLlb().lobCol(), 0x7);

		System.out.println(rrLlb);
	}
	
	@Test
	public void type3_Le() {

		var orl = OraCdcRedoLog.getLinux19c();
		
		var baLlb = hexToRaw("7800000001008905FBD3CEDE060000BC07D7CD51000000000B11000000000000000000000000000000000000000600000300000000000000080010000100240000000000000000000000310100000000031F061759000300EA832A0087EB0200A80600000000000046000000000000000000000007004500");
		var rrLlb = new OraCdcRedoRecord(orl, 0x00000589deced3fbl, "0x003659.00001c31.00c8", baLlb);

		assertFalse(rrLlb.has5_1());
		assertTrue(rrLlb.hasLlb());
		assertEquals(rrLlb.changeLlb().obj(), 191367);
		assertEquals(rrLlb.changeLlb().conId(), 3);
		assertEquals(rrLlb.changeLlb().type(), TYPE_3);
		assertEquals(rrLlb.changeLlb().lobCol(), 0x45);

		System.out.println(rrLlb);
	}

}
