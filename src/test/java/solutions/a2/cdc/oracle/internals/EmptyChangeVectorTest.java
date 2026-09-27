package solutions.a2.cdc.oracle.internals;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import org.junit.jupiter.api.Test;

public class EmptyChangeVectorTest {

	@Test
	public void testParseEmptyChangeVectorNoOutOfBounds() {
		// This hex string represents a minimal 48-byte redo record reproducing a crash report.
		// Its structure is:
		// - 4 bytes: Record length (48 bytes, 0x0030 -> "30000000")
		// - 20 bytes: Oracle Record Header (SCN, block IDs)
		// - 24 bytes: Change Vector 1 (Layer 35.230). This vector has NO payload, only a header.
		// The parser crashed when attempting to read the payload size of this empty 24-byte vector.
		var ba = hexToRaw("3000000001900800C902673A32000001000000002C03560023E600000000000000000000000000000000000000000000");
		
		var orl = OraCdcRedoLog.getLinux11g(); // Use 11g to expect standard 24-byte headers without CDB fields
		
		var rr = assertDoesNotThrow(() -> {
			return new OraCdcRedoRecord(orl, 35339567817L, "0x131568.00025dd6.0194", ba);
		});
		
		assertEquals(1, rr.changeVectors().size());
		
		// Vector is 35.230 (0x23E6)
		assertEquals(0x23E6, rr.changeVectors().get(0).operation & 0xFFFF);
	}
}
