/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Andrey Katamanov
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

package solutions.a2.cdc.oracle.internals;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import java.sql.SQLException;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import solutions.a2.oracle.utils.BinaryUtils;

public class ChainedRecordSequenceTest {

	private static final int BLOCK_SIZE = 0x200;
	private static final int SEQUENCE = 0x137021;
	private static final int FOREIGN_SEQUENCE = 0x137024;
	private static final int RECORD_OFFSET = 0x198;

	/**
	 * A record that starts at the end of block 2 continues in block 3,
	 * and block 3 belongs to another redo sequence.
	 * The record must not be assembled from blocks of different sequences:
	 * iteration stops, like on a sequence mismatch at the record start.
	 */
	@Test
	public void testContinuationBlockOfAnotherSequence() throws Exception {
		// 296 bytes (0x128) record, the first 104 bytes fit into block 2 at offset 0x198
		final var record = hexToRaw(
				"280100000100080086BCFB6F2C0000CC000000009EDAC300050116003300FFFFC837C60C86BCFB6F080000002800FFFF"
				+ "00000000000000000E00140018000C001C0000001C0000006800A010220000000300080032CCE200D2CA282561232301"
				+ "612323010C0000000A000C001D00020013000000020500002166E900DDCA12008DAA360783A16E03FA12050101000000"
				+ "2C01000059000901140000000012050108000000323032362D31302D30355430373A31303A313007EC00000001310800"
				+ "54FAFB6F6A0000300000000020010000050116000300FFFF2166E90054FAFB6F080000001300FFFF0000000000000000"
				+ "0C00140018000C00140014005800E218220000000300080032CCE200DDCA130161232301612323010C00000000000000"
				+ "0B010812000032CC");
		final var head = BLOCK_SIZE - RECORD_OFFSET;

		final var file = new byte[BLOCK_SIZE * 3];
		// Block 1: redo log header, RDBMS version 12
		blockHeader(file, 0, 1, SEQUENCE, 0);
		file[0x17] = 0x0C;
		// Block 2: record start
		blockHeader(file, 1, 2, SEQUENCE, RECORD_OFFSET);
		System.arraycopy(record, 0, file, BLOCK_SIZE + RECORD_OFFSET, head);
		// Block 3: record continuation, but from another sequence
		blockHeader(file, 2, 3, FOREIGN_SEQUENCE, 0);
		System.arraycopy(record, head, file, BLOCK_SIZE * 2 + 0x10, record.length - head);

		final var orl = new OraCdcRedoLog(new InMemoryReader(file), false, BinaryUtils.get(true), 10);
		final var iterator = orl.iterator();

		assertDoesNotThrow(() -> assertFalse(iterator.hasNext()));
		orl.close();
	}

	private static void blockHeader(final byte[] file, final int index, final int blk, final int seq, final int firstRecord) {
		final var base = index * BLOCK_SIZE;
		file[base] = 0x01;
		file[base + 1] = 0x22;
		putU32(file, base + 0x04, blk);
		putU32(file, base + 0x08, seq);
		file[base + 0x0C] = (byte) firstRecord;
		file[base + 0x0D] = (byte) (firstRecord >> 8);
	}

	private static void putU32(final byte[] file, final int pos, final int value) {
		file[pos] = (byte) value;
		file[pos + 1] = (byte) (value >> 8);
		file[pos + 2] = (byte) (value >> 16);
		file[pos + 3] = (byte) (value >> 24);
	}

	private static class InMemoryReader implements OraCdcRedoReader {

		private final byte[] data;
		private int position = 0;

		InMemoryReader(final byte[] data) {
			this.data = Arrays.copyOf(data, data.length);
		}

		@Override
		public int read(final byte[] b, final int off, final int len) throws SQLException {
			if (position + len > data.length) {
				return Integer.MIN_VALUE;
			}
			System.arraycopy(data, position, b, off, len);
			position += len;
			return len;
		}

		@Override
		public long skip(final long n) throws SQLException {
			position += (int) (n * BLOCK_SIZE);
			return n * BLOCK_SIZE;
		}

		@Override
		public void reset() throws SQLException {
			position = 0;
		}

		@Override
		public void close() throws SQLException {
		}

		@Override
		public int blockSize() {
			return BLOCK_SIZE;
		}

		@Override
		public String redoLog() {
			return "in-memory.log";
		}
	}
}
