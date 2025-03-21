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

import static solutions.a2.cdc.oracle.internals.OraCdcRedoLog.redoFileTypeByte;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.oracle.utils.BinaryUtils;

public abstract class OraCdcRedoLogFactoryBase  {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoLogFactoryBase.class);
	private static final int POS_BLOCK_SIZE = 0x0015;
	private static final int POS_MAGIC_1 = 0x001C;
	private static final int POS_MAGIC_2 = 0x001D;
	private static final int POS_MAGIC_3 = 0x001E;
	private static final int POS_MAGIC_4 = 0x001F;

	final BinaryUtils bu;
	final boolean valCheckSum;
	
	OraCdcRedoLogFactoryBase(final BinaryUtils bu, final boolean valCheckSum) {
		this.bu = bu;
		this.valCheckSum = valCheckSum;
	}

	long[] blockSizeAndCount(final InputStream fis, final String redoLog) throws IOException {
		byte[] tmpBuffer = new byte[0x200];
		//
		// Block 0x00
		//
		if (fis.read(tmpBuffer, 0, tmpBuffer.length) != tmpBuffer.length) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to read {} bytes from '{}'!" +
					"\n=====================\n",
					tmpBuffer.length, redoLog);
			fis.close();
			throw new IOException("Invalid Oracle RDBMS redo file'" + redoLog + "'!");
		}
		final int blockSize = readBlockSize(tmpBuffer, POS_BLOCK_SIZE, chkEndiness(tmpBuffer, fis, redoLog));
		final byte redoFileTypeByte = redoFileTypeByte(blockSize, redoLog);
		if (tmpBuffer[0] != 0x00 || tmpBuffer[1] != redoFileTypeByte) {
			LOGGER.error(
					"\n=====================\n" +
					"Invalid Oracle RDBMS redo file signature bytes '{}' & '{}' in file '{}'!" +
					"\n=====================\n",
					String.format("0x%02x", Byte.toUnsignedInt(tmpBuffer[0])),
					String.format("0x%02x", Byte.toUnsignedInt(tmpBuffer[1])),
					redoLog);
			fis.close();
			throw new IOException("Invalid Oracle RDBMS redo file signature in '" + redoLog + "'!");
		}
		final long blockCount = Integer.toUnsignedLong(bu.getU32(tmpBuffer, 0x18));
		tmpBuffer = null;
		final long[] result = new long[2];
		result[0] = blockSize;
		result[1] = blockCount;
		return result;
	}

	private static boolean chkEndiness(final byte[] buffer, InputStream is, final String fileName) throws IOException {
		if (buffer[POS_MAGIC_1] == 0x7D && buffer[POS_MAGIC_2] == 0x7C &&
			buffer[POS_MAGIC_3] == 0x7B && buffer[POS_MAGIC_4] == 0x7A) {
			return true;
		} else if (buffer[POS_MAGIC_1] == 0x7A && buffer[POS_MAGIC_2] == 0x7B &&
					buffer[POS_MAGIC_3] == 0x7C && buffer[POS_MAGIC_4] == 0x7D) {
			return false;
		} else {
			is.close();
			throw new IOException("Unable to find the magic signature in file '" + fileName + "'!");
		}
	}

	private static int readBlockSize(final byte[] buffer, final int offset, final boolean little) {
		return 
				buffer[offset + (little ? 0 : 1)] << 8 & 0xFF00 |
				buffer[offset + (little ? 1 : 0)] & 0xFF;
	}


}
