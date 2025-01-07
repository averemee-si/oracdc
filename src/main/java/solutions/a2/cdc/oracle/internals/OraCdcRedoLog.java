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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.jdbc.types.UnsignedLong;
import solutions.a2.oracle.utils.FormattingUtils;
import solutions.a2.oracle.utils.BinaryUtils;

/**
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Presentations/RedoInternals.ppt">Redo Internals</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * internals.
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcRedoLog implements Iterator<OraCdcRedoRecord>, Closeable {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcRedoLog.class);
	private static final int POS_MAGIC_1 = 0x001C;
	private static final int POS_MAGIC_2 = 0x001D;
	private static final int POS_MAGIC_3 = 0x001E;
	private static final int POS_MAGIC_4 = 0x001F;
	private static final int POS_BLOCK_SIZE = 0x0015;
	private static final int POS_RDBMS_VERSION = 0x0014;
	private static final int POS_INSTANCE_NAME = 0x001c;
	private static final int RECORD_SIZE_THRESHOLD = 0x18;

	private static final int ORA_CDB_START = 0x0C;

	private final String fileName;
	private InputStream fis;
	private final boolean littleEndian;
	private final int blockSize;
	private final byte[] block;
	private final int redoFileTypeByte;
	private final boolean validateChecksum;
	private final BinaryUtils bu;
	private final long firstScn;
	private final long nextScn;
	private final int firstTime;
	private final int nextTime;
	private final int resetLogsCnt;
	private final long resetLogsScn;
	private final int prevResetLogsCnt;
	private final long prevResetLogsScn;
	private final int sequence;
	private final int dbId;
	private final int activationId;
	private final int thread;
	private final long blockCount;
	private final int compatibilityVsn;
	private final StringBuilder versionString;
	private final boolean bigScn;
	private final StringBuilder oracleSid;
	private final int versionMajor;
	private boolean cdb;
	private final int largestLwn;
	private final int controlSeq;
	private final int fileSize;
	private final short fileNo;
	private final String description;
	private final int nab;
	// Used in Iterator too
	private boolean iteratorInited = false;
	private long currentBlock = 0;
	private int recordTimestamp = 0;
	private long recordScn = 0;
	private int recordHeaderSize = 0;

	public OraCdcRedoLog(final String fileName) throws IOException {
		this(fileName, 0, true, null);
	}

	public OraCdcRedoLog(final String fileName, final int blockSize,
			final boolean validateChecksum, final BinaryUtils bu) throws IOException {
		this.fileName = fileName;
		this.validateChecksum = validateChecksum;
		if (blockSize == 0) {
			final InputStream tmpStream = openRedoLog();
			final byte[] tmpBuffer = new byte[0x20];
			if (tmpStream.read(tmpBuffer, 0, tmpBuffer.length) != tmpBuffer.length) {
				LOGGER.error(
						"\n=====================\n" +
						"Unable to read {} bytes from '{}'!" +
						"\n=====================\n",
						tmpBuffer.length, fileName);
				tmpStream.close();
				throw new IOException("Invalid Oracle RDBMS redo file'" + fileName + "'!");
			}
			this.blockSize = readBlockSize(tmpBuffer, POS_BLOCK_SIZE, chkEndiness(tmpBuffer, tmpStream, fileName));
			tmpStream.close();
		} else {
			this.blockSize = blockSize;
		}

		switch (this.blockSize) {
		case 0x0200:
			redoFileTypeByte = (byte) 0x22;
			break;
		case 0x1000:
			redoFileTypeByte = (byte) 0x82;
			break;
		case 0x0400:
			redoFileTypeByte = (byte) 0x42;
			break;
		default:
			LOGGER.error(
					"\n=====================\n" +
					"The blocksize of '{}' is {}, but the only valid values for blocksize are 512, 1024, or 4096!" +
					"\n=====================\n",
					fileName, this.blockSize);
			throw new IOException("Only 512, 1024, or 4096 are valid values for blocksize!!!");
		}

		fis = openRedoLog();
		block = new byte[this.blockSize];
		//
		// Block 0x00
		//
		currentBlock = 0;
		if (fis.read(block, 0, this.blockSize) != this.blockSize) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to read {} bytes from '{}'!" +
					"\n=====================\n",
					this.blockSize, fileName);
			fis.close();
			throw new IOException("Invalid Oracle RDBMS redo file'" + fileName + "'!");
		}
		if (block[0] != 0x00 || block[1] != redoFileTypeByte) {
			LOGGER.error(
					"\n=====================\n" +
					"Invalid Oracle RDBMS redo file signature bytes '{}' & '{}' in file '{}'!" +
					"\n=====================\n",
					String.format("0x%02x", Byte.toUnsignedInt(block[0])),
					String.format("0x%02x", Byte.toUnsignedInt(block[1])),
					fileName);
			fis.close();
			throw new IOException("Invalid Oracle RDBMS redo file signature in '" + fileName + "'!");
		}
		littleEndian = chkEndiness(block, fis, fileName);

		if (bu == null) {
			this.bu = BinaryUtils.get(littleEndian);
		} else {
			if (littleEndian && !bu.isLittleEndian()) {
				fis.close();
				throw new IOException("The magic bytes in file '" + fileName + "' are little endian, but the decoder passed is not!");
			}
			if (!littleEndian && bu.isLittleEndian()) {
				fis.close();
				throw new IOException("The magic bytes in file '" + fileName + "' are big endian, but the decoder passed is not!");
			}
			this.bu = bu;
		}

		if (validateChecksum && checksum(block) != 0x00) {
			fis.close();
			throw new IOException("Invalid Oracle RDBMS redo file'" + fileName + "' checksum!");
		}
		if (readBlockSize(block, POS_BLOCK_SIZE, littleEndian) != this.blockSize) {
			LOGGER.error(
					"\n=====================\n" +
					"Blocksize {} in redo file {} is not equal to expected blocksize {}!" +
					"\n=====================\n",
					readBlockSize(block, POS_BLOCK_SIZE, littleEndian),
					this.blockSize,
					fileName);
			fis.close();
			throw new IOException("Invalid block size in Oracle RDBMS redo file '" + fileName + "'!");
		}
		blockCount = this.bu.getU32(block, 0x18);
		//
		// Block 0x01
		//
		currentBlock = 1;
		if (fis.read(block, 0, this.blockSize) != this.blockSize) {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to read {} bytes from '{}'!" +
					"\n=====================\n",
					this.blockSize, fileName);
			fis.close();
			throw new IOException("Invalid Oracle RDBMS redo file!");
		}
		if (block[0] != 0x01 || block[1] != redoFileTypeByte) {
			LOGGER.error(
					"\n=====================\n" +
					"Invalid Oracle RDBMS redo block signature bytes '{}' & '{}' in file '{}'!" +
					"\n=====================\n",
					String.format("0x%02x", Byte.toUnsignedInt(block[0])),
					String.format("0x%02x", Byte.toUnsignedInt(block[1])),
					fileName);
			fis.close();
			throw new IOException("Invalid Oracle RDBMS redo block signature!");
		}
		if (validateChecksum && checksum(block) != 0x00) {
			fis.close();
			throw new IOException("Invalid Oracle RDBMS redo file'" + fileName + "' checksum!");
		}

		sequence = this.bu.getU32(block, 0x08);
		controlSeq = this.bu.getU32(block, 0x24);
		fileSize = this.bu.getU32(block, 0x28);
		fileNo = this.bu.getU16(block, 0x30);
		firstScn = this.bu.getScn(block, 0xB4);
		firstTime = this.bu.getU32(block, 0xBC);
		nextScn = this.bu.getScn(block, 0xC0);
		nextTime = this.bu.getU32(block, 0xC8);
		resetLogsCnt= this.bu.getU32(block, 0xA0);
		resetLogsScn = this.bu.getScn(block, 0xA4);
		prevResetLogsCnt = this.bu.getU32(block, 0x124);
		prevResetLogsScn = this.bu.getScn(block, 0x11C);
		dbId = this.bu.getU32(block, 0x18);
		activationId = this.bu.getU32(block, 0x34);
		thread = this.bu.getU16(block, 0xB0);
		largestLwn = this.bu.getU32(block,  0x10C);
		compatibilityVsn = this.bu.getU32(block, POS_RDBMS_VERSION);
		description = new String(Arrays.copyOfRange(block, 0x5C, 0x9C), StandardCharsets.US_ASCII);
		nab = this.bu.getU32(block,  0x9C);
		// RDBMS version
		versionString = new StringBuilder();
		if (littleEndian) {
			versionMajor = Byte.toUnsignedInt(block[POS_RDBMS_VERSION + 3]);
			versionString
				.append(versionMajor)
				.append('.')
				.append(Byte.toUnsignedInt(block[POS_RDBMS_VERSION + 2]) >> 4)
				.append('.')
				.append(Byte.toUnsignedInt(block[POS_RDBMS_VERSION + 1]))
				.append('.')
				.append(Byte.toUnsignedInt(block[POS_RDBMS_VERSION]));
		} else {
			versionMajor = Byte.toUnsignedInt(block[POS_RDBMS_VERSION]);
			versionString
				.append(versionMajor)
				.append('.')
				.append(Byte.toUnsignedInt(block[POS_RDBMS_VERSION + 1]) >> 4)
				.append('.')
				.append(Byte.toUnsignedInt(block[POS_RDBMS_VERSION + 2]))
				.append('.')
				.append(Byte.toUnsignedInt(block[POS_RDBMS_VERSION + 3]));
		}
		if (versionMajor >= ORA_CDB_START) {
			cdb = true;
			if (compatibilityVsn > 0x0C100000) {
				//12.2+
				bigScn = true;
			} else {
				bigScn = false;
			}
		} else {
			bigScn = false;
		}
		oracleSid = new StringBuilder();
		for (int i = 0; i < 8; i++) {
			if (block[POS_INSTANCE_NAME + i] == 0) {
				break;
			} else {
				oracleSid.append((char) Byte.toUnsignedInt(block[POS_INSTANCE_NAME + i]));
			}
		}
		recordScn = firstScn;
		recordTimestamp = firstTime;
	}

	int thread() {
		return thread;
	}

	int recordHeaderSize() {
		return recordHeaderSize;
	}

	int recordTimestamp() {
		return recordTimestamp;
	}

	public BinaryUtils bu() {
		return bu;
	}

	public boolean cdb() {
		return cdb;
	}

	RedoByteAddress recordRba() {
		return recordRba;
	}

	byte[] recordBytes() {
		return recordBytes;
	}

	int versionMajor() {
		return versionMajor;
	}

	public boolean bigScn() {
		return bigScn;
	}

	public String fileName() {
		return fileName;
	}

	public int sequence() {
		return sequence;
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

	private static int checksum(final byte[] buffer) {
		final byte[] block1 = new byte[16];
		final byte[] block2 = new byte[16];
		final byte[] block3 = new byte[16];
		final byte[] block4 = new byte[16];
		final byte[] out1 = new byte[16];
		final byte[] out2 = new byte[16];
		final byte[] res = new byte[16];
		final byte[] nul = new byte[16];

		int index = 0;
		int r0 = 0, r1 = 0, r2 = 0, r3 = 0, r4 = 0;

		while (index < buffer.length) {
			System.arraycopy(buffer, index,
					block1, 0, block1.length);
			System.arraycopy(buffer, index + block1.length,
					block2, 0, block2.length);
			System.arraycopy(buffer, index + block1.length + block2.length,
					block3, 0, block3.length);
			System.arraycopy(buffer, index + block1.length + block2.length + block3.length,
					block4, 0, block4.length);

			do16ByteXor(block1, block2, out1);
			do16ByteXor(block3, block4, out2);
			do16ByteXor(nul, out1, res);
			System.arraycopy(res, 0, nul, 0, 16);
			do16ByteXor(nul, out2, res);
			System.arraycopy(res, 0, nul, 0, 16);

			index += 64;
		}

		r1 = ((res[3] & 0xFF) << 24) | ((res[2] & 0xFF) << 16) | ((res[1] & 0xFF) << 8) | (res[0] & 0xFF);
		r2 = ((res[7] & 0xFF) << 24) | ((res[6] & 0xFF) << 16) | ((res[5] & 0xFF) << 8) | (res[4] & 0xFF);
		r3 = ((res[11] & 0xFF) << 24) | ((res[10] & 0xFF) << 16) | ((res[9] & 0xFF) << 8) | (res[8] & 0xFF);
		r4 = ((res[15] & 0xFF) << 24) | ((res[14] & 0xFF) << 16) | ((res[13] & 0xFF) << 8) | (res[12] & 0xFF);

		r0 = r0 ^ r1 ^ r2 ^ r3 ^ r4;
		r1 = r0;
		r0 = r0 >>> 16;
		r0 = r0 ^ r1;
		r0 = r0 & 0xFFFF;

		return r0;
	}

	private static void do16ByteXor(byte[] block1, byte[] block2, byte[] out) {
		for (int i = 0; i < 16; i++) {
			out[i] = (byte) (block1[i] ^ block2[i]);
		}
	}

	@Override
	public void close() throws IOException {
		fis.close();
		fis = null;
	}

	public String toString(
			final long startScn, final long endScn,
			final RedoByteAddress startRba, final RedoByteAddress endRba) {
		final StringBuilder sb = new StringBuilder(0x800);
		sb
			.append("DUMP OF REDO FROM FILE '")
			.append(fileName)
			.append("'\n RBAs: ")
			.append(startRba)
			.append(" thru ")
			.append(endRba)
			.append("\n SCNs: scn: 0x");
		FormattingUtils.leftPad(sb, startScn, 0x10);
		sb.append(" thru scn: 0x");
		FormattingUtils.leftPad(sb, endScn, 0x10);
		sb
			.append("\n Endianness: ")
			.append(littleEndian ? "Little" : "Big")
			.append("\n FILE HEADER:")
			.append("\n\tCompatibility Vsn = ")
			.append(Integer.toUnsignedLong(compatibilityVsn))
			.append("=0x");
		FormattingUtils.leftPad(sb, compatibilityVsn, 0x08);
		sb
			.append("\n\tDb ID=")
			.append(Integer.toUnsignedLong(dbId))
			.append("=0x");
		FormattingUtils.leftPad(sb, dbId, 0x08);
		sb
			.append(", Db Name='")
			.append(oracleSid)
			.append('\'')
			.append("\n\tActivation ID=")
			.append(Integer.toUnsignedLong(activationId))
			.append("=0x");
		FormattingUtils.leftPad(sb, activationId, 0x08);
		sb
			.append("\n\tControl Seq=")
			.append(Integer.toUnsignedLong(controlSeq))
			.append('=')
			.append(String.format("0x%x", Integer.toUnsignedLong(controlSeq)))
			.append(", File size=")
			.append(Integer.toUnsignedLong(fileSize))
			.append('=')
			.append(String.format("0x%x", Integer.toUnsignedLong(fileSize)))
			.append("\n\tFile Number=")
			.append(Short.toUnsignedInt(fileNo))
			.append(", Blksiz=")
			.append(blockSize)
			.append(", File Type=2 LOG")
			.append("\n")
			.append(" descrip:\"")
			.append(description)
			.append("\"\n")
			.append(" thread: ")
			.append(thread)
			.append(" nab: ")
			.append(String.format("0x%x", Integer.toUnsignedLong(nab)))
			.append(" seq: 0x");
		FormattingUtils.leftPad(sb, sequence, 0x08);
		sb.append("\n resetlogs count: 0x");
		FormattingUtils.leftPad(sb, resetLogsCnt, 0x08);
		sb.append(" scn: 0x");
		FormattingUtils.leftPad(sb, resetLogsScn, 0x10);
		sb.append("\n prev resetlogs count: 0x");
		FormattingUtils.leftPad(sb, prevResetLogsCnt, 0x08);
		sb.append(" scn: 0x");
		FormattingUtils.leftPad(sb, prevResetLogsScn, 0x10);
		sb.append("\n Low  scn: 0x");
		FormattingUtils.leftPad(sb, firstScn, 0x10);
		sb
			.append(' ')
			.append(BinaryUtils.parseTimestamp(firstTime).toString());
		sb.append("\n Next scn: 0x");
		FormattingUtils.leftPad(sb, nextScn, 0x10);
		sb
			.append(' ')
			.append(nextScn == UnsignedLong.MAX_VALUE ? "" :
					BinaryUtils.parseTimestamp(nextTime).toString())
			.append("\n Largest LWN: ")
			.append(Integer.toUnsignedLong(largestLwn))
			.append(" blocks\n");
		return sb.toString();
	}

	@Override
	public String toString() {
		return toString(0, UnsignedLong.MAX_VALUE, RedoByteAddress.MIN_VALUE, RedoByteAddress.MAX_VALUE);
	}

	private RedoByteAddress recordRba;
	private boolean needNextBlock;
	private boolean chainedRecord;
	private boolean createRedoRecord;
	private int bytesRemaining;
	private int bytesCopied;
	private byte[] recordBytes;
	private int seq;
	private int blk;
	private short offset;
	private boolean lastStatus;
	private boolean iteratorLimits;
	private boolean limitedByScn;
	private boolean iteratorAlreadyAtNext;
	private long endScn;
	private RedoByteAddress endRba;
	private OraCdcRedoRecord redoRecord;

	private void initIterator(final long blocksToSkip) throws IOException {
		final long deltaBytes;
		if (iteratorInited) {
			close();
			fis = openRedoLog();
			deltaBytes = blockSize * blocksToSkip;
		} else {
			if (currentBlock < (blocksToSkip - 1)) {
				deltaBytes = blockSize * (blocksToSkip - currentBlock - 1);
			} else {
				deltaBytes = 0;
			}
			iteratorInited = true;
		}
		if (deltaBytes > 0) {
			final long skipped = fis.skip(deltaBytes);
			if (skipped != deltaBytes) {
				LOGGER.error(
						"\n=====================\n" +
						"Of the {} bytes requested to be skipped, only {} were skipped. in '{}'!" +
						"\n=====================\n",
						deltaBytes, skipped, fileName);
				fis.close();
				throw new IOException("Unable to skip " + deltaBytes + " bytes in '" + fileName + "'!");
			}
		}
		currentBlock = blocksToSkip - 1;
		needNextBlock = true;
		chainedRecord = false;
		createRedoRecord = false;
		bytesRemaining = 0;
		bytesCopied = 0;
		recordBytes = null;
		seq = -1;
		blk = -1;
		offset = -1;
		lastStatus = false;
		recordTimestamp = 0;
		recordScn = 0;
		iteratorAlreadyAtNext = false;
		iteratorLimits = false;
	}

	/**
	 * 
	 * Creates an iterator over the entire redo log file
	 * 
	 * @return
	 * @throws IOException
	 */
	public Iterator<OraCdcRedoRecord> iterator() throws IOException {
		initIterator(0x2);
		endScn = UnsignedLong.MAX_VALUE;
		return this;
	}

	/**
	 * 
	 * Creates an iterator from the specified start SCN to the specified end SCN
	 * 
	 * @param startScn
	 * @param endScn
	 * @return
	 * @throws IOException
	 */
	public Iterator<OraCdcRedoRecord> iterator(final long startScn, final long endScn) throws IOException {
		initIterator(0x2);
		limitedByScn = true;
		this.endScn = endScn;
		if (Long.compareUnsigned(startScn, firstScn) >= 0 &&
				Long.compareUnsigned(startScn, nextScn) <= 0 &&
				Long.compareUnsigned(endScn, firstScn) >= 0 &&
				Long.compareUnsigned(endScn, nextScn) <= 0) {
			while (hasNext()) {
				if (Long.compareUnsigned(recordScn, startScn) >= 0) {
					iteratorLimits = true;
					iteratorAlreadyAtNext = true;
					break;
				}
			}
		} else {
			currentBlock = blockCount;
			lastStatus = false;
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("The specified SCN range 0x{} - 0x{} does not match the SCN range 0x{} - 0x{} of redo log file '{}'!",
						FormattingUtils.leftPad(startScn, 0x10),
						FormattingUtils.leftPad(endScn, 0x10),
						FormattingUtils.leftPad(firstScn, 0x10),
						FormattingUtils.leftPad(nextScn, 0x10),
						fileName);
			}
		}
		return this;
	}

	/**
	 * 
	 * Creates an iterator from the specified start RBA to the specified end RBA
	 * 
	 * @param startRba
	 * @param endRba
	 * @return
	 * @throws IOException
	 */
	public Iterator<OraCdcRedoRecord> iterator(final RedoByteAddress startRba, final RedoByteAddress endRba) throws IOException {
		if (startRba.sqn() == sequence && endRba.sqn() == sequence) {
			initIterator(startRba.blk());
			limitedByScn = false;
			this.endRba = endRba;
			while (hasNext()) {
				if (startRba.compareTo(recordRba) <= 0) {
					iteratorLimits = true;
					iteratorAlreadyAtNext = true;
					break;
				}
			}
		} else {
			currentBlock = blockCount;
			lastStatus = false;
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("The specified RBA range {} - {} does not match the sequence {} of redo log file '{}'!",
						startRba, endRba, sequence, fileName);
			}
		}
		return this;
	}

	/**
	 * 
	 * Creates an iterator from the specified start RBA to the specified end SCN
	 * 
	 * @param startRba
	 * @param endScn
	 * @return
	 * @throws IOException
	 */
	public Iterator<OraCdcRedoRecord> iterator(final RedoByteAddress startRba, long endScn) throws IOException {
		if (startRba.sqn() == sequence) {
			initIterator(startRba.blk());
			limitedByScn = true;
			this.endScn = endScn;
			while (hasNext()) {
				if (startRba.compareTo(recordRba) <= 0) {
					iteratorLimits = true;
					iteratorAlreadyAtNext = true;
					break;
				}
			}
		} else {
			currentBlock = blockCount;
			lastStatus = false;
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("The specified range RBA {} - SCN {} does not match the sequence {} of redo log file '{}'!",
						startRba, endScn, sequence, fileName);
			}
		}
		return this;
	}

	/**
	 * 
	 * Creates an iterator from the beginning of redo log file to the specified end SCN
	 * 
	 * @param endScn
	 * @return
	 * @throws IOException
	 */
	public Iterator<OraCdcRedoRecord> iterator(long endScn) throws IOException {
			return iterator(firstScn, endScn);
	}

	@Override
	public boolean hasNext() {
		if (iteratorAlreadyAtNext) {
			iteratorAlreadyAtNext = false;
			lastStatus = true;
			return lastStatus;
		}
		while (true) {
			if (currentBlock >= blockCount) {
				needNextBlock = false;
				lastStatus = false;
				return lastStatus;
			}
			if (needNextBlock) {
				try {
					if (!nextBlock()) {
						lastStatus = false;
						return lastStatus;
					} else if (!chainedRecord) {
						seq = bu.getU32(block, 0x08);
						blk = bu.getU32(block, 0x04);
						offset = bu.getU16Special(block, 0x0C);
					}
				} catch (IOException e) {
					LOGGER.error(
							"\n=====================\n" +
							"Unable to read '{}'!" +
							"\n=====================\n",
							fileName);
					needNextBlock = false;
					lastStatus = false;
					return lastStatus;
				}
			}
			if (chainedRecord) {
				if (bytesRemaining > (blockSize - 0x10)) {
					if (createRedoRecord) {
						System.arraycopy(block, 0x10, recordBytes, bytesCopied, (blockSize - 0x10));
					}
					bytesRemaining -= (blockSize - 0x10);
					bytesCopied += (blockSize - 0x10);
					needNextBlock = true;
					continue;
				} else {
					if (createRedoRecord) {
						System.arraycopy(block, 0x10, recordBytes, bytesCopied, bytesRemaining);
					}
					if ((blockSize - bytesRemaining) < (RECORD_SIZE_THRESHOLD + 0x10)) {
						offset = 0;
						needNextBlock = true;
					} else if (bu.getU16Special(block, 0x0C) > 0) {
						blk = bu.getU32(block, 0x04);
						offset = bu.getU16Special(block, 0x0C);
						needNextBlock = false;
					} else {
						needNextBlock = true;
					}
					chainedRecord = false;
					bytesRemaining = 0;
					bytesCopied = 0;
					if (createRedoRecord) {
						if (eligibilityTest()) {
							return preParse4Iterator();
						} else {
							createRedoRecord = false;
						}
					}
				}
			}
			int recordLength = 0;
			while ((!chainedRecord) && (recordLength = bu.getU32(block, offset)) > 0 && offset > 0) {
				if (seq != sequence) {
					if (LOGGER.isTraceEnabled()) {
						LOGGER.trace(
								"Sync problem in file {}, normal for online redo log processing, expected sequence {} but got {} in block {}.",
								fileName, sequence, seq, blk);
					}
					createRedoRecord = false;
					iteratorAlreadyAtNext = false;
					lastStatus = false;
					return lastStatus;
				}
				final int vld = Byte.toUnsignedInt(block[offset + 4]);
				recordRba = new RedoByteAddress(seq, blk, offset);
				if ((vld & OraCdcRedoRecord.KCRVALID) == OraCdcRedoRecord.KCRVALID) {
					recordBytes = new byte[recordLength];
					createRedoRecord = true;
				} else {
					createRedoRecord = false;
				}

				if ((offset + recordLength) > blockSize) {
					needNextBlock = true;
					chainedRecord = true;
					bytesCopied = blockSize - offset;
					bytesRemaining = recordLength - bytesCopied;
					if (createRedoRecord) {
						System.arraycopy(block, offset, recordBytes, 0, bytesCopied);
					}
					break;
				} else {
					if (createRedoRecord) {
						System.arraycopy(block, offset, recordBytes, 0, recordLength);
					}
					if ((offset + recordLength) > (blockSize - RECORD_SIZE_THRESHOLD)) {
						needNextBlock = true;
						chainedRecord = false;
						offset = 0;
					} else {
						offset += recordLength;
						recordLength = bu.getU32(block, offset);
						if (recordLength > 0) {
							needNextBlock = false;
						} else {
							needNextBlock = true;
						}
					}
					if (createRedoRecord) {
						if (eligibilityTest()) {
							return preParse4Iterator();
						} else {
							createRedoRecord = false;
						}
					}
				}
			}
			if (recordLength == 0) {
				needNextBlock = true;
			}
		}
	}

	@Override
	public OraCdcRedoRecord next() {
		if (lastStatus) {
			return redoRecord;
		} else {
			return null;
		}
	}

	private boolean eligibilityTest() {
		if ((recordBytes[0x04] & OraCdcRedoRecord.KCRDEPND) != 0) {
			recordHeaderSize = 0x44;
			recordTimestamp = bu.getU32(recordBytes, 0x40);
		} else {
			recordHeaderSize = 0x18;			
		}
		recordScn = bu.getScn4Record(recordBytes, 0x06);
		redoRecord = new OraCdcRedoRecord(this);
		if (redoRecord.eligible()) {
			return true;
		} else {
			redoRecord = null;
			recordBytes = null;
			return false;
		}
	}

	private boolean preParse4Iterator() {
		lastStatus = true;
		if (iteratorLimits) {
			if (limitedByScn) {
				if (Long.compareUnsigned(recordScn, endScn) > 0) {
					currentBlock = blockCount; 
					lastStatus = false;
					redoRecord = null;
					recordBytes = null;
				}
			} else {
				if (recordRba.compareTo(endRba) > 0) {
					currentBlock = blockCount;
					lastStatus = false;
					redoRecord = null;
					recordBytes = null;
				}
			}
		} else {
			lastStatus = true;
		}
		return lastStatus;
	}

	private boolean nextBlock() throws IOException {
		if (blockSize == fis.read(block, 0, blockSize)) {
			if (validateChecksum && checksum(block) != 0x00) {
				fis.close();
				throw new IOException("Invalid Oracle RDBMS redo file'" + fileName + "' checksum!");
			}
			if (block[0] != 0x01 || block[1] != redoFileTypeByte) {
				LOGGER.error(
						"\n=====================\n" +
						"Invalid Oracle RDBMS redo block signature bytes '{}' & '{}' in file '{}', block#={}, bs={}!" +
						"\n=====================\n",
						String.format("0x%02x", Byte.toUnsignedInt(block[0])),
						String.format("0x%02x", Byte.toUnsignedInt(block[1])),
						fileName, currentBlock, blockSize);
				fis.close();
				throw new IOException("Invalid Oracle RDBMS redo file block signature!");
			}
			currentBlock++;
		} else {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to read block {} with size {} from '{}'!" +
					"\n=====================\n",
					currentBlock, blockSize, fileName);
			fis.close();
			throw new IOException("Unable to read block # " + currentBlock + " !");
		}
		return true;
	}

	private InputStream openRedoLog() throws IOException {
		return Files.newInputStream(Paths.get(fileName), StandardOpenOption.READ);
	}

}
