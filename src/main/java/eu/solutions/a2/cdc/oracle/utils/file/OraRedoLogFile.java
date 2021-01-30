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

package eu.solutions.a2.cdc.oracle.utils.file;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

/**
 * 
 * Based on
 *     Julian Dyke http://www.juliandyke.com/
 *   and
 *     Jure Kajzer https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf
 * internals.
 * 
 * @author averemee
 *
 */
public class OraRedoLogFile  {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraRedoLogFile.class);

	private static final int TYPE_REDO_LOG_FILE = 0x22;
	private static final int POS_BLOCK_SIZE = 0x15;
	private static final int POS_BLOCK_COUNT = 0x18;
	private static final int POS_RDBMS_VERSION = 0x14;
	private static final int POS_DBID = 0x18;
	private static final int POS_INSTANCE_NAME = 0x1c;
	private static final int POS_ACTIVATION_ID = 0x34;
	private static final int POS_DESCRIPTION = 0x5c;

	private final String fileName;
	private final long fileLength;
	private final int fileBlockSize;
	private final long blocksInFile;
	private final String versionString;
	private final int versionMajor;
	private final String instanceName;
	private final long dbId;
	private final String fileDescription;
	private final long activationId;
	private final int threadNo;
	private final long sequenceNo;
	private final long firstScn;
	private final long nextScn;

	public OraRedoLogFile(String fileName) throws IOException {
		this.fileName = fileName;
		RandomAccessFile raf = new RandomAccessFile(fileName, "r");
		if (raf.read() != 0) {
			raf.close();
			raf = null;
			throw new IOException("First byte of redo log must be 0x0!");
		}
		if (raf.read() != TYPE_REDO_LOG_FILE) {
			raf.close();
			raf = null;
			throw new IOException("Type of file (second byte) of redo log must be 0x22!");
		}

		//TODO Endianness!
		raf.seek(POS_BLOCK_SIZE);
		fileBlockSize = raf.readUnsignedShort();

		fileLength = raf.length();
		raf.seek(POS_BLOCK_COUNT);
		blocksInFile = readU32LE(raf);
		if (((blocksInFile + 1) * fileBlockSize) != fileLength) {
			raf.close();
			raf = null;
			throw new IOException("Wrong redo log file size!!!");
		}

		// Block 0x01
		raf.seek(fileBlockSize);
		// Signature
		if (raf.readUnsignedByte() != 0x01 || raf.readUnsignedByte() != TYPE_REDO_LOG_FILE) {
			raf.close();
			raf = null;
			throw new IOException("Invalid signature for redo block 0x01!!!");
		}
		// RDBMS version
		raf.seek(fileBlockSize + POS_RDBMS_VERSION);
		final int version4th = raf.readUnsignedByte();
		final int version3rd = raf.readUnsignedByte();
		final int versionMinor = raf.readUnsignedByte() >> 4;
		versionMajor = raf.readUnsignedByte();
		versionString =  versionMajor + "." + versionMinor + "." + version3rd + "." + version4th;
		// DB_ID
		raf.seek(fileBlockSize + POS_DBID);
		dbId = readU32LE(raf);

		//$ORACLE_SID/Database name
		raf.seek(fileBlockSize + POS_INSTANCE_NAME);
		instanceName = readInstanceName(raf);

		//Activation Id
		raf.seek(fileBlockSize + POS_ACTIVATION_ID);
		activationId = readU32LE(raf);

		//Description
		raf.seek(fileBlockSize + POS_DESCRIPTION);
		fileDescription = readDescription(raf);
		
		//Parse description "T 0001, S 0000000149, SCN 0x00000000003cf3ec-0x00000000003cf3f1"
		final String[] parts = fileDescription.split(",");
		if (parts.length != 3) {
			throw new IOException("Invalid archived redo description:'" + fileDescription + "'");
		}
		if (!StringUtils.startsWith(StringUtils.trim(parts[0]), "T")) {
			throw new IOException("Invalid archived redo description:'" + fileDescription + "'");
		} else {
			try {
				threadNo = Integer.parseInt(parts[0].trim().split(" ")[1]);
			} catch (Exception e) {
				throw new IOException("Can't parse THREAD# from description: '" +
										fileDescription + "'", e);
			}
		}
		if (!StringUtils.startsWith(StringUtils.trim(parts[1]), "S")) {
			throw new IOException("Invalid archived redo description:'" + fileDescription + "'");
		} else {
			try {
				sequenceNo = Long.parseLong(parts[1].trim().split(" ")[1]);
			} catch (Exception e) {
				throw new IOException("Can't parse SEQUENCE# from description: '" +
										fileDescription + "'", e);
			}
		}
		if (!StringUtils.startsWith(StringUtils.trim(parts[2]), "SCN")) {
			throw new IOException("Invalid archived redo description:'" + fileDescription + "'");
		} else {
			final String[] scns = StringUtils.trim(StringUtils.remove(StringUtils.trim(parts[2]), "SCN")).split("-");
			firstScn = Long.parseLong(StringUtils.remove(scns[0], "0x"), 16);
			nextScn = Long.parseLong(StringUtils.remove(scns[1], "0x"), 16);
		}

		raf.close();
		raf = null;
	}

	public String name() {
		return fileName;
	}

	public long length() {
		return fileLength;
	}

	public int blockSize() {
		return fileBlockSize;
	}

	public long blockCount() {
		return blocksInFile;
	}

	public String getVersionString() {
		return versionString;
	}

	public int getVersionMajor() {
		return versionMajor;
	}

	public String getInstanceName() {
		return instanceName;
	}

	public long getDbId() {
		return dbId;
	}

	public long getActivationId() {
		return activationId;
	}

	public String description() {
		return fileDescription;
	}

	public int thread() {
		return threadNo;
	}

	public long sequence() {
		return sequenceNo;
	}

	public long firstChange() {
		return firstScn;
	}

	public long nextChange() {
		return nextScn;
	}

	private long readU32LE(RandomAccessFile raf) throws IOException {
		final int b0 = raf.readUnsignedByte();
		final int b1 = raf.readUnsignedByte();
		final int b2 = raf.readUnsignedByte();
		final int b3 = raf.readUnsignedByte();
		return (b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)) & 0x00000000FFFFFFFFL;
	}

	private String readInstanceName(RandomAccessFile raf) throws IOException {
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 8; i++) {
			final char v = (char) raf.readUnsignedByte();
			if (v == 0) {
				break;
			} else {
				sb.append(v);
			}
		}
		return sb.toString();
	}

	private String readDescription(RandomAccessFile raf) throws IOException {
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 64; i++) {
			final char v = (char) raf.readUnsignedByte();
			if (v == 0) {
				break;
			} else {
				sb.append(v);
			}
		}
		return sb.toString();
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(256);
		sb.append(fileName);
		sb.append("\n");

		sb.append("BLOCK_SIZE\t=\t");
		sb.append(fileBlockSize);
		sb.append("\n");

		sb.append("ORACLE_SID\t=\t");
		sb.append(instanceName);
		sb.append("\n");

		sb.append("VERSION\t=\t");
		sb.append(versionString);
		sb.append("\n");

		sb.append("DBID\t=\t");
		sb.append(dbId);
		sb.append("\n");

		sb.append("ACTIVATION_ID\t=\t");
		sb.append(activationId);
		sb.append("\n");

		sb.append("DESCRIPTION\t=\t");
		sb.append(fileDescription);
		sb.append("\n");

		sb.append("THREAD#\t=\t");
		sb.append(threadNo);
		sb.append("\n");

		sb.append("SEQUENCE#\t=\t");
		sb.append(sequenceNo);
		sb.append("\n");

		sb.append("FIRST_CHANGE#\t=\t");
		sb.append(firstScn);
		sb.append("\n");

		sb.append("NEXT_CHANGE#\t=\t");
		sb.append(nextScn);
		sb.append("\n");

		return sb.toString();
	}

	public static void main(String[] argv) {

		if (argv.length == 0) {
			System.err.println("Usage:\n\tjava " + 
					OraRedoLogFile.class.getCanonicalName() +
					" <full path to archived Oracle RDBMS redo file>");
			System.err.println("Exiting.");
			System.exit(1);
		} else {
			BasicConfigurator.configure();
			org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
			final String redoLog = argv[0];
			try {
				OraRedoLogFile redo = new OraRedoLogFile(redoLog);
				System.out.println(redo);
				System.exit(0);
			} catch (IOException e) {
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
				System.exit(1);
			}
		}
	}

}
