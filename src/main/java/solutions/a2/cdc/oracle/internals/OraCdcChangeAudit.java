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

import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Based on
 *     <a href="https://www.linkedin.com/in/julian-dyke-9a27837/">Julian Dyke</a> <a href="http://www.juliandyke.com/Presentations/RedoInternals.ppt">Redo Internals</a>
 *     <a href="https://www.linkedin.com/in/davidlitchfield/">David Litchfield</a> <a href="http://www.davidlitchfield.com/oracle_forensics_part_1._dissecting_the_redo_logs.pdf">Oracle Forensics Part 1: Dissecting the Redo Logs</a>
 *     <a href="https://www.linkedin.com/in/jure-kajzer-198a9a13/">Jure Kajzer</a> <a href="https://www.abakus.si/download/events/2014_jure_kajzer_forenzicna_analiza_oracle_log_datotek.pdf">Forensic analysis of Oracle log files</a>
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */

public class OraCdcChangeAudit extends OraCdcChange {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcChangeAudit.class);

	private final short serialNumber;
	private final int sessionNumber;

	private String currentUser;
	private String loginUser;
	private String clientInfo;
	private String osUser;
	private String machineName;
	private String osTerminal;
	private String osPid;
	private String osProgram;
	private String transactionName;
	private short transactionFlags;
	private int version;
	private int auditSessionId;
	private String clientId;

	OraCdcChangeAudit(final short num, final OraCdcRedoRecord redoRecord, final short operation, final byte[] record, final int offset, final int headerLength) {
		super(num, redoRecord, operation, record, offset, headerLength);
		serialNumber = redoLog.bu().getU16(record, coords[0][0] + 0x02);
		if (redoLog.versionMajor() >= 19) {
			//RDBMS 19c+
			sessionNumber = redoLog.bu().getU32(record, coords[0][0] + 0x04);
		} else {
			sessionNumber = redoLog.bu().getU16(record, coords[0][0]);
		}
		for (int index = 1; index < coords.length; index++) {
			if (coords[index][1] > 0) {
				switch (index) {
				case 0x01:
					if (operation == _5_19_TSL) {
						currentUser = getStringVal(record, index);
					} else {
						transactionName = getStringVal(record, index);
					}
					break;
				case 0x02:
					if (operation == _5_19_TSL) {
						loginUser = getStringVal(record, index);
					} else {
						if (coords[index][1] < Short.BYTES) {
							LOGGER.warn("Wrong size {} for element #{}, OP 5.20, ({}) at RBA {}",
									coords[index][1], index, "transaction flags", rba);
						} else {
							transactionFlags = redoLog.bu().getU16(record, coords[index][0]);
						}
					}
					break;
				case 0x03:
					if (operation == _5_19_TSL) {
						clientInfo = getStringVal(record, index);
					} else {
						if (coords[index][1] < Integer.BYTES) {
							LOGGER.warn("Wrong size {} for element #{}, OP 5.20, ({}) at RBA {}",
									coords[index][1], index, "RDBMS version", rba);
						} else {
							version = redoLog.bu().getU32(record, coords[index][0]);
						}
					}
					break;
				case 0x04:
					if (operation == _5_19_TSL) {
						osUser = getStringVal(record, index);
					} else {
						if (coords[index][1] < Integer.BYTES) {
							LOGGER.warn("Wrong size {} for element #{}, OP 5.19, ({}) at RBA {}",
									coords[index][1], index, "Audit session Id", rba);
						} else {
							auditSessionId = redoLog.bu().getU32(record, coords[index][0]);
						}
					}
					break;
				case 0x05:
					if (operation == _5_19_TSL) {
						machineName = getStringVal(record, index);
					}
					break;
				case 0x06:
					if (operation == _5_19_TSL) {
						osTerminal = getStringVal(record, index);
					} else {
						clientId = getStringVal(record, index);
					}
					break;
				case 0x07:
					if (operation == _5_19_TSL) {
						osPid = getStringVal(record, index);
					} else {
						loginUser = getStringVal(record, index);
					}
					break;
				case 0x08:
					osProgram = getStringVal(record, index);
					break;
				case 0x09:
					transactionName = getStringVal(record, index);
					break;
				case 0x0A:
					if (coords[index][1] < Short.BYTES) {
						LOGGER.warn("Wrong size {} for element #{}, OP 5.19, ({}) at RBA {}",
								coords[index][1], index, "transaction flags", rba);
					} else {
						transactionFlags = redoLog.bu().getU16(record, coords[index][0]);
					}
					break;
				case 0x0B:
					if (coords[index][1] < Integer.BYTES) {
						LOGGER.warn("Wrong size {} for element #{}, OP 5.19, ({}) at RBA {}",
								coords[index][1], index, "RDBMS version", rba);
					} else {
						version = redoLog.bu().getU32(record, coords[index][0]);
					}
					break;
				case 0x0C:
					if (coords[index][1] < Integer.BYTES) {
						LOGGER.warn("Wrong size {} for element #{}, OP 5.19, ({}) at RBA {}",
								coords[index][1], index, "Audit session Id", rba);
					} else {
						auditSessionId = redoLog.bu().getU32(record, coords[index][0]);
					}
					break;
				case 0x0D:
					clientId = getStringVal(record, index);
					break;
				}
			}
		}
	}

	private String getStringVal(final byte[] record, final int index) {
		return new String(Arrays.copyOfRange(record, coords[index][0], coords[index][0] + coords[index][1]));
	}

	@Override
	StringBuilder toDumpFormat() {
		final StringBuilder sb = super.toDumpFormat();
		sb
			.append("\nsession number   = ")
			.append(Integer.toUnsignedLong(sessionNumber))
			.append("\nserial  number   = ")
			.append(Short.toUnsignedInt(serialNumber));
		if (operation == _5_19_TSL) {
			sb
				.append("\ncurrent username = ")
				.append(currentUser == null ? "" : currentUser)
				.append("\nlogin   username = ")
				.append(loginUser == null ? "" : loginUser)
				.append("\nclient info      = ")
				.append(clientInfo == null ? "" : clientInfo)
				.append("\nOS username      = ")
				.append(osUser == null ? "" : osUser)
				.append("\nMachine name     = ")
				.append(machineName == null ? "" : machineName)
				.append("\nOS terminal      = ")
				.append(osTerminal == null ? "" : osTerminal)
				.append("\nOS process id    = ")
				.append(osPid == null ? "" : osPid)
				.append("\nOS program name  = ")
				.append(osProgram == null ? "" : osProgram);
		}
		sb
			.append("\ntransaction name = ")
			.append(transactionName == null ? "" : transactionName);
		if ((transactionFlags & 0x0001) != 0) {
			sb.append("\nDDL transaction");
		}
		if ((transactionFlags & 0x0002) != 0) {
			sb.append("\nSpace Management transaction");
		}
		if ((transactionFlags & 0x0004) != 0) {
			sb.append("\nRecursive transaction");
		}
		if ((transactionFlags & 0x0008) != 0) {
			sb.append("\nLogMiner Internal transaction");
		}
		if ((transactionFlags & 0x0010) != 0) {
			sb.append("\nDB Open in Migrate Mode");
		}
		if ((transactionFlags & 0x0020) != 0) {
			sb.append("\nLSBY ignore");
		}
		if ((transactionFlags & 0x0040) != 0) {
			sb.append("\nLogMiner no tx chunking");
		}
		if ((transactionFlags & 0x0080) != 0) {
			sb.append("\nLogMiner Stealth transaction");
		}
		if ((transactionFlags & 0x0100) != 0) {
			sb.append("\nLSBY preserve");
		}
		if ((transactionFlags & 0x0200) != 0) {
			sb.append("\nLogMiner Marker transaction");
		}
		if ((transactionFlags & 0x0400) != 0) {
			sb.append("\nTransaction in pragama'ed plsql");
		}
		if ((transactionFlags & 0x0800) != 0) {
			sb.append("\nDisabled Logical Repln. txn.");
		}
		if ((transactionFlags & 0x1000) != 0) {
			sb.append("\nDatapump import txn");
		}
		if ((transactionFlags & 0x8000) != 0) {
			sb.append("\nTx audit CV flags undefined");
		}
		//TODO - auxiliary flags
		sb
			.append("\nversion ")
			.append(version == 0 ? "" : Integer.toUnsignedString(version))
			.append("\naudit sessionid ")
			.append(auditSessionId == 0 ? "" : Integer.toUnsignedString(auditSessionId))
			.append("\nClient Id  = ")
			.append(clientId == null ? "" : clientId);
		if (operation == _5_20_TSC) {
			sb
				.append("\nlogin   username = ")
				.append(loginUser == null ? "" : loginUser);
		}
		return sb;
	}

	@Override
	public String toString() {
		return toDumpFormat().toString();
	}

}
