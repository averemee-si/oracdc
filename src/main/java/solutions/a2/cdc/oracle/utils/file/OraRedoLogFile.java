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

package solutions.a2.cdc.oracle.utils.file;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.help.HelpFormatter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;
import solutions.a2.cdc.oracle.OraCdcSourceConnectorConfig;
import solutions.a2.cdc.oracle.internals.OraCdcChange;
import solutions.a2.cdc.oracle.internals.OraCdcChangeUndo;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLog;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogAsmFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogFileFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogSmbjFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLogSshjFactory;
import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.Xid;
import solutions.a2.oracle.utils.BinaryUtils;
import solutions.a2.utils.ExceptionUtils;

import static solutions.a2.cdc.oracle.internals.OraCdcChange.KCOCOTBK;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KCOCOTSG;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KCOCOHLB;
import static solutions.a2.cdc.oracle.internals.OraCdcChange.KCOCOTBF;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._5_12_RST;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._14_1_CUSH;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._14_2_CRLK;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._14_4_OPEMREDO;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._24_10_URU;
import static solutions.a2.cdc.oracle.internals.OraCdcChange._26_3_FRMT;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

/**
 * 
 * Easy 'ALTER SYSTEM DUMP LOGFILE' alternative for opcode layer 11
 *   also see <a href="http://www.juliandyke.com/Diagnostics/Dumps/RedoLogs.php">Redo Log Dump</a> 
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraRedoLogFile  {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraRedoLogFile.class);
	private static final String ASM_URL = "asm-jdbc-url";
	private static final String ASM_USER = "asm-username";
	private static final String ASM_PASSWORD = "asm-password";
	private static final String BIG_ENDIAN = "big-endian";
	private static final String SSH_PASSWORD = "ssh-password";
	private static final String SSH_IDENTITY = "ssh-identity-file";
	private static final String SSH_PORT = "ssh-port";
	private static final String SMB_USER = "smb-user";
	private static final String SMB_PASSWORD = "smb-password";
	private static final String TEST_CLASS = "test-data";

	public static void main(String[] argv) {
		BasicConfigurator.configure();
		var millis = System.currentTimeMillis();
		LOGGER.info("Starting...");

		// Command line options
		final var options = new Options();
		setupCliOptions(options);

		final CommandLineParser parser = new DefaultParser();
		final var formatter = HelpFormatter.builder().get();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, argv);
		} catch (ParseException pe) {
			LOGGER.error(pe.getMessage());
			try {
				formatter.printHelp(OraRedoLogFile.class.getCanonicalName(), "", options, "", true);
			} catch (IOException ioe) {}
			System.exit(1);
		}

		var redoFile = cmd.getOptionValue("f");
		final var bu = BinaryUtils.get(!cmd.hasOption(BIG_ENDIAN));
		OraCdcRedoLogFactory rlf = null;
		if (Strings.CS.startsWith(redoFile, "+")) {
			final var asmUrl = cmd.getOptionValue(ASM_URL);
			final var asmUser = cmd.getOptionValue(ASM_USER);
			final var asmPassword = cmd.getOptionValue(ASM_PASSWORD);
			if (StringUtils.isAnyBlank(asmUrl, asmUser, asmPassword)) {
				LOGGER.error(
						"""
						
						=====================
						To work with file '{}' located on Oracle ASM, parameters --{}, --{}, and --{} must be set!
						=====================
						
						""", redoFile, ASM_URL, ASM_USER, ASM_PASSWORD);
				System.exit(1);
			}
			try {
				final var props = new Properties();
				props.setProperty(OracleConnection.CONNECTION_PROPERTY_INTERNAL_LOGON, "sysasm");
				props.setProperty(OracleConnection.CONNECTION_PROPERTY_THIN_VSESSION_PROGRAM, "oracdc");
				final var ods = new OracleDataSource();
				ods.setConnectionProperties(props);
				ods.setURL(asmUrl);
				ods.setUser(asmUser);
				ods.setPassword(asmPassword);
				rlf = new OraCdcRedoLogAsmFactory(ods.getConnection(), bu, true, true);
			} catch (SQLException sqle) {
				LOGGER.error(
						"""
						
						=====================
						Unable to connect to Oracle ASM Instance at {} as {} with password {}!
						Exception: '{}'
						Stack trace:
						{}
						=====================						
						
						""", asmUrl, asmUser, asmPassword, sqle.getMessage(),
							ExceptionUtils.getExceptionStackTrace(sqle));
				System.exit(1);
			}
		} else if (Strings.CS.startsWith(redoFile, "/") ||
				(redoFile.length() > 2 &&
				StringUtils.isAlpha(StringUtils.substring(redoFile, 0, 1)) &&
				Strings.CS.equals(StringUtils.substring(redoFile, 1, 2), ":")) ||
				(redoFile.length() > 2 &&
				StringUtils.isAlpha(StringUtils.substring(redoFile, 0, 1)) &&
				!Strings.CS.contains(redoFile, "@"))) {
			rlf = new OraCdcRedoLogFileFactory(bu, true);
		} else if (Strings.CS.startsWith(redoFile, "\\\\")) {
			final var shareIndex = Strings.CS.indexOf(StringUtils.substring(redoFile, 2), "\\");
			final var pathIndex = Strings.CS.indexOf(StringUtils.substring(redoFile, shareIndex + 3), "\\");
			final var smbServer = StringUtils.substring(redoFile, 2, shareIndex + 2);
			final var shareName = StringUtils.substring(redoFile, shareIndex + 3, pathIndex + shareIndex + 3);
			final var fileName = StringUtils.substring(redoFile, shareIndex + pathIndex + 4);
			final var smbDomain = StringUtils.substringBefore(cmd.getOptionValue(SMB_USER), "\\");
			final var smbUser = StringUtils.substringAfter(cmd.getOptionValue(SMB_USER), "\\");
			final var smbPassword = cmd.getOptionValue(SMB_PASSWORD);
			if (StringUtils.isAnyBlank(smbServer, shareName, fileName, smbUser, smbDomain, smbPassword)) {
				LOGGER.error(
						"""
						
						=====================
						Unable to connect to SMB server!
						=====================
												
						""");
				System.exit(1);
			}
			final Map<String, String> smbProps = new HashMap<>();
			smbProps.put("a2.smb.server", smbServer);
			smbProps.put("a2.smb.share.online", shareName);
			smbProps.put("a2.smb.share.archive", shareName);
			smbProps.put("a2.smb.user", smbUser);
			smbProps.put("a2.smb.password", smbPassword);
			smbProps.put("a2.smb.domain", smbDomain);
			redoFile = fileName;
			try {
				rlf = new OraCdcRedoLogSmbjFactory(
						new OraCdcSourceConnectorConfig(smbProps),
						bu, true);
			} catch (SQLException sqle) {
				LOGGER.error(
						"""
						
						=====================
						Unable to connect to SMB server {} !
						Exception: '{}'
						Stack trace:
						{}
						=====================
						
						""", smbServer, sqle.getMessage(), ExceptionUtils.getExceptionStackTrace(sqle));
				System.exit(1);
			}
		} else {
			final var userName = StringUtils.substringBefore(redoFile, '@');
			if (StringUtils.isBlank(userName) ||
					Strings.CS.equals(redoFile, userName)) {
				LOGGER.error(
						"""
						
						=====================
						Unable to get username part from {}!
						ssh file specification must be in format  username@hostname:file
						=====================
						
						""", redoFile);
				System.exit(1);
			}
			final var hostname = StringUtils.substringBetween(redoFile, "@", ":");
			if (StringUtils.isBlank(hostname) ||
					Strings.CS.equals(redoFile, hostname)) {
				LOGGER.error(
						"""
						
						=====================
						Unable to get hostname part from {}!
						ssh file specification must be in format  username@hostname:file
						=====================
						
						""", redoFile);
				System.exit(1);
			}
			final var fileName = StringUtils.substringAfter(redoFile, ':');
			if (StringUtils.isBlank(fileName) ||
					Strings.CS.equals(redoFile, fileName)) {
				LOGGER.error(
						"""
						
						=====================
						Unable to get filename part from {}!
						ssh file specification must be in format  username@hostname:file
						=====================
						
						""", redoFile);
				System.exit(1);
			}
			redoFile = fileName;
			final var password = cmd.getOptionValue(SSH_PASSWORD);
			final var identityFile = cmd.getOptionValue(SSH_IDENTITY);
			var sshPort = 0x16;
			final var sshPortString =  cmd.getOptionValue(SSH_PORT);
			if (StringUtils.isNotBlank(sshPortString)) {
				try {
					sshPort = Integer.parseInt(sshPortString);
				} catch (Exception e) {
					LOGGER.error(
							"""
							
							=====================
							Unable to parse {}!
							{} is set as value for {}!
							=====================
							
							""", sshPortString, sshPort, SSH_PORT);
				}
			}
			if (StringUtils.isAllBlank(password, identityFile)) {
				LOGGER.error(
						"""
						
						=====================
						Both parameters {} and {} are not specified!
						Must specify {} or {} parameter to work with remote file
						=====================
						
						""", SSH_PASSWORD, SSH_IDENTITY, SSH_PASSWORD, SSH_IDENTITY);
				System.exit(1);
			}
			try {
				rlf = new OraCdcRedoLogSshjFactory(userName, hostname, sshPort,
						identityFile, password, false, 0x100, 0x8000, bu, true);
			} catch (SQLException sqle) {
				LOGGER.error(
						"""
						
						=====================
						Unable to connect to remote server {} using ssh!
						Exception: '{}'
						Stack trace:
						{}
						=====================
						
						""", hostname, sqle.getMessage(), ExceptionUtils.getExceptionStackTrace(sqle));
				System.exit(1);
			}
		}
		OraCdcRedoLog orl = null;
		try {
			orl = rlf.get(redoFile);
		} catch (SQLException sqle) {
			LOGGER.error(
					"""
					
					=====================
					Unable to open redo file {} using ssh!
					Exception: '{}'
					Stack trace:
					{}
					=====================
					
					""", redoFile, sqle.getMessage(), ExceptionUtils.getExceptionStackTrace(sqle));
			System.exit(1);
		}

		var useFile = false;
		PrintStream out = null;
		final var outFileName = cmd.getOptionValue("o");
		if (StringUtils.isBlank(outFileName)) {
			out = System.out;
		} else {
			try {
				out = new PrintStream(new FileOutputStream(outFileName));
				useFile = true;
			} catch (IOException ioe) {
				LOGGER.error(
						"""
						
						=====================
						Unable to open output file {} !
						Exception: '{}'
						Stack trace:
						{}
						=====================
						
						""", outFileName, ioe.getMessage(), ExceptionUtils.getExceptionStackTrace(ioe));
				System.exit(1);
			}
		}
		var records = cmd.hasOption("r");
		var dumps = cmd.hasOption("b");
		var limits = false;
		var useRba = true;
		RedoByteAddress startRba = null;
		RedoByteAddress endRba = null;
		var startScn = 0l;
		var endScn = 0l;
		if (cmd.hasOption("s")) {
			try {
				startRba = RedoByteAddress.fromLogmnrContentsRs_Id(cmd.getOptionValue("s"));
			} catch (Exception e) {
				LOGGER.error(
						"""
						
						=====================
						'{}' parsing redo byte address '{}'
						Errorstack:
						{}
						=====================
						
						""",
						e.getMessage(), cmd.getOptionValue("s"), ExceptionUtils.getExceptionStackTrace(e));
				System.exit(1);
			}
			if (cmd.hasOption("e")) {
				try {
					endRba = RedoByteAddress.fromLogmnrContentsRs_Id(cmd.getOptionValue("e"));
				} catch (Exception e) {
					LOGGER.error(
							"""
							
							=====================
							'{}' parsing redo byte address '{}'
							Errorstack:
							{}
							=====================

							""",
							e.getMessage(), cmd.getOptionValue("e"), ExceptionUtils.getExceptionStackTrace(e));
					System.exit(1);
				}
			} else {
				LOGGER.error(
						"""
						
						=====================
						If you specified an option -s/--start-rba, then you must specify the corresponding option -e/--end-rba!
						=====================
						
						""");
				System.exit(1);
			}
			limits = true;
		} else if (cmd.hasOption("c")) {
			try {
				final var strStartScn =  cmd.getOptionValue("c");
				if (Strings.CI.startsWith(strStartScn, "0x"))
					startScn = Long.parseLong(StringUtils.substring(strStartScn, 2), 0x10);
				else
					startScn = Long.parseLong(strStartScn);
			} catch (Exception e) {
				LOGGER.error(
						"""
						
						=====================
						'{}' parsing SCN '{}'
						Errorstack:
						{}
						=====================

						""",
						e.getMessage(), cmd.getOptionValue("c"), ExceptionUtils.getExceptionStackTrace(e));
				System.exit(1);
			}
			if (cmd.hasOption("n")) {
				try {
					final var strEndScn =  cmd.getOptionValue("n");
					if (Strings.CI.startsWith(strEndScn, "0x"))
						endScn = Long.parseLong(StringUtils.substring(strEndScn, 2), 0x10);
					else
						endScn = Long.parseLong(strEndScn);
				} catch (Exception e) {
					LOGGER.error(
							"""
							
							=====================
							'{}' parsing SCN '{}'
							Errorstack:
							{}
							=====================

							""",
							e.getMessage(), cmd.getOptionValue("n"), ExceptionUtils.getExceptionStackTrace(e));
					System.exit(1);
				}
			} else {
				LOGGER.error(
						"""
						
						=====================
						If you specified an option -c/--start-scn, then you must specify the corresponding option -n/--end-scn!
						=====================
						
						""");
				System.exit(1);
			}
			limits = true;
			useRba = false;
		}

		final boolean objFilter;
		final int[] objects;
		if (StringUtils.isAllBlank(cmd.getOptionValues("d"))) {
			objects = null;
			objFilter = false;
		} else {
			final var objIds = cmd.getOptionValues("d");
			objects = new int[objIds.length];
			for (int i = 0; i < objects.length; i++) {
				try {
					final var str =  objIds[i];
					if (Strings.CI.startsWith(str, "0x"))
						objects[i] = Integer.parseInt(StringUtils.substring(str, 2), 0x10);
					else
						objects[i] = Integer.parseInt(str);
				} catch (Exception e) {
					LOGGER.error(
							"""
							
							=====================
							'{}' parsing obj# '{}'
							Errorstack:
							{}
							=====================
							
							""",
							e.getMessage(), objIds[i], ExceptionUtils.getExceptionStackTrace(e));
					System.exit(1);
				}
			}
			if (objects.length > 0)
				objFilter = true;
			else
				objFilter = false;
			Arrays.sort(objects);
		}

		boolean xidFilter = false;
		Xid xid = null;
		if (!StringUtils.isBlank(cmd.getOptionValue("x"))) {
			final var strXid =  cmd.getOptionValue("x");
			try {
				if (!Strings.CS.startsWith(strXid, "0x")) {
					throw new NumberFormatException("XID must start with 0x!");
				}

				var pos = 2;
				final var sb = new StringBuilder();
				while (pos < strXid.length() && strXid.charAt(pos) != '.') {
					sb.append(strXid.charAt(pos++));
				}
				final short usn = (short) Integer.parseUnsignedInt(sb.toString(), 0x10);

				pos++;
				sb.setLength(0);
				while (pos < strXid.length() && strXid.charAt(pos) != '.') {
					sb.append(strXid.charAt(pos++));
				}
				final short slt = (short) Integer.parseUnsignedInt(sb.toString(), 0x10);

				pos++;
				sb.setLength(0);
				while (pos < strXid.length()) {
					sb.append(strXid.charAt(pos++));
				}
				final int sqn = Integer.parseUnsignedInt(sb.toString(), 0x10);

				xidFilter = true;
				xid = new Xid(usn, slt, sqn);
			} catch (NumberFormatException nfe) {
				LOGGER.error(
						"""
						
						=====================
						'{}' parsing obj# '{}'
						Errorstack:
						{}
						=====================

						""",
						nfe.getMessage(), strXid, ExceptionUtils.getExceptionStackTrace(nfe));
				System.exit(1);
			}
		}

		var generateTestData = false;
		String testFileName = null;
		OraCdcIncidentWriter iw = null;
		if (cmd.hasOption('t')) {
			if (!xidFilter) {
				LOGGER.error(
						"""
						
						""");
				System.exit(1);
			}
			generateTestData = true;
			testFileName = useFile ? outFileName : "TestCase$" + System.currentTimeMillis() + ".trc";
			try {
				iw = new OraCdcIncidentWriter(xid, testFileName);
			} catch (IOException ioe) {
				LOGGER.error(
						"""
						
						=====================
						'{}' while opening '{}'
						Errorstack:
						{}
						=====================
						
						""",
						ioe.getMessage(), testFileName, ExceptionUtils.getExceptionStackTrace(ioe));
				System.exit(1);
			}
		} else
			out.println(orl);
		if (records || dumps || xidFilter || generateTestData) {
			try {
				final Iterator<OraCdcRedoRecord> iterator;
				if (limits) {
					if (useRba) {
						iterator = orl.iterator(startRba, endRba);
					} else {
						iterator = orl.iterator(startScn, endScn);
					}
				} else {
					iterator = orl.iterator();
				}
				var first = true;
				var transEndRba = RedoByteAddress.MIN_VALUE;
				var transStartRba = RedoByteAddress.MIN_VALUE;
				var transEndScn = 0L;
				var transStartScn = 0L;
				while (iterator.hasNext()) {
					final var record = iterator.next();
					if (xidFilter) {
						if (record.xid() != null && !xid.equals(record.xid()))
							continue;
						if (record.changeVectors().size() > 0 &&
								((record.changeVectors().get(0).operation() >> 8) == KCOCOTBK ||
								(record.changeVectors().get(0).operation() >> 8) == KCOCOTSG ||
								(record.changeVectors().get(0).operation() >> 8) == KCOCOHLB ||
								(record.changeVectors().get(0).operation() >> 8) == KCOCOTBF ||
								record.changeVectors().get(0).operation() == _5_12_RST ||
								record.changeVectors().get(0).operation() == _14_1_CUSH ||
								record.changeVectors().get(0).operation() == _14_2_CRLK ||
								record.changeVectors().get(0).operation() == _14_4_OPEMREDO ||
								record.changeVectors().get(0).operation() == _24_10_URU ||
								record.changeVectors().get(0).operation() == _26_3_FRMT))
							continue;
					}
					final boolean printRecord;
					if (objFilter) {
						if (record.has5_1() || record.hasPrb()) {
							final OraCdcChangeUndo change;
							if (record.has5_1())
								change = record.change5_1();
							else
								change = record.changePrb();
							if (Arrays.binarySearch(objects, change.obj()) >= 0)
								printRecord = true;
							else
								printRecord = false;
						} else {
							printRecord = false;
						}
					} else {
						printRecord = true;
					}
					if (printRecord) {
						if (((xidFilter && !limits) || generateTestData) &&
								record.xid() != null && (xid.equals(record.xid()) || 
										(record.hasPrb() && record.xid() != null && xid.partial() == record.xid().partial()))) {
							transEndRba = record.rba();
							transEndScn = record.scn();
							if (first) {
								transStartRba = record.rba();
								transStartScn = record.scn();
								LOGGER.info("Transaction {} starts at SCN/RBA {}/{}",
										xid, Long.toUnsignedString(transStartScn), transStartRba);
								first = false;
							}
						}
						if (generateTestData)
							try {
								iw.write(record);
							} catch (IOException ioe) {
								LOGGER.error(
										"""
										
										=====================
										'{}' while writing to '{}'
										Errorstack:
										{}
										=====================
										
										""",
										ioe.getMessage(), testFileName, ExceptionUtils.getExceptionStackTrace(ioe));
								System.exit(1);
							}
						else {
							if (records) {
								out.println(record.toString());
							}
							if (dumps) {
								if (!records) {
									out.println("RBA: " + record.rba());
								}
								out.println("Content: ");
								out.print(rawToHex(record.content()));
								for (final OraCdcChange change : record.changeVectors()) {
									out.println("\nChange # " + change.num() + change.binaryDump());
								}
							}							
						}
					}
				}
				if ((xidFilter && !limits) || generateTestData)
					LOGGER.info("Transaction {} ends at SCN/RBA {}/{}",
							xid, Long.toUnsignedString(transEndScn), transEndRba);
				if (generateTestData) {
					try {
						iw.writeHeader(transStartScn, transStartRba, transEndScn, transEndRba);
					} catch (IOException ioe) {
						LOGGER.error(
								"""
								
								=====================
								'{}' while writing to '{}'
								Errorstack:
								{}
								=====================
								
								""",
								ioe.getMessage(), testFileName, ExceptionUtils.getExceptionStackTrace(ioe));
						System.exit(1);
					}
				}
			} catch (SQLException sqle) {
				LOGGER.error(
						"""
						
						=====================
						'{}' processing redo file '{}'
						Errorstack:
						{}
						=====================
						
						""", sqle.getMessage(), redoFile, ExceptionUtils.getExceptionStackTrace(sqle));
				System.exit(1);
			}
		}

		if (useFile) {
			out.flush();
			out.close();
		}
		LOGGER.info("Completed in {} ms", (System.currentTimeMillis() - millis));
	}

	private static void setupCliOptions(final Options options) {
		final Option redoFile = Option.builder("f")
				.longOpt("redo-file")
				.hasArg(true)
				.required(true)
				.desc("Full path to Oracle RDBMS archived or online redo file")
				.get();
		options.addOption(redoFile);

		final Option outputFile = Option.builder("o")
				.longOpt("output-file")
				.hasArg(true)
				.required(false)
				.desc("Output file. If not specified, stdout will be used.")
				.get();
		options.addOption(outputFile);

		final Option printRecords = Option.builder("r")
				.longOpt("redo-records")
				.hasArg(false)
				.required(false)
				.desc("If this option is specified, information about the redo records will be printed.")
				.get();
		options.addOption(printRecords);

		final Option binaryDump = Option.builder("b")
				.longOpt("binary-dump")
				.hasArg(false)
				.required(false)
				.desc("If this option is specified, binary dump of change vectors will be printed.")
				.get();
		options.addOption(binaryDump);

		final OptionGroup startAddr = new OptionGroup();
		final Option startRba = Option.builder("s")
				.longOpt("start-rba")
				.hasArg(true)
				.required(false)
				.desc("The RBA from which information about the redo records will be printed. Must be used in pair with -e/--end-rba")
				.get();
		startAddr.addOption(startRba);

		final Option startScn = Option.builder("c")
				.longOpt("start-scn")
				.hasArg(true)
				.required(false)
				.desc("The SCN from which information about the redo records will be printed. Must be used in pair with -n/--end-scn")
				.get();
		startAddr.addOption(startScn);
		options.addOptionGroup(startAddr);

		final OptionGroup endAddr = new OptionGroup();
		final Option endRba = Option.builder("e")
				.longOpt("end-rba")
				.hasArg(true)
				.required(false)
				.desc("The RBA to which information about the redo records will be printed. Must be used in pair with -s/--start-rba")
				.get();
		endAddr.addOption(endRba);
		
		final Option endScn = Option.builder("n")
				.longOpt("end-scn")
				.hasArg(true)
				.required(false)
				.desc("The RBA to which information about the redo records will be printed. Must be used in pair with -c/--start-scn")
				.get();
		endAddr.addOption(endScn);
		options.addOptionGroup(endAddr);

		final Option objects = Option.builder("d")
				.longOpt("data-objects")
				.hasArgs()
				.required(false)
				.desc("Identifier of the object(s) for which information will be printed. By default, information about all objects is printed")
				.get();
		options.addOption(objects);

		final var filter = Option.builder("x")
				.longOpt("xid")
				.hasArg()
				.required(false)
				.desc("Identifier of the transaction Id (XID) for which information will be printed. By default, information about all transaction is printed")
				.get();
		options.addOption(filter);

		final Option asmUrl = Option.builder("l")
				.longOpt(ASM_URL)
				.hasArg()
				.required(false)
				.desc("A valid JDBC URL pointing to an Oracle ASM instance. For example: -l jdbc:oracle:thin:@localhost:1521/+ASM")
				.get();
		options.addOption(asmUrl);

		final Option asmUser = Option.builder("u")
				.longOpt(ASM_USER)
				.hasArg()
				.required(false)
				.desc("Oracle ASM user with SYSASM or SYSDBA role")
				.get();
		options.addOption(asmUser);

		final Option asmPassword = Option.builder("p")
				.longOpt(ASM_PASSWORD)
				.hasArg()
				.required(false)
				.desc("Password of Oracle ASM User")
				.get();
		options.addOption(asmPassword);

		final Option endianness = Option.builder("a")
				.longOpt(BIG_ENDIAN)
				.required(false)
				.desc("When specified, Oracle redo log files are treated as big endian. By default, Oracle redo log files are assumed to be little endian.")
				.get();
		options.addOption(endianness);

		final Option sshPassword = Option.builder("S")
				.longOpt(SSH_PASSWORD)
				.hasArg()
				.required(false)
				.desc("Password for ssh connection, if the redo file is specified in ssh notation (username@hostname:filename)")
				.get();
		options.addOption(sshPassword);

		final Option sshIdentity = Option.builder("i")
				.longOpt(SSH_IDENTITY)
				.hasArg()
				.required(false)
				.desc("File from which the identity (private key) for public key authentication is read, if the redo file is specified in ssh notation (username@hostname:filename)")
				.get();
		options.addOption(sshIdentity);

		final Option sshPort = Option.builder("P")
				.longOpt(SSH_PORT)
				.hasArg()
				.required(false)
				.desc("Port to connect on the remote host, if the redo file is specified in ssh notation (username@hostname:filename)")
				.get();
		options.addOption(sshPort);

		final Option smbUser = Option.builder("U")
				.longOpt(SMB_USER)
				.hasArg()
				.required(false)
				.desc("SMB (Windows) user in form of DOMAIN\\User, if the redo file is specified in SMB notation \\\\sewrver\\share\\path-to-file")
				.get();
		options.addOption(smbUser);

		final Option smbPassword = Option.builder("W")
				.longOpt(SMB_PASSWORD)
				.hasArg()
				.required(false)
				.desc("Password for connection to SMB server")
				.get();
		options.addOption(smbPassword);

		final Option testClassMode = Option.builder("t")
				.longOpt(TEST_CLASS)
				.required(false)
				.desc("When this option is specified, the utility generates not a redo file dump, but a oracdc test class template for the specified transaction")
				.get();
		options.addOption(testClassMode);
	}

}
