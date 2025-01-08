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
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.internals.OraCdcChange;
import solutions.a2.cdc.oracle.internals.OraCdcChangeUndo;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLog;
import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.utils.ExceptionUtils;

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


	public static void main(String[] argv) {
		BasicConfigurator.configure();
		long millis = System.currentTimeMillis();
		LOGGER.info("Starting...");

		// Command line options
		final Options options = new Options();
		setupCliOptions(options);

		final CommandLineParser parser = new DefaultParser();
		final HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, argv);
		} catch (ParseException pe) {
			LOGGER.error(pe.getMessage());
			formatter.printHelp(OraRedoLogFile.class.getCanonicalName(), options);
			System.exit(1);
		}

		final String redoFile = cmd.getOptionValue("f");
		OraCdcRedoLog orl = null;
		try {
			orl = new OraCdcRedoLog(cmd.getOptionValue("f"));
		} catch (IOException ioe) {
			LOGGER.error(
					"\n=====================\n" +
					"'{}' opening redo file '{}'\nErrorstack:\n{}" +
					"\n=====================\n",
					ioe.getMessage(), redoFile, ExceptionUtils.getExceptionStackTrace(ioe));
			System.exit(1);
		}

		boolean useFile = false;
		PrintStream out = null;
		final String outFileName = cmd.getOptionValue("o");
		if (StringUtils.isBlank(outFileName)) {
			out = System.out;
		} else {
			try {
				out = new PrintStream(new FileOutputStream(outFileName));
				useFile = true;
			} catch (IOException ioe) {
				LOGGER.error(
						"\n=====================\n" +
						"'{}' opening output file '{}'\nErrorstack:\n{}" +
						"\n=====================\n",
						ioe.getMessage(), outFileName, ExceptionUtils.getExceptionStackTrace(ioe));
				System.exit(1);
			}
		}
		boolean records = cmd.hasOption("r");
		boolean dumps = cmd.hasOption("b");
		boolean limits = false;
		boolean useRba = true;
		RedoByteAddress startRba = null;
		RedoByteAddress endRba = null;
		long startScn = 0;
		long endScn = 0;
		if (cmd.hasOption("s")) {
			try {
				startRba = RedoByteAddress.fromLogmnrContentsRs_Id(cmd.getOptionValue("s"));
			} catch (Exception e) {
				LOGGER.error(
						"\n=====================\n" +
						"'{}' parsing redo byte address '{}'\nErrorstack:\n{}" +
						"\n=====================\n",
						e.getMessage(), cmd.getOptionValue("s"), ExceptionUtils.getExceptionStackTrace(e));
				System.exit(1);
			}
			if (cmd.hasOption("e")) {
				try {
					endRba = RedoByteAddress.fromLogmnrContentsRs_Id(cmd.getOptionValue("e"));
				} catch (Exception e) {
					LOGGER.error(
							"\n=====================\n" +
							"'{}' parsing redo byte address '{}'\nErrorstack:\n{}" +
							"\n=====================\n",
							e.getMessage(), cmd.getOptionValue("e"), ExceptionUtils.getExceptionStackTrace(e));
					System.exit(1);
				}
			} else {
				LOGGER.error(
						"\n=====================\n" +
						"'if you specified an option -s/--start-rba, then you must specify the corresponding option -e/--end-rba!" +
						"\n=====================\n");
				System.exit(1);
			}
			limits = true;
		} else if (cmd.hasOption("c")) {
			try {
				final String strStartScn =  cmd.getOptionValue("c");
				if (StringUtils.startsWithIgnoreCase(strStartScn, "0x"))
					startScn = Long.parseLong(StringUtils.substring(strStartScn, 2), 0x10);
				else
					startScn = Long.parseLong(strStartScn);
			} catch (Exception e) {
				LOGGER.error(
						"\n=====================\n" +
						"'{}' parsing SCN '{}'\nErrorstack:\n{}" +
						"\n=====================\n",
						e.getMessage(), cmd.getOptionValue("c"), ExceptionUtils.getExceptionStackTrace(e));
				System.exit(1);
			}
			if (cmd.hasOption("n")) {
				try {
					final String strEndScn =  cmd.getOptionValue("n");
					if (StringUtils.startsWithIgnoreCase(strEndScn, "0x"))
						endScn = Long.parseLong(StringUtils.substring(strEndScn, 2), 0x10);
					else
						endScn = Long.parseLong(strEndScn);
				} catch (Exception e) {
					LOGGER.error(
							"\n=====================\n" +
							"'{}' parsing SCN '{}'\nErrorstack:\n{}" +
							"\n=====================\n",
							e.getMessage(), cmd.getOptionValue("n"), ExceptionUtils.getExceptionStackTrace(e));
					System.exit(1);
				}
			} else {
				LOGGER.error(
						"\n=====================\n" +
						"'if you specified an option -c/--start-scn, then you must specify the corresponding option -n/--end-scn!" +
						"\n=====================\n");
				System.exit(1);
			}
			limits = true;
			useRba = false;
		}

		final boolean objFilter;
		final int[] objects;
		if (cmd.getOptionValues("d") == null || cmd.getOptionValues("d").length == 0) {
			objects = null;
			objFilter = false;
		} else {
			final String[] objIds = cmd.getOptionValues("d");
			objects = new int[objIds.length];
			for (int i = 0; i < objects.length; i++) {
				try {
					final String str =  objIds[i];
					if (StringUtils.startsWithIgnoreCase(str, "0x"))
						objects[i] = Integer.parseInt(StringUtils.substring(str, 2), 0x10);
					else
						objects[i] = Integer.parseInt(str);
				} catch (Exception e) {
					LOGGER.error(
							"\n=====================\n" +
							"'{}' parsing objId '{}'\nErrorstack:\n{}" +
							"\n=====================\n",
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

		out.println(orl);
		if (records || dumps) {
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
				while (iterator.hasNext()) {
					final OraCdcRedoRecord record = iterator.next();
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
						if (records) {
							out.println(record.toString());
						}
						if (dumps) {
							if (!records) {
								out.println("RBA: " + record.rba());
							}
							for (final OraCdcChange change : record.changeVectors()) {
								out.println("\nChange # " + change.num() + change.binaryDump());
							}
						}
					}
				}
			} catch (IOException ioe) {
				LOGGER.error(
						"\n=====================\n" +
						"'{}' processing redo file '{}'\nErrorstack:\n{}" +
						"\n=====================\n",
						ioe.getMessage(), redoFile, ExceptionUtils.getExceptionStackTrace(ioe));
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
				.build();
		options.addOption(redoFile);

		final Option outputFile = Option.builder("o")
				.longOpt("output-file")
				.hasArg(true)
				.required(false)
				.desc("Output file. If not specified, stdout will be used.")
				.build();
		options.addOption(outputFile);

		final Option printRecords = Option.builder("r")
				.longOpt("redo-records")
				.hasArg(false)
				.required(false)
				.desc("If this option is specified, information about the redo records will be printed.")
				.build();
		options.addOption(printRecords);

		final Option binaryDump = Option.builder("b")
				.longOpt("binary-dump")
				.hasArg(false)
				.required(false)
				.desc("If this option is specified, binary dump of change vectors will be printed.")
				.build();
		options.addOption(binaryDump);

		final OptionGroup startAddr = new OptionGroup();
		final Option startRba = Option.builder("s")
				.longOpt("start-rba")
				.hasArg(true)
				.required(false)
				.desc("The RBA from which information about the redo records will be printed. Must be used in pair with -e/--end-rba")
				.build();
		startAddr.addOption(startRba);

		final Option startScn = Option.builder("c")
				.longOpt("start-scn")
				.hasArg(true)
				.required(false)
				.desc("The SCN from which information about the redo records will be printed. Must be used in pair with -n/--end-scn")
				.build();
		startAddr.addOption(startScn);
		options.addOptionGroup(startAddr);

		final OptionGroup endAddr = new OptionGroup();
		final Option endRba = Option.builder("e")
				.longOpt("end-rba")
				.hasArg(true)
				.required(false)
				.desc("The RBA to which information about the redo records will be printed. Must be used in pair with -s/--start-rba")
				.build();
		endAddr.addOption(endRba);
		
		final Option endScn = Option.builder("n")
				.longOpt("end-scn")
				.hasArg(true)
				.required(false)
				.desc("The RBA to which information about the redo records will be printed. Must be used in pair with -c/--start-scn")
				.build();
		endAddr.addOption(endScn);
		options.addOptionGroup(endAddr);

		final Option objects = Option.builder("d")
				.longOpt("data-objects")
				.hasArgs()
				.required(false)
				.desc("Identifier of the object(s) for which information will be printed. By default, information about all objects is printed")
				.build();
		options.addOption(objects);

	}

}
