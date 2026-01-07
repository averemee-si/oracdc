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

import static solutions.a2.cdc.oracle.OraCdcTransaction.LobProcessingStatus.REDOMINER;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.sql.SQLException;
import java.time.ZoneId;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.help.HelpFormatter;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.OraCdcLobExtras;
import solutions.a2.cdc.oracle.OraCdcRawTransaction;
import solutions.a2.cdc.oracle.OraCdcTransaction;
import solutions.a2.cdc.oracle.OraCdcTransactionChronicleQueue;
import solutions.a2.cdc.oracle.internals.OraCdcRedoLog;
import solutions.a2.cdc.oracle.internals.OraCdcRedoRecord;
import solutions.a2.oracle.internals.RedoByteAddress;
import solutions.a2.oracle.internals.Xid;

/**
 * 
 * Reads transaction data from a binary file
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraCdcIncidentReader extends OraCdcIncidentBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcIncidentReader.class);

	private final Xid xid;
	private final OraCdcRedoLog orl;

	public OraCdcIncidentReader(final String incFileName) throws IOException {
		super(incFileName);
		xid = new Xid(raf.readShort(), raf.readShort(), raf.readInt());
		var startScn = raf.readLong();
		var startRba = new RedoByteAddress(raf.readInt(), raf.readInt(), raf.readShort());
		var endScn = raf.readLong();
		var endRba = new RedoByteAddress(raf.readInt(), raf.readInt(), raf.readShort());
		var commitScn = raf.readLong();
		var commitRba = new RedoByteAddress(raf.readInt(), raf.readInt(), raf.readShort());
		raf.seek(HEADER_SIZE);
		orl = OraCdcRedoLog.getLinux19c();
		LOGGER.info(
				"""
				
				=====================
				Reading transaction {} from {}
				Start  SCN/RBA: {}/{}
				End    SCN/RBA: {}/{}
				Commit SCN/RBA: {}/{}
				=====================
				
				""", xid, incFileName,
					Long.toUnsignedString(startScn), startRba,
					Long.toUnsignedString(endScn), endRba,
					Long.toUnsignedString(commitScn), commitRba);
	}

	public OraCdcTransaction get() throws SQLException, IOException {
		var queuesRoot = FileSystems.getDefault().getPath(System.getProperty("java.io.tmpdir"));
		transFromLobId.clear();
		var lobExtras = new OraCdcLobExtras();
		long commitScn = -1;

		var raw = new OraCdcRawTransaction(xid, ZoneId.systemDefault(), 0x10, lobExtras);
		while (true) {
			RedoByteAddress rba = null;
			try {
				rba = new RedoByteAddress(raf.readInt(), raf.readInt(), raf.readShort());
			} catch (EOFException ee) {
				break;
			}
			var scn = raf.readLong();
			var len = raf.readInt();
			var content = new byte[len];
			var actual = raf.read(content);
			if (actual != len) {
				LOGGER.error(
						"""
						
						=====================
						Expected {} bytes in redo record content, but {} were read.
						=====================
						
						""");
				throw new IOException("File " + incFileName + " read error!");
			}
			var rr = new OraCdcRedoRecord(orl, scn, rba, content);
			if (rr.has5_4()) {
				if (rr.change5_4().rollback()) {
					LOGGER.error(
							"""
							
							=====================
							Transactions ending with the ROLLBACK statement are not supported.
							=====================
							
							""");
					throw new IllegalArgumentException("Transactions ending with the ROLLBACK statement are not supported.");
				} else {
					LOGGER.debug("Commit at SCN/RBA {}/{}", Long.toUnsignedString(rr.scn()), rr.rba());
					commitScn = rr.scn();
				}
			} else {
				if (rr.has10_x() && !rr.change5_1().supplementalLogData())
					continue;
				else
					raw.add(new OraCdcRedoRecord(orl, scn, rba, content), (int)(System.currentTimeMillis() / 1000));
			}
		}
		if (commitScn > 0)
			raw.commitScn(commitScn);
		else {
			LOGGER.error(
					"""
					
					=====================
					Transactions without the COMMIT statement are not supported.
					=====================
					
					""");
			throw new IllegalArgumentException("Transactions without the COMMIT statement are not supported.");
		}

		final int[] halfQuarter = {0x800000, 0x200000};
		var transaction = new OraCdcTransactionChronicleQueue(raw, orl.cdb(), REDOMINER, queuesRoot, halfQuarter);
		raf.seek(HEADER_SIZE);
		return transaction;
	}

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
				formatter.printHelp(OraCdcIncidentReader.class.getCanonicalName(), "", options, "", true);
			} catch (IOException ioe) {}
			System.exit(1);
		}
		var transFile = cmd.getOptionValue("t");
		
		try {
			var ir = new OraCdcIncidentReader(transFile);
			var transaction = (OraCdcTransactionChronicleQueue) ir.get();
			millis = System.currentTimeMillis() - millis;
			var errorExit = false;
			if (transaction.completed())
				LOGGER.info(
						"""
						
						=====================
						Transaction from file {} processed successfully in {} ms.
						=====================
						
						""", transFile, millis);
			else {
				LOGGER.error(
						"""
						
						=====================
						The transaction from file {} was not fully processed.
						Please send the output of the utility and a file containing transaction data to oracle@a2.solutions
						=====================
						
						""", transFile);
				errorExit = true;
			}
			ir.close();
			transaction.close();
			if (errorExit)
				System.exit(1);
		} catch (IOException | SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void setupCliOptions(final Options options) {
		final Option testClassMode = Option.builder("t")
				.longOpt("test-data")
				.hasArg()
				.required(true)
				.desc("The name of the file created by the OraRedoLogFile utility that contains the transaction data")
				.get();
		options.addOption(testClassMode);

	}
}
