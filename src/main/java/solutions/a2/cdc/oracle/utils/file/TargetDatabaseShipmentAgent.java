package solutions.a2.cdc.oracle.utils.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.cdc.oracle.ParamConstants;
import solutions.a2.utils.ExceptionUtils;

public class TargetDatabaseShipmentAgent {

	private static final Logger LOGGER = LoggerFactory.getLogger(TargetDatabaseShipmentAgent.class);
	private static final int COMMAND_LENGTH = 1024;

	private final InetSocketAddress targetServerAddress;
	private final InetSocketAddress sourceServerAddress;
	private final String directoryName;
	private ServerSocketChannel listener = null;

	TargetDatabaseShipmentAgent(final String bindAddress, final int serverPort,
			final String directoryName, final String sourceHost, final int sourcePort) {
		targetServerAddress = new InetSocketAddress(bindAddress, serverPort);
		sourceServerAddress = new InetSocketAddress(sourceHost, sourcePort);

		final Path path = Paths.get(directoryName);
		boolean useTmpDir = false;
		if (!Files.exists(path)) {
			LOGGER.error("Directory '{}' does not exist!", directoryName);
			useTmpDir = true;
		}
		if (!Files.isDirectory(path)) {
			LOGGER.error("'{}' must be directory!", directoryName);
			useTmpDir = true;
		}

		if (useTmpDir) {
			final String tmpDir = System.getProperty("java.io.tmpdir");
			LOGGER.error("'{}' will be used for storing files from Source RDBMS", tmpDir);
			if (!Strings.CS.endsWith(tmpDir, File.separator)) {
				this.directoryName = tmpDir + File.separator;
			} else {
				this.directoryName = tmpDir;
			}
		}
		else {
			if (!Strings.CS.endsWith(directoryName, File.separator)) {
				this.directoryName = directoryName + File.separator;
			} else {
				this.directoryName = directoryName;
			}
		}
	}

	private void startServer() throws IOException {
		try {
			listener = ServerSocketChannel.open();
			ServerSocket ss = listener.socket();
			ss.setReuseAddress(true);
			ss.bind(targetServerAddress);
			LOGGER.info("Listening on {}:{}",
					targetServerAddress.getHostName(), targetServerAddress.getPort());
		} catch (IOException e) {
			LOGGER.error("Failed to listen on {}:{}",
					targetServerAddress.getHostName(), targetServerAddress.getPort());
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new IOException(e);
		}

		try {
			while (true) {
				SocketChannel listenerChannel = listener.accept();
				LOGGER.debug("Client connection request accepted from {}", 
						listenerChannel.getRemoteAddress().toString());
				listenerChannel.configureBlocking(true);
				final ByteBuffer inputCommand = ByteBuffer.allocate(COMMAND_LENGTH);
				int bytesRead = 0;
				bytesRead = listenerChannel.read(inputCommand);
				inputCommand.rewind();
				final String fileRequest = StringUtils.trim(new String(inputCommand.array(), "UTF-8"));
				if (fileRequest == null || "".equals(fileRequest)) {
					LOGGER.error("Empty request received from {}", listenerChannel.getRemoteAddress().toString());
					continue;
				}
				LOGGER.debug("Read {} bytes, input request = '{}'", bytesRead, fileRequest);

				//Pass request to source database server
				SocketChannel channelToSource = SocketChannel.open();
				channelToSource.connect(sourceServerAddress);
				channelToSource.configureBlocking(true);
				channelToSource.write(inputCommand);

				//TODO - size?
				final ByteBuffer dst = ByteBuffer.allocate(4096);
				final String localFileName = directoryName + 
						StringUtils.substringAfterLast(fileRequest, File.separator);
				LOGGER.debug("Remote file will be copied to {}.", localFileName);
				FileOutputStream fos = new FileOutputStream(localFileName);
				FileChannel fc = fos.getChannel();
				long totalBytes = 0; 
				bytesRead = 0;
				while (bytesRead != -1) {
					bytesRead = channelToSource.read(dst);
					dst.flip();
					fc.write(dst);
					totalBytes += bytesRead;
					dst.rewind();
				}
				fc.close();
				fos.close();
				channelToSource.close();
				LOGGER.debug("{} bytes read from socket", totalBytes);

				final String result = "OK" + "\n" +
						localFileName + "\n" + totalBytes;
				final ByteBuffer response = ByteBuffer.allocate(COMMAND_LENGTH);
				response.put(result.getBytes("UTF-8"));
				response.flip();
				listenerChannel.write(response);

				listenerChannel.close();

			}
		} catch (IOException e) {
			LOGGER.error("IOException: Unable to process data!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new IOException(e);
		}
	}

	public static void main(String[] argv) {
		// Check for valid log4j configuration
		final String log4jConfig = System.getProperty("a2.log4j.configuration");
		if (log4jConfig == null || "".equals(log4jConfig)) {
			BasicConfigurator.configure();
			LOGGER.error("JVM argument -Da2.log4j.configuration not set!");
		} else {
			// Check that log4j configuration file exist
			Path path = Paths.get(log4jConfig);
			if (!Files.exists(path) || Files.isDirectory(path)) {
				BasicConfigurator.configure();
				LOGGER.error("JVM argument -Da2.log4j.configuration points to unknown file {}.", log4jConfig);
			} else {
				// Initialize log4j
				PropertyConfigurator.configure(log4jConfig);
			}
		}

		final Options options = new Options();
		final Option bindAddress = new Option("b", "bind-address", true,
				"bind address, if not specified 0.0.0.0 used");
		bindAddress.setRequired(false);
		options.addOption(bindAddress);
		final Option port = new Option("p", "port", true,
				"port to listen on, if not specified " +  ParamConstants.DISTRIBUTED_TARGET_PORT_DEFAULT + " used");
		port.setRequired(false);
		options.addOption(port);
		final Option sourceAgentHost = new Option("h", "source-host", true,
				"hostname or IP address where SourceDatabaseShipmentAgent runs");
		sourceAgentHost.setRequired(true);
		options.addOption(sourceAgentHost);
		final Option sourceAgentPort = new Option("s", "source-port", true,
				"port on which SourceDatabaseShipmentAgent listens for requests");
		sourceAgentPort.setRequired(true);
		options.addOption(sourceAgentPort);
		final Option directory = new Option("d", "file-destination", true,
				"directory to store files from SourceDatabaseShipmentAgent");
		directory.setRequired(true);
		options.addOption(directory);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, argv);
		} catch (ParseException pe) {
			LOGGER.error(pe.getMessage());
			formatter.printHelp(TargetDatabaseShipmentAgent.class.getCanonicalName(), options);
		}

		final String bindAddressArg = cmd.getOptionValue("bind-address", "0.0.0.0");
		final String portNumberArg = cmd.getOptionValue("port",
				Integer.toString(ParamConstants.DISTRIBUTED_TARGET_PORT_DEFAULT));
		int portNumberArgInt = ParamConstants.DISTRIBUTED_TARGET_PORT_DEFAULT;
		try {
			portNumberArgInt = Integer.parseInt(portNumberArg);
		} catch (Exception e) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			LOGGER.error("{} will be used as port number!", ParamConstants.DISTRIBUTED_TARGET_PORT_DEFAULT);
		}
		final String sourceAgentHostArg = cmd.getOptionValue("source-host");
		int sourceAgentPortArg = -1;
		try {
			sourceAgentPortArg = Integer.parseInt(cmd.getOptionValue("source-port"));
		} catch (Exception e) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			System.exit(1);
		}

		TargetDatabaseShipmentAgent tdsa = new TargetDatabaseShipmentAgent(bindAddressArg, portNumberArgInt,
				cmd.getOptionValue("file-destination"), sourceAgentHostArg, sourceAgentPortArg);
		try {
			tdsa.startServer();
		} catch (IOException e) {
			LOGGER.error("IOException while running {}", tdsa.getClass().getCanonicalName());
		} finally {
			LOGGER.info("Exiting {}", tdsa.getClass().getCanonicalName());
		}

	}

}
