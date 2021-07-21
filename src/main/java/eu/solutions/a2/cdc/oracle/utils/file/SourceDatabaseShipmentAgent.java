package eu.solutions.a2.cdc.oracle.utils.file;

import java.io.File;
import java.io.FileInputStream;
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
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.ParamConstants;
import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

public class SourceDatabaseShipmentAgent {

	private static final Logger LOGGER = LoggerFactory.getLogger(SourceDatabaseShipmentAgent.class);
	private static final int INPUT_COMMAND_LENGTH = 1024;
	private static final int MAX_FILE_SIZE = Integer.MAX_VALUE - 4096 + 1;

	private final InetSocketAddress serverAddress;
	private ServerSocketChannel listener = null;

	SourceDatabaseShipmentAgent(final String bindAddress, final int serverPort) {
		serverAddress = new InetSocketAddress(bindAddress, serverPort);
	}

	private void startServer() throws IOException {
		try {
			listener = ServerSocketChannel.open();
			ServerSocket ss = listener.socket();
			ss.setReuseAddress(true);
			ss.bind(serverAddress);
			LOGGER.info("Listening on {}:{}",
					serverAddress.getHostName(), serverAddress.getPort());
		} catch (IOException e) {
			LOGGER.error("Failed to listen on {}:{}",
					serverAddress.getHostName(), serverAddress.getPort());
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new IOException(e);
		}

		try {
			ByteBuffer inputCommand = ByteBuffer.allocate(INPUT_COMMAND_LENGTH);
			while (true) {
				SocketChannel channel = listener.accept();
				LOGGER.debug("Client connection request accepted from {}", 
						channel.getRemoteAddress().toString());
				channel.configureBlocking(true);
				int bytesRead = 0;
				try {
					bytesRead = channel.read(inputCommand);
				} catch (IOException ioe) {
					LOGGER.error("IOException: Unable to read command from socket");
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
				}
				inputCommand.rewind();
				final String fileRequest = StringUtils.trim(new String(inputCommand.array(), "UTF-8"));
				LOGGER.debug("Read {} bytes, input request = '{}'", bytesRead, fileRequest);
				final Path path = Paths.get(fileRequest);
				if (!Files.exists(path) || Files.isDirectory(path)) {
					LOGGER.error("File '{}' does not exist or is directory!", fileRequest);
					channel.close();
				} else {
					final File file = path.toFile();
					final FileInputStream fis = new FileInputStream(file);
					final FileChannel fc = fis.getChannel();
					final long fileSize = file.length();
					if (fileSize < MAX_FILE_SIZE) {
						fc.transferTo(0, fileSize, channel);
					} else {
						long position = 0;
						while (position < fileSize) {
								//TODO
								//TODO - chunk size!!!
								//TODO
								position += fc.transferTo(position, 1048576, channel);
						}
					}
					LOGGER.debug("File {} with length {} bytes sent.", path, fileSize);
					fc.close();
					fis.close();
					channel.close();
				}
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

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, argv);
		} catch (ParseException pe) {
			LOGGER.error(pe.getMessage());
			formatter.printHelp(SourceDatabaseShipmentAgent.class.getCanonicalName(), options);
		}

		final String bindAddressArg = cmd.getOptionValue("bind-address", "0.0.0.0");
		final String portNumberArg = cmd.getOptionValue("port",
				Integer.toString(ParamConstants.DISTRIBUTED_TARGET_PORT_DEFAULT));
		int portNumber = ParamConstants.DISTRIBUTED_TARGET_PORT_DEFAULT;
		try {
			portNumber = Integer.parseInt(portNumberArg);
		} catch (Exception e) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			LOGGER.error("{} will be used as port number!", ParamConstants.DISTRIBUTED_TARGET_PORT_DEFAULT);
		}

		SourceDatabaseShipmentAgent sdsa = new SourceDatabaseShipmentAgent(bindAddressArg, portNumber);
		try {
			sdsa.startServer();
		} catch (IOException e) {
			LOGGER.error("IOException while running {}", sdsa.getClass().getCanonicalName());
		} finally {
			LOGGER.info("Exiting {}", sdsa.getClass().getCanonicalName());
		}

	}

}
