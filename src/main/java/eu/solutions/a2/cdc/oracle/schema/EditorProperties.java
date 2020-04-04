package eu.solutions.a2.cdc.oracle.schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.solutions.a2.cdc.oracle.utils.ExceptionUtils;

public class EditorProperties {

	private static final Logger LOGGER = LoggerFactory.getLogger(EditorProperties.class);

	private final String fileName;
	private final Properties props;
	private File propsFile;

	public EditorProperties() {
		final String directory = System.getProperty("user.home") + File.separator + ".oracdc";
		fileName = directory + File.separator + "schemaEditor.properties";
		props = new Properties();
		try {
			final File propsDir = new File(directory);
			if (!propsDir.exists()) {
				propsDir.mkdirs();
			}
			propsFile = new File(fileName);
			if (propsFile.exists()) {
				try (InputStream is = new FileInputStream(propsFile)) {
					props.load(is);
				} catch (IOException ioe) {
					LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
				}
			}
		} catch (SecurityException se) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(se));
		}
	}

	public String get(String key) {
		final String value = props.getProperty(key);
		return value == null ? "" : value;
	}

	public void putOraParams(String host, String port, String sid, String user) {
		props.put("host", host);
		props.put("port", port);
		props.put("sid", sid);
		props.put("user", user);
		writeProps();
	}

	public void putFileParams(String fileName) {
		props.put("dir", fileName);
		writeProps();
	}

	private void writeProps() {
		if (propsFile != null) {
			try {
				OutputStream os = new FileOutputStream(propsFile);
				props.store(os, "Created by oracdc Schema Editor tools");
				os.flush();
				os.close();
			} catch (IOException ioe) {
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
			}
		}
	}

}
