package eu.solutions.a2.cdc.oracle.utils;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Version {

	private static final Logger LOGGER = LoggerFactory.getLogger(Version.class);
	private static final String PROPS_PATH = "/oracdc-version.properties";
	private static String version = "undefined";

	static {
		try (InputStream is = Version.class.getResourceAsStream(PROPS_PATH)) {
			Properties props = new Properties();
			props.load(is);
			version = props.getProperty("version", version).trim();
		} catch (Exception e) {
			LOGGER.warn(ExceptionUtils.getExceptionStackTrace(e));
		}
	}

	 public static String getVersion() {
		 return version;
	 }

}
