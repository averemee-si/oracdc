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

package solutions.a2.cdc.oracle.schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
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
