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

package solutions.a2.cdc.oracle.utils;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import solutions.a2.utils.ExceptionUtils;

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
