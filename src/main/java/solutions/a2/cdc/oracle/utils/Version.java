/**
 * This file is part of the oracdc project.
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 * Authors: Aleksei Veremeev
 *
 * This program is offered under a commercial and under the AGPL license.
 * For commercial licensing, contact us at sales@a2.solutions.
 * For AGPL licensing, see below.
 *
 * AGPL licensing:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
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
