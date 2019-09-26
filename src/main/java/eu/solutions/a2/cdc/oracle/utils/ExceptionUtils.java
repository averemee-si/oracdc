/**
 * Copyright (c) 2018-present, http://a2-solutions.eu
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

package eu.solutions.a2.cdc.oracle.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Logger;

public class ExceptionUtils {

	private static final Logger LOGGER = Logger.getLogger(ExceptionUtils.class);

	/**
	 * Display the stacktrace contained in an exception.
	 * @param exception Exception
	 * @return String with the output from printStackTrace
	 * @see Exception.printStackTrace()
	 **/
	public static String getExceptionStackTrace(Exception exception) {
		String result = exception.getMessage();
		try (
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw)) {
			exception.printStackTrace(pw);
			result += sw.toString();
		} catch (Exception e) {
			LOGGER.error(
					"Exception while converting exception's stack trace to string!\n" +
					e.getMessage());
		}
		return result;
	}

}
