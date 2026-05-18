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

package solutions.a2.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class ExceptionUtils {

	private static final Logger LOGGER = LogManager.getLogger(ExceptionUtils.class);

	/**
	 * Display the stacktrace contained in an exception.
	 * @param exception Exception
	 * @return String with the output from printStackTrace
	 * @see java.lang.Exception#printStackTrace()
	 **/
	public static String getExceptionStackTrace(Exception exception) {
		final StringBuilder sb = new StringBuilder(1024);
		sb.append(exception.getMessage());
		sb.append("\n");
		try (StringWriter sw = new StringWriter(); PrintWriter pw = new PrintWriter(sw)) {
			exception.printStackTrace(pw);
			sb.append(sw.toString());
		} catch (Exception e) {
			LOGGER.error("Exception while converting exception's stack trace to string!\n{}",
					e.getMessage());
		}
		return sb.toString();
	}

}
