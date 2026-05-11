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

 * You should have received a copy of the GNU Affero General Public
 * License along with this program; see the file GNU-AGPL-v3.0.adoc.
 * If not, see <https://www.gnu.org/licenses/>.
 */

package solutions.a2.cdc.oracle.utils;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class KafkaUtils {

	public static final String AVRO_FIELD_VALID_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_";

	private static final String CHARS_TO_REPLACE = "[^a-zA-Z0-9._\\-]+";
	private static final String COL_NAME_CHARS_TO_REPLACE = "[^a-zA-Z0-9_]+";
	private static final String TOPIC_NAME_VALID_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ._-";

	public static boolean validTopicName(final String topicName) {
		return StringUtils.containsOnly(topicName, TOPIC_NAME_VALID_CHARS);
	}

	public static String fixTopicName(final String topicName, final String replacement) {
		return RegExUtils.replaceAll(topicName, CHARS_TO_REPLACE, replacement);
	}

	public static boolean validAvroFieldName(final String name) {
		return StringUtils.containsOnly(name, AVRO_FIELD_VALID_CHARS);
	}

	public static String fixAvroFieldName(final String name, final String replacement) {
		return RegExUtils.replaceAll(name, COL_NAME_CHARS_TO_REPLACE, replacement);
	}

}
