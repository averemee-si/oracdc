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
