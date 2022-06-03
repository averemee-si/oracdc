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

package solutions.a2.cdc.oracle.jmx;

import java.time.Duration;

/**
 * 
 * @author averemee
 *
 */
public class OraCdcMBeanUtils {

	private static final String DURATION_FMT = "%sdays %shrs %smin %ssec.\n";

	protected static String formatDuration(Duration duration) {
		return String.format(DURATION_FMT,
				duration.toDays(),
				duration.toHours() % 24,
				duration.toMinutes() % 60,
				duration.getSeconds() % 60);
	}


}
