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

package solutions.a2.cdc;

import java.util.List;
import java.util.Map;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public abstract class AbstractConfiguration {

	private final Map<String, Object> values;

	public AbstractConfiguration(final Configuration configuration, final Map<?, ?> originals) {
		values = configuration.parse(originals);
	}

	Object get(final String key) {
		if (!values.containsKey(key))
			throw new ConfigurationException(String.format("Unknown parameter '%s'", key));
		return values.get(key);
	}

	public boolean getBoolean(final String key) {
		return (boolean) get(key);
	}

	public int getInt(final String key) {
		return (int) get(key);
	}

	public long getLong(final String key) {
		return (long) get(key);
	}

	@SuppressWarnings("unchecked")
	public List<String> getList(final String key) {
		return (List<String>) get(key);
	}

	public String getString(final String key) {
		return (String) get(key);
	}

	public String getPassword(final String key) {
		return (String) get(key);
	}

}
