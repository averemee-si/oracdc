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
