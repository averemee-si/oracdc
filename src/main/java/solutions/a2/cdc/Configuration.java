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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class Configuration {

    private static final Pattern COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*");

	private final Map<String, Parameter> configParams;

	public Configuration() {
		configParams = new HashMap<>();
	}

	public Set<String> parameters() {
		return Collections.unmodifiableSet(configParams.keySet());
	}

	private Configuration define(Parameter parameter) {
		if (configParams.containsKey(parameter.name()))
			throw new IllegalArgumentException("Parameter " + parameter.name() + " is defined twice.");
		configParams.put(parameter.name(), parameter);
		return this;
	}

	public Configuration define(String name, ParameterType type, Object defaultValue) {
		return define(new Parameter(name, type, defaultValue));
	}
 
	public Map<String, Object> parse(Map<?, ?> props) {
		Map<String, Object> values = new HashMap<>();
		for (var param : configParams.values()) {
			values.put(param.name(), parseValue(param, props.get(param.name()), props.containsKey(param.name())));
		}
		return values;
	}

	private Object parseValue(Parameter param, Object value, boolean isSet) {
		return parseType(param.name(), isSet ? value : param.defaultValue(), param.type());
	}

	Object parseType(String name, Object value, ParameterType type) {
		if (value == null)
			return null;
		try {
			var trimmed = (value instanceof String s) ? StringUtils.trim(s) : null;
			switch (type) {
				case BOOLEAN -> {
					if (value instanceof String) {
						if (Strings.CI.equals(trimmed, "true"))
							return true;
						else if (Strings.CI.equals(trimmed, "false"))
							return false;
						else
							throw new ConfigurationException(name, value, "Expected value to be either true or false");
					} else if (value instanceof Boolean)
						return value;
					else
						throw new ConfigurationException(name, value, "Expected value to be either true or false");
				}
				case INT -> {
					if (value instanceof String)
						return Integer.parseUnsignedInt(trimmed);
					else if (value instanceof Integer)
						return value;
					else
						throw new ConfigurationException(name, value, "Expected value to be a 32-bit integer, but it was a " + value.getClass().getName());
				}
				case LONG -> {
					if (value instanceof String)
						return Long.parseUnsignedLong(trimmed);
					else if (value instanceof Integer i)
						return i.longValue();
					else if (value instanceof Long)
						return value;
					else
						throw new ConfigurationException(name, value, "Expected value to be a 64-bit integer, but it was a " + value.getClass().getName());
				}
				case LIST -> {
					if (value instanceof String)
						if (StringUtils.isEmpty(trimmed))
							return Collections.emptyList();
						else
							return Arrays.asList(COMMA_WITH_WHITESPACE.split(trimmed, -1));
					else if (value instanceof List)
						return value;
					else
						throw new ConfigurationException(name, value, "Expected a comma separated list.");
				}
				case STRING, PASSWORD -> {
					if (value instanceof String)
						return trimmed;
					else
						throw new ConfigurationException(name, value, "Expected value to be a string, but it was a " + value.getClass().getName());
				}
				default -> {
					return null;
				}
			}
		} catch (NumberFormatException nfe) {
			throw new ConfigurationException(name, value, "Not a number of type " + type);
		}
	}

}
