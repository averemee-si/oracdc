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

package solutions.a2.cdc.oracle.runtime.config;

import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_DEBEZIUM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_DEBEZIUM;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_KAFKA_STD;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_INT_SINGLE;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_KAFKA;
import static solutions.a2.cdc.oracle.runtime.config.Parameters.SCHEMA_TYPE_SINGLE;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class BaseConfig {

	private final int schemaType;

	BaseConfig(final String schemaTypeAsString) {
		switch (schemaTypeAsString) {
			case SCHEMA_TYPE_KAFKA -> schemaType = SCHEMA_TYPE_INT_KAFKA_STD;
			case SCHEMA_TYPE_SINGLE -> schemaType = SCHEMA_TYPE_INT_SINGLE;
			case SCHEMA_TYPE_DEBEZIUM -> schemaType = SCHEMA_TYPE_INT_DEBEZIUM;
			default -> schemaType = -1;
		}
	}

	int schemaType() {
		return schemaType;
	}

}
