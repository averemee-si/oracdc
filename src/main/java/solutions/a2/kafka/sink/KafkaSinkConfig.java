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

package solutions.a2.kafka.sink;

import java.util.List;

import solutions.a2.cdc.TargetDbConfig;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public interface KafkaSinkConfig extends TargetDbConfig {

	int schemaType();
	String topicPrefix();
	TableNameMapper tableNameMapper();
	List<String> schemaPrefix();

	public static final String TABLE_MAPPER_DEFAULT = "solutions.a2.kafka.sink.DefaultTableNameMapper";
	public static final String TABLE_MAPPER_PARAM = "a2.table.mapper";
	public static final String TABLE_MAPPER_DOC =
			"""
			The fully-qualified class name of the class that specifies which table in which to sink the data.
			If value of thee parameter 'a2.shema.type' is set to 'debezium', the default DefaultTableNameMapper uses the 'source'.'table' field value from Sinkrecord,
			otherwise it constructs the table name as the Kafka topic name without the prefix specified by the 'a2.topic.prefix' parameter.
			If the values of the parameters 'a2.table.name.prefix' and/or 'a2.table.name.suffix' are specified, then the values of these parameters are added to the table name, respectively, either at the beginning or at the end.
			Default - """ + TABLE_MAPPER_DEFAULT;

	public static final String SCHEMA_NAME_PREFIX_PARAM = "a2.schema.name.prefix";
	public static final String SCHEMA_NAME_PREFIX_DOC =
			"""
			Prefix of existing Kafka Connect schema.
			Default - "" (Empty string - no prefix)
			""";


}
