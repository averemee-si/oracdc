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

package solutions.a2.kafka.sink;

import static java.sql.Types.BINARY;
import static java.sql.Types.NUMERIC;
import static solutions.a2.kafka.ConnectorParams.SCHEMA_TYPE_INT_DEBEZIUM;
import static solutions.a2.cdc.oracle.data.JdbcTypes.getTypeName;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public abstract class OraTableDefinition {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraTableDefinition.class);

	String tableOwner;
	String tableName;
	final int schemaType;
	int version;

	List<JdbcSinkColumn> allColumns;
	Map<String, JdbcSinkColumn> pkColumns;

	OraTableDefinition(final int schemaType) {
		this.pkColumns = new LinkedHashMap<>();
		this.schemaType = schemaType;
		this.allColumns = new ArrayList<>();
		this.version = 1;
	}

	OraTableDefinition(final String tableOwner, final String tableName, final int schemaType) {
		this(schemaType);
		this.tableOwner = tableOwner;
		this.tableName = tableName;
	}

	String tableName() {
		return tableName;
	}

	String structValueAsString(final JdbcSinkColumn column, final Struct struct) {
		final var sb = new StringBuilder(0x80);
		sb
			.append("Column Type =")
			.append(getTypeName(column.getJdbcType()))
			.append(", Column Value='");
		switch (column.getJdbcType()) {
			case NUMERIC, BINARY -> {
				ByteBuffer bb = (ByteBuffer) struct.get(column.getColumnName());
				sb.append(rawToHex(bb.array()));
			}
			default -> sb.append(struct.get(column.getColumnName()));
		}
		sb.append("'");
		return sb.toString();
	}

	char getOpType(final SinkRecord record) {
		var opType = 'c';
		if (schemaType == SCHEMA_TYPE_INT_DEBEZIUM) {
			opType = ((Struct) record.value())
							.getString("op")
							.charAt(0);
			LOGGER.debug("Operation type set payload to {}.", opType);
		} else {
			//SCHEMA_TYPE_INT_KAFKA_STD
			//SCHEMA_TYPE_INT_SINGLE
			var iterator = record.headers().iterator();
			while (iterator.hasNext()) {
				var header = iterator.next();
				if ("op".equals(header.key())) {
					opType = ((String) header.value())
							.charAt(0);
					break;
				}
			}
			LOGGER.debug("Operation type set from headers to {}.", opType);
		}
		return opType;
	}


}
