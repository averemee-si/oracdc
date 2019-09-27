/**
 * Copyright (c) 2018-present, http://a2-solutions.eu
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

package eu.solutions.a2.cdc.oracle.standalone.avro;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

public class Envelope implements Serializable {

	private static final long serialVersionUID = -7464095366511208593L;

	@JsonInclude(Include.NON_NULL)
	private final AvroSchema schema;
	private final Payload payload;

	public Envelope(final AvroSchema schema, final Payload payload) {
		this.schema = schema;
		this.payload = payload;
	}

	public AvroSchema getSchema() {
		return schema;
	}

	public Payload getPayload() {
		return payload;
	}

}
