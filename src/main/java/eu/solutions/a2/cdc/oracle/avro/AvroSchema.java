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

package eu.solutions.a2.cdc.oracle.avro;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

public class AvroSchema implements Serializable {

	private static final long serialVersionUID = 6661643576035090070L;

	final private String type;
	final private boolean optional;
	@JsonInclude(Include.NON_NULL)
	private String field;
	@JsonInclude(Include.NON_NULL)
	private String name;
	@JsonInclude(Include.NON_NULL)
	private List<AvroSchema> fields;

	private AvroSchema(String type, boolean optional) {
		this.type = type;
		this.optional = optional;
	}

	public static AvroSchema STRUCT_MANDATORY() {
		return new AvroSchema("struct", false);
	}

	public static AvroSchema INT8_MANDATORY() {
		return new AvroSchema("int8", false);
	}
	public static AvroSchema INT16_MANDATORY() {
		return new AvroSchema("int16", false);
	}
	public static AvroSchema INT32_MANDATORY() {
		return new AvroSchema("int32", false);
	}
	public static AvroSchema INT64_MANDATORY() {
		return new AvroSchema("int64", false);
	}

	public static AvroSchema FLOAT32_MANDATORY() {
		return new AvroSchema("float32", false);
	}
	public static AvroSchema FLOAT64_MANDATORY() {
		return new AvroSchema("float64", false);
	}

	public static AvroSchema STRING_MANDATORY() {
		return new AvroSchema("string", false);
	}

	public static AvroSchema BOOLEAN_MANDATORY() {
		return new AvroSchema("boolean", false);
	}

	public static AvroSchema STRUCT_OPTIONAL() {
		return new AvroSchema("struct", true);
	}

	public static AvroSchema INT8_OPTIONAL() {
		return new AvroSchema("int8", true);
	}
	public static AvroSchema INT16_OPTIONAL() {
		return new AvroSchema("int16", true);
	}
	public static AvroSchema INT32_OPTIONAL() {
		return new AvroSchema("int32", true);
	}
	public static AvroSchema INT64_OPTIONAL() {
		return new AvroSchema("int64", true);
	}

	public static AvroSchema FLOAT32_OPTIONAL() {
		return new AvroSchema("float32", true);
	}
	public static AvroSchema FLOAT64_OPTIONAL() {
		return new AvroSchema("float64", true);
	}

	public static AvroSchema STRING_OPTIONAL() {
		return new AvroSchema("string", true);
	}

	public static AvroSchema BOOLEAN_OPTIONAL() {
		return new AvroSchema("boolean", true);
	}

	public void initFields() {
		this.fields = new ArrayList<AvroSchema>();
	}

	public String getType() {
		return type;
	}

	public boolean isOptional() {
		return optional;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<AvroSchema> getFields() {
		return fields;
	}

}
