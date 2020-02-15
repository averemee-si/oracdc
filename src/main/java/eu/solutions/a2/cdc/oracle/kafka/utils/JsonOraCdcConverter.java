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

package eu.solutions.a2.cdc.oracle.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

public class JsonOraCdcConverter {

	private static final int CACHE_SIZE = 512;

	private static final ObjectWriter writer = new ObjectMapper()
//			.enable(SerializationFeature.INDENT_OUTPUT)
			.writer();
	private static final ObjectReader reader = new ObjectMapper()
			.reader();

	private static final ConcurrentHashMap<Schema, ObjectNode> fromConnectCache =
			new ConcurrentHashMap<>(CACHE_SIZE);
	private static final ConcurrentHashMap<JsonNode, Schema> toConnectCache =
			new ConcurrentHashMap<>(CACHE_SIZE);


	public static byte[] fromConnectData(Schema schema, Object value) throws JsonConverterException {
		if (schema == null && value == null) {
			return null;
		}
		JsonNode jsonValue = new JsonSchema.Envelope(asJsonSchema(schema), convertToJson(schema, value)).toJsonNode();
		try {
			return writer.writeValueAsBytes(jsonValue);
		} catch (JsonProcessingException e) {
			throw new JsonConverterException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
		}
	}

	public static SchemaAndValue toConnectData(byte[] value) throws JsonConverterException {
		if (value == null) {
			return SchemaAndValue.NULL;
		}
		JsonNode jsonValue = null;
		try {
			jsonValue = reader.readTree(value);
		} catch (IOException e) {
			throw new JsonConverterException(e);
		}
		if (!jsonValue.isObject() || jsonValue.size() != 2 || !jsonValue.has("schema") || !jsonValue.has("payload"))
			throw new JsonConverterException("JSON data must contain \"schema\" and \"payload\" fields only!");

		Schema schema = asConnectSchema(jsonValue.get("schema"));
		return new SchemaAndValue(schema,
				convertToConnect(schema, jsonValue.get("payload")));
	}

	private static ObjectNode asJsonSchema(Schema schema) throws JsonConverterException {
		if (schema == null) {
			return null;
		}
		ObjectNode cachedSchema = fromConnectCache.get(schema);
		if (cachedSchema != null) {
			return cachedSchema;
		}

		final ObjectNode jsonSchema;
		switch (schema.type()) {
		case BOOLEAN:
			jsonSchema = JsonSchema.SCHEMA_BOOLEAN.deepCopy();
			break;
		case INT8:
			jsonSchema = JsonSchema.SCHEMA_INT8.deepCopy();
			break;
		case INT16:
			jsonSchema = JsonSchema.SCHEMA_INT16.deepCopy();
			break;
		case INT32:
			jsonSchema = JsonSchema.SCHEMA_INT32.deepCopy();
			break;
		case INT64:
			jsonSchema = JsonSchema.SCHEMA_INT64.deepCopy();
			break;
		case FLOAT32:
			jsonSchema = JsonSchema.SCHEMA_FLOAT.deepCopy();
			break;
		case FLOAT64:
			jsonSchema = JsonSchema.SCHEMA_DOUBLE.deepCopy();
			break;
		case BYTES:
			jsonSchema = JsonSchema.SCHEMA_BYTES.deepCopy();
			break;
		case STRING:
			jsonSchema = JsonSchema.SCHEMA_STRING.deepCopy();
			break;
		case STRUCT:
			jsonSchema = JsonNodeFactory.instance.objectNode().put(JsonSchema.SCHEMA_TYPE, JsonSchema.TYPE_STRUCT);
			ArrayNode fields = JsonNodeFactory.instance.arrayNode();
			for (Field field : schema.fields()) {
				final ObjectNode fieldJsonSchema = asJsonSchema(field.schema()).deepCopy();
				fieldJsonSchema.put("field", field.name());
				fields.add(fieldJsonSchema);
			}
			jsonSchema.set("fields", fields);
			break;
		default:
			throw new JsonConverterException("Couldn't translate unsupported schema type " + schema + ".");
		}
		jsonSchema.put("optional", schema.isOptional());
		if (schema.name() != null)
			jsonSchema.put("name", schema.name());
		if (schema.version() != null)
			jsonSchema.put("version", schema.version());
		if (schema.doc() != null)
			jsonSchema.put("doc", schema.doc());
		if (schema.parameters() != null) {
			final ObjectNode jsonSchemaParams = JsonNodeFactory.instance.objectNode();
			for (Map.Entry<String, String> prop : schema.parameters().entrySet())
				jsonSchemaParams.put(prop.getKey(), prop.getValue());
			jsonSchema.set("parameters", jsonSchemaParams);
		}
		if (schema.defaultValue() != null) {
			jsonSchema.set("default", convertToJson(schema, schema.defaultValue()));
		}
		fromConnectCache.put(schema, jsonSchema);
		return jsonSchema;
	}

	private static Schema asConnectSchema(JsonNode jsonSchema) throws JsonConverterException {
		if (jsonSchema.isNull()) {
			return null;
		}
		Schema cached = toConnectCache.get(jsonSchema);
		if (cached != null) {
			return cached;
		}
		JsonNode schemaTypeNode = jsonSchema.get(JsonSchema.SCHEMA_TYPE);
		if (schemaTypeNode == null || !schemaTypeNode.isTextual()) {
			throw new JsonConverterException("Schema must contain 'type' field");
		}
		final SchemaBuilder builder;
		switch (schemaTypeNode.textValue()) {
			case JsonSchema.TYPE_BOOLEAN:
				builder = SchemaBuilder.bool();
				break;
			case JsonSchema.TYPE_INT8:
				builder = SchemaBuilder.int8();
				break;
			case JsonSchema.TYPE_INT16:
				builder = SchemaBuilder.int16();
				break;
			case JsonSchema.TYPE_INT32:
				builder = SchemaBuilder.int32();
				break;
			case JsonSchema.TYPE_INT64:
				builder = SchemaBuilder.int64();
				break;
			case JsonSchema.TYPE_FLOAT:
				builder = SchemaBuilder.float32();
				break;
			case JsonSchema.TYPE_DOUBLE:
				builder = SchemaBuilder.float64();
				break;
			case JsonSchema.TYPE_BYTES:
				builder = SchemaBuilder.bytes();
				break;
			case JsonSchema.TYPE_STRING:
				builder = SchemaBuilder.string();
				break;
			case JsonSchema.TYPE_STRUCT:
				builder = SchemaBuilder.struct();
				JsonNode fields = jsonSchema.get("fields");
				if (fields == null || !fields.isArray())
					throw new JsonConverterException("Struct schema's 'fields' is not an array.");
				for (JsonNode field : fields) {
					JsonNode jsonFieldName = field.get("field");
					if (jsonFieldName == null || !jsonFieldName.isTextual())
						throw new JsonConverterException("Struct schema's field name not specified properly");
					builder.field(jsonFieldName.asText(), asConnectSchema(field));
				}
				break;
			default:
				throw new JsonConverterException("Unknown or unsupported schema type: " + schemaTypeNode.textValue());
		}

		JsonNode schemaOptionalNode = jsonSchema.get("optional");
		if (schemaOptionalNode != null && schemaOptionalNode.isBoolean() && schemaOptionalNode.booleanValue())
			builder.optional();
		else
			builder.required();
		JsonNode schemaNameNode = jsonSchema.get("name");
		if (schemaNameNode != null && schemaNameNode.isTextual())
			builder.name(schemaNameNode.textValue());
		JsonNode schemaVersionNode = jsonSchema.get("version");
		if (schemaVersionNode != null && schemaVersionNode.isIntegralNumber())
			builder.version(schemaVersionNode.intValue());
		JsonNode schemaDocNode = jsonSchema.get("doc");
		if (schemaDocNode != null && schemaDocNode.isTextual())
			builder.doc(schemaDocNode.textValue());
		JsonNode schemaParamsNode = jsonSchema.get("parameters");
		if (schemaParamsNode != null && schemaParamsNode.isObject()) {
			Iterator<Map.Entry<String, JsonNode>> paramsIt = schemaParamsNode.fields();
			while (paramsIt.hasNext()) {
				Map.Entry<String, JsonNode> entry = paramsIt.next();
				JsonNode paramValue = entry.getValue();
				if (!paramValue.isTextual())
					throw new JsonConverterException("Schema parameters must have string values.");
				builder.parameter(entry.getKey(), paramValue.textValue());
			}
		}
		JsonNode schemaDefaultNode = jsonSchema.get("default");
		if (schemaDefaultNode != null)
			builder.defaultValue(convertToConnect(builder, schemaDefaultNode));
		Schema result = builder.build();
		toConnectCache.put(jsonSchema, result);
		return result;
	}

	private static JsonNode convertToJson(Schema schema, Object logicalValue) throws JsonConverterException {
		if (logicalValue == null) {
			if (schema == null)
				return null;
			if (schema.defaultValue() != null)
				return convertToJson(schema, schema.defaultValue());
			if (schema.isOptional())
				return JsonNodeFactory.instance.nullNode();
			throw new JsonConverterException("Conversion error: null value for field " + schema.name() +
				" that is required and has no default value");
		}

		Object value = logicalValue;
		if (schema != null && schema.name() != null) {
			switch (schema.name()) {
				case Decimal.LOGICAL_NAME:
					if (!(value instanceof BigDecimal))
						throw new JsonConverterException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
					value = Decimal.fromLogical(schema, (BigDecimal) value);
					break;
				case Timestamp.LOGICAL_NAME:
					if (!(value instanceof java.util.Date))
						throw new JsonConverterException("Invalid type for Timestamp, expected Date but was " + value.getClass());
					value = Timestamp.fromLogical(schema, (java.util.Date) value);
					break;
				case Date.LOGICAL_NAME:
					if (!(value instanceof java.util.Date))
						throw new JsonConverterException("Invalid type for Date, expected Date but was " + value.getClass());
					value = Date.fromLogical(schema, (java.util.Date) value);
					break;
				case Time.LOGICAL_NAME:
					if (!(value instanceof java.util.Date))
						throw new JsonConverterException("Invalid type for Time, expected Date but was " + value.getClass());
					value = Time.fromLogical(schema, (java.util.Date) value);
					break;
			}
		}

		try {
			final Schema.Type schemaType;
			if (schema == null) {
				schemaType = ConnectSchema.schemaType(value.getClass());
				if (schemaType == null)
					throw new JsonConverterException("Java class " + value.getClass() + " does not have corresponding schema type.");
			} else {
				schemaType = schema.type();
			}
			switch (schemaType) {
				case BOOLEAN:
					return JsonNodeFactory.instance.booleanNode((Boolean) value);
				case INT8:
					return JsonNodeFactory.instance.numberNode((Byte) value);
				case INT16:
					return JsonNodeFactory.instance.numberNode((Short) value);
				case INT32:
					return JsonNodeFactory.instance.numberNode((Integer) value);
				case INT64:
					return JsonNodeFactory.instance.numberNode((Long) value);
				case FLOAT32:
					return JsonNodeFactory.instance.numberNode((Float) value);
				case FLOAT64:
					return JsonNodeFactory.instance.numberNode((Double) value);
				case BYTES:
					if (value instanceof byte[])
						return JsonNodeFactory.instance.binaryNode((byte[]) value);
					else if (value instanceof ByteBuffer)
						return JsonNodeFactory.instance.binaryNode(((ByteBuffer) value).array());
					else
						throw new JsonConverterException("Invalid type for bytes type: " + value.getClass());
				case STRING:
					CharSequence charSeq = (CharSequence) value;
					return JsonNodeFactory.instance.textNode(charSeq.toString());
				case STRUCT: {
					Struct struct = (Struct) value;
					if (!struct.schema().equals(schema))
						throw new JsonConverterException("Mismatching schema.");
					ObjectNode obj = JsonNodeFactory.instance.objectNode();
					for (Field field : schema.fields()) {
						obj.set(field.name(), convertToJson(field.schema(), struct.get(field)));
					}
					return obj;
				}
				default:
					throw new JsonConverterException("Couldn't convert " + value + " to JSON.");
			}
		} catch (ClassCastException e) {
			String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
			throw new JsonConverterException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
		}
	}

	private static Object convertToConnect(Schema schema, JsonNode jsonValue) throws JsonConverterException {
		final Schema.Type schemaType;
		if (schema != null) {
			schemaType = schema.type();
			if (jsonValue.isNull()) {
				if (schema.defaultValue() != null)
					return schema.defaultValue();
				if (schema.isOptional())
					return null;
				throw new JsonConverterException("Invalid null value for required " + schemaType +  " field");
			}
		} else {
			switch (jsonValue.getNodeType()) {
				case NULL:
					return null;
				case BOOLEAN:
					schemaType = Schema.Type.BOOLEAN;
					break;
				case NUMBER:
					if (jsonValue.isIntegralNumber())
						schemaType = Schema.Type.INT64;
					else
						schemaType = Schema.Type.FLOAT64;
					break;
				case STRING:
					schemaType = Schema.Type.STRING;
					break;
				case ARRAY:
					schemaType = Schema.Type.ARRAY;
					break;
				case OBJECT:
					schemaType = Schema.Type.MAP;
					break;
				case BINARY:
			 	case MISSING:
			 	case POJO:
			 	default:
			 		schemaType = null;
			 		break;
			}
		}
		Object converted;
		switch (schemaType) {
			case BOOLEAN:
				converted = jsonValue.booleanValue();
				break;
			case INT8:
				converted = (byte) jsonValue.intValue();
				break;
			case INT16:
				converted = (short) jsonValue.intValue();
				break;
			case INT32:
				converted = jsonValue.intValue();
				break;
			case INT64:
				converted = jsonValue.longValue();
				break;
			case FLOAT32:
				converted = jsonValue.floatValue();
				break;
			case FLOAT64:
				converted = jsonValue.doubleValue();
				break;
			case BYTES:
				try {
					converted = jsonValue.binaryValue();
				} catch (IOException e) {
					throw new JsonConverterException("Invalid bytes field", e);
				}
				break;
			case STRING:
				converted = jsonValue.textValue();
				break;
			case STRUCT:
				if (!jsonValue.isObject())
					throw new JsonConverterException("Structs should be encoded as JSON objects, but found " + jsonValue.getNodeType());
				Struct struct = new Struct(schema.schema());
				for (Field field : schema.fields())
					struct.put(field, convertToConnect(field.schema(), jsonValue.get(field.name())));
				converted = struct;
				break;
			default:
				throw new JsonConverterException("Unknown or unsupported schema type: " + String.valueOf(schemaType));
		}

		if (schema != null && schema.name() != null) {
			switch (schema.name()) {
				case Decimal.LOGICAL_NAME:
					if (!(converted instanceof byte[]))
						throw new JsonConverterException("\"Invalid type for Decimal, underlying representation should be bytes but was " + converted.getClass());
					converted = Decimal.toLogical(schema, (byte[]) converted);
					break;
				case Timestamp.LOGICAL_NAME:
					if (!(converted instanceof Long))
						throw new JsonConverterException("Invalid type for Timestamp, underlying representation should be int64 but was " + converted.getClass());
					converted = Timestamp.toLogical(schema, (long) converted);
					break;
				case Date.LOGICAL_NAME:
					if (!(converted instanceof Integer))
						throw new JsonConverterException("Invalid type for Date, underlying representation should be int32 but was " + converted.getClass());
					converted = Date.toLogical(schema, (int) converted);
				break;
				case Time.LOGICAL_NAME:
					if (!(converted instanceof Integer))
						throw new JsonConverterException("Invalid type for Time, underlying representation should be int32 but was " + converted.getClass());
					converted = Time.toLogical(schema, (int) converted);
				break;
			}
		}
		return converted;
	}
	
}
