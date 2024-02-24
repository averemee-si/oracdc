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

package solutions.a2.cdc.oracle.schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

import solutions.a2.cdc.oracle.OraRdbmsInfo;
import solutions.a2.cdc.oracle.OraTable4LogMiner;
import solutions.a2.cdc.oracle.data.OraCdcDefaultLobTransformationsImpl;
import solutions.a2.cdc.oracle.data.OraCdcLobTransformationsIntf;
import solutions.a2.kafka.ConnectorParams;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class FileUtils {

	public static Map<Long, OraTable4LogMiner> readDictionaryFile(
			final String fileName, Integer schemaType,
			final OraCdcLobTransformationsIntf transformLobs,
			final OraRdbmsInfo rdbmsInfo)
					throws IOException {
		InputStream is = new FileInputStream(fileName);
		Map<Long, OraTable4LogMiner> schemas =
				readDictionaryFile(is, schemaType, transformLobs, rdbmsInfo);
		is.close();
		return schemas;
	}

	public static Map<Long, OraTable4LogMiner> readDictionaryFile(
			final File file, Integer schemaType) throws IOException {
		InputStream is = new FileInputStream(file);
		// GUI call - OK for default implementation
		Map<Long, OraTable4LogMiner> schemas = readDictionaryFile(
				is, schemaType, new OraCdcDefaultLobTransformationsImpl(), null);
		is.close();
		return schemas;
	}

	private static Map<Long, OraTable4LogMiner> readDictionaryFile(
			final InputStream is, Integer schemaType,
			final OraCdcLobTransformationsIntf transformLobs,
			final OraRdbmsInfo rdbmsInfo) throws IOException {
		Map<String, Map<String, Object>> fileData = new HashMap<>();
		final ObjectReader reader = new ObjectMapper()
				.readerFor(fileData.getClass());
		fileData = reader.readValue(is);
		final Map<Long, OraTable4LogMiner> schemas = new ConcurrentHashMap<>();
		try {
			fileData.forEach((k, v) -> {
				schemas.put(
						Long.parseLong(k),
						new OraTable4LogMiner(
								v, 
								(schemaType == null) ? ConnectorParams.SCHEMA_TYPE_INT_KAFKA_STD : schemaType,
								transformLobs,
								rdbmsInfo));
			});
		} catch (Exception e) {
			throw new IOException(e);
		}
		fileData = null;
		return schemas;
	}

	public static void writeDictionaryFile(
			final Map<Long, OraTable4LogMiner> fileData,
			final String fileName) throws IOException {
		final ObjectWriter writer = new ObjectMapper()
				.enable(SerializationFeature.INDENT_OUTPUT)
				.writer();
		OutputStream os = new FileOutputStream(fileName);
		writer.writeValue(os, fileData);
		os.flush();
		os.close();
	}

}
