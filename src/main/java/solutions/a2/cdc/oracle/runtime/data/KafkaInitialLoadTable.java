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

package solutions.a2.cdc.oracle.runtime.data;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BLOB;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.SQLXML;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static solutions.a2.cdc.oracle.OraCdcStatementBase.BE;
import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_942;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.OracleResultSet;
import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.NUMBER;
import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;
import solutions.a2.cdc.OffHeapMmf;
import solutions.a2.cdc.oracle.OraCdcColumn;
import solutions.a2.cdc.oracle.OraCdcTableBase;
import solutions.a2.cdc.oracle.OraConnectionObjects;
import solutions.a2.cdc.oracle.OraRdbmsInfo;
import solutions.a2.cdc.oracle.data.OraTimestamp;
import solutions.a2.cdc.oracle.jmx.OraCdcInitialLoad;
import solutions.a2.cdc.oracle.runtime.config.Parameters;
import solutions.a2.utils.ExceptionUtils;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class KafkaInitialLoadTable {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaInitialLoadTable.class);
	private static final byte NULL_LENGTH_BYTE = (byte) -1;
	private static final short NULL_LENGTH_SHORT = (short) -1;
	private static final byte NULL_LENGTH_INT = (int) -1;
	private static final int LOB_CHUNK_SIZE = 16384;

	private final KafkaRdbmsInfoStruct kris;
	private final String pdbName;
	private final String tableOwner;
	private final String tableName;
	private final List<OraCdcColumn> allColumns;
	private final Map<String, OraCdcColumn> pkColumns;
	private final OraRdbmsInfo rdbmsInfo;
	private final String mappedFile;
	private final OraCdcInitialLoad metrics;
	private final String sqlSelect;
	private final String tableFqn;
	private final String kafkaTopic;
	private final int schemaType;
	private final boolean rowLevelScn;
	private Schema schema;
	private Schema keySchema;
	private Schema valueSchema;
	private Map<String, String> sourcePartition;
	private OffHeapMmf tableRows;
	private int queueSize;
	private int tailerOffset;
	private OracleResultSet rsMaster;
	private Struct keyStruct;
	private Struct valueStruct;
	private Connection connTzData;

	/**
	 * 
	 * Creates OraCdcTableBuffer queue
	 * 
	 * @param rootDir
	 * @param oraTable
	 * @param metrics
	 * @param rdbmsInfo
	 * @throws IOException
	 */
	public KafkaInitialLoadTable(final Path rootDir, final OraCdcTableBase oraTable,
				final OraCdcInitialLoad metrics,
				final OraRdbmsInfo rdbmsInfo) throws IOException {
		LOGGER.trace("BEGIN: create KafkaInitialLoadTable");
		pdbName = oraTable.pdb();
		tableOwner = oraTable.owner();
		tableName = oraTable.name();
		allColumns = oraTable.allColumns();
		pkColumns = oraTable.pkColumns();
		kris = ((KafkaStructDataBinder) oraTable.dataBinder()).kris;
		schema = ((KafkaStructDataBinder) oraTable.dataBinder()).schema;
		keySchema = ((KafkaStructDataBinder) oraTable.dataBinder()).keySchema;
		valueSchema = ((KafkaStructDataBinder) oraTable.dataBinder()).valueSchema;
		schemaType = ((KafkaStructDataBinder) oraTable.dataBinder()).schemaType;
		kafkaTopic = ((KafkaStructDataBinder) oraTable.dataBinder()).kafkaTopic;
		sourcePartition = rdbmsInfo.partition();
		rowLevelScn = oraTable.rowLevelScn();
		tableFqn = oraTable.fqn();
		this.metrics = metrics;
		this.rdbmsInfo = rdbmsInfo;
		// Build SQL select
		var sb = new StringBuilder(0x200);
		sb.append("select ");
		for (int i = 0; i < allColumns.size(); i++) {
			OraCdcColumn oraColumn = allColumns.get(i);
			if (oraColumn.getColumnName().equals(OraCdcColumn.ROWID_KEY)) {
				sb.append("ROWID as ");
				sb.append(OraCdcColumn.ROWID_KEY);
			} else {
				sb.append(oraColumn.getColumnName());
			}
			if (i < allColumns.size() - 1) {
				sb.append(",");
			}
		}
		sb.append(" from ");
		sb.append(tableOwner);
		sb.append(".");
		sb.append(tableName);
		if (rowLevelScn) {
			sb.append(" where ORA_ROWSCN < ?");
		}
		sqlSelect = sb.toString();
		LOGGER.debug("{} will be used for initial data load.", sqlSelect);

		// Create memory-mapped file
		mappedFile = rootDir + File.separator + Strings.CS.replace(tableFqn, ":", "-") + "." + System.nanoTime();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Created queue directory {} .", mappedFile);
		}
		try {
			tableRows = new OffHeapMmf(mappedFile, 0x400000);
			queueSize = 0;
			tailerOffset = 0;
		} catch (Exception e) {
			LOGGER.error("Unable to create Memory Mapped File!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new IOException(e);
		}
		LOGGER.trace("END: create KafkaInitialLoadTable");
	}

	private byte[] row2Bytes() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(allColumns.size() * 0x20);
		try {
			for (var i = 0; i < allColumns.size(); i++) {
				var oraColumn = allColumns.get(i);
				var columnName = oraColumn.getColumnName();
				switch (oraColumn.getJdbcType()) {
					case DATE, TIMESTAMP -> {
						var timeStampValue = rsMaster.getTIMESTAMP(columnName);
						if (rsMaster.wasNull())
							baos.write(NULL_LENGTH_BYTE);
						else {
							final byte[] baData = timeStampValue.getBytes();
							baos.write((byte) baData.length);
							baos.write(baData);
						}
					}
					case TIMESTAMP_WITH_TIMEZONE -> {
						byte[] baTsTzData = null;
						if (oraColumn.localTimeZone()) {
							var ltz = rsMaster.getTIMESTAMPLTZ(columnName);
							if (ltz != null)
								baTsTzData = ltz.getBytes();
						} else {
							var tz = rsMaster.getTIMESTAMPTZ(columnName);
							if (tz != null)
								baTsTzData = tz.getBytes();
						}
						if (baTsTzData == null)
							baos.write(NULL_LENGTH_BYTE);
						else {
							baos.write((byte) baTsTzData.length);
							baos.write(baTsTzData);
						}
					}
					case TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, NUMERIC -> {
						var numberValue = rsMaster.getNUMBER(columnName);
						if (rsMaster.wasNull())
							baos.write(NULL_LENGTH_BYTE);
						else {
							final byte[] baData = numberValue.getBytes();
							baos.write((byte) baData.length);
							baos.write(baData);
						}
					}
					case FLOAT -> {
						byte[] baFloat = null;
						if (oraColumn.IEEE754()) {
							var floatValue = rsMaster.getFloat(columnName);
							if (!rsMaster.wasNull())
								baFloat = (new BINARY_FLOAT(floatValue)).getBytes();
						} else {
							final NUMBER floatValue = rsMaster.getNUMBER(columnName);
							if (!rsMaster.wasNull())
								baFloat = floatValue.getBytes();
						}
						if (baFloat == null)
							baos.write(NULL_LENGTH_BYTE);
						else {
							baos.write((byte) baFloat.length);
							baos.write(baFloat);
						}
					}
					case DOUBLE -> {
						byte[] baDouble = null;
						if (oraColumn.IEEE754()) {
							var doubleValue = rsMaster.getDouble(columnName);
							if (!rsMaster.wasNull())
								baDouble = (new BINARY_DOUBLE(doubleValue)).getBytes();
						} else {
							final NUMBER doubleValue = rsMaster.getNUMBER(columnName);
							if (!rsMaster.wasNull())
								baDouble = doubleValue.getBytes();
						}
						if (baDouble == null)
							baos.write(NULL_LENGTH_BYTE);
						else {
							baos.write((byte) baDouble.length);
							baos.write(baDouble);
						}
					}
					case BINARY -> {
						var rawValue = rsMaster.getRAW(columnName);
						if (rawValue == null)
							putU16(baos, NULL_LENGTH_SHORT);
						else {
							var ba = rawValue.getBytes();
							putU16(baos, (short) ba.length);
							baos.write(ba);
						}
					}
					case CHAR, VARCHAR, NCHAR, NVARCHAR -> {
						var charValue = rsMaster.getCHAR(columnName);
						if (charValue == null)
							putU16(baos, NULL_LENGTH_SHORT);
						else {
							var ba = charValue.getBytes();
							putU16(baos, (short) ba.length);
							baos.write(ba);
						}
					}
					case ROWID -> {
						var ba = rsMaster.getROWID(columnName).getBytes();
						baos.write((byte) ba.length);
						baos.write(ba);
					}
					case CLOB, NCLOB -> {
						Clob clobValue = oraColumn.getJdbcType() == CLOB
								? rsMaster.getClob(columnName)
								: rsMaster.getNClob(columnName);
						if (rsMaster.wasNull() || clobValue.length() < 1)
							putU32(baos, NULL_LENGTH_INT);
						else {
							if (Integer.MAX_VALUE < clobValue.length()) {
								LOGGER.error(
										"""
										
										=====================
										Unable to process {} column {}({}) with length ({}) greater than Integer.MAX_VALUE ({})
										=====================
										
										""", oraColumn.getJdbcType() == Types.CLOB ? "CLOB" : "NCLOB",
										tableFqn, columnName, clobValue.length(), Integer.MAX_VALUE);
								throw new IOException(
										"Unable to process " +
										(oraColumn.getJdbcType() == Types.CLOB ? "CLOB" : "NCLOB") +
										"column with length " + clobValue.length() + " chars!");
							}
							try (var reader = clobValue.getCharacterStream()) {
								var baosClob = new ByteArrayOutputStream((int) (clobValue.length() * 2));
								var charsRead = 0;
								var data = new char[LOB_CHUNK_SIZE];
								while ((charsRead = reader.read(data, 0, data.length)) != -1) {
									var charBuffer = CharBuffer.wrap(data, 0, charsRead);
									var byteBuffer = UTF_8.encode(charBuffer);
									baosClob.write(byteBuffer.array());
								}
								putU32(baos, baosClob.size());
								baos.write(baosClob.toByteArray());
								baosClob = null;
								clobValue = null;
							} catch (IOException ioe) {
								LOGGER.error(
										"""
										
										=====================
										IO Error while processing {} column {}({})
										{}
										=====================
										
										""",  oraColumn.getJdbcType() == Types.CLOB ? "CLOB" : "NCLOB",
										tableFqn, columnName, ExceptionUtils.getExceptionStackTrace(ioe));
								throw ioe;
							}
						}
					}
					case BLOB -> {
						var blobValue = rsMaster.getBlob(columnName);
						if (rsMaster.wasNull() || blobValue.length() < 1)
							putU32(baos, NULL_LENGTH_INT);
						else {
							if (Integer.MAX_VALUE < blobValue.length()) {
								LOGGER.error(
										"""
										
										=====================
										Unable to process BLOB column {}({}) with length ({}) greater than Integer.MAX_VALUE ({})
										=====================
										
										""", tableFqn, columnName, blobValue.length(), Integer.MAX_VALUE);
								throw new IOException(
										"Unable to process BLOB column with length " + blobValue.length() + " bytes!");
							}
							try (var is = blobValue.getBinaryStream()) {
								var baosBlob = new ByteArrayOutputStream();
								var data = new byte[LOB_CHUNK_SIZE];
								int bytesRead;
								while ((bytesRead = is.read(data, 0, data.length)) != -1) {
									baosBlob.write(data, 0, bytesRead);
								}
								putU32(baos, baosBlob.size());
								baos.write(baosBlob.toByteArray());
								baosBlob = null;
								blobValue = null;
							} catch (IOException ioe) {
								LOGGER.error(
										"""
										
										=====================
										IO Error while processing column {}({})
										{}
										=====================
										
										""", tableFqn, columnName, ExceptionUtils.getExceptionStackTrace(ioe));
								throw ioe;
							}
						}
					}
					case SQLXML -> {
						var xmlValue = rsMaster.getSQLXML(columnName);
						if (rsMaster.wasNull())
							putU32(baos, NULL_LENGTH_INT);
						else {
							var ba = xmlValue.getString().getBytes();
							putU32(baos, ba.length);
							baos.write(ba);
						}
					}
					default ->
						throw new SQLException("Unsupported JDBC Type " + oraColumn.getJdbcType());
				}
			}
		} catch (SQLException | IOException e) {
			throw new ConnectException(e);
		}
		var aligned = (baos.size() + 3) & 0xFFFFFFFC;
		switch (aligned - baos.size()) {
			case 3 -> {
				baos.write(0); baos.write(0); baos.write(0);
			}
			case 2 -> {
				baos.write(0); baos.write(0);
			}
			case 1 -> {
				baos.write(0);
			}
		}
		return baos.toByteArray();
	}

	public void bytes2Row(final byte[] data) {
		try {
			var pos = 0;
			for (int i = 0; i < allColumns.size(); i++) {
				var oraColumn = allColumns.get(i);
				var columnName = oraColumn.getColumnName();
				Object columnValue = null;
				byte sizeByte;
				switch (oraColumn.getJdbcType()) {
					case DATE, TIMESTAMP -> {
						sizeByte = data[pos++];
						if (sizeByte != NULL_LENGTH_BYTE) {
							var next = pos + sizeByte;
							columnValue = (new TIMESTAMP(Arrays.copyOfRange(data, pos, next))).timestampValue();
							pos = next;
						}
					}
					case TIMESTAMP_WITH_TIMEZONE -> {
						sizeByte = data[pos++];
						if (sizeByte != NULL_LENGTH_BYTE) {
							var next = pos + sizeByte;
							if (oraColumn.localTimeZone()) {
								var ltz = new TIMESTAMPLTZ(Arrays.copyOfRange(data, pos, next));
								columnValue = OraTimestamp.ISO_8601_FMT.format(ltz.offsetDateTimeValue(connTzData));
							} else {
								var tz = new TIMESTAMPTZ(Arrays.copyOfRange(data, pos, next));
								columnValue = OraTimestamp.ISO_8601_FMT.format(tz.offsetDateTimeValue(connTzData));
							}
							pos = next;
						}
					}
					case TINYINT -> {
						sizeByte = data[pos++];
						if (sizeByte != NULL_LENGTH_BYTE) {
							var next = pos + sizeByte;
							columnValue = new NUMBER(Arrays.copyOfRange(data, pos, next)).byteValue();
							pos = next;
						}
					}
					case SMALLINT -> {
						sizeByte = data[pos++];
						if (sizeByte != NULL_LENGTH_BYTE) {
							var next = pos + sizeByte;
							columnValue = new NUMBER(Arrays.copyOfRange(data, pos, next)).shortValue();
							pos = next;
						}
					}
					case INTEGER -> {
						sizeByte = data[pos++];
						if (sizeByte != NULL_LENGTH_BYTE) {
							var next = pos + sizeByte;
							columnValue = new NUMBER(Arrays.copyOfRange(data, pos, next)).intValue();
							pos = next;
						}
					}
					case BIGINT -> {
						sizeByte = data[pos++];
						if (sizeByte != NULL_LENGTH_BYTE) {
							var next = pos + sizeByte;
							columnValue = new NUMBER(Arrays.copyOfRange(data, pos, next)).longValue();
							pos = next;
						}
					}
					case DECIMAL -> {
						sizeByte = data[pos++];
						if (sizeByte != NULL_LENGTH_BYTE) {
							var next = pos + sizeByte;
							var bdColumnValue = new NUMBER(Arrays.copyOfRange(data, pos, next)).bigDecimalValue();
							columnValue = bdColumnValue.setScale(oraColumn.getDataScale());
							pos = next;
						}
					}
					case NUMERIC -> {
						sizeByte = data[pos++];
						if (sizeByte != NULL_LENGTH_BYTE) {
							var next = pos + sizeByte;
							columnValue = Arrays.copyOfRange(data, pos, next);
							pos = next;
						}
					}
					case FLOAT -> {
						sizeByte = data[pos++];
						if (sizeByte != NULL_LENGTH_BYTE) {
							var next = pos + sizeByte;
							columnValue = oraColumn.IEEE754()
									? new BINARY_FLOAT(Arrays.copyOfRange(data, pos, next)).floatValue()
									: new NUMBER(Arrays.copyOfRange(data, pos, next)).floatValue();
							pos = next;
						}
					}
					case DOUBLE -> {
						sizeByte = data[pos++];
						if (sizeByte != NULL_LENGTH_BYTE) {
							var next = pos + sizeByte;
							columnValue = oraColumn.IEEE754()
									? new BINARY_FLOAT(Arrays.copyOfRange(data, pos, next)).doubleValue()
									: new NUMBER(Arrays.copyOfRange(data, pos, next)).doubleValue();
							pos = next;
						}
					}
					case BINARY -> {
						var sizeShort = BE.getU16(data, pos);
						pos += Short.BYTES;
						if (sizeShort != NULL_LENGTH_SHORT) {
							var next = pos + sizeShort;
							columnValue = Arrays.copyOfRange(data, pos, next);
							pos = next;
						}
					}
					case CHAR, VARCHAR, NCHAR, NVARCHAR -> {
						var sizeShort = BE.getU16(data, pos);
						pos += Short.BYTES;
						if (sizeShort != NULL_LENGTH_SHORT) {
							var next = pos + sizeShort;
							columnValue = new String(Arrays.copyOfRange(data, pos, next));
							pos = next;
						}
					}
					case ROWID -> {
						sizeByte = data[pos++];
						var next = pos + sizeByte;
						columnValue = new String(Arrays.copyOfRange(data, pos, next));
						pos = next;
					}
					case BLOB -> {
						var sizeInt = BE.getU32(data, pos);
						pos += Integer.BYTES;
						if (sizeInt != NULL_LENGTH_INT) {
							var next = pos + sizeInt;
							columnValue = Arrays.copyOfRange(data, pos, next);
							pos = next;
						}
					}
					case CLOB, NCLOB, SQLXML -> {
						var sizeInt = BE.getU32(data, pos);
						pos += Integer.BYTES;
						if (sizeInt != NULL_LENGTH_INT) {
							var next = pos + sizeInt;
							columnValue = new String(Arrays.copyOfRange(data, pos, next));
							pos = next;
						}
					}
					default ->
						throw new SQLException("Unsupported JDBC Type " +
								oraColumn.getJdbcType() + " for column " + columnName);
				}
				if (keyStruct != null && pkColumns.containsKey(columnName)) {
					try {
						keyStruct.put(columnName, columnValue);
					} catch (DataException de) {
						LOGGER.error("Data exception while performing initial load for table {}, COLUMN={}, VALUE={}",
								this.tableFqn, columnName, columnValue);
						LOGGER.error("Primary key column(s) for table {}:", this.tableFqn);
						pkColumns.forEach((k, v) -> {
							LOGGER.error("\t" + v.getColumnName());
						});
						LOGGER.error("Key schema elements for table {}:", this.tableFqn);
						keySchema.fields().forEach((f) -> {
							LOGGER.error("\t" + f.name());
						});
						throw new DataException(de);
					}
				}
				// Don't process PK again in case of SCHEMA_TYPE_INT_KAFKA_STD
				if ((schemaType == Parameters.SCHEMA_TYPE_INT_KAFKA_STD && !pkColumns.containsKey(columnName)) ||
						schemaType == Parameters.SCHEMA_TYPE_INT_SINGLE ||
						schemaType == Parameters.SCHEMA_TYPE_INT_DEBEZIUM) {
					valueStruct.put(columnName, columnValue);
				}
			}
		} catch (SQLException sqle) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			throw new ConnectException(sqle);
		}
	}

	public void readTableData(
			final Long asOfScn,
			final CountDownLatch runLatch,
			final AtomicBoolean running,
			final BlockingQueue<KafkaInitialLoadTable> tablesQueue,
			final OraConnectionObjects oraConnections) {
		metrics.startSelectTable(tableFqn);
		boolean success = false;
		String userName = null;
		try (Connection connection = oraConnections.getConnection()) {
			connTzData = connection;
			userName = connection.getSchema();
			if (pdbName != null) {
				Statement alterSession = connection.createStatement();
				alterSession.execute("alter session set CONTAINER=" + pdbName);
				alterSession.close();
				alterSession = null;
			}
			PreparedStatement statement = connection.prepareStatement(sqlSelect,
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			if (rowLevelScn) {
				statement.setLong(1, asOfScn);
				LOGGER.info("Table {} initial load (read phase) up to SCN {} started.", tableFqn, asOfScn);
			} else {
				LOGGER.info("Table {} (DEPENDENCY='DISABLED') initial load (read phase) started.", tableFqn);
			}
			final long startTime = System.nanoTime();
			rsMaster = (OracleResultSet) statement.executeQuery();
			while (rsMaster.next() && running.get()) {
				tableRows.writeRecord(row2Bytes());
				queueSize++;
			}
			if (running.get()) {
				rsMaster.close();
				rsMaster = null;
				statement.close();
				statement = null;
				metrics.finishSelectTable(tableFqn,
						queueSize, queueSize * this.allColumns.size(), (System.nanoTime() - startTime));
				success = true;
				LOGGER.info("Table {} initial load (read phase) completed. {} rows read.", tableFqn, queueSize);
				if (pdbName != null) {
					Statement alterSession = connection.createStatement();
					alterSession.execute("alter session set CONTAINER=" + rdbmsInfo.getPdbName());
					alterSession.close();
					alterSession = null;
				}
			}
		} catch (SQLException sqle) {
			if (sqle.getErrorCode() == ORA_942) {
				LOGGER.error(
						"""
						
						=====================
						Please run as SYSDBA:
							grant select on {} to {};
						And restart connector!
						=====================
						
						""",
						tableFqn, userName);
			} else if (running.get()) {
				LOGGER.error("Error while performing initial load of {}!", tableFqn);
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
				throw new ConnectException(sqle);
			} else {
				success = false;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (success) {
			try {
				tablesQueue.put(this);
			} catch (InterruptedException ie) {
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(ie));
				throw new ConnectException(ie);
			}
		} else {
			LOGGER.warn("Incomplete initial load data for {} are removed.", tableFqn);
			this.close();
		}
		runLatch.countDown();
	}

	public SourceRecord getSourceRecord() {
		final long startNanos = System.nanoTime();
		var data = tableRows.readNext(); 
		if (data != null) {
			if (schemaType != Parameters.SCHEMA_TYPE_INT_SINGLE) {
				keyStruct = new Struct(keySchema);
			}
			valueStruct = new Struct(valueSchema);
			bytes2Row(data);

			final Map<String, Object> offset = new HashMap<>();
			offset.put("ROWNUM", tailerOffset);
			SourceRecord sourceRecord = null;
			if (schemaType == Parameters.SCHEMA_TYPE_INT_DEBEZIUM) {
				final long ts = System.currentTimeMillis();
				final Struct struct = new Struct(schema);
				//TODO
				//TODO Improvement required!
				//TODO
				final Struct source = kris.getStruct(
						this.tableFqn,
						pdbName, tableOwner, tableName,
						0L, ts,
						"", 0L, "");
				struct.put("source", source);
				struct.put("before", keyStruct);
				struct.put("after", valueStruct);
				struct.put("op", "c");
				struct.put("ts_ms", ts);
				sourceRecord = new SourceRecord(
						sourcePartition,
						offset,
						kafkaTopic,
						schema,
						struct);
			} else {
				if (schemaType == Parameters.SCHEMA_TYPE_INT_KAFKA_STD) {
					sourceRecord = new SourceRecord(
							sourcePartition,
							offset,
							kafkaTopic,
							keySchema,
							keyStruct,
							valueSchema,
							valueStruct);
				} else if (schemaType == Parameters.SCHEMA_TYPE_INT_SINGLE) {
					sourceRecord = new SourceRecord(
							sourcePartition,
							offset,
							kafkaTopic,
							valueSchema,
							valueStruct);
				}
				sourceRecord.headers().addString("op", "c");
			}
			metrics.addSendInfo(allColumns.size(), System.nanoTime() - startNanos);
			return sourceRecord;
		} else
			return null;
	}

	public String fqn() {
		return tableFqn;
	}

	public void close() {
		LOGGER.trace("Closing and deleting the memory mapped file {}.", mappedFile);
		try {
			if (tableRows != null) {
				tableRows.close();
			}
			tableRows = null;
		} catch (IOException ioe) {
			LOGGER.error(
					"""
					
					=====================
					Unable to close memory mapped files!
					{}
					=====================
					
					""", ExceptionUtils.getExceptionStackTrace(ioe));
		}
	}

	private static void putU16(ByteArrayOutputStream baos, final short u16) {
		baos.write((byte)(u16 >>> 8));
		baos.write((byte)u16);
	}

	private static void putU32(ByteArrayOutputStream baos, final int u32) {
		baos.write((byte)(u32 >>> 24));
		baos.write((byte)(u32 >>> 16));
		baos.write((byte)(u32 >>> 8));
		baos.write((byte)u32);
	}

}
