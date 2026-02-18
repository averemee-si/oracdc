package solutions.a2.cdc.oracle;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Types;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.Strings;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import oracle.jdbc.OracleResultSet;
import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.NUMBER;
import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;
import solutions.a2.cdc.oracle.data.OraTimestamp;
import solutions.a2.cdc.oracle.jmx.OraCdcInitialLoad;
import solutions.a2.cdc.oracle.runtime.config.Parameters;
import solutions.a2.utils.ExceptionUtils;

import static solutions.a2.cdc.oracle.OraRdbmsInfo.ORA_942;

/**
 * 
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraTable4InitialLoad extends OraTable4SourceConnector implements ReadMarshallable, WriteMarshallable {

	private static final Logger LOGGER = LoggerFactory.getLogger(OraTable4InitialLoad.class);
	private static final byte NULL_LENGTH_BYTE = (byte) -1;
	private static final short NULL_LENGTH_SHORT = (short) -1;
	private static final int NULL_LENGTH_INT = (int) -1;
	private static final int LOB_CHUNK_SIZE = 16384;

	private final String pdbName;
	private final Path queueDirectory;
	private final OraCdcInitialLoad metrics;
	private final String sqlSelect;
	private final String tableFqn;
	private final String kafkaTopic;
	private ChronicleQueue tableRows;
	private ExcerptAppender appender;
	private ExcerptTailer tailer;
	private int queueSize;
	private int tailerOffset;
	private OracleResultSet rsMaster;

	//TODO
	//TODO
	//TODO
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
	public OraTable4InitialLoad(final Path rootDir, final OraTable oraTable,
				final OraCdcInitialLoad metrics,
				final OraRdbmsInfo rdbmsInfo) throws IOException {
		super(oraTable.tableOwner, oraTable.tableName, oraTable.schemaType);
		LOGGER.trace("BEGIN: create OraCdcTableBuffer");
		this.pdbName = oraTable.pdbName();
		this.allColumns = oraTable.allColumns;
		this.pkColumns = oraTable.pkColumns;
		//TODO
		this.schema = oraTable.schema;
		this.keySchema = oraTable.keySchema;
		this.valueSchema = oraTable.valueSchema;
		this.sourcePartition = oraTable.sourcePartition;
		this.metrics = metrics;
		this.tableFqn = oraTable.fqn();
		this.kafkaTopic = oraTable.kafkaTopic();
		this.rdbmsInfo = rdbmsInfo;
		this.rowLevelScn = oraTable.rowLevelScn;
		// Build SQL select
		final StringBuilder sb = new StringBuilder(512);
		sb.append("select ");
		for (int i = 0; i < allColumns.size(); i++) {
			OraColumn oraColumn = allColumns.get(i);
			if (oraColumn.getColumnName().equals(OraColumn.ROWID_KEY)) {
				sb.append("ROWID as ");
				sb.append(OraColumn.ROWID_KEY);
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

		// Create ChronicleQueue
		queueDirectory = Files.createTempDirectory(rootDir, Strings.CS.replace(tableFqn, ":", "-") + ".");
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Created queue directory {} .", queueDirectory.toString());
		}
		try {
			tableRows = ChronicleQueue
				.singleBuilder(queueDirectory)
				.build();
			tailer = this.tableRows.createTailer();
			appender = this.tableRows.createAppender();
			queueSize = 0;
			tailerOffset = 0;
		} catch (Exception e) {
			LOGGER.error("Unable to create Chronicle Queue!");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(e));
			throw new IOException(e);
		}
		LOGGER.trace("END: create OraCdcTableBuffer");
	}

	@Override
	public void writeMarshallable(WireOut wire) {
		Bytes<?> bytes = wire.bytes();
		try {
			for (int i = 0; i < allColumns.size(); i++) {
				final OraColumn oraColumn = allColumns.get(i);
				final String columnName = oraColumn.getColumnName();
				switch (oraColumn.getJdbcType()) {
					case Types.DATE:
					case Types.TIMESTAMP:
						final TIMESTAMP timeStampValue = rsMaster.getTIMESTAMP(columnName);
						if (rsMaster.wasNull()) {
							bytes.writeByte(NULL_LENGTH_BYTE);
						} else {
							final byte[] baData = timeStampValue.getBytes();
							bytes.writeByte((byte) baData.length);
							bytes.write(baData);
						}
						break;
					case Types.TIMESTAMP_WITH_TIMEZONE:
						byte[] baTsTzData = null;
						if (oraColumn.isLocalTimeZone()) {
							final TIMESTAMPLTZ ltz = rsMaster.getTIMESTAMPLTZ(columnName);
							if (ltz != null) {
								baTsTzData = ltz.getBytes();
							}
						} else {
							final TIMESTAMPTZ tz = rsMaster.getTIMESTAMPTZ(columnName);
							if (tz != null) {
								baTsTzData = tz.getBytes();
							}
						}
						if (baTsTzData == null) {
							bytes.writeByte(NULL_LENGTH_BYTE);
						} else {
							bytes.writeByte((byte) baTsTzData.length);
							bytes.write(baTsTzData);
						}
						break;
					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.INTEGER:
					case Types.BIGINT:
					case Types.DECIMAL:
					case Types.NUMERIC:
						final NUMBER numberValue = rsMaster.getNUMBER(columnName);
						if (rsMaster.wasNull()) {
							bytes.writeByte(NULL_LENGTH_BYTE);
						} else {
							final byte[] baData = numberValue.getBytes();
							bytes.writeByte((byte) baData.length);
							bytes.write(baData);
						}
						break;
					case Types.FLOAT:
						byte[] baFloat = null;
						if (oraColumn.isBinaryFloatDouble()) {
							final float floatValue = rsMaster.getFloat(columnName);
							if (!rsMaster.wasNull()) {
								baFloat = (new BINARY_FLOAT(floatValue)).getBytes();
							}
						} else {
							final NUMBER floatValue = rsMaster.getNUMBER(columnName);
							if (!rsMaster.wasNull()) {
								baFloat = floatValue.getBytes();
							}
						}
						if (baFloat == null) {
							bytes.writeByte(NULL_LENGTH_BYTE);
						} else {
							bytes.writeByte((byte) baFloat.length);
							bytes.write(baFloat);
						}
						break;
					case Types.DOUBLE:
						byte[] baDouble = null;
						if (oraColumn.isBinaryFloatDouble()) {
							final double doubleValue = rsMaster.getDouble(columnName);
							if (!rsMaster.wasNull()) {
								baDouble = (new BINARY_DOUBLE(doubleValue)).getBytes();
							}
						} else {
							final NUMBER doubleValue = rsMaster.getNUMBER(columnName);
							if (!rsMaster.wasNull()) {
								baDouble = doubleValue.getBytes();
							}
						}
						if (baDouble == null) {
							bytes.writeByte(NULL_LENGTH_BYTE);
						} else {
							bytes.writeByte((byte) baDouble.length);
							bytes.write(baDouble);
						}
						break;
					case Types.BINARY:
						final byte[] rawValue = rsMaster.getBytes(columnName);
						if (rawValue == null) {
							bytes.writeShort(NULL_LENGTH_SHORT);
						} else {
							bytes.writeShort((short) rawValue.length);
							bytes.write(rawValue);
						}
						break;
					case Types.CHAR:
					case Types.VARCHAR:
						bytes.writeUtf8(rsMaster.getString(columnName));
						break;
					case Types.NCHAR:
					case Types.NVARCHAR:
						bytes.writeUtf8(rsMaster.getNString(columnName));
						break;
					case Types.ROWID:
						final RowId rowIdValue = rsMaster.getRowId(columnName);
						bytes.write8bit(rowIdValue == null ? ((String) null): rowIdValue.toString());
						break;
					case Types.CLOB:
					case Types.NCLOB:
						final Clob clobValue;
						if (oraColumn.getJdbcType() == Types.CLOB) {
							clobValue = rsMaster.getClob(columnName);
						} else {
							//NCLOB
							clobValue = rsMaster.getNClob(columnName);
						}
						if (rsMaster.wasNull() || clobValue.length() < 1) {
							bytes.writeInt(NULL_LENGTH_INT);
						} else {
							if (Integer.MAX_VALUE < clobValue.length()) {
								LOGGER.error(
										"Unable to process {} column {}({}) with length ({}) greater than Integer.MAX_VALUE ({})",
										oraColumn.getJdbcType() == Types.CLOB ? "CLOB" : "NCLOB",
										this.fqn(), columnName, clobValue.length(), Integer.MAX_VALUE);
								throw new SQLException(
										"Unable to process " +
										(oraColumn.getJdbcType() == Types.CLOB ? "CLOB" : "NCLOB") +
										"column with length " + clobValue.length() + " chars!");
							}
							try (Reader reader = clobValue.getCharacterStream()) {
								final StringBuilder sbClob = new StringBuilder((int) clobValue.length());
								int charsRead;
								final char[] data = new char[LOB_CHUNK_SIZE];
								while ((charsRead = reader.read(data, 0, data.length)) != -1) {
									sbClob.append(data, 0, charsRead);
								}
								bytes.writeInt(sbClob.length());
								bytes.writeUtf8(sbClob.toString());
							} catch (IOException ioe) {
								LOGGER.error("IO Error while processing {} column {}({})",
										oraColumn.getJdbcType() == Types.CLOB ? "CLOB" : "NCLOB",
										this.fqn(), columnName);
								LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
								throw new ConnectException(ioe);
							}
						}
						break;
					case Types.BLOB:
						final Blob blobValue = rsMaster.getBlob(columnName);
						if (rsMaster.wasNull() || blobValue.length() < 1) {
							bytes.writeInt(NULL_LENGTH_INT);
						} else {
							if (Integer.MAX_VALUE < blobValue.length()) {
								LOGGER.error(
										"Unable to process BLOB column {}({}) with length ({}) greater than Integer.MAX_VALUE ({})",
										this.fqn(), columnName, blobValue.length(), Integer.MAX_VALUE);
								throw new SQLException(
										"Unable to process BLOB column with length " + blobValue.length() + " bytes!");
							}
							try (InputStream is = blobValue.getBinaryStream();
									ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
								final byte[] data = new byte[LOB_CHUNK_SIZE];
								int bytesRead;
								while ((bytesRead = is.read(data, 0, data.length)) != -1) {
									baos.write(data, 0, bytesRead);
								}
								bytes.writeInt(baos.size());
								bytes.write(baos.toByteArray());
							} catch (IOException ioe) {
								LOGGER.error("IO Error while processing BLOB column {}({})", 
										this.fqn(), columnName);
								LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
								throw new ConnectException(ioe);
							}
						}
						break;
					case Types.SQLXML:
						final SQLXML xmlValue = rsMaster.getSQLXML(columnName);
						if (rsMaster.wasNull()) {
							bytes.writeInt(NULL_LENGTH_INT);
						} else {
							//TODO
							//TODO or better to use xmlValue.getCharacterStream() ?
							//TODO
							final String xmlAsString = xmlValue.getString();
							if (xmlAsString.length() < 1) {
								bytes.writeInt(NULL_LENGTH_INT);
							} else {
								bytes.writeInt(xmlAsString.length());
								bytes.writeUtf8(xmlAsString);
							}
						}
						break;
					default:
						throw new SQLException("Unsupported JDBC Type " + oraColumn.getJdbcType());
				}
			}
		} catch (SQLException sqle) {
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
			throw new ConnectException(sqle);
		}
	}

	@Override
	public void readMarshallable(WireIn wire) throws IORuntimeException {
		Bytes<?> raw = wire.bytes();
		try {
			for (int i = 0; i < allColumns.size(); i++) {
				final OraColumn oraColumn = allColumns.get(i);
				final String columnName = oraColumn.getColumnName();
				Object columnValue = null;
				byte sizeByte;
				switch (oraColumn.getJdbcType()) {
					case Types.DATE:
					case Types.TIMESTAMP:
						sizeByte = raw.readByte();
						if (sizeByte != NULL_LENGTH_BYTE) {
							final byte[] ba = new byte[sizeByte];
							raw.read(ba);
							columnValue = (new TIMESTAMP(ba)).timestampValue();
						}
						break;
					case Types.TIMESTAMP_WITH_TIMEZONE:
						sizeByte = raw.readByte();
						if (sizeByte != NULL_LENGTH_BYTE) {
							final byte[] ba = new byte[sizeByte];
							raw.read(ba);
							if (oraColumn.isLocalTimeZone()) {
								TIMESTAMPLTZ ltz = new TIMESTAMPLTZ(ba);
								columnValue = OraTimestamp.ISO_8601_FMT.format(ltz.offsetDateTimeValue(connTzData));
							} else {
								TIMESTAMPTZ tz = new TIMESTAMPTZ(ba);
								columnValue = OraTimestamp.ISO_8601_FMT.format(tz.offsetDateTimeValue(connTzData));
							}
						}
						break;
					case Types.TINYINT:
						final NUMBER tinyIntNumber = readNUMBER(raw);
						if (tinyIntNumber != null) {
							columnValue = tinyIntNumber.byteValue();
						}
						break;
					case Types.SMALLINT:
						final NUMBER smallIntNumber = readNUMBER(raw);
						if (smallIntNumber != null) {
							columnValue = smallIntNumber.shortValue();
						}
						break;
					case Types.INTEGER:
						final NUMBER integerNumber = readNUMBER(raw);
						if (integerNumber != null) {
							columnValue = integerNumber.intValue();
						}
						break;
					case Types.BIGINT:
						final NUMBER bigIntNumber = readNUMBER(raw);
						if (bigIntNumber != null) {
							columnValue = bigIntNumber.longValue();
						}
						break;
					case Types.DECIMAL:
						final NUMBER decimalNumber = readNUMBER(raw);
						if (decimalNumber != null) {
							final BigDecimal bdColumnValue = decimalNumber.bigDecimalValue();
							columnValue = bdColumnValue.setScale(oraColumn.getDataScale());
						}
						break;
					case Types.NUMERIC:
						sizeByte = raw.readByte();
						if (sizeByte != NULL_LENGTH_BYTE) {
							final byte[] ba = new byte[sizeByte];
							raw.read(ba);
							columnValue = ba;
						}
						break;
					case Types.FLOAT:
						if (oraColumn.isBinaryFloatDouble()) {
							sizeByte = raw.readByte();
							if (sizeByte != NULL_LENGTH_BYTE) {
								final byte[] ba = new byte[sizeByte];
								raw.read(ba);
								final BINARY_FLOAT floatNumber = new BINARY_FLOAT(ba);
								columnValue = floatNumber.floatValue();
							}
						} else {
							final NUMBER floatNumber = readNUMBER(raw);
							if (floatNumber != null) {
								columnValue = floatNumber.floatValue();
							}
						}
						break;
					case Types.DOUBLE:
						if (oraColumn.isBinaryFloatDouble()) {
							sizeByte = raw.readByte();
							if (sizeByte != NULL_LENGTH_BYTE) {
								final byte[] ba = new byte[sizeByte];
								raw.read(ba);
								final BINARY_DOUBLE doubleNumber = new BINARY_DOUBLE(ba);
								columnValue = doubleNumber.doubleValue();
							}
						} else {
							final NUMBER doubleNumber = readNUMBER(raw);
							if (doubleNumber != null) {
								columnValue = doubleNumber.doubleValue();
							}
						}
						break;
					case Types.BINARY:
						final short sizeShort = raw.readShort();
						if (sizeShort != NULL_LENGTH_SHORT) {
							final byte[] ba = new byte[sizeShort];
							raw.read(ba);
							columnValue = ba;
						}
						break;
					case Types.CHAR:
					case Types.VARCHAR:
					case Types.NCHAR:
					case Types.NVARCHAR:
						columnValue = raw.readUtf8();
						break;
					case Types.ROWID:
						columnValue = raw.read8bit();
						break;
					case Types.CLOB:
					case Types.NCLOB:
					case Types.BLOB:
					case Types.SQLXML:
						final int sizeInt = raw.readInt();
						if (sizeInt != NULL_LENGTH_INT) {
							final byte[] ba = new byte[sizeInt];
							raw.read(ba);
							columnValue = ba;
						} else {
							//For nullify BLOB/CLOB/NCLOB/XMLTYPE we need to pass zero length array
							//NULL at Kafka side is for "not touch LOB"
							columnValue = new byte[0];
						}
						break;
					default:
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

	private NUMBER readNUMBER(Bytes<?> raw) {
		final byte sizeByte = raw.readByte();
		if (sizeByte != NULL_LENGTH_BYTE) {
			final byte[] ba = new byte[sizeByte];
			raw.read(ba);
			return new NUMBER(ba);
		} else {
			return null;
		}
	}

	@Override
	public boolean usesSelfDescribingMessage() {
		// TODO Auto-generated method stub
		return ReadMarshallable.super.usesSelfDescribingMessage();
	}

	public void readTableData(
			final Long asOfScn,
			final CountDownLatch runLatch,
			final AtomicBoolean running,
			final BlockingQueue<OraTable4InitialLoad> tablesQueue,
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
				appender.writeDocument(this);
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
						"\n" +
						"=====================\n" +
						"Please run as SYSDBA:\n" +
						"\tgrant select on {} to {};\n" +
						"And restart connector!\n" +
						"=====================\n",
						tableFqn, userName);
			} else if (running.get()) {
				LOGGER.error("Error while performing initial load of {}!", tableFqn);
				LOGGER.error(ExceptionUtils.getExceptionStackTrace(sqle));
				throw new ConnectException(sqle);
			} else {
				success = false;
			}
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
		if (schemaType != Parameters.SCHEMA_TYPE_INT_SINGLE) {
			keyStruct = new Struct(keySchema);
		}
		valueStruct = new Struct(valueSchema);
		final boolean result = tailer.readDocument(this);
		tailerOffset++;
		if (result) {
			final Map<String, Object> offset = new HashMap<>();
			offset.put("ROWNUM", tailerOffset);
			SourceRecord sourceRecord = null;
			if (schemaType == Parameters.SCHEMA_TYPE_INT_DEBEZIUM) {
				final long ts = System.currentTimeMillis();
				final Struct struct = new Struct(schema);
				//TODO
				//TODO Improvement required!
				//TODO
				final Struct source = rdbmsInfo.getStruct(
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
		} else {
			return null;
		}
	}

	public String fqn() {
		return tableFqn;
	}

	public void close() {
		LOGGER.trace("Closing Cronicle Queue and deleting files.");
		if (tableRows != null) {
			tableRows.close();
		}
		tableRows = null;
		try {
			Files.walk(queueDirectory)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile)
				.forEach(File::delete);
		} catch (IOException ioe) {
			LOGGER.error("Unable to delete Cronicle Queue files.");
			LOGGER.error(ExceptionUtils.getExceptionStackTrace(ioe));
		}
	}

	public int length() {
		return queueSize;
	}

	public int offset() {
		return tailerOffset;
	}

	public Path getPath() {
		return queueDirectory;
	}

}
