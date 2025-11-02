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

package solutions.a2.cdc.oracle;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.charset.StandardCharsets.UTF_16;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.TINYINT;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.BIGINT;
import static java.sql.Types.FLOAT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.DATE;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.SQLXML;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static oracle.jdbc.OracleTypes.BINARY_FLOAT;
import static oracle.jdbc.OracleTypes.BINARY_DOUBLE;
import static oracle.jdbc.OracleTypes.VECTOR;
import static oracle.sql.NUMBER.toBigDecimal;
import static oracle.sql.NUMBER.toDouble;
import static oracle.sql.NUMBER.toFloat;
import static solutions.a2.cdc.oracle.OraColumn.JAVA_SQL_TYPE_INTERVALDS_STRING;
import static solutions.a2.cdc.oracle.OraColumn.JAVA_SQL_TYPE_INTERVALYM_STRING;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toByte;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toInt;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toLong;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toShort;
import static solutions.a2.oracle.utils.BinaryUtils.getU16BE;
import static solutions.a2.oracle.utils.BinaryUtils.getU32BE;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.jdbc.driver.VectorData;
import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.json.OracleJsonException;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonParser;
import solutions.a2.cdc.oracle.data.OraBlob;
import solutions.a2.cdc.oracle.data.OraClob;
import solutions.a2.cdc.oracle.data.OraJson;
import solutions.a2.cdc.oracle.data.OraNClob;
import solutions.a2.cdc.oracle.data.OraVector;
import solutions.a2.cdc.oracle.data.OraXml;
import solutions.a2.cdc.oracle.internals.OraCdcTdeColumnDecrypter;
import solutions.a2.oracle.internals.LobId;
import solutions.a2.oracle.internals.LobLocator;
import solutions.a2.oracle.jdbc.types.IntervalDayToSecond;
import solutions.a2.oracle.jdbc.types.IntervalYearToMonth;
import solutions.a2.oracle.jdbc.types.OracleDate;
import solutions.a2.oracle.jdbc.types.OracleTimestamp;
import solutions.a2.oracle.jdbc.types.TimestampWithTimeZone;

/**
 *
 * Objects for converting Oracle Database internal byte representation
 * to Java/Kafka Connect data types.
 * 
 *  For more information about Oracle NUMBER format:
 *	   <a href="https://gotodba.com/2015/03/24/how-are-numbers-saved-in-oracle/">How are Numbers Saved in Oracle?</a>
 *	   <a href="https://www.orafaq.com/wiki/Number">Number</a>
 *     <a href="https://support.oracle.com/rs?type=doc&id=1031902.6">How Does Oracle Store Internal Numeric Data? (Doc ID 1031902.6)</a>
 *     <a href="https://docs.oracle.com/en/database/oracle/oracle-database/26/jajdb/oracle/sql/NUMBER.html">Class NUMBER</a>
 *
 *  For more information about Oracle DATE, TIMESTAMP* format:
 *     <a href="https://docs.oracle.com/en/database/oracle/oracle-database/26/jajdb/oracle/sql/DATE.html">DATE</a>
 *     <a href="https://docs.oracle.com/en/database/oracle/oracle-database/26/jajdb/oracle/sql/TIMESTAMP.html">TIMESTAMP</a>
 *     <a href="https://docs.oracle.com/en/database/oracle/oracle-database/26/jajdb/oracle/sql/TIMESTAMPLTZ.html">TIMESTAMP WITH LOCAL TIMEZONE</a>
 *     <a href="https://docs.oracle.com/en/database/oracle/oracle-database/26/jajdb/oracle/sql/TIMESTAMPTZ.html">TIMESTAMP WITH TIMEZONE</a>
 *     <a href="https://support.oracle.com/rs?type=doc&id=69028.1">How does Oracle store the DATE datatype internally? (Doc ID 69028.1)</a>
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcDecoderFactory {

	private static final Map<Integer, OraCdcDecoder> decoders = new HashMap<>();
	private static final Map<String, String> oraToJava = new HashMap<>();

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcDecoderFactory.class);
	
	static OraCdcDecoder get() {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				return raw;
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return Arrays.copyOfRange(raw, off, off + len);
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
				final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "extended size RAW");
				if (lobIds.contains(ll.lid()))
					return cqTrans.getLob(ll);
				else if (ll.dataInRow())
					return Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len);
				else
					return throwLobNotFound(ll, "extended size RAW");
			}
		};
	}

	static OraCdcDecoder get(final Schema schema) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				final var struct = new Struct(schema);
					struct.put("V", raw);
					return struct;
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				final var struct = new Struct(schema);
					struct.put("V", Arrays.copyOfRange(raw, off, off + len));
					return struct;
			}
		};
	}

	static OraCdcDecoder get(final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
				return plaintext;
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
				final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
				final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final var ll = new LobLocator(plaintext, 0, plaintext.length);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size RAW");
				if (lobIds.contains(ll.lid()))
					return decrypter.decrypt(cqTrans.getLob(ll), salted);
				else if (ll.dataInRow())
					return Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length);
				else
					return throwLobNotFound(ll, "enc extended size RAW");
			}
		};
	}

	static OraCdcDecoder get(final Schema schema, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				final var struct = new Struct(schema);
					final var plaintext = decrypter.decrypt(raw, salted);
					struct.put("V", plaintext);
					return struct;
				}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				final var struct = new Struct(schema);
					final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
					struct.put("V", plaintext);
					return struct;
				}
			};
	}

	static OraCdcDecoder get(final int jdbcType) {
		return decoders.get(jdbcType);
	}

	static OraCdcDecoder get(final Schema schema, final int jdbcType) {
		switch (jdbcType) {
			case BOOLEAN -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var struct = new Struct(schema);
						struct.put("V", toBoolean(raw, off, len));
						return struct;
					}
				};
			}
			case TINYINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var struct = new Struct(schema);
						struct.put("V", toByte(raw, off, len));
						return struct;
					}
				};
			}
			case SMALLINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var struct = new Struct(schema);
						struct.put("V", toShort(raw, off, len));
						return struct;
					}
				};
			}
			case INTEGER -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var struct = new Struct(schema);
						struct.put("V", toInt(raw, off, len));
						return struct;
					}
				};
			}
			case BIGINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var struct = new Struct(schema);
						struct.put("V", toLong(raw, off, len));
						return struct;
					}
				};
			}
			case BINARY_FLOAT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw) throws SQLException {
						try {
							final var bf = new BINARY_FLOAT(raw);
							final var struct = new Struct(schema);
							struct.put("V", bf.floatValue());
							return struct;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_FLOAT encrypted data " + rawToHex(raw), e);
						}
					}
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							final var bf = new BINARY_FLOAT(Arrays.copyOfRange(raw, off, off + len));
							final var struct = new Struct(schema);
							struct.put("V", bf.floatValue());
							return struct;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_FLOAT encrypted data " + rawToHex(raw), e);
						}
					}
				};
			}
			case BINARY_DOUBLE -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw) throws SQLException {
						try {
							final var struct = new Struct(schema);
							final var bd = new BINARY_DOUBLE(raw);
							struct.put("V", bd.doubleValue());
							return struct;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_DOUBLE encrypted data " + rawToHex(raw), e);
						}
					}
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							final var struct = new Struct(schema);
							final var bd = new BINARY_DOUBLE(Arrays.copyOfRange(raw, off, off + len));
							struct.put("V", bd.doubleValue());
							return struct;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_DOUBLE encrypted data " + rawToHex(raw), e);
						}
					}
				};
			}
			// Special cases for extended size VARCHAR2/NVARCHAR2/RAW
			case LONGNVARCHAR, LONGVARCHAR -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
						final var struct = new Struct(schema);
						final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
						final var ll = new LobLocator(raw, off, len);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size NVARCHAR2/VARCHAR2");
						if (lobIds.contains(ll.lid()))
							struct.put("V", new String(cqTrans.getLob(ll), UTF_16));
						else if (ll.dataInRow()) 
							struct.put("V", new String(raw, off + len - ll.dataLength(), off + len, UTF_16));
						else
							throwLobNotFound(ll, "enc extended size NVARCHAR2/VARCHAR2");
						return struct;
					}
				};
			}
			case LONGVARBINARY -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
						final var struct = new Struct(schema);
						final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
						final var ll = new LobLocator(raw, off, len);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size RAW");
						if (lobIds.contains(ll.lid()))
							struct.put("V", cqTrans.getLob(ll));
						else if (ll.dataInRow())
							struct.put("V", Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
						else
							throwLobNotFound(ll, "enc extended size RAW");
						return struct;
					}
				};
			}
		}
		return null;
	}

	static OraCdcDecoder get(final int jdbcType, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		switch (jdbcType) {
			case TINYINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						return toByte(plaintext, 0, plaintext.length);
					}
				};
			}
			case SMALLINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						return toShort(plaintext, 0, plaintext.length);
					}
				};
			}
			case INTEGER -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						return toInt(plaintext, 0, plaintext.length);
					}
				};
			}
			case BIGINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						return toLong(plaintext, 0, plaintext.length);
					}
				};
			}
			case FLOAT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						return toFloat(plaintext);
					}
				};
			}
			case BINARY_FLOAT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, 0, raw.length), salted);
						try {
							var bf = new BINARY_FLOAT(plaintext);
							return bf.floatValue();
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_FLOAT encrypted data " + rawToHex(raw), e);
						}
					}
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						try {
							var bf = new BINARY_FLOAT(plaintext);
							return bf.floatValue();
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_FLOAT encrypted data " + rawToHex(raw), e);
						}
					}
				};
			}
			case DOUBLE -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						return toDouble(plaintext);
					}
				};
			}
			case BINARY_DOUBLE -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, 0, raw.length), salted);
						try {
							var bd = new BINARY_DOUBLE(plaintext);
							return bd.doubleValue();
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_DOUBLE encrypted data " + rawToHex(raw), e);
						}
					}
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						try {
							var bd = new BINARY_DOUBLE(plaintext);
							return bd.doubleValue();
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_DOUBLE encrypted data " + rawToHex(raw), e);
						}
					}
				};
			}
			case DATE -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						try {
							return OracleDate.toTimestamp(plaintext);
						} catch (DateTimeException dte) {
							throw new SQLException(dte);
						}
					}
				};
			}
			case TIMESTAMP -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						try {
							return OracleTimestamp.toTimestamp(plaintext);
						} catch (DateTimeException dte) {
							throw new SQLException(dte);
						}
					}
				};
			}
			case JAVA_SQL_TYPE_INTERVALDS_STRING -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						return IntervalDayToSecond.toString(plaintext);
					}
				};
			}
			case JAVA_SQL_TYPE_INTERVALYM_STRING -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						return IntervalYearToMonth.toString(plaintext);
					}
				};
			}
			case BLOB -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc BLOB");
						if (lobIds.contains(ll.lid()))
							return oraBlob(decrypter.decrypt(cqTrans.getLob(ll), salted));
						else if (ll.dataInRow())
							return oraBlob(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return throwLobNotFound(ll, "enc BLOB");
					}
				};
			}
			case CLOB -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc CLOB");
						if (lobIds.contains(ll.lid()))
							return oraClob(decrypter.decrypt(cqTrans.getLob(ll), salted));
						else if (ll.dataInRow())
							return oraClob(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return throwLobNotFound(ll, "enc CLOB");
					}
				};
			}
			case NCLOB -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc NCLOB");
						if (lobIds.contains(ll.lid()))
							return oraNClob(decrypter.decrypt(cqTrans.getLob(ll), salted));
						else if (ll.dataInRow())
							return oraNClob(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return throwLobNotFound(ll, "enc NCLOB");
					}
				};
			}
			case SQLXML -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc SQLXML");
						if (lobIds.contains(ll.lid()))
							return oraXml(decrypter.decrypt(cqTrans.getLob(ll), salted), ll.type() == CLOB);
						else if (ll.dataInRow())
							return oraXml(
									Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length),
									ll.type() == CLOB);
						else
							return throwLobNotFound(ll, "enc SQLXML");
					}
				};
			}
			case VECTOR -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc VECTOR");
						if (lobIds.contains(ll.lid()))
							return oraVector(decrypter.decrypt(cqTrans.getLob(ll), salted));
						else if (ll.dataInRow())
							return oraVector(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return throwLobNotFound(ll, "enc VECTOR");
					}
				};
			}
			// Special cases for extended size VARCHAR2/NVARCHAR2/RAW
			case LONGNVARCHAR -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size NVARCHAR2");
						if (lobIds.contains(ll.lid()))
							return new String(decrypter.decrypt(cqTrans.getLob(ll), salted), UTF_16);
						else if (ll.dataInRow()) 
							return new String(plaintext, plaintext.length - ll.dataLength(), plaintext.length, UTF_16);
						else
							return throwLobNotFound(ll, "enc extended size NVARCHAR2");
					}
				};
			}
			case LONGVARCHAR -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size VARCHAR2");
						if (lobIds.contains(ll.lid()))
							return new String(decrypter.decrypt(cqTrans.getLob(ll), salted), UTF_16);
						else if (ll.dataInRow())
							return new String(plaintext, plaintext.length - ll.dataLength(), plaintext.length, UTF_16);
						else
							return throwLobNotFound(ll, "enc extended size VARCHAR2");
					}
				};
			}
			case LONGVARBINARY -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size RAW");
						if (lobIds.contains(ll.lid()))
							return decrypter.decrypt(cqTrans.getLob(ll), salted);
						else if (ll.dataInRow())
							return Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length);
						else
							return throwLobNotFound(ll, "enc extended size RAW");
					}
				};
			}
		}
		return null;
	}

	static OraCdcDecoder get(final Schema schema, final int jdbcType, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		switch (jdbcType) {
			case TINYINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var struct = new Struct(schema);
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						struct.put("V", toByte(plaintext, 0, plaintext.length));
						return struct;
					}
				};						
			}
			case SMALLINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var struct = new Struct(schema);
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						struct.put("V", toShort(plaintext, 0, plaintext.length));
						return struct;
					}
				};						
			}
			case INTEGER -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var struct = new Struct(schema);
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						struct.put("V", toInt(plaintext, 0, plaintext.length));
						return struct;
					}
				};						
			}
			case BIGINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var struct = new Struct(schema);
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						struct.put("V", toLong(plaintext, 0, plaintext.length));
						return struct;
					}
				};						
			}
			case BINARY_FLOAT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, 0, raw.length), salted);
						try {
							final var bf = new BINARY_FLOAT(plaintext);
							final var struct = new Struct(schema);
							struct.put("V", bf.floatValue());
							return struct;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_FLOAT encrypted data " + rawToHex(raw), e);
						}
					}
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						try {
							final var bf = new BINARY_FLOAT(plaintext);
							final var struct = new Struct(schema);
							struct.put("V", bf.floatValue());
							return struct;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_FLOAT encrypted data " + rawToHex(raw), e);
						}
					}
				};
			}
			case BINARY_DOUBLE -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, 0, raw.length), salted);
						try {
							final var struct = new Struct(schema);
							final var bd = new BINARY_DOUBLE(plaintext);
							struct.put("V", bd.doubleValue());
							return struct;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_DOUBLE encrypted data " + rawToHex(raw), e);
						}
					}
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						try {
							final var struct = new Struct(schema);
							final var bd = new BINARY_DOUBLE(plaintext);
							struct.put("V", bd.doubleValue());
							return struct;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_DOUBLE encrypted data " + rawToHex(raw), e);
						}
					}
				};
			}
			// Special cases for extended size VARCHAR2/NVARCHAR2/RAW
			case LONGNVARCHAR, LONGVARCHAR -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
						final var struct = new Struct(schema);
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size NVARCHAR2/VARCHAR2");
						if (lobIds.contains(ll.lid()))
							struct.put("V", new String(decrypter.decrypt(cqTrans.getLob(ll), salted), UTF_16));
						else if (ll.dataInRow()) 
							struct.put("V", new String(plaintext, plaintext.length - ll.dataLength(), plaintext.length, UTF_16));
						else
							throwLobNotFound(ll, "enc extended size NVARCHAR2/VARCHAR2");
						return struct;
					}
				};
			}
			case LONGVARBINARY -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
						final var struct = new Struct(schema);
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size RAW");
						if (lobIds.contains(ll.lid()))
							struct.put("V", decrypter.decrypt(cqTrans.getLob(ll), salted));
						else if (ll.dataInRow())
							struct.put("V", Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							throwLobNotFound(ll, "enc extended size RAW");
						return struct;
					}
				};
			}
		}
		return null;
	}

	static OraCdcDecoder get(final String oraCharset) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return new String(raw, off, len, charset(oraCharset));
			}
		};
	}

	static OraCdcDecoder get(final Schema schema, final String oraCharset) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				final var struct = new Struct(schema);
				struct.put("V", new String(Arrays.copyOfRange(raw, off, off + len), charset(oraCharset)));
				return struct;
			}
		};
	}

	static OraCdcDecoder get(final String oraCharset, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
				return new String(plaintext, charset(oraCharset));
			}
		};
	}

	static OraCdcDecoder get(final Schema schema, final String oraCharset, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				final var struct = new Struct(schema);
				struct.put("V", new String(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted), charset(oraCharset)));
				return struct;
			}
		};
	}

	static OraCdcDecoder get(final ZoneId zoneId, final boolean local) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				final ZonedDateTime zdt;
				if (local)
					zdt = OracleTimestamp.toZonedDateTime(raw, off, len, zoneId);
				else
					zdt = TimestampWithTimeZone.toZonedDateTime(raw, off);
				return ISO_OFFSET_DATE_TIME.format(zdt);
			}
		};
	}

	static OraCdcDecoder get(final ZoneId zoneId, final boolean local, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
				final ZonedDateTime zdt;
				if (local)
					zdt = OracleTimestamp.toZonedDateTime(plaintext, 0, plaintext.length, zoneId);
				else
					zdt = TimestampWithTimeZone.toZonedDateTime(plaintext, 0);
				return ISO_OFFSET_DATE_TIME.format(zdt);
			}
		};
	}

	static OraCdcDecoder get(final OracleJsonFactory jsonFactory) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
				final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "JSON");
				if (lobIds.contains(ll.lid()))
					return oraJson(cqTrans.getLob(ll), jsonFactory);
				else if (ll.dataInRow())
					return oraJson(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len), jsonFactory);
				else
					return throwLobNotFound(ll, "JSON");
			}
		};
	}

	static OraCdcDecoder get(final OracleJsonFactory jsonFactory, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
				final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
				final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final var ll = new LobLocator(plaintext, 0, plaintext.length);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc JSON");
				if (lobIds.contains(ll.lid()))
					return oraJson(decrypter.decrypt(cqTrans.getLob(ll), salted), jsonFactory);
				else if (ll.dataInRow())
					return oraJson(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length), jsonFactory);
				else
					return throwLobNotFound(ll, "enc JSON");
			}
		};
	}

	static OraCdcDecoder getNUMBER(final int scale) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				var bd = toBigDecimal(raw);
				if (bd.scale() == scale)
					return bd;
				else if (bd.scale() > scale)
					return bd.setScale(scale, RoundingMode.HALF_UP);
				else
					return bd.setScale(scale, RoundingMode.UNNECESSARY);
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return decode(Arrays.copyOfRange(raw, off, off + len));
			}
		};
	}

	static OraCdcDecoder getNUMBER(final int scale, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, 0, raw.length), salted);
				var bd = toBigDecimal(plaintext);
				if (bd.scale() == scale)
					return bd;
				else if (bd.scale() > scale)
					return bd.setScale(scale, RoundingMode.HALF_UP);
				else
					return bd.setScale(scale, RoundingMode.UNNECESSARY);
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return decode(Arrays.copyOfRange(raw, off, off + len));
			}
		};
	}

	private static Struct oraBlob(final byte[] data) {
		final var lob = new Struct(OraBlob.schema());
		lob.put("V",  data);
		return lob;
	}

	private static Struct oraClob(final byte[] data) {
		final var lob = new Struct(OraClob.schema());
		lob.put("V",  new String(data, UTF_16));
		return lob;
	}

	private static Struct oraNClob(final byte[] data) {
		final var lob = new Struct(OraNClob.schema());
		lob.put("V",  new String(data, UTF_16));
		return lob;
	}

	private static Struct oraXml(final byte[] data, final boolean clob) throws SQLException {
		final var xml = new Struct(OraXml.schema());
		if (clob)
			xml.put("V",  new String(data, UTF_16));
		else
			//TODO not all XML are in UTF-8!
			//TODO <?xml version="1.0" encoding="UTF-8"?>
			xml.put("V", new String(data, UTF_8));
		return xml;
	}

	private static Struct oraJson(final byte[] data, final OracleJsonFactory jsonFactory) throws SQLException {
		try (OracleJsonParser parser =
				jsonFactory.createJsonBinaryParser(
						ByteBuffer.wrap(data))) {
			parser.next();
			final var json = new Struct(OraJson.schema());
			json.put("V", parser.getValue().toString());
			return json;
		} catch(OracleJsonException oje) {
			throw new SQLException(oje);
		}
	}

	private static final byte VECTOR_MAGIC_BYTE = (byte)0xDB;
	private static  Struct oraVector(final byte[] data) throws SQLException {
		final var vector = new Struct(OraVector.schema());
		if (data[0] != VECTOR_MAGIC_BYTE)
			throw new SQLException("Vector data don't start with a magic byte!");
		// final byte version = data[1];
		final short flags = getU16BE(data, 2);
		final byte type = data[4];
		final int size = getU32BE(data, 5);
		switch (type) {
			case 2 -> {
				final List<Float> f32 = new ArrayList<Float>(size);
				for (float f : VectorData.decode(data, float[].class, (flags & 0x8) != 0))
					f32.add(f);
				vector.put("F", f32);
			}
			case 3 -> {
				final List<Double> f64 = new ArrayList<Double>(size);
				for (double d : VectorData.decode(data, double[].class, (flags & 0x8) != 0))
					f64.add(d);
				vector.put("D", f64);
			}
			case 4 -> {
				final List<Byte> i8 = new ArrayList<Byte>(size);
				for (byte b : VectorData.decode(data, byte[].class, false))
					i8.add(b);
				vector.put("I", i8);
			}
			case 5 -> {
				final List<Boolean> b2 = new ArrayList<Boolean>(size);
				for (boolean b : VectorData.decode(data, boolean[].class, false))
					b2.add(b);
				vector.put("B", b2);
			}
			default ->  throw new SQLException("Unrecdognized VECTOR type " + type + "!");
		}
		return vector;
	}

	private static void traceLobInfo(final LobLocator ll, final byte[] ba, final String entity) {
		LOGGER.trace("Processing {} LID={}, DATALENGTH={}, DATA IN ROW?={}, LOB LOCATOR CONTENT=>{}",
				entity, ll.lid(), ll.dataLength(), ll.dataInRow(), rawToHex(ba));
	}

	private static Object throwLobNotFound(final LobLocator ll, final String entity) throws SQLException {
		LOGGER.error(
				"""
				
				=====================
				"Unable to find large object for {} with LID={}
				locator length={}, locator data in row={}, locator type={}
				=====================
				
				""", entity, ll.lid(), ll.dataLength(), ll.dataInRow(), ll.type());
		throw new SQLException("Lob with LID=" + ll.lid().toString() + " not found!");
	}

	private static Charset charset(final String oraCharset) {
		try {
			return Charset.forName(oraToJava.get(oraCharset));				
		} catch (UnsupportedCharsetException | IllegalCharsetNameException e) {
			throw new IllegalArgumentException("Invalid or unsupported Oracle character set: " + oraCharset +  ".", e);
		}
	}

	private static boolean toBoolean(final byte[] raw, final int off, final int len) throws SQLException {
		if (raw[off] == (byte) 1)
			return true;
		else if (raw[off] == (byte)0)
			return false;
		else
			throw new SQLException("Incorrect value " + String.format("0x%02x", raw[off]) + " for BOOLEAN (252) data type!");
	}

	static {
		decoders.put(BOOLEAN, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return toBoolean(raw, off, len);
			}
		});
		decoders.put(TINYINT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return toByte(raw, off, len);
			}
		});
		decoders.put(SMALLINT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return toShort(raw, off, len);
			}
		});
		decoders.put(INTEGER, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return toInt(raw, off, len);
			}
		});
		decoders.put(BIGINT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return toLong(raw, off, len);
			}
		});
		decoders.put(FLOAT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				return toFloat(raw);
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return decode(Arrays.copyOfRange(raw, off, off + len));
			}
		});
		decoders.put(BINARY_FLOAT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				try {
					BINARY_FLOAT bf = new BINARY_FLOAT(raw);
					return bf.floatValue();
				} catch (Exception e) {
					throw new SQLException("Invalid Oracle BINARY_FLOAT data " + rawToHex(raw), e);
				}
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return decode(Arrays.copyOfRange(raw, off, off + len));
			}
		});
		decoders.put(DOUBLE, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				return toDouble(raw);
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return decode(Arrays.copyOfRange(raw, off, off + len));
			}
		});
		decoders.put(BINARY_DOUBLE, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				try {
					BINARY_DOUBLE bd = new BINARY_DOUBLE(raw);
					return bd.doubleValue();
				} catch (Exception e) {
					throw new SQLException("Invalid Oracle BINARY_DOUBLE data " + rawToHex(raw), e);
				}
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return decode(Arrays.copyOfRange(raw, off, off + len));
			}
		});
		decoders.put(DATE, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				try {
					return OracleDate.toTimestamp(raw, off);
				} catch (DateTimeException dte) {
					throw new SQLException(dte);
				}
			}
		});
		decoders.put(TIMESTAMP, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				try {
					return OracleTimestamp.toTimestamp(raw, off);
				} catch (DateTimeException dte) {
					throw new SQLException(dte);
				}
			}
		});
		decoders.put(JAVA_SQL_TYPE_INTERVALDS_STRING, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return IntervalDayToSecond.toString(raw, off);
			}
		});
		decoders.put(JAVA_SQL_TYPE_INTERVALYM_STRING, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return IntervalYearToMonth.toString(raw, off);
			}
		});
		decoders.put(BLOB, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
				final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "BLOB");
				if (lobIds.contains(ll.lid()))
					return oraBlob(cqTrans.getLob(ll));
				else if (ll.dataInRow())
					return oraBlob(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return throwLobNotFound(ll, "BLOB");
			}
		});
		decoders.put(CLOB, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
				final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "CLOB");
				if (lobIds.contains(ll.lid()))
					return oraClob(cqTrans.getLob(ll));
				else if (ll.dataInRow())
					return oraClob(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return throwLobNotFound(ll, "CLOB");
			}
		});
		decoders.put(NCLOB, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
				final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "NCLOB");
				if (lobIds.contains(ll.lid()))
					return oraNClob(cqTrans.getLob(ll));
				else if (ll.dataInRow())
					return oraNClob(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return throwLobNotFound(ll, "NCLOB");
			}
		});
		decoders.put(SQLXML, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
				final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "SQLXML");
				if (lobIds.contains(ll.lid()))
					return oraXml(cqTrans.getLob(ll), ll.type() == LobLocator.CLOB);
				else if (ll.dataInRow())
					return oraXml(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len), ll.type() == LobLocator.CLOB);
				else
					return throwLobNotFound(ll, "SQLXML");
			}
		});
		decoders.put(VECTOR, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
				final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "VECTOR");
				if (lobIds.contains(ll.lid()))
					return oraVector(cqTrans.getLob(ll));
				else if (ll.dataInRow())
					return oraVector(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return throwLobNotFound(ll, "VECTOR");
			}
		});
		// Special cases for extended size VARCHAR2/NVARCHAR2/RAW
		decoders.put(LONGNVARCHAR, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
				final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "extended size NVARCHAR2");
				if (lobIds.contains(ll.lid()))
					return new String(cqTrans.getLob(ll), UTF_16);
				else if (ll.dataInRow())
					return new String(raw, off + len - ll.dataLength(), off + len, UTF_16);
				else
					return throwLobNotFound(ll, "extended size NVARCHAR2");
			}
		});
		decoders.put(LONGVARCHAR, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
				final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "extended size VARCHAR2");
				if (lobIds.contains(ll.lid()))
					return new String(cqTrans.getLob(ll), UTF_16);
				else if (ll.dataInRow())
					return new String(raw, off + len - ll.dataLength(), off + len, UTF_16);
				else
					return throwLobNotFound(ll, "extended size VARCHAR2");
			}
		});
		decoders.put(LONGVARBINARY, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction, final Set<LobId> lobIds) throws SQLException {
				final var cqTrans = (OraCdcTransactionChronicleQueue) transaction;
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "extended size RAW");
				if (lobIds.contains(ll.lid()))
					return cqTrans.getLob(ll);
				else if (ll.dataInRow())
					return Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len);
				else
					return throwLobNotFound(ll, "extended size RAW");
			}
		});

		oraToJava.put("AL16UTF16", "UTF-16BE");
		oraToJava.put("AL16UTF16LE", "UTF-16LE");
		oraToJava.put("AL24UTFFSS", "UTF-8");
		oraToJava.put("AL32UTF8", "UTF-8");
		oraToJava.put("AR8ARABICMAC", "MacArabic");
		oraToJava.put("AR8ARABICMACS", "MacArabic");
		oraToJava.put("AR8ARABICMACT", "MacArabic");
		oraToJava.put("AR8EBCDIC420S", "CP420");
		oraToJava.put("AR8EBCDICX", "CP420");
		oraToJava.put("AR8ISO8859P6", "ISO-8859-6");
		oraToJava.put("AR8MSAWIN", "WINDOWS-1256");
		oraToJava.put("AR8MSWIN1256", "WINDOWS-1256");
		oraToJava.put("BLT8CP921", "CP921");
		oraToJava.put("BLT8EBCDIC1112", "CP1112");
		oraToJava.put("BLT8EBCDIC1112S", "CP1112");
		oraToJava.put("BLT8ISO8859P13", "ISO-8859-13");
		oraToJava.put("BLT8MSWIN1257", "WINDOWS-1257");
		oraToJava.put("BLT8PC775", "CP775");
		oraToJava.put("CDN8PC863", "CP863");
		oraToJava.put("CL8EBCDIC1025", "CP1025");
		oraToJava.put("CL8EBCDIC1025C", "CP1025");
		oraToJava.put("CL8EBCDIC1025R", "CP1025");
		oraToJava.put("CL8EBCDIC1025S", "CP1025");
		oraToJava.put("CL8EBCDIC1025X", "CP1025");
		oraToJava.put("CL8ISO8859P5", "ISO-8859-5");
		oraToJava.put("CL8KOI8R", "KOI8-R");
		oraToJava.put("CL8MACCYRILLIC", "MacCyrillic");
		oraToJava.put("CL8MACCYRILLICS", "MacCyrillic");
		oraToJava.put("CL8MSWIN1251", "WINDOWS-1251");
		oraToJava.put("D8EBCDIC1141", "Cp1141");
		oraToJava.put("D8EBCDIC273", "CP273");
		oraToJava.put("DK8EBCDIC1142", "Cp1142");
		oraToJava.put("DK8EBCDIC277", "IBM277");
		oraToJava.put("EE8EBCDIC870", "CP870");
		oraToJava.put("EE8EBCDIC870C", "CP870");
		oraToJava.put("EE8EBCDIC870S", "CP870");
		oraToJava.put("EE8ISO8859P2", "ISO-8859-2");
		oraToJava.put("EE8MACCE", "MacCentralEurope");
		oraToJava.put("EE8MACCES", "MacCentralEurope");
		oraToJava.put("EE8MACCROATIAN", "MacCroatian");
		oraToJava.put("EE8MACCROATIANS", "MacCroatian");
		oraToJava.put("EE8MSWIN1250", "WINDOWS-1250");
		oraToJava.put("EE8PC852", "CP852");
		oraToJava.put("EL8EBCDIC875", "CP875");
		oraToJava.put("EL8EBCDIC875R", "CP875");
		oraToJava.put("EL8EBCDIC875S", "CP875");
		oraToJava.put("EL8ISO8859P7", "ISO-8859-7");
		oraToJava.put("EL8MSWIN1253", "WINDOWS-1253");
		oraToJava.put("EL8PC737", "CP737");
		oraToJava.put("EL8PC869", "CP869");
		oraToJava.put("F8EBCDIC1147", "Cp1147");
		oraToJava.put("F8EBCDIC297", "CP297");
		oraToJava.put("I8EBCDIC1144", "Cp1144");
		oraToJava.put("I8EBCDIC280", "CP280");
		oraToJava.put("IS8MACICELANDIC", "MacIceland");
		oraToJava.put("IS8MACICELANDICS", "MacIceland");
		oraToJava.put("IS8PC861", "CP861");
		oraToJava.put("ISO2022-CN", "ISO2022CN_GB");
		oraToJava.put("ISO2022-JP", "ISO-2022-JP");
		oraToJava.put("ISO2022-KR", "ISO-2022-KR");
		oraToJava.put("IW8EBCDIC424", "CP424");
		oraToJava.put("IW8EBCDIC424S", "CP424");
		oraToJava.put("IW8ISO8859P8", "ISO-8859-8");
		oraToJava.put("IW8MACHEBREW", "MacHebrew");
		oraToJava.put("IW8MACHEBREWS", "MacHebrew");
		oraToJava.put("IW8MSWIN1255", "WINDOWS-1255");
		oraToJava.put("IW8PC1507", "CP862");
		oraToJava.put("JA16EBCDIC930", "CP930");
		oraToJava.put("JA16EUC", "EUC-JP");
		oraToJava.put("JA16EUCTILDE", "EUC-JP");
		oraToJava.put("JA16EUCYEN", "EUC-JP");
		oraToJava.put("JA16SJIS", "MS932");
		oraToJava.put("JA16SJISTILDE", "MS932");
		oraToJava.put("JA16SJISYEN", "MS932");
		oraToJava.put("JA16VMS", "EUC-JP");
		oraToJava.put("KO16KSC5601", "MS949");
		oraToJava.put("KO16MSWIN949", "MS949");
		oraToJava.put("LT8MSWIN921", "CP921");
		oraToJava.put("N8PC865", "CP865");
		oraToJava.put("NEE8ISO8859P4", "ISO-8859-4");
		oraToJava.put("RU8PC855", "CP855");
		oraToJava.put("RU8PC866", "CP866");
		oraToJava.put("S8EBCDIC1143", "Cp1143");
		oraToJava.put("S8EBCDIC278", "CP278");
		oraToJava.put("SE8ISO8859P3", "ISO-8859-3");
		oraToJava.put("TH8MACTHAI", "MacThai");
		oraToJava.put("TH8MACTHAIS", "MacThai");
		oraToJava.put("TH8TISASCII", "MS874");
		oraToJava.put("TH8TISEBCDIC", "CP838");
		oraToJava.put("TH8TISEBCDICS", "CP838");
		oraToJava.put("TR8EBCDIC1026", "CP1026");
		oraToJava.put("TR8EBCDIC1026S", "CP1026");
		oraToJava.put("TR8MACTURKISH", "MacTurkish");
		oraToJava.put("TR8MACTURKISHS", "MacTurkish");
		oraToJava.put("TR8MSWIN1254", "WINDOWS-1254");
		oraToJava.put("TR8PC857", "CP857");
		oraToJava.put("UCS2", "UTF-16");
		oraToJava.put("US7ASCII", "US-ASCII");
		oraToJava.put("US8PC437", "CP437");
		oraToJava.put("UTF16", "UTF-16");
		oraToJava.put("UTF8", "UTF-8");
		oraToJava.put("VN8MSWIN1258", "WINDOWS-1258");
		oraToJava.put("WE8EBCDIC1140", "Cp1140");
		oraToJava.put("WE8EBCDIC1140C", "Cp1140");
		oraToJava.put("WE8EBCDIC1145", "Cp1145");
		oraToJava.put("WE8EBCDIC1146", "Cp1146");
		oraToJava.put("WE8EBCDIC1148", "Cp1148");
		oraToJava.put("WE8EBCDIC1148C", "Cp1148");
		oraToJava.put("WE8EBCDIC284", "CP284");
		oraToJava.put("WE8EBCDIC285", "CP285");
		oraToJava.put("WE8EBCDIC37", "CP037");
		oraToJava.put("WE8EBCDIC37C", "CP037");
		oraToJava.put("WE8EBCDIC500", "CP500");
		oraToJava.put("WE8EBCDIC500C", "CP500");
		oraToJava.put("WE8EBCDIC871", "CP871");
		oraToJava.put("WE8ISO8859P1", "ISO-8859-1");
		oraToJava.put("WE8ISO8859P15", "ISO-8859-15");
		oraToJava.put("WE8ISO8859P9", "ISO-8859-9");
		oraToJava.put("WE8MACROMAN8", "MacRoman");
		oraToJava.put("WE8MACROMAN8S", "MacRoman");
		oraToJava.put("WE8MSWIN1252", "WINDOWS-1252");
		oraToJava.put("WE8PC850", "CP850");
		oraToJava.put("WE8PC858", "Cp858");
		oraToJava.put("WE8PC860", "CP860");
		oraToJava.put("ZHS16CGB231280", "EUC_CN");
		oraToJava.put("ZHS16GBK", "MS936");
		oraToJava.put("ZHS32GB18030", "GB18030");
		oraToJava.put("ZHT16BIG5", "MS950");
		oraToJava.put("ZHT16HKSCS", "MS950_HKSCS");
		oraToJava.put("ZHT16MSWIN950", "MS950");
		oraToJava.put("ZHT32EUC", "EUC-TW");
	}

}
