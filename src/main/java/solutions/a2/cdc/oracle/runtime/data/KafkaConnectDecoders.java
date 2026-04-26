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
import static java.nio.charset.StandardCharsets.UTF_16;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.TINYINT;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.BIGINT;
import static java.sql.Types.FLOAT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.NUMERIC;
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
import static solutions.a2.cdc.oracle.data.JdbcTypes.getTypeName;
import static solutions.a2.cdc.oracle.OraCdcColumn.JAVA_SQL_TYPE_INTERVALDS_STRING;
import static solutions.a2.cdc.oracle.OraCdcColumn.JAVA_SQL_TYPE_INTERVALYM_STRING;
import static solutions.a2.cdc.oracle.runtime.data.RdbmsCharsetMapping.ora2JvmCharset;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toByte;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toInt;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toLong;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toShort;
import static solutions.a2.oracle.utils.BinaryUtils.getU16BE;
import static solutions.a2.oracle.utils.BinaryUtils.getU32BE;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.agrona.collections.Int2ObjectHashMap;
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
import solutions.a2.cdc.oracle.OraCdcDecoder;
import solutions.a2.cdc.oracle.OraCdcTransaction;
import solutions.a2.cdc.oracle.data.OraBlob;
import solutions.a2.cdc.oracle.data.OraClob;
import solutions.a2.cdc.oracle.data.OraJson;
import solutions.a2.cdc.oracle.data.OraNClob;
import solutions.a2.cdc.oracle.data.OraVector;
import solutions.a2.cdc.oracle.data.OraXml;
import solutions.a2.cdc.oracle.internals.OraCdcTdeColumnDecrypter;
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
public class KafkaConnectDecoders {

	private static final Int2ObjectHashMap<OraCdcDecoder> decoders = new Int2ObjectHashMap<>();

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectDecoders.class);
	
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
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "extended size RAW");
				if (ll.dataInRow())
					return Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len);
				else
					return transaction.getLob(ll);
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
					final OraCdcTransaction transaction) throws SQLException {
				final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
				final var ll = new LobLocator(plaintext, 0, plaintext.length);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size RAW");
				if (ll.dataInRow())
					return Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length);
				else
					return decrypter.decrypt(transaction.getLob(ll), salted);
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
						try {
							final var struct = new Struct(schema);
							if (len > 0) struct.put("V", toBoolean(raw, off, len));
							return struct;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, false, jdbcType);
						}
					}
				};
			}
			case TINYINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							final var struct = new Struct(schema);
							if (len > 0) struct.put("V", toByte(raw, off, len));
							return struct;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, false, jdbcType);
						}
					}
				};
			}
			case SMALLINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							final var struct = new Struct(schema);
							if (len > 0) struct.put("V", toShort(raw, off, len));
							return struct;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, false, jdbcType);
						}
					}
				};
			}
			case INTEGER -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							final var struct = new Struct(schema);
							if (len > 0) struct.put("V", toInt(raw, off, len));
							return struct;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, false, jdbcType);
						}
					}
				};
			}
			case BIGINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							final var struct = new Struct(schema);
							if (len > 0) struct.put("V", toLong(raw, off, len));
							return struct;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, false, jdbcType);
						}
					}
				};
			}
			case DOUBLE -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							final var struct = new Struct(schema);
							if (len > 0) struct.put("V", toDouble(Arrays.copyOfRange(raw, off, off + len)));
							return struct;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, false, jdbcType);
						}
					}
				};
			}
			case BINARY_FLOAT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw) throws SQLException {
						try {
							final var struct = new Struct(schema);
							if (raw.length > 0) {
								final var bf = new BINARY_FLOAT(raw);
								struct.put("V", bf.floatValue());
							}
							return struct;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_FLOAT data " + rawToHex(raw), e);
						}
					}
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							final var struct = new Struct(schema);
							if (len > 0) {
								final var bf = new BINARY_FLOAT(Arrays.copyOfRange(raw, off, off + len));
								struct.put("V", bf.floatValue());
							}
							return struct;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_FLOAT data " + rawToHex(raw), e);
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
							if (raw.length > 0) {
								final var bd = new BINARY_DOUBLE(raw);
								struct.put("V", bd.doubleValue());
							}
							return struct;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_DOUBLE data " + rawToHex(raw), e);
						}
					}
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							final var struct = new Struct(schema);
							if (len > 0) {
								final var bd = new BINARY_DOUBLE(Arrays.copyOfRange(raw, off, off + len));
								struct.put("V", bd.doubleValue());
							}
							return struct;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_DOUBLE data " + rawToHex(raw), e);
						}
					}
				};
			}
			// Special cases for extended size VARCHAR2/NVARCHAR2/RAW
			case LONGNVARCHAR, LONGVARCHAR -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction) throws SQLException {
						final var struct = new Struct(schema);
						final var ll = new LobLocator(raw, off, len);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size NVARCHAR2/VARCHAR2");
						if (ll.dataInRow()) 
							struct.put("V", new String(raw, off + len - ll.dataLength(), off + len, UTF_16));
						else
							struct.put("V", new String(transaction.getLob(ll), UTF_16));
						return struct;
					}
				};
			}
			case LONGVARBINARY -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction) throws SQLException {
						final var struct = new Struct(schema);
						final var ll = new LobLocator(raw, off, len);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size RAW");
						if (ll.dataInRow())
							struct.put("V", Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
						else
							struct.put("V", transaction.getLob(ll));
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
						try {
							if (len > 0) return toByte(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted));
							else return null;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, true, jdbcType);
						}
					}
				};
			}
			case SMALLINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							if (len > 0) return toShort(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted));
							else return null;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, true, jdbcType);
						}
					}
				};
			}
			case INTEGER -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							if (len > 0) return toInt(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted));
							else return null;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, true, jdbcType);
						}
					}
				};
			}
			case BIGINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							if (len > 0) return toLong(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted));
							else return null;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, true, jdbcType);
						}
					}
				};
			}
			case FLOAT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							if (len > 0) return toFloat(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted));
							return null;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, true, jdbcType);
						}
					}
				};
			}
			case BINARY_FLOAT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw) throws SQLException {
						try {
							if (raw.length > 0) return new BINARY_FLOAT(
										decrypter.decrypt(Arrays.copyOfRange(raw, 0, raw.length), salted))
									.floatValue();
							else return null;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_FLOAT encrypted data " + rawToHex(raw), e);
						}
					}
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							if (len > 0) return new BINARY_FLOAT(
									decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted))
								.floatValue();
							else return null;
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
						try {
							if (len > 0) return toDouble(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted));
							else return null;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, true, jdbcType);
						}
					}
				};
			}
			case BINARY_DOUBLE -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw) throws SQLException {
						try {
							if (raw.length > 0) return new BINARY_DOUBLE(
									decrypter.decrypt(Arrays.copyOfRange(raw, 0, raw.length), salted))
									.doubleValue();
							else return null;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_DOUBLE encrypted data " + rawToHex(raw), e);
						}
					}
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							if (len > 0) return new BINARY_DOUBLE(
									decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted))
									.doubleValue();
							return null;
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
							final OraCdcTransaction transaction) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc BLOB");
						if (ll.dataInRow())
							return oraBlob(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return oraBlob(decrypter.decrypt(transaction.getLob(ll), salted));
					}
				};
			}
			case CLOB -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc CLOB");
						if (ll.dataInRow())
							return oraClob(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return oraClob(decrypter.decrypt(transaction.getLob(ll), salted));
					}
				};
			}
			case NCLOB -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc NCLOB");
						if (ll.dataInRow())
							return oraNClob(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return oraNClob(decrypter.decrypt(transaction.getLob(ll), salted));
					}
				};
			}
			case SQLXML -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc SQLXML");
						if (ll.type() == LobLocator.CLOB)
							if (ll.dataInRow())
								return oraXml(
									Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length),
									true);
							else
								return oraXml(decrypter.decrypt(transaction.getLob(ll), salted), true);
						else
							return oraXml(decrypter.decrypt(transaction.getLob(ll), salted), false);
					}
				};
			}
			case VECTOR -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc VECTOR");
						if (ll.dataInRow())
							return oraVector(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return oraVector(decrypter.decrypt(transaction.getLob(ll), salted));
					}
				};
			}
			// Special cases for extended size VARCHAR2/NVARCHAR2/RAW
			case LONGNVARCHAR -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size NVARCHAR2");
						if (ll.dataInRow()) 
							return new String(plaintext, plaintext.length - ll.dataLength(), plaintext.length, UTF_16);
						else
							return new String(decrypter.decrypt(transaction.getLob(ll), salted), UTF_16);
					}
				};
			}
			case LONGVARCHAR -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size VARCHAR2");
						if (ll.dataInRow())
							return new String(plaintext, plaintext.length - ll.dataLength(), plaintext.length, UTF_16);
						else
							return new String(decrypter.decrypt(transaction.getLob(ll), salted), UTF_16);
					}
				};
			}
			case LONGVARBINARY -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size RAW");
						if (ll.dataInRow())
							return Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length);
						else
							return decrypter.decrypt(transaction.getLob(ll), salted);
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
						try {
							final var struct = new Struct(schema);
							if (len > 0) struct.put("V", toByte(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
							return struct;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, true, jdbcType);
						}
					}
				};						
			}
			case SMALLINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							final var struct = new Struct(schema);
							if (len > 0) struct.put("V", toShort(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
							return struct;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, true, jdbcType);
						}
					}
				};						
			}
			case INTEGER -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							final var struct = new Struct(schema);
							if (len > 0) struct.put("V", toInt(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
							return struct;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, true, jdbcType);
						}
					}
				};						
			}
			case BIGINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							final var struct = new Struct(schema);
							if (len > 0) struct.put("V", toLong(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
							return struct;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, true, jdbcType);
						}
					}
				};						
			}
			case DOUBLE -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							final var struct = new Struct(schema);
							if (len > 0) struct.put("V", toDouble(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
							return struct;
						} catch (Exception e) {
							throw invalidNumberData(e, raw, off, len, true, jdbcType);
						}
					}
				};						
			}
			case BINARY_FLOAT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, 0, raw.length), salted);
						try {
							final var struct = new Struct(schema);
							if (raw.length > 0) {
								final var bf = new BINARY_FLOAT(plaintext);
								struct.put("V", bf.floatValue());
							}
							return struct;
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_FLOAT encrypted data " + rawToHex(raw), e);
						}
					}
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						try {
							final var struct = new Struct(schema);
							if (len > 0) {
								final var bf = new BINARY_FLOAT(plaintext);
								struct.put("V", bf.floatValue());
							}
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
							if (raw.length > 0) {
								final var bd = new BINARY_DOUBLE(plaintext);
								struct.put("V", bd.doubleValue());
							}
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
							if (len > 0) {
								final var bd = new BINARY_DOUBLE(plaintext);
								struct.put("V", bd.doubleValue());
							}
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
							final OraCdcTransaction transaction) throws SQLException {
						final var struct = new Struct(schema);
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size NVARCHAR2/VARCHAR2");
						if (ll.dataInRow()) 
							struct.put("V", new String(plaintext, plaintext.length - ll.dataLength(), plaintext.length, UTF_16));
						else
							struct.put("V", new String(decrypter.decrypt(transaction.getLob(ll), salted), UTF_16));
						return struct;
					}
				};
			}
			case LONGVARBINARY -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len,
							final OraCdcTransaction transaction) throws SQLException {
						final var struct = new Struct(schema);
						final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
						final var ll = new LobLocator(plaintext, 0, plaintext.length);
						if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size RAW");
						if (ll.dataInRow())
							struct.put("V", Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							struct.put("V", decrypter.decrypt(transaction.getLob(ll), salted));
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
				return new String(raw, off, len, ora2JvmCharset(oraCharset));
			}
		};
	}

	static OraCdcDecoder get(final Schema schema, final String oraCharset) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				final var struct = new Struct(schema);
				struct.put("V", new String(Arrays.copyOfRange(raw, off, off + len), ora2JvmCharset(oraCharset)));
				return struct;
			}
		};
	}

	static OraCdcDecoder get(final String oraCharset, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
				return new String(plaintext, ora2JvmCharset(oraCharset));
			}
		};
	}

	static OraCdcDecoder get(final Schema schema, final String oraCharset, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				final var struct = new Struct(schema);
				struct.put("V", new String(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted), ora2JvmCharset(oraCharset)));
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
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "JSON");
				if (ll.dataInRow())
					return oraJson(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len), jsonFactory);
				else
					return oraJson(transaction.getLob(ll), jsonFactory);
			}
		};
	}

	static OraCdcDecoder get(final OracleJsonFactory jsonFactory, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
				final var ll = new LobLocator(plaintext, 0, plaintext.length);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc JSON");
				if (ll.dataInRow())
					return oraJson(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length), jsonFactory);
				else
					return oraJson(decrypter.decrypt(transaction.getLob(ll), salted), jsonFactory);
			}
		};
	}

	static OraCdcDecoder getNUMBER(final int scale) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				if (raw.length == 0) return null;
				try {
					var bd = toBigDecimal(raw);
					if (bd.scale() == scale)
						return bd;
					else if (bd.scale() > scale)
						return bd.setScale(scale, RoundingMode.HALF_UP);
					else
						return bd.setScale(scale, RoundingMode.UNNECESSARY);
				} catch (Exception e) {
					throw invalidNumberData(e, raw, 0, raw.length, false, NUMERIC);
				}
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return null;
				try {
					return decode(Arrays.copyOfRange(raw, off, off + len));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, NUMERIC);
				}
			}
		};
	}

	static OraCdcDecoder getNUMBER(final int scale, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				if (raw.length == 0) return null;
				try {
					var bd = toBigDecimal(decrypter.decrypt(Arrays.copyOfRange(raw, 0, raw.length), salted));
					if (bd.scale() == scale)
						return bd;
					else if (bd.scale() > scale)
						return bd.setScale(scale, RoundingMode.HALF_UP);
					else
						return bd.setScale(scale, RoundingMode.UNNECESSARY);
				} catch (Exception e) {
					throw invalidNumberData(e, raw, 0, raw.length, true, NUMERIC);
				}
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return null;
				try {
					return decode(Arrays.copyOfRange(raw, off, off + len));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, true, NUMERIC);
				}
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

	private static boolean toBoolean(final byte[] raw, final int off, final int len) throws SQLException {
		if (raw[off] == (byte) 1)
			return true;
		else if (raw[off] == (byte)0)
			return false;
		else
			throw new SQLException("Incorrect value " + String.format("0x%02x", raw[off]) + " for BOOLEAN (252) data type!");
	}

	private static SQLException invalidNumberData(
			final Exception e, final byte[] raw, final int off, final int len, final boolean enc, final int jdbcType) {
		final var errMsg = new StringBuilder(0x80);
		errMsg
			.append("Invalid Oracle NUMBER")
			.append(enc ? " encrypted " : " ")
			.append("data '")
			.append(rawToHex(Arrays.copyOfRange(raw, off, off + len)))
			.append("' for ")
			.append(getTypeName(jdbcType))
			.append("!");
		return new SQLException(errMsg.toString(), e);
	}

	static {
		decoders.put(BOOLEAN, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return null;
				try {
					return toBoolean(raw, off, len);
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, BOOLEAN);
				}
			}
		});
		decoders.put(TINYINT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return null;
				try {
					return toByte(raw, off, len);
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, TINYINT);
				}
			}
		});
		decoders.put(SMALLINT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return null;
				try {
					return toShort(raw, off, len);
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, SMALLINT);
				}
			}
		});
		decoders.put(INTEGER, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return null;
				try {
					return toInt(raw, off, len);
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, INTEGER);
				}
			}
		});
		decoders.put(BIGINT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return null;
				try {
					return toLong(raw, off, len);
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, BIGINT);
				}
			}
		});
		decoders.put(FLOAT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				if (raw.length == 0) return null;
				try {
					return toFloat(raw);
				} catch (Exception e) {
					throw invalidNumberData(e, raw, 0, raw.length, false, FLOAT);
				}
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return null;
				try {
					return decode(Arrays.copyOfRange(raw, off, off + len));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, FLOAT);
				}
			}
		});
		decoders.put(BINARY_FLOAT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				if (raw.length == 0) return null;
				try {
					return new BINARY_FLOAT(raw).floatValue();
				} catch (Exception e) {
					throw new SQLException("Invalid Oracle BINARY_FLOAT data " + rawToHex(raw), e);
				}
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return null;
				try {
					return new BINARY_FLOAT(Arrays.copyOfRange(raw, off, off + len)).floatValue();
				} catch (Exception e) {
					throw new SQLException("Invalid Oracle BINARY_FLOAT data " + rawToHex(raw), e);
				}
			}
		});
		decoders.put(DOUBLE, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				if (raw.length == 0) return null;
				try {
					return toDouble(raw);
				} catch (Exception e) {
					throw invalidNumberData(e, raw, 0, raw.length, false, DOUBLE);
				}
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return null;
				try {
					return toDouble(Arrays.copyOfRange(raw, off, off + len));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, DOUBLE);
				}
			}
		});
		decoders.put(BINARY_DOUBLE, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				if (raw.length == 0) return null;
				try {
					return new BINARY_DOUBLE(raw).doubleValue();
				} catch (Exception e) {
					throw new SQLException("Invalid Oracle BINARY_DOUBLE data " + rawToHex(raw), e);
				}
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return null;
				try {
					return new BINARY_DOUBLE(Arrays.copyOfRange(raw, off, off + len)).doubleValue();
				} catch (Exception e) {
					throw new SQLException("Invalid Oracle BINARY_DOUBLE data " + rawToHex(raw), e);
				}
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
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "BLOB");
				if (ll.dataInRow())
					return oraBlob(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return oraBlob(transaction.getLob(ll));
			}
		});
		decoders.put(CLOB, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "CLOB");
				if (ll.dataInRow())
					return oraClob(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return oraClob(transaction.getLob(ll));
			}
		});
		decoders.put(NCLOB, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "NCLOB");
				if (ll.dataInRow())
					return oraNClob(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return oraNClob(transaction.getLob(ll));
			}
		});
		decoders.put(SQLXML, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "SQLXML");
				if (ll.type() == LobLocator.CLOB)
					if (ll.dataInRow())
						return oraXml(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len), true);
					else
						return oraXml(transaction.getLob(ll), true);
				else
					return oraXml(transaction.getLob(ll), false);
			}
		});
		decoders.put(VECTOR, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "VECTOR");
				if (ll.dataInRow())
					return oraVector(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return oraVector(transaction.getLob(ll));
			}
		});
		// Special cases for extended size VARCHAR2/NVARCHAR2/RAW
		decoders.put(LONGNVARCHAR, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "extended size NVARCHAR2");
				if (ll.dataInRow())
					return new String(raw, off + len - ll.dataLength(), off + len, UTF_16);
				else
					return new String(transaction.getLob(ll), UTF_16);
			}
		});
		decoders.put(LONGVARCHAR, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "extended size VARCHAR2");
				if (ll.dataInRow())
					return new String(raw, off + len - ll.dataLength(), off + len, UTF_16);
				else
					return new String(transaction.getLob(ll), UTF_16);
			}
		});
		decoders.put(LONGVARBINARY, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "extended size RAW");
				if (ll.dataInRow())
					return Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len);
				else
					return transaction.getLob(ll);
			}
		});
	}

}
