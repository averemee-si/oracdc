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
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Optional;

import org.agrona.collections.Int2ObjectHashMap;
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
 * to Java data types.
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
public class GenericDecoders {

	private static final Int2ObjectHashMap<OraCdcDecoder> decoders = new Int2ObjectHashMap<>();
	private static final Int2ObjectHashMap<OraCdcDecoder> wrappedDecoders = new Int2ObjectHashMap<>();

	private static final Logger LOGGER = LoggerFactory.getLogger(GenericDecoders.class);

	static final byte VECTOR_MAGIC_BYTE = (byte)0xDB;

	
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

	static OraCdcDecoder getWrapped() {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				return Optional.of(raw);
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return Optional.of(Arrays.copyOfRange(raw, off, off + len));
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "extended size RAW");
				if (ll.dataInRow())
					return Optional.of(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return Optional.of(transaction.getLob(ll));
			}
		};
	}

	static OraCdcDecoder get(final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
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

	static OraCdcDecoder getWrapped(final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return Optional.of(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted));
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
				final var ll = new LobLocator(plaintext, 0, plaintext.length);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "enc extended size RAW");
				if (ll.dataInRow())
					return Optional.of(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
				else
					return Optional.of(decrypter.decrypt(transaction.getLob(ll), salted));
			}
		};
	}

	static OraCdcDecoder get(final int jdbcType) {
		return decoders.get(jdbcType);
	}

	static OraCdcDecoder getWrapped(final int jdbcType) {
		return wrappedDecoders.get(jdbcType);
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
							return Optional.ofNullable(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return Optional.ofNullable(decrypter.decrypt(transaction.getLob(ll), salted));
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
							return Optional.ofNullable(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return Optional.ofNullable(decrypter.decrypt(transaction.getLob(ll), salted));
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
							return Optional.ofNullable(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return Optional.ofNullable(decrypter.decrypt(transaction.getLob(ll), salted));
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

	static OraCdcDecoder getWrapped(final int jdbcType, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		switch (jdbcType) {
			case TINYINT -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							if (len > 0) return Optional.of(toByte(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
							else return Optional.empty();
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
							if (len > 0) return Optional.of(toShort(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
							else return Optional.empty();
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
							if (len > 0) return Optional.of(toInt(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
							else return Optional.empty();
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
							if (len > 0) return Optional.of(toLong(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
							else return Optional.empty();
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
							if (len > 0) return Optional.of(toFloat(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
							return Optional.empty();
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
							if (raw.length > 0) return Optional.of(
									new BINARY_FLOAT(
										decrypter.decrypt(Arrays.copyOfRange(raw, 0, raw.length), salted))
											.floatValue());
							else return Optional.empty();
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_FLOAT encrypted data " + rawToHex(raw), e);
						}
					}
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							if (len > 0) return Optional.of(
									new BINARY_FLOAT(
										decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted))
											.floatValue());
							else return Optional.empty();
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
							if (len > 0) return Optional.of(toDouble(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
							else return Optional.empty();
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
							if (raw.length > 0) return Optional.of(
									new BINARY_DOUBLE(
										decrypter.decrypt(Arrays.copyOfRange(raw, 0, raw.length), salted))
											.doubleValue());
							else return Optional.empty();
						} catch (Exception e) {
							throw new SQLException("Invalid Oracle BINARY_DOUBLE encrypted data " + rawToHex(raw), e);
						}
					}
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						try {
							if (len > 0) return Optional.of(
									new BINARY_DOUBLE(
										decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted))
											.doubleValue());
							return Optional.empty();
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
						try {
							return Optional.of(OracleDate.toTimestamp(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
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
						try {
							return Optional.of(OracleTimestamp.toTimestamp(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
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
						return Optional.of(IntervalDayToSecond.toString(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
					}
				};
			}
			case JAVA_SQL_TYPE_INTERVALYM_STRING -> {
				return new OraCdcDecoder() {
					@Override
					public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
						return Optional.of(IntervalYearToMonth.toString(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted)));
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
							return Optional.ofNullable(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return Optional.ofNullable(decrypter.decrypt(transaction.getLob(ll), salted));
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
							return Optional.ofNullable(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return Optional.ofNullable(decrypter.decrypt(transaction.getLob(ll), salted));
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
							return Optional.ofNullable(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return Optional.ofNullable(decrypter.decrypt(transaction.getLob(ll), salted));
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
							return Optional.of(new String(plaintext, plaintext.length - ll.dataLength(), plaintext.length, UTF_16));
						else
							return Optional.of(new String(decrypter.decrypt(transaction.getLob(ll), salted), UTF_16));
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
							return Optional.of(new String(plaintext, plaintext.length - ll.dataLength(), plaintext.length, UTF_16));
						else
							return Optional.of(new String(decrypter.decrypt(transaction.getLob(ll), salted), UTF_16));
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
							return Optional.of(Arrays.copyOfRange(plaintext, plaintext.length - ll.dataLength(), plaintext.length));
						else
							return Optional.of(decrypter.decrypt(transaction.getLob(ll), salted));
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

	static OraCdcDecoder getWrapped(final String oraCharset) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return Optional.of(new String(raw, off, len, ora2JvmCharset(oraCharset)));
			}
		};
	}

	static OraCdcDecoder get(final String oraCharset, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return new String(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted), ora2JvmCharset(oraCharset));
			}
		};
	}

	static OraCdcDecoder getWrapped(final String oraCharset, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return Optional.of(new String(decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted), ora2JvmCharset(oraCharset)));
			}
		};
	}

	static OraCdcDecoder get(final ZoneId zoneId, final boolean local) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return ISO_OFFSET_DATE_TIME.format(local
						? OracleTimestamp.toZonedDateTime(raw, off, len, zoneId)
						: TimestampWithTimeZone.toZonedDateTime(raw, off));
			}
		};
	}

	static OraCdcDecoder getWrapped(final ZoneId zoneId, final boolean local) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return Optional.of(ISO_OFFSET_DATE_TIME.format(local
						? OracleTimestamp.toZonedDateTime(raw, off, len, zoneId)
						: TimestampWithTimeZone.toZonedDateTime(raw, off)));
			}
		};
	}

	static OraCdcDecoder get(final ZoneId zoneId, final boolean local, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
				return ISO_OFFSET_DATE_TIME.format(local
						? OracleTimestamp.toZonedDateTime(plaintext, 0, plaintext.length, zoneId)
						: TimestampWithTimeZone.toZonedDateTime(plaintext, 0));
			}
		};
	}

	static OraCdcDecoder getWrapped(final ZoneId zoneId, final boolean local, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				final var plaintext = decrypter.decrypt(Arrays.copyOfRange(raw, off, off + len), salted);
				return ISO_OFFSET_DATE_TIME.format(local
						? OracleTimestamp.toZonedDateTime(plaintext, 0, plaintext.length, zoneId)
						: TimestampWithTimeZone.toZonedDateTime(plaintext, 0));
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

	static OraCdcDecoder getWrappedNUMBER(final int scale) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				if (raw.length == 0) return Optional.empty();
				try {
					var bd = toBigDecimal(raw);
					if (bd.scale() == scale)
						return Optional.of(bd);
					else if (bd.scale() > scale)
						return Optional.of(bd.setScale(scale, RoundingMode.HALF_UP));
					else
						return Optional.of(bd.setScale(scale, RoundingMode.UNNECESSARY));
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

	static OraCdcDecoder getWrappedNUMBER(final int scale, final OraCdcTdeColumnDecrypter decrypter, final boolean salted) {
		return new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				if (raw.length == 0) return Optional.empty();
				try {
					var bd = toBigDecimal(decrypter.decrypt(Arrays.copyOfRange(raw, 0, raw.length), salted));
					if (bd.scale() == scale)
						return Optional.of(bd);
					else if (bd.scale() > scale)
						return Optional.of(bd.setScale(scale, RoundingMode.HALF_UP));
					else
						return Optional.of(bd.setScale(scale, RoundingMode.UNNECESSARY));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, 0, raw.length, true, NUMERIC);
				}
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return Optional.empty();
				try {
					return decode(Arrays.copyOfRange(raw, off, off + len));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, true, NUMERIC);
				}
			}
		};
	}

	private static Object oraXml(final byte[] data, final boolean clob) throws SQLException {
		if (clob)
			return Optional.of(new String(data, UTF_16));
		else
			//TODO not all XML are in UTF-8!
			//TODO <?xml version="1.0" encoding="UTF-8"?>
			return Optional.of(new String(data, UTF_8));
	}

	private static Object oraJson(final byte[] data, final OracleJsonFactory jsonFactory) throws SQLException {
		try (OracleJsonParser parser =
				jsonFactory.createJsonBinaryParser(
						ByteBuffer.wrap(data))) {
			parser.next();
			return Optional.of(parser.getValue().toString());
		} catch(OracleJsonException oje) {
			throw new SQLException(oje);
		}
	}

	private static Object oraVector(final byte[] data) throws SQLException {
		if (data[0] != VECTOR_MAGIC_BYTE)
			throw new SQLException("Vector data don't start with a magic byte!");
		// final byte version = data[1];
		switch (data[4]) {
			case 2 -> {
				return Optional.of(VectorData.decode(data, float[].class, (getU16BE(data, 2) & 0x8) != 0));
			}
			case 3 -> {
				return Optional.of(VectorData.decode(data, double[].class, (getU16BE(data, 2) & 0x8) != 0));
			}
			case 4 -> {
				return Optional.of(VectorData.decode(data, byte[].class, false));
			}
			case 5 -> {
				return Optional.of(VectorData.decode(data, boolean[].class, false));
			}
			default ->  throw new SQLException("Unrecdognized VECTOR type " + data[4] + "!");
		}
	}

	static void traceLobInfo(final LobLocator ll, final byte[] ba, final String entity) {
		LOGGER.trace("Processing {} LID={}, DATALENGTH={}, DATA IN ROW?={}, LOB LOCATOR CONTENT=>{}",
				entity, ll.lid(), ll.dataLength(), ll.dataInRow(), rawToHex(ba));
	}

	static boolean toBoolean(final byte[] raw, final int off, final int len) throws SQLException {
		if (raw[off] == (byte) 1)
			return true;
		else if (raw[off] == (byte)0)
			return false;
		else
			throw new SQLException("Incorrect value " + String.format("0x%02x", raw[off]) + " for BOOLEAN (252) data type!");
	}

	static SQLException invalidNumberData(
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
					return Optional.ofNullable(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return Optional.ofNullable(transaction.getLob(ll));
			}
		});
		decoders.put(CLOB, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "CLOB");
				if (ll.dataInRow())
					return Optional.ofNullable(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return Optional.ofNullable(transaction.getLob(ll));
			}
		});
		decoders.put(NCLOB, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "NCLOB");
				if (ll.dataInRow())
					return Optional.ofNullable(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return Optional.ofNullable(transaction.getLob(ll));
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

		wrappedDecoders.put(BOOLEAN, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return Optional.empty();
				try {
					return Optional.of(toBoolean(raw, off, len));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, BOOLEAN);
				}
			}
		});
		wrappedDecoders.put(TINYINT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return Optional.empty();
				try {
					return Optional.of(toByte(raw, off, len));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, TINYINT);
				}
			}
		});
		wrappedDecoders.put(SMALLINT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return Optional.empty();
				try {
					return Optional.of(toShort(raw, off, len));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, SMALLINT);
				}
			}
		});
		wrappedDecoders.put(INTEGER, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return Optional.empty();
				try {
					return Optional.of(toInt(raw, off, len));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, INTEGER);
				}
			}
		});
		wrappedDecoders.put(BIGINT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return Optional.empty();
				try {
					return Optional.of(toLong(raw, off, len));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, BIGINT);
				}
			}
		});
		wrappedDecoders.put(FLOAT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				if (raw.length == 0) return Optional.empty();
				try {
					return Optional.of(toFloat(raw));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, 0, raw.length, false, FLOAT);
				}
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return Optional.empty();
				try {
					return decode(Arrays.copyOfRange(raw, off, off + len));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, FLOAT);
				}
			}
		});
		wrappedDecoders.put(BINARY_FLOAT, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				if (raw.length == 0) return Optional.empty();
				try {
					return Optional.of(new BINARY_FLOAT(raw).floatValue());
				} catch (Exception e) {
					throw new SQLException("Invalid Oracle BINARY_FLOAT data " + rawToHex(raw), e);
				}
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return Optional.empty();
				try {
					return Optional.of(new BINARY_FLOAT(Arrays.copyOfRange(raw, off, off + len)).floatValue());
				} catch (Exception e) {
					throw new SQLException("Invalid Oracle BINARY_FLOAT data " + rawToHex(raw), e);
				}
			}
		});
		wrappedDecoders.put(DOUBLE, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				if (raw.length == 0) return Optional.empty();
				try {
					return Optional.of(toDouble(raw));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, 0, raw.length, false, DOUBLE);
				}
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return Optional.empty();
				try {
					return Optional.of(toDouble(Arrays.copyOfRange(raw, off, off + len)));
				} catch (Exception e) {
					throw invalidNumberData(e, raw, off, len, false, DOUBLE);
				}
			}
		});
		wrappedDecoders.put(BINARY_DOUBLE, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw) throws SQLException {
				if (raw.length == 0) return Optional.empty();
				try {
					return Optional.of(new BINARY_DOUBLE(raw).doubleValue());
				} catch (Exception e) {
					throw new SQLException("Invalid Oracle BINARY_DOUBLE data " + rawToHex(raw), e);
				}
			}
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				if (len == 0) return Optional.empty();
				try {
					return Optional.of(new BINARY_DOUBLE(Arrays.copyOfRange(raw, off, off + len)).doubleValue());
				} catch (Exception e) {
					throw new SQLException("Invalid Oracle BINARY_DOUBLE data " + rawToHex(raw), e);
				}
			}
		});
		wrappedDecoders.put(DATE, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				try {
					return Optional.of(OracleDate.toTimestamp(raw, off));
				} catch (DateTimeException dte) {
					throw new SQLException(dte);
				}
			}
		});
		wrappedDecoders.put(TIMESTAMP, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				try {
					return Optional.of(OracleTimestamp.toTimestamp(raw, off));
				} catch (DateTimeException dte) {
					throw new SQLException(dte);
				}
			}
		});
		wrappedDecoders.put(JAVA_SQL_TYPE_INTERVALDS_STRING, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return Optional.of(IntervalDayToSecond.toString(raw, off));
			}
		});
		wrappedDecoders.put(JAVA_SQL_TYPE_INTERVALYM_STRING, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len) throws SQLException {
				return Optional.of(IntervalYearToMonth.toString(raw, off));
			}
		});
		wrappedDecoders.put(BLOB, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "BLOB");
				if (ll.dataInRow())
					return Optional.ofNullable(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return Optional.ofNullable(transaction.getLob(ll));
			}
		});
		wrappedDecoders.put(CLOB, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "CLOB");
				if (ll.dataInRow())
					return Optional.ofNullable(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return Optional.ofNullable(transaction.getLob(ll));
			}
		});
		wrappedDecoders.put(NCLOB, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "NCLOB");
				if (ll.dataInRow())
					return Optional.ofNullable(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return Optional.ofNullable(transaction.getLob(ll));
			}
		});
		wrappedDecoders.put(SQLXML, new OraCdcDecoder() {
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
		wrappedDecoders.put(VECTOR, new OraCdcDecoder() {
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
		wrappedDecoders.put(LONGNVARCHAR, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "extended size NVARCHAR2");
				if (ll.dataInRow())
					return Optional.of(new String(raw, off + len - ll.dataLength(), off + len, UTF_16));
				else
					return Optional.of(new String(transaction.getLob(ll), UTF_16));
			}
		});
		wrappedDecoders.put(LONGVARCHAR, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "extended size VARCHAR2");
				if (ll.dataInRow())
					return Optional.of(new String(raw, off + len - ll.dataLength(), off + len, UTF_16));
				else
					return Optional.of(new String(transaction.getLob(ll), UTF_16));
			}
		});
		wrappedDecoders.put(LONGVARBINARY, new OraCdcDecoder() {
			@Override
			public Object decode(final byte[] raw, final int off, final int len,
					final OraCdcTransaction transaction) throws SQLException {
				final var ll = new LobLocator(raw, off, len);
				if (LOGGER.isTraceEnabled()) traceLobInfo(ll, Arrays.copyOfRange(raw, off, off + len), "extended size RAW");
				if (ll.dataInRow())
					return Optional.of(Arrays.copyOfRange(raw, off + len - ll.dataLength(), off + len));
				else
					return Optional.of(transaction.getLob(ll));
			}
		});
	}

}
