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
import static java.sql.Types.DOUBLE;
import static java.sql.Types.BLOB;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.SQLXML;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static oracle.jdbc.OracleTypes.BINARY_FLOAT;
import static oracle.jdbc.OracleTypes.BINARY_DOUBLE;
import static oracle.jdbc.OracleTypes.VECTOR;
import static oracle.sql.NUMBER.toDouble;
import static solutions.a2.cdc.oracle.runtime.data.RdbmsCharsetMapping.ora2JvmCharset;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toByte;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toInt;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toLong;
import static solutions.a2.oracle.jdbc.types.OracleNumber.toShort;
import static solutions.a2.oracle.utils.BinaryUtils.getU16BE;
import static solutions.a2.oracle.utils.BinaryUtils.getU32BE;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
public class KafkaConnectDecoders extends GenericDecoders {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectDecoders.class);
	
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
		switch (jdbcType) {
			case BLOB -> {
				return new OraCdcDecoder() {
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
				};
			}
			case CLOB -> {
				return new OraCdcDecoder() {
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
				};
			}
			case NCLOB -> {
				return new OraCdcDecoder() {
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
				};
			}
			case SQLXML -> {
				return new OraCdcDecoder() {
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
				};
			}
			case VECTOR -> {
				return new OraCdcDecoder() {
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
				};
			}
			default -> {
				return GenericDecoders.get(jdbcType);
			}
		}
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

	private static Struct oraVector(final byte[] data) throws SQLException {
		final var vector = new Struct(OraVector.schema());
		if (data[0] != VECTOR_MAGIC_BYTE)
			throw new SQLException("Vector data don't start with a magic byte!");
		// final byte version = data[1];
		final int size = getU32BE(data, 5);
		switch (data[4]) {
			case 2 -> {
				final List<Float> f32 = new ArrayList<Float>(size);
				for (float f : VectorData.decode(data, float[].class, (getU16BE(data, 2) & 0x8) != 0))
					f32.add(f);
				vector.put("F", f32);
			}
			case 3 -> {
				final List<Double> f64 = new ArrayList<Double>(size);
				for (double d : VectorData.decode(data, double[].class, (getU16BE(data, 2) & 0x8) != 0))
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
			default ->  throw new SQLException("Unrecdognized VECTOR type " + data[4] + "!");
		}
		return vector;
	}

}
