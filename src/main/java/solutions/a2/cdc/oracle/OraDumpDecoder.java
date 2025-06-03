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

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Hashtable;

import org.apache.kafka.connect.data.Struct;

import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.NUMBER;
import oracle.sql.json.OracleJsonException;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonParser;
import solutions.a2.cdc.oracle.data.OraJson;
import solutions.a2.cdc.oracle.data.OraXml;
import solutions.a2.oracle.jdbc.types.OracleDate;
import solutions.a2.oracle.jdbc.types.OracleTimestamp;
import solutions.a2.oracle.jdbc.types.TimestampWithTimeZone;


/**
 * 
 * Wrapper for converting Oracle internal NUMBER/DATE/TIMESTAMP/VARCHAR2 representation to Java types
 * For more information about Oracle NUMBER format:
 *	   <a href="https://gotodba.com/2015/03/24/how-are-numbers-saved-in-oracle/">How are Numbers Saved in Oracle?</a>
 *	   <a href="https://www.orafaq.com/wiki/Number">Number</a>
 *     <a href="https://support.oracle.com/rs?type=doc&id=1031902.6">How Does Oracle Store Internal Numeric Data? (Doc ID 1031902.6)</a>
 *     <a href="https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/sql/NUMBER.html">Class NUMBER</a>
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class OraDumpDecoder {

	private final String nlsCharacterSet;
	private final String nlsNcharCharacterSet;
	private final OracleJsonFactory jsonFactory;

	private final static Hashtable<String, String> charsetMap = new Hashtable<>();
	private static final char[] HEX_CHARS_UPPER = new char[]
			{0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46};

	/**
	 * 
	 * @param nlsCharacterSet        - Oracle RDBMS NLS_CHARACTERSET
	 * @param nlsNcharCharacterSet   - Oracle RDBMS NLS_NCHAR_CHARACTERSET
	 */
	public OraDumpDecoder(final String nlsCharacterSet, final String nlsNcharCharacterSet) {
		this.nlsCharacterSet = charsetMap.get(nlsCharacterSet);
		this.nlsNcharCharacterSet = charsetMap.get(nlsNcharCharacterSet);
		this.jsonFactory = new OracleJsonFactory();
	}

	public static float fromBinaryFloat(final String hex) throws SQLException {
		return fromBinaryFloat(hexToRaw(hex));
	}

	public static float fromBinaryFloat(final byte[] data) throws SQLException {
		try {
			BINARY_FLOAT bf = new BINARY_FLOAT(data);
			return bf.floatValue();
		} catch (Exception e) {
			throw new SQLException("Invalid Oracle NUMBER", e);
		}
	}

	public static double fromBinaryDouble(final String hex) throws SQLException {
		return fromBinaryDouble(hexToRaw(hex));
	}

	public static double fromBinaryDouble(final byte[] data) throws SQLException {
		try {
			BINARY_DOUBLE bd = new BINARY_DOUBLE(data);
			return bd.doubleValue();
		} catch (Exception e) {
			throw new SQLException("Invalid Oracle NUMBER", e);
		}
	}

	public static BigDecimal toBigDecimal(final String hex) throws SQLException {
		return NUMBER.toBigDecimal(hexToRaw(hex));
	}

	public static BigDecimal toBigDecimal(final byte[] data) throws SQLException {
		try {
			return NUMBER.toBigDecimal(data);
		} catch (Exception e) {
			throw new SQLException("Invalid Oracle NUMBER", e);
		}
	}

	public String fromVarchar2(final String hex) throws SQLException {
		return fromVarchar2(hexToRaw(hex));
	}

	public String fromVarchar2(final byte[] data) throws SQLException {
		try {
			return new String(data, nlsCharacterSet);
		} catch (UnsupportedEncodingException e) {
			throw new SQLException("Invalid encoding " + nlsCharacterSet + " for " + rawToHex(data) +  ".", e);
		}
	}

	public String fromVarchar2(final byte[] data, final int offset, final int length) throws SQLException {
		try {
			return new String(data, offset, length, nlsCharacterSet);
		} catch (UnsupportedEncodingException e) {
			throw new SQLException("Invalid encoding " + nlsCharacterSet + " for " +
					rawToHex(Arrays.copyOfRange(data, offset, offset + length)) +  ".", e);
		}
	}

	public String fromNvarchar2(final String hex) throws SQLException {
		return fromNvarchar2(hexToRaw(hex));
	}

	public String fromNvarchar2(final byte[] data) throws SQLException {
		try {
			return new String(data, nlsNcharCharacterSet);
		} catch (UnsupportedEncodingException e) {
			throw new SQLException("Invalid encoding " + nlsNcharCharacterSet + " for " + rawToHex(data) +  ".", e);
		}
	}

	public String fromNvarchar2(final byte[] data, final int offset, final int length) throws SQLException {
		try {
			return new String(data, offset, length, nlsNcharCharacterSet);
		} catch (UnsupportedEncodingException e) {
			throw new SQLException("Invalid encoding " + nlsNcharCharacterSet + " for " + 
					rawToHex(Arrays.copyOfRange(data, offset, offset + length)) +  ".", e);
		}
	}

	public static String fromClobNclob(final String hex) throws SQLException {
		try {
			return new String(hexToRaw(hex), "UTF-16");
		} catch (UnsupportedEncodingException e) {
			throw new SQLException("Invalid encoding UTF-16 encoded CLOB/NCLOB for HEXTORAW " + hex +  ".", e);
		}
	}

	public static String fromClobNclob(final byte[] ba) throws SQLException {
		return fromClobNclob(ba, 0, ba.length);
	}

	public static String fromClobNclob(final byte[] ba, final int off, final int len) throws SQLException {
		try {
			return new String(ba, off, len, "UTF-16");
		} catch (UnsupportedEncodingException e) {
			throw new SQLException("Invalid encoding UTF-16 encoded CLOB/NCLOB for HEXTORAW " + rawToHex(ba) +  ".", e);
		}
	}

	/**
	 * 
	 * Convert Oracle Type 12 dump to LocalDateTime
	 * 
	 * Based on:
	 *    <a href="https://docs.oracle.com/en/database/oracle/oracle-database/12.2/jajdb/index.html?oracle/sql/TIMESTAMPTZ.html">Class TIMESTAMPTZ</a>
	 *    <a href="https://support.oracle.com/rs?type=doc&id=69028.1">How does Oracle store the DATE datatype internally? (Doc ID 69028.1)</a>
	 * 
	 * @param hex - Oracle Type 12 DATE
	 *              Oracle Type 180 TIMESTAMP
	 *              Oracle Type 181 TIMESTAMP WITH TIME ZONE
	 * @return
	 */
	public static Timestamp toTimestamp(final String hex) throws SQLException {
		return toTimestamp(hexToRaw(hex));
	}

	public static Timestamp toTimestamp(final byte[] data) throws SQLException {
		if (data.length == OracleDate.DATA_LENGTH) {
			return OracleDate.toTimestamp(data);
		} else if (data.length == OracleTimestamp.DATA_LENGTH) {
			return OracleTimestamp.toTimestamp(data);
		} else if (data.length == TimestampWithTimeZone.DATA_LENGTH) {
			return TimestampWithTimeZone.toTimestamp(data);
		} else {
			throw new SQLException("Invalid Oracle HEX value DATE/TIMESTAMP - " + rawToHex(data) + "!");
		}
	}

	public static Timestamp toTimestamp(final byte[] data, final int offset, final int length) throws SQLException {
		if (length == OracleDate.DATA_LENGTH) {
			return OracleDate.toTimestamp(data, offset);
		} else if (length == OracleTimestamp.DATA_LENGTH) {
			return OracleTimestamp.toTimestamp(data, offset);
		} else if (length == TimestampWithTimeZone.DATA_LENGTH) {
			return TimestampWithTimeZone.toTimestamp(data, offset);
		} else {
			throw new SQLException("Invalid Oracle HEX value DATE/TIMESTAMP - " + rawToHex(data) + "!");
		}
	}

	public static byte[] hexToRaw(final String hex) {
		final int len = hex.length();
		final byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4) +
									Character.digit(hex.charAt(i+1), 16));
		}
		return data;
	}

	public static String toHexString(final byte[] hex) {
		final char[] data = new char[hex.length * 2];
		for (int i = 0; i < hex.length; i++) {
			data[i << 1] = HEX_CHARS_UPPER[(hex[i] >> 4) & 0xF];
			data[(i << 1) + 1] = HEX_CHARS_UPPER[(hex[i] & 0xF)];
		}
		return new String(data);
	}

	public static String rawToHex(final byte[] data) {
		final StringBuilder sb = new StringBuilder(data.length * 3 + 0x20);
		sb.append("HEXTORAW('");
		for (int i = 0; i < data.length; i++) {
			sb.append(String.format("%02x", data[i]));
		}
		sb.append("')");
		return sb.toString();
	}

	public Struct toOraXml(final byte[] data, final boolean clob) throws SQLException {
		return toOraXml(data, 0, data.length, clob);
	}

	public Struct toOraXml(final byte[] data, final int off, final int len, final boolean clob) throws SQLException {
		final Struct xml = new Struct(OraXml.schema());
		if (clob) {
			try {
				xml.put("V",  new String(data, off, len, "UTF-16"));
			} catch (UnsupportedEncodingException e) {
				throw new SQLException("Invalid encoding for UTF-16 encoded XML for HEXTORAW " + rawToHex(data) +  ".", e);
			}
		} else {
			//TODO not all XML are in UTF-8!
			//TODO <?xml version="1.0" encoding="UTF-8"?>
			String charsetName = "UTF-8";
			try {
				xml.put("V", new String(data, off, len, charsetName));
			} catch (UnsupportedEncodingException e) {
				throw new SQLException("Invalid encoding " + charsetName + " in binary XML for HEXTORAW " + rawToHex(data) +  ".", e);
			}
		}
		return xml;
	}

	public Struct toOraJson(final byte[] data) throws SQLException {
		return toOraJson(data, 0, data.length);
	}

	public Struct toOraJson(final byte[] data, final int off, final int len) throws SQLException {
		try (OracleJsonParser parser =
				jsonFactory.createJsonBinaryParser(
						ByteBuffer.wrap(data, off, len))) {
			parser.next();
			final Struct json = new Struct(OraJson.schema());
			json.put("V", parser.getValue().toString());
			return json;
		} catch(OracleJsonException oje) {
			throw new SQLException(oje);
		}
	}

	static {

		charsetMap.put("AL16UTF16", "UTF-16BE");
		charsetMap.put("AL16UTF16LE", "UTF-16LE");
		charsetMap.put("AL24UTFFSS", "UTF-8");
		charsetMap.put("AL32UTF8", "UTF-8");
		charsetMap.put("AR8ARABICMAC", "MacArabic");
		charsetMap.put("AR8ARABICMACS", "MacArabic");
		charsetMap.put("AR8ARABICMACT", "MacArabic");
		charsetMap.put("AR8EBCDIC420S", "CP420");
		charsetMap.put("AR8EBCDICX", "CP420");
		charsetMap.put("AR8ISO8859P6", "ISO-8859-6");
		charsetMap.put("AR8MSAWIN", "WINDOWS-1256");
		charsetMap.put("AR8MSWIN1256", "WINDOWS-1256");
		charsetMap.put("BLT8CP921", "CP921");
		charsetMap.put("BLT8EBCDIC1112", "CP1112");
		charsetMap.put("BLT8EBCDIC1112S", "CP1112");
		charsetMap.put("BLT8ISO8859P13", "ISO-8859-13");
		charsetMap.put("BLT8MSWIN1257", "WINDOWS-1257");
		charsetMap.put("BLT8PC775", "CP775");
		charsetMap.put("CDN8PC863", "CP863");
		charsetMap.put("CL8EBCDIC1025", "CP1025");
		charsetMap.put("CL8EBCDIC1025C", "CP1025");
		charsetMap.put("CL8EBCDIC1025R", "CP1025");
		charsetMap.put("CL8EBCDIC1025S", "CP1025");
		charsetMap.put("CL8EBCDIC1025X", "CP1025");
		charsetMap.put("CL8ISO8859P5", "ISO-8859-5");
		charsetMap.put("CL8KOI8R", "KOI8-R");
		charsetMap.put("CL8MACCYRILLIC", "MacCyrillic");
		charsetMap.put("CL8MACCYRILLICS", "MacCyrillic");
		charsetMap.put("CL8MSWIN1251", "WINDOWS-1251");
		charsetMap.put("D8EBCDIC1141", "Cp1141");
		charsetMap.put("D8EBCDIC273", "CP273");
		charsetMap.put("DK8EBCDIC1142", "Cp1142");
		charsetMap.put("DK8EBCDIC277", "IBM277");
		charsetMap.put("EE8EBCDIC870", "CP870");
		charsetMap.put("EE8EBCDIC870C", "CP870");
		charsetMap.put("EE8EBCDIC870S", "CP870");
		charsetMap.put("EE8ISO8859P2", "ISO-8859-2");
		charsetMap.put("EE8MACCE", "MacCentralEurope");
		charsetMap.put("EE8MACCES", "MacCentralEurope");
		charsetMap.put("EE8MACCROATIAN", "MacCroatian");
		charsetMap.put("EE8MACCROATIANS", "MacCroatian");
		charsetMap.put("EE8MSWIN1250", "WINDOWS-1250");
		charsetMap.put("EE8PC852", "CP852");
		charsetMap.put("EL8EBCDIC875", "CP875");
		charsetMap.put("EL8EBCDIC875R", "CP875");
		charsetMap.put("EL8EBCDIC875S", "CP875");
		charsetMap.put("EL8ISO8859P7", "ISO-8859-7");
		charsetMap.put("EL8MSWIN1253", "WINDOWS-1253");
		charsetMap.put("EL8PC737", "CP737");
		charsetMap.put("EL8PC869", "CP869");
		charsetMap.put("F8EBCDIC1147", "Cp1147");
		charsetMap.put("F8EBCDIC297", "CP297");
		charsetMap.put("I8EBCDIC1144", "Cp1144");
		charsetMap.put("I8EBCDIC280", "CP280");
		charsetMap.put("IS8MACICELANDIC", "MacIceland");
		charsetMap.put("IS8MACICELANDICS", "MacIceland");
		charsetMap.put("IS8PC861", "CP861");
		charsetMap.put("ISO2022-CN", "ISO2022CN_GB");
		charsetMap.put("ISO2022-JP", "ISO-2022-JP");
		charsetMap.put("ISO2022-KR", "ISO-2022-KR");
		charsetMap.put("IW8EBCDIC424", "CP424");
		charsetMap.put("IW8EBCDIC424S", "CP424");
		charsetMap.put("IW8ISO8859P8", "ISO-8859-8");
		charsetMap.put("IW8MACHEBREW", "MacHebrew");
		charsetMap.put("IW8MACHEBREWS", "MacHebrew");
		charsetMap.put("IW8MSWIN1255", "WINDOWS-1255");
		charsetMap.put("IW8PC1507", "CP862");
		charsetMap.put("JA16EBCDIC930", "CP930");
		charsetMap.put("JA16EUC", "EUC-JP");
		charsetMap.put("JA16EUCTILDE", "EUC-JP");
		charsetMap.put("JA16EUCYEN", "EUC-JP");
		charsetMap.put("JA16SJIS", "MS932");
		charsetMap.put("JA16SJISTILDE", "MS932");
		charsetMap.put("JA16SJISYEN", "MS932");
		charsetMap.put("JA16VMS", "EUC-JP");
		charsetMap.put("KO16KSC5601", "MS949");
		charsetMap.put("KO16MSWIN949", "MS949");
		charsetMap.put("LT8MSWIN921", "CP921");
		charsetMap.put("N8PC865", "CP865");
		charsetMap.put("NEE8ISO8859P4", "ISO-8859-4");
		charsetMap.put("RU8PC855", "CP855");
		charsetMap.put("RU8PC866", "CP866");
		charsetMap.put("S8EBCDIC1143", "Cp1143");
		charsetMap.put("S8EBCDIC278", "CP278");
		charsetMap.put("SE8ISO8859P3", "ISO-8859-3");
		charsetMap.put("TH8MACTHAI", "MacThai");
		charsetMap.put("TH8MACTHAIS", "MacThai");
		charsetMap.put("TH8TISASCII", "MS874");
		charsetMap.put("TH8TISEBCDIC", "CP838");
		charsetMap.put("TH8TISEBCDICS", "CP838");
		charsetMap.put("TR8EBCDIC1026", "CP1026");
		charsetMap.put("TR8EBCDIC1026S", "CP1026");
		charsetMap.put("TR8MACTURKISH", "MacTurkish");
		charsetMap.put("TR8MACTURKISHS", "MacTurkish");
		charsetMap.put("TR8MSWIN1254", "WINDOWS-1254");
		charsetMap.put("TR8PC857", "CP857");
		charsetMap.put("UCS2", "UTF-16");
		charsetMap.put("US7ASCII", "US-ASCII");
		charsetMap.put("US8PC437", "CP437");
		charsetMap.put("UTF16", "UTF-16");
		charsetMap.put("UTF8", "UTF-8");
		charsetMap.put("VN8MSWIN1258", "WINDOWS-1258");
		charsetMap.put("WE8EBCDIC1140", "Cp1140");
		charsetMap.put("WE8EBCDIC1140C", "Cp1140");
		charsetMap.put("WE8EBCDIC1145", "Cp1145");
		charsetMap.put("WE8EBCDIC1146", "Cp1146");
		charsetMap.put("WE8EBCDIC1148", "Cp1148");
		charsetMap.put("WE8EBCDIC1148C", "Cp1148");
		charsetMap.put("WE8EBCDIC284", "CP284");
		charsetMap.put("WE8EBCDIC285", "CP285");
		charsetMap.put("WE8EBCDIC37", "CP037");
		charsetMap.put("WE8EBCDIC37C", "CP037");
		charsetMap.put("WE8EBCDIC500", "CP500");
		charsetMap.put("WE8EBCDIC500C", "CP500");
		charsetMap.put("WE8EBCDIC871", "CP871");
		charsetMap.put("WE8ISO8859P1", "ISO-8859-1");
		charsetMap.put("WE8ISO8859P15", "ISO-8859-15");
		charsetMap.put("WE8ISO8859P9", "ISO-8859-9");
		charsetMap.put("WE8MACROMAN8", "MacRoman");
		charsetMap.put("WE8MACROMAN8S", "MacRoman");
		charsetMap.put("WE8MSWIN1252", "WINDOWS-1252");
		charsetMap.put("WE8PC850", "CP850");
		charsetMap.put("WE8PC858", "Cp858");
		charsetMap.put("WE8PC860", "CP860");
		charsetMap.put("ZHS16CGB231280", "EUC_CN");
		charsetMap.put("ZHS16GBK", "MS936");
		charsetMap.put("ZHS32GB18030", "GB18030");
		charsetMap.put("ZHT16BIG5", "MS950");
		charsetMap.put("ZHT16HKSCS", "MS950_HKSCS");
		charsetMap.put("ZHT16MSWIN950", "MS950");
		charsetMap.put("ZHT32EUC", "EUC-TW");
	}

}
