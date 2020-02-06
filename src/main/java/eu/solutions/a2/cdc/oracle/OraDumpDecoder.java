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

package eu.solutions.a2.cdc.oracle;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Hashtable;


/**
 * 
 * Wrapper for converting Oracle internal NUMBER/DATE/TIMESTAMP/VARCHAR2 representation to Java types
 * For more information about Oracle NUMBER format:
 *	   https://gotodba.com/2015/03/24/how-are-numbers-saved-in-oracle/
 *	   https://www.orafaq.com/wiki/Number
 *     https://support.oracle.com/rs?type=doc&id=1031902.6
 *       (How Does Oracle Store Internal Numeric Data? (Doc ID 1031902.6))
 * * 
 * @author averemee
 *
 */
public class OraDumpDecoder {

	private final String nlsCharacterSet;
	private final String nlsNcharCharacterSet;
	private final Class<?> klazzNUMBER;
	private final Method methodNUMBERtoByte;
	private final Method methodNUMBERtoShort;
	private final Method methodNUMBERtoInt;
	private final Method methodNUMBERtoLong;
	private final Method methodNUMBERtoBigInteger;
	private final Method methodNUMBERtoFloat;
	private final Method methodNUMBERtoDouble;
	private final Method methodNUMBERtoBigDecimal;

	private final static Hashtable<String, String> charsetMap = new Hashtable<>(131);
	private final static int TS_OFFSET_HOUR = 20;
	private final static int TS_OFFSET_MINUTE = 60;

	/**
	 * 
	 * @param nlsCharacterSet        - Oracle RDBMS NLS_CHARACTERSET
	 * @param nlsNcharCharacterSet   - Oracle RDBMS NLS_NCHAR_CHARACTERSET
	 * @throws ClassNotFoundException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 */
	OraDumpDecoder(final String nlsCharacterSet, final String nlsNcharCharacterSet)
			throws ClassNotFoundException, NoSuchMethodException, SecurityException {
		this.nlsCharacterSet = charsetMap.get(nlsCharacterSet);
		this.nlsNcharCharacterSet = charsetMap.get(nlsNcharCharacterSet);
		klazzNUMBER = Class.forName("oracle.sql.NUMBER");
		methodNUMBERtoByte = klazzNUMBER.getMethod("toByte", byte[].class);
		methodNUMBERtoShort = klazzNUMBER.getMethod("toShort", byte[].class);
		methodNUMBERtoInt = klazzNUMBER.getMethod("toInt", byte[].class);
		methodNUMBERtoLong = klazzNUMBER.getMethod("toLong", byte[].class);
		methodNUMBERtoBigInteger = klazzNUMBER.getMethod("toBigInteger", byte[].class);
		methodNUMBERtoFloat = klazzNUMBER.getMethod("toFloat", byte[].class);
		methodNUMBERtoDouble = klazzNUMBER.getMethod("toDouble", byte[].class);
		methodNUMBERtoBigDecimal = klazzNUMBER.getMethod("toBigDecimal", byte[].class);
	}

	public byte toByte(String hex) throws SQLException {
		try {
			return (byte) methodNUMBERtoByte.invoke(null, hexStringToByteArray(hex));
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new SQLException("Invalid Oracle NUMBER", e);
		}
	}

	public short toShort(String hex) throws SQLException {
		try {
			return (short) methodNUMBERtoShort.invoke(null, hexStringToByteArray(hex));
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new SQLException("Invalid Oracle NUMBER", e);
		}
	}

	public int toInt(String hex) throws SQLException {
		try {
			return (int) methodNUMBERtoInt.invoke(null, hexStringToByteArray(hex));
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new SQLException("Invalid Oracle NUMBER", e);
		}
	}

	public long toLong(String hex) throws SQLException {
		try {
			return (long) methodNUMBERtoLong.invoke(null, hexStringToByteArray(hex));
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new SQLException("Invalid Oracle NUMBER", e);
		}
	}

	public BigInteger toBigInteger(String hex) throws SQLException {
		try {
			return (BigInteger) methodNUMBERtoBigInteger.invoke(null, hexStringToByteArray(hex));
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new SQLException("Invalid Oracle NUMBER", e);
		}
	}

	public float toFloat(String hex) throws SQLException {
		try {
			return (float) methodNUMBERtoFloat.invoke(null, hexStringToByteArray(hex));
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new SQLException("Invalid Oracle NUMBER", e);
		}
	}

	public double toDouble(String hex) throws SQLException {
		try {
			return (double) methodNUMBERtoDouble.invoke(null, hexStringToByteArray(hex));
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new SQLException("Invalid Oracle NUMBER", e);
		}
	}

	public BigDecimal toBigDecimal(String hex) throws SQLException {
		try {
			return (BigDecimal) methodNUMBERtoBigDecimal.invoke(null, hexStringToByteArray(hex));
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new SQLException("Invalid Oracle NUMBER", e);
		}
	}

	public String fromVarchar2(String hex) throws SQLException {
		try {
			return new String(hexStringToByteArray(hex), nlsCharacterSet);
		} catch (UnsupportedEncodingException e) {
			throw new SQLException("Invalid encoding " + nlsCharacterSet + " for HEXTORAW " + hex +  ".", e);
		}
	}

	public String fromNvarchar2(String hex) throws SQLException {
		try {
			return new String(hexStringToByteArray(hex), nlsNcharCharacterSet);
		} catch (UnsupportedEncodingException e) {
			throw new SQLException("Invalid encoding " + nlsNcharCharacterSet + " for HEXTORAW " + hex +  ".", e);
		}
	}

	/**
	 * 
	 * Convert Oracle Type 12 dump to LocalDateTime
	 * 
	 * Based on:
	 *    How does Oracle store the DATE datatype internally? (Doc ID 69028.1)
	 *    https://support.oracle.com/rs?type=doc&id=69028.1
	 * 
	 * @param hex - Oracle Type 12 DATE
	 *              Oracle Type 180 TIMESTAMP
	 *              Oracle Type 181 TIMESTAMP WITH TIME ZONE
	 * @return
	 */
	public Timestamp toTimestamp(String hex) throws SQLException {
		int[] data = hexStringToIntArray(hex);
		int year = (data[0] - 100) *100 +			// 1st byte century - 100
				(data[1] - 100);					// 2nd byte year - 100
		if (data.length == 7 || data.length == 11) {
			final Calendar calendar = Calendar.getInstance();
			// Oracle Type 12 (7 byte) or Oracle Type 180 (11 byte)
			calendar.set(year, data[2] - 1, data[3], data[4] - 1, data[5] - 1, data[6] - 1);
			Timestamp ts = new Timestamp(calendar.getTime().getTime());
			if (data.length == 11) {
				ts.setNanos(getOraTsNanos(data));
			} else {
				ts.setNanos(0);
			}
			return ts;
		} else if (data.length == 13) {
			// Oracle Type 181
			final Calendar calendar = Calendar.getInstance();
			calendar.set(year, data[2] - 1, data[3], data[4] - 1, data[5] - 1, data[6] - 1);
			if ((data[11] & 0x80) == 0x0) {
				calendar.add(Calendar.HOUR, data[11] - TS_OFFSET_HOUR);
				calendar.add(Calendar.MINUTE, data[12] - TS_OFFSET_MINUTE);
			} else {
				throw new SQLException("TIMESTAMP WITH TIME ZONE with TZ name currently unsupported!!!");
			}
			Timestamp ts = new Timestamp(calendar.getTime().getTime());
			ts.setNanos(getOraTsNanos(data));
			return ts;
		} else {
			//TODO
			//TODO
			//TODO Need to handle this & DST!!!
			//TODO
			//TODO
			throw new SQLException("Invalid Oracle HEX value DATE/TIMESTAMP - " + hex + "!");
		}
	}

	private static byte[] hexStringToByteArray(String hex) {
		int len = hex.length();
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4) +
									Character.digit(hex.charAt(i+1), 16));
		}
		return data;
	}

	private static int[] hexStringToIntArray(String hex) {
		int len = hex.length();
		int[] data = new int[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = ((Character.digit(hex.charAt(i), 16) << 4) +
							Character.digit(hex.charAt(i+1), 16));
		}
		return data;
	}

	private static int getOraTsNanos(int[] ts) {
		return ts[7] << 24 | ts[8] << 16 | ts[9] << 8 | ts[10];
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
