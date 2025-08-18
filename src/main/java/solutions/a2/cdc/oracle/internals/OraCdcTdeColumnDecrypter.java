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

package solutions.a2.cdc.oracle.internals;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.crypto.Cipher.DECRYPT_MODE;
import static solutions.a2.cdc.oracle.OraDictSqlTexts.COLUMN_ENCRYPTION_INFO;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcTdeColumnDecrypter {

	public static final byte _3DES168 = 0x01;
	public static final byte AES128 = 0x02;
	public static final byte AES192 = 0x04;
	public static final byte AES256 = 0x08;
	public static final byte SHA_1 = 0x10;
	public static final byte NOMAC = 0x20;
	public static final byte GCMTAG = 0x40;

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTdeColumnDecrypter.class);
	private static final byte[] IV_CBC_NOSALT_AES = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
	private static final byte[] IV_CBC_NOSALT_3DES = {0, 0, 0, 0, 0, 0, 0, 0};

	private byte parameters = 0;
	private final Cipher cipher;
	private final SecretKeySpec secretKey;

	OraCdcTdeColumnDecrypter(
			final byte[] decDataKey, final byte encAlg, final byte intAlg) throws IOException {
		final int keyLength;
		if (encAlg == 1 || encAlg == 3) {
			parameters = (byte) (parameters | (encAlg == 1 ? _3DES168 : AES192));
			keyLength = 0x18;
		} else if (encAlg == 2) {
			parameters |= AES128;
			keyLength = 0x10;
		} else if (encAlg == 4) {
			parameters |= AES256;
			keyLength = 0x20;
		} else {
			throw new IOException("Unknown or unsupported ENC$.ENCALG " + encAlg + " !");
		}
		if (intAlg == 1)
			parameters |= SHA_1;
		else if (intAlg == 2)
			parameters |= NOMAC;
		else if (intAlg == 3)
			parameters |= GCMTAG;
		else
			throw new IOException("Unknown or unsupported ENC$.INTALG " + intAlg + " !");
		secretKey = new SecretKeySpec(Arrays.copyOfRange(decDataKey, 0x10, keyLength + 0x10), encAlg == 1 ? "DESede" : "AES");
		try {
			if (encAlg == 1)
				cipher = Cipher.getInstance("DESede/CBC/NoPadding");
			else {
				if (intAlg == 3)
					cipher = Cipher.getInstance("AES/GCM/NoPadding");
				else
					cipher = Cipher.getInstance("AES/CBC/NoPadding");
			}
		} catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
			throw new IOException(e);
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Cipher set to {} with key set to '{}'", cipher.getAlgorithm(), rawToHex(Arrays.copyOfRange(decDataKey, 0x10, keyLength + 0x10)));
		}
	}

	public static OraCdcTdeColumnDecrypter get(
			final Connection connection, final OraCdcTdeWallet wallet,
			final String tableOwner, final String tableName) throws SQLException {
		OraCdcTdeColumnDecrypter decrypter = null;
		PreparedStatement statement = connection.prepareStatement(COLUMN_ENCRYPTION_INFO);
		statement.setString(1, tableOwner);
		statement.setString(2, tableName);
		ResultSet resultSet = statement.executeQuery();
		if (resultSet.next()) {
			try {
				final byte[] masterKeyId = resultSet.getBytes("MKEYID");
				int pos = masterKeyId.length - 1;
				for (; pos > -1; )
					if (masterKeyId[pos] != 0) break;
					else pos--;
				final byte[] decDataKey = wallet.decryptDataKey(
						new String(Arrays.copyOf(masterKeyId, pos + 1)), 
						Base64.getDecoder().decode(resultSet.getBytes("COLKLC")));
				decrypter = new OraCdcTdeColumnDecrypter(decDataKey,
						resultSet.getByte("ENCALG"), resultSet.getByte("INTALG"));
			} catch (IOException ioe) {
				throw new SQLException(ioe);
			}
		} else {
			LOGGER.error(
					"\n=====================\n" +
					"Unable to create OraCdcTdeColumnDecrypter. No data found while executing\n{}\nwith parameters '{}' '{}'" +
					"\n=====================\n",
					COLUMN_ENCRYPTION_INFO, tableOwner, tableName);
			throw new SQLException("Unable to create OraCdcTdeColumnDecrypter!");
		}
		resultSet.close();
		resultSet = null;
		statement.close();
		statement = null;
		return decrypter;
	}

	public byte[] decrypt(final byte[] columnData, final boolean salt) throws SQLException {
		final AlgorithmParameterSpec iv;
		int cipherTextLen = columnData.length;
		if (salt) {
			if ((parameters & _3DES168) != 0)
				cipherTextLen -= 0x8;
			else
				cipherTextLen -= 0x10;
			if ((parameters & GCMTAG) != 0)
				iv = new GCMParameterSpec(0x80, columnData, cipherTextLen, 0x10);
			else
				iv = new IvParameterSpec(columnData, cipherTextLen, (parameters & _3DES168) != 0 ? 0x8 : 0x10);
		} else {
			if ((parameters & _3DES168) != 0)
				iv = new IvParameterSpec(IV_CBC_NOSALT_3DES);
			else
				iv = new IvParameterSpec(IV_CBC_NOSALT_AES);
		}
		if ((parameters & SHA_1) != 0)
			cipherTextLen -= 0x14;
		else if ((parameters & GCMTAG) != 0)
			cipherTextLen -= 0x10;
		
		try {
			if ((parameters & GCMTAG) != 0) {
				cipher.init(DECRYPT_MODE, secretKey, iv);
				cipher.updateAAD(Arrays.copyOfRange(columnData, cipherTextLen, cipherTextLen + 0x10));
			} else
				cipher.init(DECRYPT_MODE, secretKey, iv);
			byte[] plaintext = cipher.doFinal(columnData, 0, cipherTextLen);
			int padBytes = padOrclBytes(plaintext);
			if (padBytes == 0)
				return plaintext;
			else
				return Arrays.copyOfRange(plaintext, 0, plaintext.length - padBytes);
		} catch (IllegalBlockSizeException | BadPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
			throw new SQLException(e);
		}
	}

	private int padOrclBytes(final byte[] plaintext) {
		int l = plaintext.length - 1;
		if (plaintext[l] == 0x1)
			return 0x1;
		else if (plaintext[l] == 0x2 && plaintext[l-1] == 0x2)
			return 0x2;
		else if (plaintext[l] == 0x3 && plaintext[l-1] == 0x3 && plaintext[l-2] == 0x3)
			return 0x3;
		else if (plaintext[l] == 0x4 && plaintext[l-1] == 0x4 && plaintext[l-2] == 0x4 && plaintext[l-3] == 0x4)
			return 0x4;
		else if (plaintext[l] == 0x5 && plaintext[l-1] == 0x5 && plaintext[l-2] == 0x5 && plaintext[l-3] == 0x5 && plaintext[l-4] == 0x5)
			return 0x5;
		else if (plaintext[l] == 0x6 && plaintext[l-1] == 0x6 && plaintext[l-2] == 0x6 && plaintext[l-3] == 0x6 && plaintext[l-4] == 0x6 && plaintext[l-5] == 0x6)
			return 0x6;
		else if (plaintext[l] == 0x7 && plaintext[l-1] == 0x7 && plaintext[l-2] == 0x7 && plaintext[l-3] == 0x7 && plaintext[l-4] == 0x7 && plaintext[l-5] == 0x7 && plaintext[l-6] == 0x7)
			return 0x7;
		else if (plaintext[l] == 0x8 && plaintext[l-1] == 0x8 && plaintext[l-2] == 0x8 && plaintext[l-3] == 0x8 && plaintext[l-4] == 0x8 && plaintext[l-5] == 0x8 && plaintext[l-6] == 0x8 && plaintext[l-7] == 0x8)
			return 0x8;
		else if (plaintext[l] == 0x9 && plaintext[l-1] == 0x9 && plaintext[l-2] == 0x9 && plaintext[l-3] == 0x9 && plaintext[l-4] == 0x9 && plaintext[l-5] == 0x9 && plaintext[l-6] == 0x9 && plaintext[l-7] == 0x9 && plaintext[l-8] == 0x9)
			return 0x9;
		else if (plaintext[l] == 0xA && plaintext[l-1] == 0xA && plaintext[l-2] == 0xA && plaintext[l-3] == 0xA && plaintext[l-4] == 0xA && plaintext[l-5] == 0xA && plaintext[l-6] == 0xA && plaintext[l-7] == 0xA && plaintext[l-8] == 0xA && plaintext[l-9] == 0xA)
			return 0xA;
		else if (plaintext[l] == 0xB && plaintext[l-1] == 0xB && plaintext[l-2] == 0xB && plaintext[l-3] == 0xB && plaintext[l-4] == 0xB && plaintext[l-5] == 0xB && plaintext[l-6] == 0xB && plaintext[l-7] == 0xB && plaintext[l-8] == 0xB && plaintext[l-9] == 0xB && plaintext[l-10] == 0xB)
			return 0xB;
		else if (plaintext[l] == 0xC && plaintext[l-1] == 0xC && plaintext[l-2] == 0xC && plaintext[l-3] == 0xC && plaintext[l-4] == 0xC && plaintext[l-5] == 0xC && plaintext[l-6] == 0xC && plaintext[l-7] == 0xC && plaintext[l-8] == 0xC && plaintext[l-9] == 0xC && plaintext[l-10] == 0xC && plaintext[l-11] == 0xC)
			return 0xC;
		else if (plaintext[l] == 0xD && plaintext[l-1] == 0xD && plaintext[l-2] == 0xD && plaintext[l-3] == 0xD && plaintext[l-4] == 0xD && plaintext[l-5] == 0xD && plaintext[l-6] == 0xD && plaintext[l-7] == 0xD && plaintext[l-8] == 0xD && plaintext[l-9] == 0xD && plaintext[l-10] == 0xD && plaintext[l-11] == 0xD && plaintext[l-12] == 0xD)
			return 0xD;
		else if (plaintext[l] == 0xE && plaintext[l-1] == 0xE && plaintext[l-2] == 0xE && plaintext[l-3] == 0xE && plaintext[l-4] == 0xE && plaintext[l-5] == 0xE && plaintext[l-6] == 0xE && plaintext[l-7] == 0xE && plaintext[l-8] == 0xE && plaintext[l-9] == 0xE && plaintext[l-10] == 0xE && plaintext[l-11] == 0xE && plaintext[l-12] == 0xE && plaintext[l-13] == 0xE)
			return 0xE;
		else if (plaintext[l] == 0xF && plaintext[l-1] == 0xF && plaintext[l-2] == 0xF && plaintext[l-3] == 0xF && plaintext[l-4] == 0xF && plaintext[l-5] == 0xF && plaintext[l-6] == 0xF && plaintext[l-7] == 0xF && plaintext[l-8] == 0xF && plaintext[l-9] == 0xF && plaintext[l-10] == 0xF && plaintext[l-11] == 0xF && plaintext[l-12] == 0xF && plaintext[l-13] == 0xF && plaintext[l-14] == 0xF)
			return 0xF;
		else if (plaintext[l] == 0x10 && plaintext[l-1] == 0x10 && plaintext[l-2] == 0x10 && plaintext[l-3] == 0x10 && plaintext[l-4] == 0x10 && plaintext[l-5] == 0x10 && plaintext[l-6] == 0x10 && plaintext[l-7] == 0x10 && plaintext[l-8] == 0x10 && plaintext[l-9] == 0x10 && plaintext[l-10] == 0x10 && plaintext[l-11] == 0x10 && plaintext[l-12] == 0x10 && plaintext[l-13] == 0x10 && plaintext[l-14] == 0x10 && plaintext[l-15] == 0x10)
			return 0x10;
		else
			return 0;
	}

}
