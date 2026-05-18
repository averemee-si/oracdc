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

package solutions.a2.cdc.oracle.internals;

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import oracle.security.pki.OracleSecretStoreException;
import oracle.security.pki.OracleWallet;

import solutions.a2.oracle.utils.BinaryUtils;

import static javax.crypto.Cipher.SECRET_KEY;
import static javax.crypto.Cipher.UNWRAP_MODE;
import static solutions.a2.oracle.jdbc.types.OracleDate.toLocalDateTime;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;


/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcTdeWallet {

	private final OracleWallet wallet;
	private final Map<String, Kek> secrets;
	private String masterKeyId = null;

	private static final Logger LOGGER = LogManager.getLogger(OraCdcTdeWallet.class);
	private static final String KEY_PREFIX = "ORACLE.SECURITY.DB.ENCRYPTION.";

	OraCdcTdeWallet(String path, String password) throws IOException {
		this(BinaryUtils.get(true), path, password.toCharArray());
	}

	OraCdcTdeWallet(BinaryUtils bu, String path, char[] password) throws IOException {
		wallet = new OracleWallet();
		try {
			secrets = new HashMap<>();
			final var decoder = Base64.getDecoder();
			wallet.open(path, password);
			var aliases = wallet.getSecretStore().aliases();
			while (aliases.hasMoreElements()) {
				var element = aliases.nextElement();
				if (element instanceof String) {
					final var fullKeyName = (String) element;
					if (Strings.CS.startsWith(fullKeyName, KEY_PREFIX)) {
						final String key = StringUtils.substringAfter(fullKeyName, KEY_PREFIX);
						final String keyValue = new String(wallet.getSecretStore().getSecret(fullKeyName));
						if (Strings.CS.endsWith(fullKeyName, "MASTERKEY")) {
							masterKeyId = keyValue;
							if (LOGGER.isDebugEnabled())
								LOGGER.debug("Master Key ID set to '{}'", masterKeyId);
						} else if (!Strings.CS.contains(fullKeyName, "MASTERKEY")) {
							final byte[] value = decoder.decode(keyValue);
							if (value[0] != 0) {
								LOGGER.error(
										"\n=====================\n" +
										"Invalid first byte '{}' for key '{}'!" +
										"\n=====================\n",
										Byte.toUnsignedInt(value[0]), fullKeyName);
								throw new IOException("The first byte of the value structure must be 0!");
							}
							final int length = Short.toUnsignedInt(bu.getU16(value, 1));
							if (length != value.length) {
								LOGGER.error(
										"\n=====================\n" +
										"Invalid value length for key '{}'! There are {} bytes in structure, but the length in the struct content is {}" +
										"\n=====================\n",
										fullKeyName, Byte.toUnsignedInt(value[0]), value.length, length);
								throw new IOException("The first byte of the value structure must be 0!");
							}
							int pos = 3;
							final Kek kek = new Kek();
							while (pos < length) {
								int type = Byte.toUnsignedInt(value[pos++]);
								int len = Short.toUnsignedInt(bu.getU16(value, pos));
								pos += Short.BYTES;
								if (type == 1 && len == 0x20)
									kek.key = Arrays.copyOfRange(value, pos, pos + len);
								else if (type == 3 && len == 0x10)
									kek.iv = Arrays.copyOfRange(value, pos, pos + len);
								else if (type == 5 && len == 0x07) {
									try {
										kek.ts = toLocalDateTime(value, pos);
									} catch (SQLException sqle) {
										throw new IOException(sqle);
									}
								} else {
									LOGGER.error(
											"\n=====================\n" +
											"Unable to parse Oracle Wallet KEK struct with type '{}' and length '{}'!\n" +
											"Please send this information to oracle@a2.solutions" +
											"\n=====================\n",
											type, len);
									throw new IOException("Unable to parse Oracle Wallet data!");
								}
								pos += len;
							}
							secrets.put(key, kek);
							if (LOGGER.isDebugEnabled())
								LOGGER.debug("Added KEK with name '{}', short name '{}', created on '{}'", 
										fullKeyName, key, kek.ts);
						} else {
							if (LOGGER.isDebugEnabled())
								LOGGER.debug("Skipping key {}", fullKeyName);
						}
					} 
				}
			}
		} catch (OracleSecretStoreException osse) {
			throw new IOException(osse);
		}
	}

	public static OraCdcTdeWallet get(
			final BinaryUtils bu, final String path, final String password) throws SQLException {
		try {
			return new OraCdcTdeWallet(bu, path, password.toCharArray());
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	byte[] decryptDataKey(final String masterKeyId, final String encDataKey) throws IOException {
		return decryptDataKey(masterKeyId, Base64.getDecoder().decode(encDataKey));
	}

	byte[] decryptDataKey(final String masterKeyId, final byte[] encDataKey) throws IOException {
		return decryptDataKey(masterKeyId, encDataKey, false);
	}

	public byte[] decryptDataKey(final String masterKeyId, final byte[] encDataKey, boolean tbsKey) throws IOException {
		final Kek kek = secrets.get(masterKeyId);
		if (kek == null) {
			LOGGER.error(
					"\n=====================\n" +
					"No data found for master key id '{}'!" +
					"\n=====================\n",
					masterKeyId);
			throw new IOException("No data found for master key!");
		}
		if (encDataKey == null || encDataKey.length == 0) {
			LOGGER.error(
					"\n=====================\n" +
					"Empty encrypted data key for master key id '{}'!" +
					"\n=====================\n",
					masterKeyId);
			throw new IOException("Empty encrypted data key!");
		}
		try {
			Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
			final SecretKeySpec masterKey = new SecretKeySpec(kek.key, "AES");
			final IvParameterSpec iv = new IvParameterSpec(kek.iv);
			cipher.init(UNWRAP_MODE, masterKey, iv);
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Decrypting DEK {} using KEK id '{}'.", rawToHex(encDataKey), masterKeyId);
			return cipher.unwrap(tbsKey
					? encDataKey
					: Arrays.copyOfRange(encDataKey, 1, encDataKey.length),
					"AES", SECRET_KEY).getEncoded();
		} catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
			throw new IOException(e);
		}
	}

	private static class Kek {
		byte[] key;
		byte[] iv;
		LocalDateTime ts;
	}

}
