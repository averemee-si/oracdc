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
import java.security.NoSuchProviderException;
import java.security.Security;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Decoder;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oracle.security.pki.OracleSecretStoreException;
import oracle.security.pki.OracleWallet;

import static org.bouncycastle.jce.provider.BouncyCastleProvider.PROVIDER_NAME;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;


/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcTdeWallet {

	private final OracleWallet wallet;
	private final Map<String, byte[]> secrets;

	private static final Logger LOGGER = LoggerFactory.getLogger(OraCdcTdeWallet.class);
	private static final String KEY_PREFIX = "ORACLE.SECURITY.DB.ENCRYPTION.";

	OraCdcTdeWallet(String path, String password) throws IOException {
		this(path, password.toCharArray());
	}

	OraCdcTdeWallet(String path, char[] password) throws IOException {
		if (Security.getProvider(PROVIDER_NAME) == null) {
			Security.addProvider(new BouncyCastleProvider());
		}
		wallet = new OracleWallet();
		try {
			secrets = new HashMap<>();
			final Decoder decoder = Base64.getDecoder();
			wallet.open(path, password);
			Enumeration<?> aliases = wallet.getSecretStore().aliases();
			while (aliases.hasMoreElements()) {
				Object element = aliases.nextElement();
				if (element instanceof String) {
					final String fullKeyName = (String) element;
					if (Strings.CS.startsWith(fullKeyName, KEY_PREFIX)) {
						final String key = StringUtils.substringAfter(fullKeyName, KEY_PREFIX);
						final byte[] value = decoder.decode(new String(wallet.getSecretStore().getSecret(fullKeyName)));
						secrets.put(
								key,
								value);
						if (LOGGER.isDebugEnabled())
							LOGGER.debug("Added key with name '{}', short name '{}' and value '{}'", 
									fullKeyName, key, rawToHex(value));
					}
				}
			}
		} catch (OracleSecretStoreException osse) {
			throw new IOException(osse);
		}
	}

	Map<String, byte[]> masterKeys() {
		return secrets;
	}

	byte[] decryptDataKey(final String masterKeyId, final String encDataKey) throws IOException {
		return decryptDataKey(masterKeyId, Base64.getDecoder().decode(encDataKey));
	}

	byte[] decryptDataKey(final String masterKeyId, final byte[] encDataKey) throws IOException {
		final byte[] masterBytes = secrets.get(masterKeyId);
		if (masterBytes == null) {
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
			Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding", "BC");
			final byte[] secretBytes = Arrays.copyOfRange(masterBytes, 0x6, 0x26);
			final byte[] ivBytes = Arrays.copyOfRange(masterBytes, 0x29, 0x39);
			final SecretKeySpec masterKey = new SecretKeySpec(secretBytes, "AES");
			final IvParameterSpec iv = new IvParameterSpec(ivBytes);
			cipher.init(Cipher.DECRYPT_MODE, masterKey, iv);
			if (LOGGER.isDebugEnabled())
				LOGGER.debug("Decrypting data key {} using master key id '{}'.\n\tSecret key='{}', iv='{}'",
						rawToHex(encDataKey), masterKeyId, rawToHex(secretBytes), rawToHex(ivBytes));
			return cipher.doFinal(Arrays.copyOfRange(encDataKey, 1, encDataKey.length));
		} catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException | IllegalBlockSizeException | BadPaddingException e) {
			throw new IOException(e);
		}
	}


}
