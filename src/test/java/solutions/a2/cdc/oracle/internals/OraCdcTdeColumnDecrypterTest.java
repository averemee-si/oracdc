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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static solutions.a2.oracle.utils.BinaryUtils.hexToRaw;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

/**
 *  
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 * 
 */
public class OraCdcTdeColumnDecrypterTest {

	@Test
	public void test() {

		try {
			OraCdcTdeColumnDecrypter decrypter;
			// AES-256, SHA-1, SALT
			decrypter = new OraCdcTdeColumnDecrypter(
				hexToRaw("8AC759A42F0D447528014DEA2E9830631B3EB88E529DDE924D658AC280138F9D21342A1AE4323EB0E70060DEE6F1C030CC5064D1DA70E70F9D49416A39B2EADE449CB91F0C0C0C0C0C0C0C0C0C0C0C0C"),
				(byte) 4, (byte) 1);
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("2a 1a 58 1d 67 44 5f fe 2d 42 09 ee fd bd 53 10 11 56 76 90 66 53 d5 de 29 28 9d 77 ca 5e ab 69 42 58 88 da 42 74 b5 d7 24 f7 ad 0b 08 93 80 74 48 7f 65 af"
							.replaceAll(" ", "")),
					true)), "ABCDEFGHIJKLMN");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("66 ec 00 e3 44 5b ec 46 d7 f8 cb 68 3c f2 9b ef 99 7a 24 2c 19 57 87 fd 03 67 2f 8a b6 0a 97 6d 2a 05 39 40 e7 13 92 f1 13 b1 52 c4 02 b3 5c a9 26 57 bd 14"
							.replaceAll(" ", "")),
					true)), "ABCDEFGHIJKLMN");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("a5 6d 27 a2 66 78 08 da 42 d6 0d 04 df 54 2d d0 f0 7a 2f e5 b0 ee 7e 2c ba f0 a0 28 42 54 38 e7 c5 74 1c 9b 39 f1 a3 38 23 26 82 f4 df 5b 67 12 a8 0c 74 a6 64 a5 d5 0b eb 26 4d dd af 3a 41 65 83 1c 7c 1f d2 6c c9 f0 30 03 5b 87 88 47 c4 38 b8 3a 83 77"
							.replaceAll(" ", "")),
					true)), "ABCDEFGHIJKLMN===================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("b1 18 f8 9b 54 7b 40 85 e8 99 4a 4a bd 84 df 85 f6 02 f8 f0 4a 7a ba 19 b5 15 d9 f2 05 50 b8 db 9f 12 aa d0 24 21 94 21 73 35 47 0b b1 fa 4a fb 7b 79 f9 75"
							.replaceAll(" ", "")),
					true)), "abcdefghijklmn");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("00 f9 8d d3 9a fa f0 be 40 58 0b 91 ec 35 68 8a e5 11 d8 3e 70 1e e0 fe 6c a0 88 eb 89 69 e0 89 80 57 a6 0a 19 2c 68 d4 17 7a a1 cd 8c 55 df f6 26 25 00 d8"
							.replaceAll(" ", "")),
					true)), "abcdefghijklmn");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("89 57 91 d1 8d e2 3a 85 48 d5 b7 b2 62 b9 0e b3 d3 44 81 66 54 71 37 45 be 38 40 a3 9d 2b 53 26 33 a2 6b 15 b3 77 bc 0b 88 41 ea dd 88 49 98 96 49 45 a6 85 f6 f4 a3 f2 8a eb 39 44 de ba 7a 9b 05 11 c0 2d c8 b2 18 1a b6 41 24 cd 85 ee ed 72 d9 42 b8 85"
							.replaceAll(" ", "")),
					true)), "abcdefghijklmn=====================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("cd 34 03 93 b0 c2 84 f6 80 24 57 4c 64 81 76 5d 30 23 4b b8 b2 52 88 98 85 f8 b5 e3 0b e0 97 e7 c5 ad 9c 78 f0 ec 06 6e 8c 0f cb 76 3f 56 57 49 c0 c4 25 03"
							.replaceAll(" ", "")),
					true)), "OPQRSTUVWXYZ");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("a7 c1 b3 0c 97 31 5a 37 01 c7 df 4d 87 a4 9c 36 0d 22 c4 0e ad 28 5c 6d fa c1 3a 80 28 bc db c0 85 b2 0e 0c 14 3d f3 e8 30 56 a1 b4 e5 36 36 83 fb f8 d0 bf"
							.replaceAll(" ", "")),
					true)), "OPQRSTUVWXYZ");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("ab ef 85 cf 7e bc d9 20 a7 5e 7b dd 52 45 83 61 e8 0c 13 75 81 7d fa fa 76 08 22 68 67 26 a9 1d a2 52 0b 42 b8 9c b1 16 6c b0 2a 63 a3 26 d5 e9 69 9d 11 4f 68 29 5a f3 e9 f3 24 77 fe 8b a2 4e 85 db a9 60 07 c1 7e 31 b9 ad 55 f6 2a d6 04 21 a4 d5 67 69"
							.replaceAll(" ", "")),
					true)), "OPQRSTUVWXYZ=============================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("e3 0c dc dd 6e 38 ca 49 af 3b 39 ae 8b 86 8a 10 46 64 94 c6 fb d4 72 85 31 dd a0 7d 48 29 57 1d 8c 8c 74 d4 14 1e 84 43 4d b0 2e 99 33 04 0b 1f fc 2e 30 6e"
							.replaceAll(" ", "")),
					true)), "opqrstuvwxyz");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("a1 cf 77 d5 f8 96 78 21 a1 5c 09 0e 02 88 8d 8c a3 12 90 7a d6 d0 99 63 31 97 e3 8b ac 0a fe bc 32 f6 d2 1b e2 1e f7 06 4a f0 78 8d a8 46 53 81 4e da df 8f"
							.replaceAll(" ", "")),
					true)), "opqrstuvwxyz");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("f7 0e e4 86 16 70 96 89 d4 e4 24 0e e7 7b 98 2a 81 33 04 03 5f 63 84 2b 5e e0 a4 58 9e f5 7e 38 59 45 68 14 bc da 32 64 aa c7 7a a4 e1 53 94 de c9 fc fb e0 5e ea 3e 5b 83 c0 10 2e 6a 11 df a5 6c 4e 53 a0 df b3 d1 5d 3d 08 8e 50 fd 7e 01 41 dc af ef 01"
							.replaceAll(" ", "")),
					true)), "opqrstuvwxyz===============================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("a9 6c 84 fd ea f1 59 4c d9 db a6 3b 33 e2 9d f2 11 a0 3b 21 f3 50 c1 d2 82 2e 01 d3 0f 9e 3f 17 d9 07 74 d9 81 5b b3 88 a8 7a ba f7 9c 78 96 18 98 55 85 d2"
							.replaceAll(" ", "")),
					true)), "0123456789");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("68 e5 59 24 f7 bc 6f 8f 0e ff 24 e7 9d 88 45 80 fa 15 ec 26 e7 86 eb 89 36 0c df 6b aa 8f 9c b7 ea f5 55 c3 a1 db 3a a3 8a 68 07 80 b7 8b b5 13 b2 c3 be 7f"
							.replaceAll(" ", "")),
					true)), "0123456789");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("9a 80 6e 9a fe ef 83 7d 07 05 fe 7a a2 ff 2c ac 9b 80 db be ec a2 b5 cb 62 61 a4 82 03 b0 ad 24 3d ce 07 8a 25 be 90 3e 15 60 01 32 48 03 b1 13 86 52 96 2b a2 bb c8 48 7c 15 d7 f5 5c 37 72 c5 3c ce a5 ee 66 f5 9f 9d 81 e3 50 38 38 cf 1f aa 9b 5e 88 e9 f2 87 81 6a df dc 63 b5 81 95 a4 a6 e4 0c 11 07"
							.replaceAll(" ", "")),
					true)), "0123456789=======================================");
			// AES-256, NOMAC, NOSALT
			decrypter = new OraCdcTdeColumnDecrypter(
				hexToRaw("8AC759A42F0D447528014DEA2E983063CE874BD9AC7DD1D3116F1ACB0733F83FB6A725765986FF72F3436E8C5DBB15B24628FCEC8F1803B9ADFDD31CEFAF61249A7C283D0C0C0C0C0C0C0C0C0C0C0C0C"),
				(byte) 4, (byte) 2);
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("8c f7 16 10 a8 a8 2e 47 9b c3 36 7f 8f 1e 14 ba"
							.replaceAll(" ", "")),
					false)), "ABCDEFGHIJKLMN");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("77 cf 17 21 31 5a 45 23 c5 9c f6 89 54 83 54 37 8e 30 e6 23 70 bd aa 3d e1 db 51 7b 62 71 5a 9a 4c 69 74 d4 98 e2 c5 b4 74 af 86 12 57 92 c7 c8"
							.replaceAll(" ", "")),
					false)), "ABCDEFGHIJKLMN===================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("53 79 40 af 91 fe 37 bd 49 d8 e2 84 a8 b7 50 b4"
							.replaceAll(" ", "")),
					false)), "abcdefghijklmn");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("f8 df e4 48 67 9d 64 37 ad 20 af ad ce cb 17 23 42 b1 5d b2 5d b6 1e e9 9c 36 e2 16 4c d7 e2 d1 fe 15 be 50 31 30 a3 c4 48 75 10 5e bd ab 65 38"
							.replaceAll(" ", "")),
					false)), "abcdefghijklmn=====================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("7f 22 ac b3 d7 37 36 04 59 93 19 15 b4 6e 19 48"
							.replaceAll(" ", "")),
					false)), "OPQRSTUVWXYZ");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("52 ca 73 c6 54 09 ad 9d 4d d4 8c 78 87 23 8a c2 36 c2 d3 9a 51 5c 37 9b fe 73 61 0a ad 39 e4 ee 01 f9 3e e7 25 30 94 7b 46 c3 d8 f9 32 58 91 75"
							.replaceAll(" ", "")),
					false)), "OPQRSTUVWXYZ=============================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("92 40 39 69 98 5a 22 59 3e c0 d1 85 5a 84 74 65"
							.replaceAll(" ", "")),
					false)), "opqrstuvwxyz");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("d1 77 b6 71 3d 16 c9 b6 f5 a2 29 bd 9c f5 45 30 84 22 93 6d 72 97 c9 5b a0 de 75 73 cf b2 ab a4 2d 46 87 64 94 7e 3e 42 0e 0e 41 77 e6 30 2f a6"
							.replaceAll(" ", "")),
					false)), "opqrstuvwxyz===============================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("da 06 e4 5b 3c 1f 87 6f 54 be 5b 13 11 65 27 cc"
							.replaceAll(" ", "")),
					false)), "0123456789");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("62 c2 a3 77 21 b7 8e 79 f1 d7 f1 6e 7c 97 0e ff 14 ed d1 f2 36 3e 11 d9 41 a3 25 dc 6a 4c c5 49 4d 42 a5 c7 57 fa e9 51 85 a6 f2 fb 11 b9 5c 1c cd 55 b1 b0 fb 89 c5 3f 27 cf 97 33 95 89 10 96"
							.replaceAll(" ", "")),
					false)), "0123456789=======================================");
			// AES-192, SHA-1
			decrypter = new OraCdcTdeColumnDecrypter(
					hexToRaw("8AC759A42F0D447528014DEA2E983063CC5D432F4C62F8297E1C62068F06AD6854322095944A21C4901B9F6305D655437A58D87AEABD7C81B062A60504040404"),
					(byte) 3, (byte) 1);
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("d0 81 04 ba 02 44 ca 29 94 ce f5 3f 76 eb 2b 02 27 85 a9 ec 66 63 c5 0b d1 77 d9 34 4f ed 6c a3 9f 19 83 44 33 db d9 e2 20 4f 20 f1 ee 77 4a e9 b3 f4 38 20"
							.replaceAll(" ", "")),
					true)), "ABCDEFGHIJKLMN");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("e8 50 6d 55 1d 0b c2 a3 6c b5 c0 56 3d b6 f7 be 65 9f 14 29 d1 9d b8 ae 59 1d e7 e5 b2 aa c8 97 8c 43 17 94"
							.replaceAll(" ", "")),
					false)), "ABCDEFGHIJKLMN");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("50 22 1e 15 46 db c8 f2 0b 25 f6 d5 01 4b 98 31 b0 cd 4e 38 00 c8 be 6a d1 af 3f a2 c3 ac fc 80 71 79 77 f3 80 38 c6 d5 68 cc 47 77 bb 75 50 68 81 8b 3c c2 23 54 45 f8 bd 93 f8 da c2 6f 50 94 c9 a8 49 70 93 81 1c fd 8d df bb cf 4d 66 35 cd d5 ac 7d b0"
							.replaceAll(" ", "")),
					true)), "ABCDEFGHIJKLMN===================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("91 e9 f9 03 1f 49 ed aa b9 a6 14 16 72 cd 3c 3e 04 37 62 f5 67 3d 97 98 a4 87 8b 32 25 65 20 8d e9 af a7 ff 3b b4 83 f2 89 d8 4e 0f b9 c6 53 59 24 dd 04 3c d3 be 64 ac 92 85 7b 31 c9 74 3d 8d 97 52 da f1"
							.replaceAll(" ", "")),
					false)), "ABCDEFGHIJKLMN===================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("94 dc 08 dd 9e 2c 57 6e 02 8f 5d 83 97 fa 38 eb 24 d9 17 8c d7 58 b8 8c 76 00 ee 06 96 4f 78 79 d4 62 c2 b5 1c e1 be 03 7b d0 6e e8 10 19 65 f0 c6 41 23 96"
							.replaceAll(" ", "")),
					true)), "abcdefghijklmn");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("22 0f 58 67 0a 74 78 fa 26 6c 4d aa 7d b9 b8 d9 d1 95 83 e4 65 44 74 7f 19 36 4c 0e 79 c7 a6 89 2e 57 00 38"
							.replaceAll(" ", "")),
					false)), "abcdefghijklmn");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("6e fe 37 be 33 1e 82 00 14 4d a4 d5 b1 37 0e d4 c1 66 09 02 66 2b 96 eb 63 d3 cd 52 10 f3 f0 ed ef 52 6e d4 63 3b 60 cb 4b a7 11 92 84 93 fd 4a a6 36 fa 46 c4 15 20 d9 22 a8 47 31 7b 52 08 2c 7f d5 72 c2 e2 d2 7e a5 05 7c 34 3b 20 ca 9a 3f b0 27 55 b6"
							.replaceAll(" ", "")),
					true)), "abcdefghijklmn=====================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("34 0d 0b ae 34 67 ab 3a 99 d4 35 05 0f d9 c3 18 0c b4 84 24 05 4c eb d7 09 63 13 8e f7 55 75 04 86 85 28 97 7c e8 3f fb b2 5c 9e 6d b9 06 99 7e be 4a 06 af 28 7d 76 87 18 94 ab 3c 7f 9a 85 ba 2e aa 33 e5"
							.replaceAll(" ", "")),
					false)), "abcdefghijklmn=====================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("16 24 6f ce 24 4f 76 63 b7 89 7b 00 35 28 2f 06 41 d0 3a 80 cf e6 a6 c6 ac 30 c7 d3 33 d6 e4 f0 7d 21 03 c3 9a b6 fc ae c7 27 f7 6b f2 2e f5 5e 70 3a f0 69"
							.replaceAll(" ", "")),
					true)), "OPQRSTUVWXYZ");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("09 9c 42 47 6f 33 ef 53 1d 5d e7 6c 0f 5c c4 6d 4d 47 a0 09 b5 85 92 e2 2f f3 4d e2 53 f1 01 2d 97 97 7a ff"
							.replaceAll(" ", "")),
					false)), "OPQRSTUVWXYZ");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("57 6d cc 49 4d 8b 6f 5f 89 99 80 38 3e 35 5c 8b cd a3 1f 1b a8 de b3 eb b0 9f 2c cd 6a 97 03 da df 9b 4a 63 cc 8a b9 b5 60 18 ae 41 b1 5e e8 5b d3 ea b9 c6 7e 4c 0c 39 1a 6d 54 88 94 f8 05 de 7c cf 61 9c dd 67 ef e6 d2 59 15 b8 fb bc c5 40 7c 7c ce 87"
							.replaceAll(" ", "")),
					true)), "OPQRSTUVWXYZ=============================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("31 92 d5 90 48 31 e3 6f 07 e1 09 1c 02 03 cc 3f 81 34 88 50 4c c0 ae d8 51 84 75 d9 a1 ae e7 03 4c bc 18 c1 5e fc 4d 8b 34 a6 7e be 33 c1 a4 d9 48 1b a3 3e 43 87 48 fe b0 8a b0 6e 0a 66 cc 42 41 89 07 52"
							.replaceAll(" ", "")),
					false)), "OPQRSTUVWXYZ=============================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("e1 31 6c 4f 7d 06 c2 46 40 56 55 22 de 4d 51 51 4d 97 40 c9 04 6b 6b 6d 9e c7 2f 2b 64 45 af 65 6d a2 39 13 ff 61 02 80 b7 c0 85 90 31 1c e9 9b 83 36 ab 52"
							.replaceAll(" ", "")),
					true)), "opqrstuvwxyz");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("d9 57 23 d7 22 42 19 de 40 b3 e5 ea 7d 7b 78 40 38 0b 72 3f 2b 49 ec 7f 31 b6 e5 19 bb c8 37 58 fc 35 d2 33"
							.replaceAll(" ", "")),
					false)), "opqrstuvwxyz");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("75 e5 28 52 84 77 ab 5c 71 0e 5f 7f 27 8f 5f 27 c8 55 b8 44 45 d5 63 ed 17 02 d0 ea cd ec 51 b6 aa 30 b7 2e f1 1f bf c0 07 2b c8 12 16 6a 0e 28 d4 75 cd 35 61 e2 e4 02 e3 bf 07 a0 c2 31 a1 75 47 7f bf cf b2 65 d4 e2 cd 38 81 d2 9e 95 99 b7 9f 06 45 c4"
							.replaceAll(" ", "")),
					true)), "opqrstuvwxyz===============================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("c4 98 1d c4 dc cc 66 d9 45 59 1c 65 c8 cf 42 ea ee 18 eb 9a 26 c6 7c 8e 28 03 64 f5 a9 07 3e bc 8d a6 4a 0c bc b5 66 5b 3e b6 75 11 a0 e4 1e c2 5a 99 cf a9 26 15 40 13 ea 7b 10 0c aa 3b af 4b 41 ca a2 ce"
							.replaceAll(" ", "")),
					false)), "opqrstuvwxyz===============================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("79 c7 9b af 7b 15 55 da 4d dc b1 9f 1a fe 33 51 ca 25 e6 db 11 9b 75 73 12 fa a0 8f 5b 0a 4d 43 81 5e 78 3c ea 05 83 e4 df bf df ca ed 49 d0 36 7a 94 0d 59"
							.replaceAll(" ", "")),
					true)), "0123456789");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("b9 c0 e9 3f 5f e5 57 ff 50 88 93 41 47 2b 58 ff e5 0e c7 46 10 58 dd 15 1c 8f 28 3c a3 55 46 66 6d 9f 67 58"
							.replaceAll(" ", "")),
					false)), "0123456789");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("63 41 58 36 f4 7b fd bd dd ef de fb 7f 20 67 5a a6 d8 d4 08 fd b9 f3 47 0e 5f 75 79 f8 16 d6 56 40 83 d3 44 24 c6 96 c6 91 4e ec 0a dc bb 62 59 28 3b ec cc 20 55 42 0b 03 33 50 b2 a6 f9 b1 38 0e c8 71 ac 6e 56 c7 3b 8d e1 47 b5 87 49 d1 d3 4f 16 19 ce 6d 34 bc 87 62 43 7d 19 56 4e 85 77 95 1d a5 8d"
							.replaceAll(" ", "")),
					true)), "0123456789=======================================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("b3 e2 62 2b 90 7e 8f 62 c9 ea 60 1e c4 2c 31 32 6d 0d c3 18 61 38 34 29 36 0a ad bb ae 31 cf de e1 46 8b 7b 72 29 cb 3b 1e c1 fa 7f f8 87 c7 91 49 14 f0 8a 15 9d 23 1b c0 fe d4 9f c6 39 5b ec 4e 92 34 d5 98 b4 32 c1 88 1a 4f 87 42 00 b1 64 74 d1 43 15"
							.replaceAll(" ", "")),
					false)), "0123456789=======================================");
			// 3DES168, NOMAC
			decrypter = new OraCdcTdeColumnDecrypter(
					hexToRaw("8AC759A42F0D447528014DEA2E98306301A275D1FB2BF09D66D7C099012922F42F5863536CCDB1B481D1B71C83C91397E6F166D5F805833EC95A5FDB04040404"),
					(byte) 1, (byte) 2);
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("b1 47 c3 75 78 e8 b4 1a b6 88 73 bf 73 55 43 cb 8c 92 3e 19 fc fa aa 8c"
							.replaceAll(" ", "")),
					true)), "ABCDEFGHIJKLMN");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("38 0e 30 4a f6 50 cb c4 34 ae d4 ad b3 6e a9 1e"
							.replaceAll(" ", "")),
					false)), "ABCDEFGHIJKLMN");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("88 67 c1 b5 d9 d1 b2 07 6d c1 3b 59 5d 85 d2 fb d9 35 cb 3e 89 fd c6 d8 98 6c 85 f5 8c a0 ba 20 9a 4f 9e 50 87 9c 7c f2 31 66 92 19 d1 4c 8d f1"
							.replaceAll(" ", "")),
					true)), "ABCDEFGHIJKLMN===================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("38 0e 30 4a f6 50 cb c4 d2 fe 7a f1 1a f6 c0 7e a3 69 65 60 70 9a 74 25 fa fe a4 75 8f 48 99 5a 6c fb 7b 8c bd c0 c2 64"
							.replaceAll(" ", "")),
					false)), "ABCDEFGHIJKLMN===================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("55 95 b3 1f 0c f5 3a cd 4a 8c 61 c4 70 78 fc f3 56 02 39 9e 6c 2b d7 ac"
							.replaceAll(" ", "")),
					true)), "abcdefghijklmn");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("db a8 0c 5f 2c 60 5e ce 68 b4 94 7c 63 1d 94 52"
							.replaceAll(" ", "")),
					false)), "abcdefghijklmn");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("dd 7f aa d1 3d d3 49 63 98 ca b1 40 0a b5 b1 ce 96 3b 49 ef a1 49 22 bf e5 55 c8 86 9f f1 bf b6 9c 7d ac c3 e9 17 5b 9e 8b 25 24 a4 47 60 94 00"
							.replaceAll(" ", "")),
					true)), "abcdefghijklmn=====================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("db a8 0c 5f 2c 60 5e ce 5b 31 5f 66 b0 e4 8e 37 8f 96 0b 49 15 ac 75 0e 2b ba 5e 2f 54 bf b0 ad 20 9a 01 e2 ec 07 1b f9"
							.replaceAll(" ", "")),
					false)), "abcdefghijklmn=====================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("0f 47 c7 ff 31 cf ab 1d bd 77 6f 99 5f ae de a5 c6 0e ea 76 a8 1f d5 5b"
							.replaceAll(" ", "")),
					true)), "OPQRSTUVWXYZ");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("a8 ca 0a 51 9f 35 7a 59 9c 76 25 2c e6 73 99 b3"
							.replaceAll(" ", "")),
					false)), "OPQRSTUVWXYZ");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("b9 e7 1c 5c 28 38 dd 34 7f ef 20 21 4c 5a aa f3 2a ec 87 31 58 f6 e9 ea 06 1b 44 32 5a e4 6b 76 9e 55 d9 ae d6 82 e8 1b 17 57 cf 91 90 a4 b2 18 26 7b f6 b1 d1 47 7c 9b"
							.replaceAll(" ", "")),
					true)), "OPQRSTUVWXYZ=============================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("a8 ca 0a 51 9f 35 7a 59 1a f3 8a ad b3 94 c9 8d a6 aa fe aa 50 7f 71 a5 68 c5 44 7d 55 f5 8d 2e a6 0b b5 19 65 02 b6 fa 60 3b 7e 0e c2 9d 6e e5"
							.replaceAll(" ", "")),
					false)), "OPQRSTUVWXYZ=============================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("d8 cc c4 18 24 24 b6 71 ad 72 6f 85 ba 2a 74 f5 91 82 24 5a 6f 0b 1c 5a"
							.replaceAll(" ", "")),
					true)), "opqrstuvwxyz");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("ca ab 53 d3 7f 64 85 07 97 76 dd c5 79 ee 48 6d"
							.replaceAll(" ", "")),
					false)), "opqrstuvwxyz");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("b6 7f 6b 32 b9 50 62 27 d2 98 24 11 b2 bb 26 68 6b 28 90 9b 9f 5d 28 6c 13 ea d3 dd 11 d2 ed 65 fb 8e cb d4 9d 75 cb b8 ae 63 58 03 42 b5 56 8a fb e1 24 18 21 e8 80 25"
							.replaceAll(" ", "")),
					true)), "opqrstuvwxyz===============================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("ca ab 53 d3 7f 64 85 07 8e 80 c2 d7 88 1e 05 9f 56 7e f4 e0 b1 f7 37 b6 07 ad c8 a7 8d 96 75 40 de 9b ff 84 0e d4 f3 32 3d 4b cb 30 9e 50 3a d6"
							.replaceAll(" ", "")),
					false)), "opqrstuvwxyz===============================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("e2 5f b5 0d af bc c6 88 15 f9 bf b3 21 64 2f 74 35 e4 13 06 8e 59 2d 6e"
							.replaceAll(" ", "")),
					true)), "0123456789");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("07 24 4c 80 14 69 a1 a6 15 7c 6a 14 22 63 f1 72"
							.replaceAll(" ", "")),
					false)), "0123456789");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("a3 b2 a3 08 68 7a 0f e3 7b 10 12 41 0f 80 96 f4 dd 20 da b1 f1 f3 c0 88 65 e2 6a f7 cb b5 06 25 01 6c 89 48 e3 b2 80 70 09 31 fd 86 92 1e d4 4c de 76 eb 01 c2 ed 8a d7 26 64 08 a4 2f d8 39 0b"
							.replaceAll(" ", "")),
					true)), "0123456789=======================================");
			assertEquals(new String(decrypter.decrypt(
					hexToRaw("07 24 4c 80 14 69 a1 a6 5b 6b 87 b3 2e a8 9e 05 dc 9f 19 7f 37 15 49 6e 92 ae ac db 2b c0 30 89 47 ff 3d e8 47 65 f8 ef a0 b9 15 12 12 3c c0 14 a1 be fe cf 05 ed f9 c0"
							.replaceAll(" ", "")),
					false)), "0123456789=======================================");

		} catch (IOException | SQLException e) {
			e.printStackTrace();
		}
	}

}
