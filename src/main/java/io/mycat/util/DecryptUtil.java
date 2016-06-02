/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.util;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPrivateKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.Cipher;

import io.mycat.config.util.ConfigException;

/**
 * @author songwie
 *
 */
public class DecryptUtil {

	private static final String DEFAULT_PRIVATE_KEY_STRING = "MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEAocbCrurZGbC5GArEHKlAfDSZi7gFBnd4yxOt0rwTqKBFzGyhtQLu5PRKjEiOXVa95aeIIBJ6OhC2f8FjqFUpawIDAQABAkAPejKaBYHrwUqUEEOe8lpnB6lBAsQIUFnQI/vXU4MV+MhIzW0BLVZCiarIQqUXeOhThVWXKFt8GxCykrrUsQ6BAiEA4vMVxEHBovz1di3aozzFvSMdsjTcYRRo82hS5Ru2/OECIQC2fAPoXixVTVY7bNMeuxCP4954ZkXp7fEPDINCjcQDywIgcc8XLkkPcs3Jxk7uYofaXaPbg39wuJpEmzPIxi3k0OECIGubmdpOnin3HuCP/bbjbJLNNoUdGiEmFL5hDI4UdwAdAiEAtcAwbm08bKN7pwwvyqaCBC//VnEWaq39DCzxr+Z2EIk=";
	public static final String DEFAULT_PUBLIC_KEY_STRING = "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAKHGwq7q2RmwuRgKxBypQHw0mYu4BQZ3eMsTrdK8E6igRcxsobUC7uT0SoxIjl1WveWniCASejoQtn/BY6hVKWsCAwEAAQ==";

	public static void main(String[] args) throws Exception {
		String password = args[0];
		System.out.println(encrypt(password));
	}
	
	public static String mycatDecrypt(String usingDecrypt,String user ,String passwrod){
		if("1".equals(usingDecrypt)){
        	//type:user:password
        	//0:test:test
        	boolean flag = false;
        	try {
        		String passwrods[] = DecryptUtil.decrypt(passwrod).split(":");
            	if("0".equals(passwrods[0])
						&& user.equals(passwrods[1])){
                        flag = true;
            			return passwrods[2];
            	}
            	if(flag==false){
            		 throw new ConfigException("user " + user + " passwrod need to decrype ,but decrype password is wrong !");
            	}
        	} catch (Exception e2) {
       		    throw new ConfigException("user " + user + " passwrod need to decrype ,but decrype password is wrong !",e2);
			}
		}
		return passwrod;
	}
	public static String DBHostDecrypt(String usingDecrypt,String host,String user ,String passwrod){
		if("1".equals(usingDecrypt)){
			//type:host:user:password
        	//1:myhost1:test:test
        	boolean flag = false;
        	try {
        		String passwrods[] = DecryptUtil.decrypt(passwrod).split(":");
            	if("1".equals(passwrods[0]) && host.equals(passwrods[1]) && user.equals(passwrods[2])){
            		return passwrods[3];
            	}
            	if(flag==false){
            		 throw new ConfigException("user " + user + " passwrod need to decrype ,but decrype password is wrong !");
            	}
        	} catch (Exception e2) {
       		    throw new ConfigException("host " + host + ",user " + user + " passwrod need to decrype ,but decrype password is wrong !",e2);
			}
		}
		return passwrod;
	}
	

	public static String decrypt(String cipherText) throws Exception {
		return decrypt((String) null, cipherText);
	}

	public static String decrypt(String publicKeyText, String cipherText)
			throws Exception {
		PublicKey publicKey = getPublicKey(publicKeyText);

		return decrypt(publicKey, cipherText);
	}

	public static PublicKey getPublicKey(String publicKeyText) {
		if (publicKeyText == null || publicKeyText.length() == 0) {
			publicKeyText = DecryptUtil.DEFAULT_PUBLIC_KEY_STRING;
		}

		try {
			byte[] publicKeyBytes = Base64.base64ToByteArray(publicKeyText);
			X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(
					publicKeyBytes);

			KeyFactory keyFactory = KeyFactory.getInstance("RSA");
			return keyFactory.generatePublic(x509KeySpec);
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed to get public key", e);
		}
	}


	public static String decrypt(PublicKey publicKey, String cipherText)
			throws Exception {
		Cipher cipher = Cipher.getInstance("RSA");
		try {
			cipher.init(Cipher.DECRYPT_MODE, publicKey);
		} catch (InvalidKeyException e) {
            // 因为 IBM JDK 不支持私钥加密, 公钥解密, 所以要反转公私钥
            // 也就是说对于解密, 可以通过公钥的参数伪造一个私钥对象欺骗 IBM JDK
            RSAPublicKey rsaPublicKey = (RSAPublicKey) publicKey;
            RSAPrivateKeySpec spec = new RSAPrivateKeySpec(rsaPublicKey.getModulus(), rsaPublicKey.getPublicExponent());
            Key fakePrivateKey = KeyFactory.getInstance("RSA").generatePrivate(spec);
            cipher = Cipher.getInstance("RSA"); //It is a stateful object. so we need to get new one.
            cipher.init(Cipher.DECRYPT_MODE, fakePrivateKey);
		}
		
		if (cipherText == null || cipherText.length() == 0) {
			return cipherText;
		}

		byte[] cipherBytes = Base64.base64ToByteArray(cipherText);
		byte[] plainBytes = cipher.doFinal(cipherBytes);

		return new String(plainBytes);
	}

	public static String encrypt(String plainText) throws Exception {
		return encrypt((String) null, plainText);
	}

	public static String encrypt(String key, String plainText) throws Exception {
		if (key == null) {
			key = DEFAULT_PRIVATE_KEY_STRING;
		}

		byte[] keyBytes = Base64.base64ToByteArray(key);
		return encrypt(keyBytes, plainText);
	}

	public static String encrypt(byte[] keyBytes, String plainText)
			throws Exception {
		PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
		KeyFactory factory = KeyFactory.getInstance("RSA");
		PrivateKey privateKey = factory.generatePrivate(spec);
		Cipher cipher = Cipher.getInstance("RSA");
        try {
		    cipher.init(Cipher.ENCRYPT_MODE, privateKey);
        } catch (InvalidKeyException e) {
            //For IBM JDK, 原因请看解密方法中的说明
            RSAPrivateKey rsaPrivateKey = (RSAPrivateKey) privateKey;
            RSAPublicKeySpec publicKeySpec = new RSAPublicKeySpec(rsaPrivateKey.getModulus(), rsaPrivateKey.getPrivateExponent());
            Key fakePublicKey = KeyFactory.getInstance("RSA").generatePublic(publicKeySpec);
            cipher = Cipher.getInstance("RSA");
            cipher.init(Cipher.ENCRYPT_MODE, fakePublicKey);
        }

		byte[] encryptedBytes = cipher.doFinal(plainText.getBytes("UTF-8"));
		String encryptedString = Base64.byteArrayToBase64(encryptedBytes);

		return encryptedString;
	}

	public static byte[][] genKeyPairBytes(int keySize)
			throws NoSuchAlgorithmException {
		byte[][] keyPairBytes = new byte[2][];

		KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
		gen.initialize(keySize, new SecureRandom());
		KeyPair pair = gen.generateKeyPair();

		keyPairBytes[0] = pair.getPrivate().getEncoded();
		keyPairBytes[1] = pair.getPublic().getEncoded();

		return keyPairBytes;
	}

	public static String[] genKeyPair(int keySize)
			throws NoSuchAlgorithmException {
		byte[][] keyPairBytes = genKeyPairBytes(keySize);
		String[] keyPairs = new String[2];

		keyPairs[0] = Base64.byteArrayToBase64(keyPairBytes[0]);
		keyPairs[1] = Base64.byteArrayToBase64(keyPairBytes[1]);

		return keyPairs;
	}
	
	static class Base64 {

	    /**
	     * Translates the specified byte array into a Base64 string as per Preferences.put(byte[]).
	     */
	    public static String byteArrayToBase64(byte[] a) {
	        return byteArrayToBase64(a, false);
	    }

	    /**
	     * Translates the specified byte array into an "alternate representation" Base64 string. This non-standard variant
	     * uses an alphabet that does not contain the uppercase alphabetic characters, which makes it suitable for use in
	     * situations where case-folding occurs.
	     */
	    public static String byteArrayToAltBase64(byte[] a) {
	        return byteArrayToBase64(a, true);
	    }

	    private static String byteArrayToBase64(byte[] a, boolean alternate) {
	        int aLen = a.length;
	        int numFullGroups = aLen / 3;
	        int numBytesInPartialGroup = aLen - 3 * numFullGroups;
	        int resultLen = 4 * ((aLen + 2) / 3);
	        StringBuilder result = new StringBuilder(resultLen);
	        char[] intToAlpha = (alternate ? intToAltBase64 : intToBase64);

	        // Translate all full groups from byte array elements to Base64
	        int inCursor = 0;
	        for (int i = 0; i < numFullGroups; i++) {
	            int byte0 = a[inCursor++] & 0xff;
	            int byte1 = a[inCursor++] & 0xff;
	            int byte2 = a[inCursor++] & 0xff;
	            result.append(intToAlpha[byte0 >> 2]);
	            result.append(intToAlpha[(byte0 << 4) & 0x3f | (byte1 >> 4)]);
	            result.append(intToAlpha[(byte1 << 2) & 0x3f | (byte2 >> 6)]);
	            result.append(intToAlpha[byte2 & 0x3f]);
	        }

	        // Translate partial group if present
	        if (numBytesInPartialGroup != 0) {
	            int byte0 = a[inCursor++] & 0xff;
	            result.append(intToAlpha[byte0 >> 2]);
	            if (numBytesInPartialGroup == 1) {
	                result.append(intToAlpha[(byte0 << 4) & 0x3f]);
	                result.append("==");
	            } else {
	                // assert numBytesInPartialGroup == 2;
	                int byte1 = a[inCursor++] & 0xff;
	                result.append(intToAlpha[(byte0 << 4) & 0x3f | (byte1 >> 4)]);
	                result.append(intToAlpha[(byte1 << 2) & 0x3f]);
	                result.append('=');
	            }
	        }
	        // assert inCursor == a.length;
	        // assert result.length() == resultLen;
	        return result.toString();
	    }

	    /**
	     * This array is a lookup table that translates 6-bit positive integer index values into their "Base64 Alphabet"
	     * equivalents as specified in Table 1 of RFC 2045.
	     */
	    private static final char intToBase64[]    = { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
	            'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
	            'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2',
	            '3', '4', '5', '6', '7', '8', '9', '+', '/' };

	    /**
	     * This array is a lookup table that translates 6-bit positive integer index values into their
	     * "Alternate Base64 Alphabet" equivalents. This is NOT the real Base64 Alphabet as per in Table 1 of RFC 2045. This
	     * alternate alphabet does not use the capital letters. It is designed for use in environments where "case folding"
	     * occurs.
	     */
	    private static final char intToAltBase64[] = { '!', '"', '#', '$', '%', '&', '\'', '(', ')', ',', '-', '.', ':',
	            ';', '<', '>', '@', '[', ']', '^', '`', '_', '{', '|', '}', '~', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
	            'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2',
	            '3', '4', '5', '6', '7', '8', '9', '+', '?' };

	    /**
	     * Translates the specified Base64 string (as per Preferences.get(byte[])) into a byte array.
	     * 
	     * @throw IllegalArgumentException if <tt>s</tt> is not a valid Base64 string.
	     */
	    public static byte[] base64ToByteArray(String s) {
	        return base64ToByteArray(s, false);
	    }

	    /**
	     * Translates the specified "alternate representation" Base64 string into a byte array.
	     * 
	     * @throw IllegalArgumentException or ArrayOutOfBoundsException if <tt>s</tt> is not a valid alternate
	     * representation Base64 string.
	     */
	    public static byte[] altBase64ToByteArray(String s) {
	        return base64ToByteArray(s, true);
	    }

	    private static byte[] base64ToByteArray(String s, boolean alternate) {
	        byte[] alphaToInt = (alternate ? altBase64ToInt : base64ToInt);
	        int sLen = s.length();
	        int numGroups = sLen / 4;
	        if (4 * numGroups != sLen) {
	            throw new IllegalArgumentException("String length must be a multiple of four.");
	        }
	        int missingBytesInLastGroup = 0;
	        int numFullGroups = numGroups;
	        if (sLen != 0) {
	            if (s.charAt(sLen - 1) == '=') {
	                missingBytesInLastGroup++;
	                numFullGroups--;
	            }
	            if (s.charAt(sLen - 2) == '=') {
	                missingBytesInLastGroup++;
	            }
	        }
	        byte[] result = new byte[3 * numGroups - missingBytesInLastGroup];

	        // Translate all full groups from base64 to byte array elements
	        int inCursor = 0, outCursor = 0;
	        for (int i = 0; i < numFullGroups; i++) {
	            int ch0 = base64toInt(s.charAt(inCursor++), alphaToInt);
	            int ch1 = base64toInt(s.charAt(inCursor++), alphaToInt);
	            int ch2 = base64toInt(s.charAt(inCursor++), alphaToInt);
	            int ch3 = base64toInt(s.charAt(inCursor++), alphaToInt);
	            result[outCursor++] = (byte) ((ch0 << 2) | (ch1 >> 4));
	            result[outCursor++] = (byte) ((ch1 << 4) | (ch2 >> 2));
	            result[outCursor++] = (byte) ((ch2 << 6) | ch3);
	        }

	        // Translate partial group, if present
	        if (missingBytesInLastGroup != 0) {
	            int ch0 = base64toInt(s.charAt(inCursor++), alphaToInt);
	            int ch1 = base64toInt(s.charAt(inCursor++), alphaToInt);
	            result[outCursor++] = (byte) ((ch0 << 2) | (ch1 >> 4));

	            if (missingBytesInLastGroup == 1) {
	                int ch2 = base64toInt(s.charAt(inCursor++), alphaToInt);
	                result[outCursor++] = (byte) ((ch1 << 4) | (ch2 >> 2));
	            }
	        }
	        // assert inCursor == s.length()-missingBytesInLastGroup;
	        // assert outCursor == result.length;
	        return result;
	    }

	    /**
	     * Translates the specified character, which is assumed to be in the "Base 64 Alphabet" into its equivalent 6-bit
	     * positive integer.
	     * 
	     * @throw IllegalArgumentException or ArrayOutOfBoundsException if c is not in the Base64 Alphabet.
	     */
	    private static int base64toInt(char c, byte[] alphaToInt) {
	        int result = alphaToInt[c];
	        if (result < 0) {
	            throw new IllegalArgumentException("Illegal character " + c);
	        }
	        return result;
	    }

	    /**
	     * This array is a lookup table that translates unicode characters drawn from the "Base64 Alphabet" (as specified in
	     * Table 1 of RFC 2045) into their 6-bit positive integer equivalents. Characters that are not in the Base64
	     * alphabet but fall within the bounds of the array are translated to -1.
	     */
	    private static final byte base64ToInt[]    = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62,
	            -1, -1, -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7,
	            8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28,
	            29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51 };

	    /**
	     * This array is the analogue of base64ToInt, but for the nonstandard variant that avoids the use of uppercase
	     * alphabetic characters.
	     */
	    private static final byte altBase64ToInt[] = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, -1, 62, 9, 10,
	            11, -1, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 12, 13, 14, -1, 15, 63, 16, -1, -1, -1, -1, -1, -1, -1, -1,
	            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 17, -1, 18, 19, 21, 20, 26, 27, 28,
	            29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 22, 23, 24, 25 };

	}

}
