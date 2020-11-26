package com.bigdata.util;

import java.security.MessageDigest;
import java.util.Locale;

public class MD5EncryptTools {
	public static String str2MD5Encrypt(String passWordString) {
		try {
			if ((passWordString == null) || ("".equals(passWordString))) {
				return null;
			}
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(passWordString.getBytes());

			String s = byte2string(md.digest());
			s = s.replace("0x", "").replaceAll(" ", "");
			return s.toUpperCase(Locale.US);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private static String byte2string(byte[] b) {
		if (b == null) {
			return null;
		}

		StringBuffer strHEX = new StringBuffer(16);
		strHEX.append("0x ");

		for (int i = 0; i < b.length; i++) {
			strHEX.append(Integer.toHexString(b[i] >> 4 & 0xF));
			strHEX.append(Integer.toHexString(b[i] & 0xF));
			strHEX.append(' ');
		}

		return strHEX.toString();
	}

	public static String str2MD5SaltEncrypt(String passWordString, String salt) {
		try {
			if ((passWordString == null) || ("".equals(passWordString))) {
				return null;
			}

			if ((salt != null) && (!"".equals(salt))) {
				passWordString = passWordString + salt;
			}

			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(passWordString.getBytes());

			String s = byte2string(md.digest());
			s = s.replace("0x", "").replaceAll(" ", "");
			return s.toUpperCase(Locale.US);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {
		System.out.println(MD5EncryptTools.str2MD5Encrypt("18601723328"));
		System.out.println("18601723328".substring(0, 7));
	}
}
