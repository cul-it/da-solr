package edu.cornell.library.integration.utilities;

public class StringUtils {
	public static String stringAfter(String src, String delimiter) {
		int delimiterIndex = src.indexOf(delimiter);
		if (delimiterIndex != -1)
			return src.substring(delimiterIndex + delimiter.length());
		return null;
	}
}
