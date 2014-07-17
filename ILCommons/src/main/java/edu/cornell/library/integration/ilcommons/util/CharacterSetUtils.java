package edu.cornell.library.integration.ilcommons.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CharacterSetUtils {
	
	static Pattern isCJK_pattern = null;
	static Pattern isNotCJK_pattern = null;
	
	public static final float MIN_RATIO = (float)0.15;
	public static final int MIN_CHAR = 2;
	
	/*
	 * If CJK characters constitute a ratio of MIN_RATIO or a count of MIN_CHAR,
	 * the string will be deemed to be CJK. (Primary consideration is whether the string 
	 * warrants CJK search analysis.)
	 */
	public static Boolean isCJK( String s ) {
		if (!hasCJK(s)) return false;
		if (isNotCJK_pattern == null)
			isNotCJK_pattern = Pattern.compile("[^\\p{IsHan}\\p{IsHangul}\\p{IsKatakana}\\p{IsHiragana}]");
		String s2 = isNotCJK_pattern.matcher(s).replaceAll("");
		float ratio = (float) s2.length() / s.length();
		if ((ratio >= MIN_RATIO) || (s2.length() >= MIN_CHAR))
			return true;
		return false;		
	}
	
	/*
	 * Unlike isCJK(s), hasCJK(s) will return true if any CJK characters appear in String s.
	 */
	public static Boolean hasCJK( String s ) {
		if (isCJK_pattern == null)
			isCJK_pattern = Pattern.compile("[\\p{IsHan}\\p{IsHangul}\\p{IsKatakana}\\p{IsHiragana}]");
		Matcher m = isCJK_pattern.matcher(s);
		return m.find();
	}

	/*
	 * Java's built-in String.trim() method trims leading and trailing whitespace, but only
	 * looks for whitespace characters within ASCII ranges. Instead, we want to trim all the
	 * whitespace from Unicode ranges.
	 */
	public static String trimInternationally( String s ) {
		if (s == null) return null;
		if (s.length() == 0) return s;
		int leadingOffset = 0;
		while (leadingOffset < s.length() &&
				Character.isWhitespace(s.charAt(leadingOffset)))
			leadingOffset++;
		if (leadingOffset == s.length())
			return ""; // No non-whitespace characters found. Return empty string
		int trailingOffset = s.length() - 1;
		while (Character.isWhitespace(s.charAt(trailingOffset)))
			trailingOffset--;
		return s.substring(leadingOffset, trailingOffset + 1);
	}

}
