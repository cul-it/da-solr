package edu.cornell.library.integration.ilcommons.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CharacterSetUtils {

//	public static String RLE = "RLE"; //\u202b - Begin Right-to-left Embedding
//	public static String PDF = "PDF"; //\u202c - Pop(End) Directional Formatting
	public static String RLE_openRTL = "\u200E\u202B\u200F";//\u200F - strong RTL invis char
	public static String PDF_closeRTL = "\u200F\u202C\u200E"; //\u200E - strong LTR invis char

	static Pattern isCJK_pattern = null;
	static Pattern isNotCJK_pattern = null;
	static Pattern nonStandardApostrophes_pattern = null;

	public static final float MIN_RATIO = (float)0.15;
	public static final int MIN_CHAR = 2;

	/**
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
	
	/**
	 * Unlike isCJK(s), hasCJK(s) will return true if any CJK characters appear in String s.
	 */
	public static Boolean hasCJK( String s ) {
		if (isCJK_pattern == null)
			isCJK_pattern = Pattern.compile("[\\p{IsHan}\\p{IsHangul}\\p{IsKatakana}\\p{IsHiragana}]");
		Matcher m = isCJK_pattern.matcher(s);
		return m.find();
	}

	/**
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

	/**
	 * Some of the Unicode apostrophe-like characters are not handled as expected by the
	 * ICUFoldingFilter (they are removed rather than folded into standard apostrophes).
	 * This causes search discrepancies. See DISCOVERYACCESS-1084, DISCOVERYACCESS-1408.
	 * This method is only for values intended for searching and not display (incl. facets).
	 */
	public static String standardizeApostrophes( String s ) {
		if (nonStandardApostrophes_pattern == null)
			nonStandardApostrophes_pattern = Pattern.compile("[\u02bb\u02be\u02bc\u02b9\u02bf]");
		if (s == null) return null;
		return nonStandardApostrophes_pattern.matcher(s).replaceAll("'");
	}

}
