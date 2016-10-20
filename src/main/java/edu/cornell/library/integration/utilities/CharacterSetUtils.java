package edu.cornell.library.integration.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

	/**
	 * Remove the number of characters from the beginning of <b>s</b> to constitute the first
	 * <b>b</b> bytes of the UTF-8 representation of <b>s</b>. Any characters found in 
	 * <b>reserves</b> will not be removed, but will still count toward the <b>b</b> bytes.<br/><br/>
	 * The implementation attempts to identify the target character count without actually casting
	 * the string to UTF-8. Logic for predicting the UTF-8 byte size of Java char values is adapted
	 * from http://stackoverflow.com/questions/8511490
	 * @param s
	 *  Original string
	 * @param b
	 *  Number of UTF-8 bytes to remove from the beginning of string
	 * @param reserves
	 *  Characters that should not be removed, but will still count toward the target <b>b</b> bytes.
	 * @return
	 *  Stripped string
	 * @throws IllegalArgumentException
	 *  will be thrown in two conditions:<br/>
	 *  <ul><li> <b>b</b> &gt; <b>s</b>.getBytes(StandardCharsets.UTF-8).length </li>
	 *      <li> byte number <b>b</b> is not the last byte of its containing character</li></ul>
	 */
	public static String stripBytesFromString( String s, Integer b, String reserves )
			throws IllegalArgumentException {

		int pos = 0;
		int byteCount = 0;
		List<Character> foundReserves = new ArrayList<>();
		while (pos < s.length() && byteCount < b) {
			char c = s.charAt(pos);
			int charSize = 0;

			if (c <= 0x7F)
				charSize = 1;
			else if (c < 0x7FF)
				charSize = 2;
			else if (Character.isHighSurrogate(c))
				charSize = 4;
			else
				charSize = 3;

			if (charSize + byteCount <= b) {
				pos += (charSize == 4)?2:1;
				byteCount+=charSize;
				if (reserves.indexOf(c) != -1)
					foundReserves.add(c);					
			} else
				throw new IllegalArgumentException(
						"Requested bytes to strip would divide a wide character");
		}
		if ( byteCount < b )
			throw new IllegalArgumentException(
					"Requested bytes to strip is longer than string in UTF-8");

		return foundReserves.stream().map(Object::toString).collect(Collectors.joining())
				+ s.substring(pos);

	}
}
