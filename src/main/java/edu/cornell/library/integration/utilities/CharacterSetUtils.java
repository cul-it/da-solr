package edu.cornell.library.integration.utilities;

import java.text.Normalizer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
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
	 * Remove various diacritics and characters from the provided string
	 * that are not supported by the GSM character set used for SMS. Note
	 * that is method does <i>not</i> include a character set conversion.
	 * @param orig
	 * @return GSM-sanitized version of orig
	 */
	public static String limitStringToGSMChars( String orig ) {
		if (orig == null)
			return null;
		if (gsmChars == null) {
			// Identification of characters in the GSM character set comes from:
			// https://www.clockworksms.com/blog/the-gsm-character-set/
			Character[] gsmCharsArray =
				{ /*00-07*/ '@','£','$','¥','è','é','ù','ì',
				  /*08-0F*/ 'ò','Ç','\r','Ø','ø','\n','Å','å',
				  /*10-17*/ '∆','_','Φ','Γ','Λ','Ω','Π','Ψ',
				  /*18-1F*/ 'Σ','Θ','Ξ','\u001B','Æ','æ','ß','É',
				  /*20-27*/ ' ','!','"','#','¤','%','&','\'',
				  /*28-2F*/ '(',')','*','+',',','-','.','/',
				  /*30-37*/ '0','1','2','3','4','5','6','7',
				  /*38-3F*/ '8','9',':',';','<','=','>','?',
				  /*40-47*/ '¡','A','B','C','D','E','F','G',
				  /*48-4F*/ 'H','I','J','K','L','M','N','O',
				  /*50-57*/ 'P','Q','R','S','T','U','V','W',
				  /*58-5F*/ 'X','Y','Z','Ä','Ö','Ñ','Ü','§',
				  /*60-67*/ '¿','a','b','c','d','e','f','g',
				  /*68-6F*/ 'h','i','j','k','l','m','n','o',
				  /*70-77*/ 'p','q','r','s','t','u','v','w',
				  /*78-7F*/ 'x','y','z','ä','ö','ñ','ü','à',
				  // Extended characters used with escape char (1B)
				  '\u000c',//form feed
				  '^','{','}','\\','[','~',']','|','€'
				};
			gsmChars = new HashSet<>( Arrays.asList(gsmCharsArray ));
		}
		String s = Normalizer.normalize(orig, Normalizer.Form.NFC);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < s.length(); i++) {
			Character c = s.charAt(i);
			if (gsmChars.contains(c)) {
				sb.append(c);
				continue;
			}
			String s_withoutDiacritics = 
					Normalizer.normalize(String.valueOf(c), Normalizer.Form.NFD).
					replaceAll("[\\p{InCombiningDiacriticalMarks}]+", "");
			if ( ! s_withoutDiacritics.isEmpty()) {
				Character c_withoutDiacritics = s_withoutDiacritics.charAt(0);
				if (gsmChars.contains(c_withoutDiacritics)) {
					sb.append(c_withoutDiacritics);
					continue;
				}
				String s_compatibility = Normalizer.normalize(s_withoutDiacritics, Normalizer.Form.NFKD);
				if ( ! s_compatibility.isEmpty() ) {
					Character c_compatibility = s_compatibility.charAt(0);
					if (gsmChars.contains(c_compatibility)) {
						sb.append(c_compatibility);
						continue;
					}
				}
			}
			// c is not supported by the GSM character set.
		}
		return sb.toString().trim();
	}
	private static Set<Character> gsmChars = null;


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
	 * Removes any whitespace characters from the front and end of the string, while replacing
	 * any interior sequences of consecutive whitespace characters with single, standard spaces.
	 * @param s
	 * Original String
	 * @return
	 * Standardized String
	 */
	public static String standardizeSpaces( String s ) {
		if (s == null) return null;
		StringBuilder sb = new StringBuilder();
		boolean prevSpace = true;
		for (int i = 0; i < s.length(); i++){
			char c = s.charAt(i);
			if (Character.isWhitespace(c)) {
				if (prevSpace)
					continue;
				prevSpace = true;
				sb.append(' ');
			} else {
				sb.append(c);
				prevSpace = false;
			}
		}
		if (prevSpace && sb.length() > 0)
			sb.setLength(sb.length() - 1);
		return sb.toString();
	}

	/**
	 * Remove the number of characters from the beginning of <b>s</b> to constitute the first
	 * <b>targetCount</b> characters <b>s</b>, counting combining diacritics as separate characters.
	 * Any characters found in <b>reserves</b> will not be removed, but will still count toward the
	 * <b>targetCount</b> characters.<br/><br/>
	 * If character number <b>targetCount</b> is not the last byte of its containing grapheme, the entire
	 * partial character will be removed.<br/><br/>
	 * The implementation will return the shortened string in Unicode NFC form, even if it was not supplied
	 * that way.
	 * @param s
	 *  Original string
	 * @param targetCount
	 *  Number of characters (counting combining diacritics separately) to remove from the beginning of string
	 * @param reserves
	 *  Characters that should not be removed, but will still count toward the <b>targetCount</b> characters.
	 * @return
	 *  Stripped string
	 * @throws IllegalArgumentException
	 *  will be thrown if <b>targetCount</b> characters exceeds the length of string <b>s</b>.
	 */
	public static String stripLeadCharsFromString( String s, Integer targetCount, String reserves )
			throws IllegalArgumentException {

		int pos = 0;
		int count = 0;
		StringBuilder foundReserves = new StringBuilder();
		s = Normalizer.normalize(s, Normalizer.Form.NFD);
		if ( s.length() < targetCount )
			throw new IllegalArgumentException(
					"Requested bytes to strip is longer than string in UTF-8");
		while (count < targetCount) {
			char c = s.charAt(pos);

			if (Character.isHighSurrogate(c))
				pos++;
			pos++;
			count++;
			if (reserves.indexOf(c) != -1)
				foundReserves.append(c);
		}
		String substr = s.substring(pos);
		while (substr.length() > 0 &&
				Character.getType(substr.charAt(0)) == Character.NON_SPACING_MARK)
			substr = substr.substring(1);
		return foundReserves.toString()
				+ Normalizer.normalize(substr,Normalizer.Form.NFC);

	}
}
