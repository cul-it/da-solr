package edu.cornell.library.integration.utilities;

import java.text.Normalizer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CharacterSetUtils {

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
				  /*08-0F*/ 'ò','Ç','\n','Ø','ø','\r','Å','å',
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
			nonStandardApostrophes_pattern = Pattern.compile("[\u02b9\u02bb\u02bc\u02bd\u02be\u02bf\u2018\u2019]");
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
}
