package edu.cornell.library.integration.ilcommons.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CharacterSetUtils {
	
	static Pattern IS_CJK = null;
	static Pattern IS_NOT_CJK = null;
	
	public static final float MIN_RATIO = (float)0.15;
	public static final int MIN_CHAR = 3;
	
	/*
	 * If CJK characters constitute a ratio of MIN_RATIO or a count of MIN_CHAR,
	 * the string will be deemed to be CJK. (Primary consideration is whether the string 
	 * warrants CJK search analysis.)
	 */
	public static Boolean isCJK( String s ) {
		if (IS_CJK == null)
			IS_CJK = Pattern.compile("[\\p{IsHan}\\p{IsHangul}\\p{IsKatakana}\\p{IsHiragana}]");
		Matcher m = IS_CJK.matcher(s);
		if (!m.find())
			return false;
		if (IS_NOT_CJK == null)
			IS_NOT_CJK = Pattern.compile("[^\\p{IsHan}\\p{IsHangul}\\p{IsKatakana}\\p{IsHiragana}]");
		String s2 = IS_NOT_CJK.matcher(s).replaceAll("");
		float ratio = (float) s2.length() / s.length();
		if ((ratio >= MIN_RATIO) || (s2.length() >= MIN_CHAR))
			return true;
		return false;		
	}

}
