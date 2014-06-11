package edu.cornell.library.integration.ilcommons.util;


import static org.junit.Assert.*;
import org.junit.Test;
import static edu.cornell.library.integration.ilcommons.util.CharacterSetUtils.*;

public class CharacterSetUtilsTest {
	
	@Test
	public void testIsCJK() {
		assertFalse(isCJK("القاهرة : شمس للنشر والتوزيع، 2012.‏‬‎")); // Arabic publication info
		//Japanese title with English Translation
		assertTrue(isCJK("論究ジュリスト = Quarterly jurist"));
		assertTrue(isCJK("一九五〇年代的台灣")); // Chinese Title
		assertFalse(isCJK("Saint Antoine le Grand dans l'Orient chretien")); // French Title
		assertTrue(isCJK("제국 의 위안부")); // Korean Title
		assertFalse(isCJK("Advanced linear algebra"));  // English Title
		//English text with Japanese character sometimes used as emoticon
		assertFalse(isCJK("Hi! How are you? シ"));
	}
	
}
