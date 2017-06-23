package edu.cornell.library.integration.utilities;


import static edu.cornell.library.integration.utilities.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.isCJK;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.trimInternationally;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.limitStringToGSMChars;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

@SuppressWarnings("static-method")
public class CharacterSetUtilsTest {

	@Test
	public void testLimitStringToGSMChars() {
		assertEquals("Advanced linear algebra",
		 limitStringToGSMChars("Advanced linear algebra"));
		assertEquals("Dedha ishqiya 11/2",
		 limitStringToGSMChars("Ḍeḍha ishqiya 11/2"));
		assertEquals("= Quarterly jurist",
		 limitStringToGSMChars("論究ジュリスト = Quarterly jurist"));
		assertEquals("Andrei Makine : hétérotopies, hétérochronies",
		 limitStringToGSMChars("Andreï Makine : hétérotopies, hétérochronies"));
		assertEquals("Marci Tullii Ciceronis epistolarum libri IV Cum postremis H. Stephani &"
				+ " D. Lambini editionibus diligenter collati, & quàm accuratissimè emendati. A Joanne Sturmo"
				+ " ... Huic editioni accesserunt Græca Latinis expressa",
		 limitStringToGSMChars(
					"Marci Tullii Ciceronis epistolarum libri IV Cum postremis H. Stephani &"
				+ " D. Lambini editionibus diligenter collati, & quàm accuratissimè emendati. A Joanne Sturmı́o"
				+ " ... Huic editioni accesserunt Græca Latinis expressa"));
	}

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
	
	/*
	 * We expect the same results from hasCJK() as isCJK(), except for the final test,
	 * which should return true instead of false.
	 */
	@Test
	public void testHasCJK() {
		assertFalse(hasCJK("القاهرة : شمس للنشر والتوزيع، 2012.‏‬‎")); 
		assertTrue(hasCJK("論究ジュリスト = Quarterly jurist"));
		assertTrue(hasCJK("一九五〇年代的台灣"));
		assertFalse(hasCJK("Saint Antoine le Grand dans l'Orient chretien"));
		assertTrue(hasCJK("제국 의 위안부"));
		assertFalse(hasCJK("Advanced linear algebra"));
		assertTrue(hasCJK("Hi! How are you? シ"));
}
	
	@Test
	public void testTrimInternationally() {
		// tests involving standard ASCII spacing
		assertTrue(trimInternationally(" abc ").equals("abc"));
		assertTrue(trimInternationally("   ").equals(""));
		assertTrue(trimInternationally("\tHello,   World !\n").equals("Hello,   World !"));
		assertTrue(trimInternationally("Hello, World.").equals("Hello, World."));
		// CJK spacing
		assertTrue(trimInternationally("　　俄国　东正教　侵　华　史略　").equals("俄国　东正教　侵　华　史略"));
	}

	@Test
	public void testStandardizeSpaces() {
		assertTrue(CharacterSetUtils.standardizeSpaces(" hello ").equals("hello"));
		assertTrue(CharacterSetUtils.standardizeSpaces(" hello  world!").equals("hello world!"));
		assertTrue(CharacterSetUtils.standardizeSpaces("　　俄国　东正教　侵　华　史略　").equals("俄国 东正教 侵 华 史略"));
		assertTrue(CharacterSetUtils.standardizeSpaces("").equals(""));
		assertTrue(CharacterSetUtils.standardizeSpaces(" ").equals(""));
		assertTrue(CharacterSetUtils.standardizeSpaces("  ").equals(""));
		assertTrue(CharacterSetUtils.standardizeSpaces("　　").equals(""));
	}

}
