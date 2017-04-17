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


	private final String titlePrefixChars = "[.\"“";
	@Test
	public void testStripLeadCharsFromString(){
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"1234", 0, titlePrefixChars).equals("1234"));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"1234", 3, titlePrefixChars).equals("4"));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"1234", 4, titlePrefixChars).equals(""));

		/* The following examples come from the Library of Congress examples for encoding of
		 * non-filing characters. The stripped forms do not. http://www.loc.gov/marc/bibliographic/bd245.html
		 * The last example appears appears in a modified form, using standard ASCII quotes in the title
		 * rather than the Unicode left and right quotes used on LoC's web page. The original form
		 * is problematic because the Unicode quotes are two-bytes wide, making the prescribed non-
		 * filing character count of 5 include the first byte but not the second byte of the quote. 
		 * If this ever becomes an issue in our actual catalog, we may need to make an adaptive exception
		 * to the logic for the Unicode left double quote character. */
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"The Year book of medicine.",4,titlePrefixChars)
				.equals("Year book of medicine."));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"A report to the legislature for the year ...", 2, titlePrefixChars)
				.equals("report to the legislature for the year ..."));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"L'enfant criminal.", 2, titlePrefixChars)
				.equals("enfant criminal."));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"[The Part of Pennsylvania that ... townships].", 5, titlePrefixChars)
				.equals("[Part of Pennsylvania that ... townships]."));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"--the serpent--snapping eye", 6, titlePrefixChars)
				.equals("serpent--snapping eye"));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"The ... annual report to the Governor.", 8, titlePrefixChars)
				.equals("...annual report to the Governor."));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"L'été.", 2, titlePrefixChars).equals("été."));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"Hē Monē tou Horous Sina.", 4, titlePrefixChars)
				.equals("Monē tou Horous Sina."));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"Tōn meionotētōn eunoia :", 5, titlePrefixChars)
				.equals("meionotētōn eunoia :"));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"Tōn Diōnos Rōmaikōn historiōn eikositria biblia =", 5, titlePrefixChars)
				.equals("Diōnos Rōmaikōn historiōn eikositria biblia ="));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"The “winter mind” :", 5, titlePrefixChars).equals("“winter mind” :"));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"The \"winter mind\" :", 5, titlePrefixChars)
				.equals("\"winter mind\" :"));
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"ʻImma, ou, Rites, coutumes et croyances...", 1, titlePrefixChars)
				.equals("Imma, ou, Rites, coutumes et croyances..."));

		// An incorrectly coded count of characters to remove may leave a floating diacritic.
		// That diacritic should also be removed.
		assertTrue(CharacterSetUtils.stripLeadCharsFromString(
				"Hē Monē tou Horous Sina.", 2, titlePrefixChars)
				.equals(" Monē tou Horous Sina."));
	}
	@Test(expected=IllegalArgumentException.class)
	public void testStripBytesFromStringTooGreedyException(){
		CharacterSetUtils.stripLeadCharsFromString("Hē Monē tou Horous Sina.", 50, titlePrefixChars);
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
