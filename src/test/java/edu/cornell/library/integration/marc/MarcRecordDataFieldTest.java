package edu.cornell.library.integration.marc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

@SuppressWarnings("static-method")
public class MarcRecordDataFieldTest {

	@Test
	/*
	 * If we can successfully round-trip the field, converting the subfield separator to a
	 * different character, then we demonstrate that we are parsing the original string into
	 * appropriate subfields.
	 */
	public void createDataFieldFromSubfieldsStringTest() {
		assertEquals( "100 00 $a Field a $b Field b $a Field a #2",
				new DataField(1,"100",'0','0',"‡a Field a ‡b Field b ‡a Field a #2").toString('$') );
	}


	private final String titlePrefixChars = "[.\"“";
	@Test
	public void testStripLeadCharsFromString(){
		assertTrue(DataField.stripLeadCharsFromString(
				"1234", 0, titlePrefixChars).equals("1234"));
		assertTrue(DataField.stripLeadCharsFromString(
				"1234", 3, titlePrefixChars).equals("4"));
		assertTrue(DataField.stripLeadCharsFromString(
				"1234", 4, titlePrefixChars).equals(""));

		/* The following examples come from the Library of Congress examples for encoding of
		 * non-filing characters. The stripped forms do not. http://www.loc.gov/marc/bibliographic/bd245.html
		 * The last example appears appears in a modified form, using standard ASCII quotes in the title
		 * rather than the Unicode left and right quotes used on LoC's web page. The original form
		 * is problematic because the Unicode quotes are two-bytes wide, making the prescribed non-
		 * filing character count of 5 include the first byte but not the second byte of the quote. 
		 * If this ever becomes an issue in our actual catalog, we may need to make an adaptive exception
		 * to the logic for the Unicode left double quote character. */
		assertTrue(DataField.stripLeadCharsFromString(
				"The Year book of medicine.",4,titlePrefixChars)
				.equals("Year book of medicine."));
		assertTrue(DataField.stripLeadCharsFromString(
				"A report to the legislature for the year ...", 2, titlePrefixChars)
				.equals("report to the legislature for the year ..."));
		assertTrue(DataField.stripLeadCharsFromString(
				"L'enfant criminal.", 2, titlePrefixChars)
				.equals("enfant criminal."));
		assertTrue(DataField.stripLeadCharsFromString(
				"[The Part of Pennsylvania that ... townships].", 5, titlePrefixChars)
				.equals("[Part of Pennsylvania that ... townships]."));
		assertTrue(DataField.stripLeadCharsFromString(
				"--the serpent--snapping eye", 6, titlePrefixChars)
				.equals("serpent--snapping eye"));
		assertTrue(DataField.stripLeadCharsFromString(
				"The ... annual report to the Governor.", 8, titlePrefixChars)
				.equals("...annual report to the Governor."));
		assertTrue(DataField.stripLeadCharsFromString(
				"L'été.", 2, titlePrefixChars).equals("été."));
		assertTrue(DataField.stripLeadCharsFromString(
				"Hē Monē tou Horous Sina.", 4, titlePrefixChars)
				.equals("Monē tou Horous Sina."));
		assertTrue(DataField.stripLeadCharsFromString(
				"Tōn meionotētōn eunoia :", 5, titlePrefixChars)
				.equals("meionotētōn eunoia :"));
		assertTrue(DataField.stripLeadCharsFromString(
				"Tōn Diōnos Rōmaikōn historiōn eikositria biblia =", 5, titlePrefixChars)
				.equals("Diōnos Rōmaikōn historiōn eikositria biblia ="));
		assertTrue(DataField.stripLeadCharsFromString(
				"The “winter mind” :", 5, titlePrefixChars).equals("“winter mind” :"));
		assertTrue(DataField.stripLeadCharsFromString(
				"The \"winter mind\" :", 5, titlePrefixChars)
				.equals("\"winter mind\" :"));
		assertTrue(DataField.stripLeadCharsFromString(
				"ʻImma, ou, Rites, coutumes et croyances...", 1, titlePrefixChars)
				.equals("Imma, ou, Rites, coutumes et croyances..."));

		// An incorrectly coded count of characters to remove may leave a floating diacritic.
		// That diacritic should also be removed.
		assertTrue(DataField.stripLeadCharsFromString(
				"Hē Monē tou Horous Sina.", 2, titlePrefixChars)
				.equals(" Monē tou Horous Sina."));
	}
	@Test(expected=IllegalArgumentException.class)
	public void testStripBytesFromStringTooGreedyException(){
		DataField.stripLeadCharsFromString("Hē Monē tou Horous Sina.", 50, titlePrefixChars);
	}


}
