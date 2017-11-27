package edu.cornell.library.integration.utilities;

import static edu.cornell.library.integration.marc.DataField.PDF_closeRTL;
import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.insertSpaceAfterCommas;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("static-method")
public class IndexingUtilitiesTest {

	@Test
	public void insertSpaceAfterCommasTest() {
	 assertTrue(true);
	 assertTrue(insertSpaceAfterCommas("hello, world").equals("hello, world"));
	 assertTrue(insertSpaceAfterCommas("hello,world").equals("hello, world"));
	 assertTrue(insertSpaceAfterCommas("hello, world,").equals("hello, world,"));
	 assertTrue(insertSpaceAfterCommas("hello, world").equals("hello, world"));
	 assertTrue(insertSpaceAfterCommas("hello,world,  of,fishes").equals("hello, world,  of, fishes"));
	}

	@Test
	public void getSortHeadingTest() {
		assertTrue(getFilingForm("hello").equals("hello"));
		assertTrue(getFilingForm("Hello").equals("hello"));
		assertTrue(getFilingForm("Hello,").equals("hello"));
		assertTrue(getFilingForm("  hello     ").equals("hello"));
		assertTrue(getFilingForm("héllo!").equals("hello"));
		assertTrue(getFilingForm("!!- :::hello::: -!!").equals("hello"));
		assertFalse(getFilingForm("hello").equals("goodbye"));
		assertTrue(getFilingForm("hello-  -,:()@#$%^* hello").equals("hello hello"));
		assertTrue(getFilingForm("Hello>hello").equals("hello 0000 hello"));
		assertTrue(getFilingForm("Hello > hello").equals("hello 0000 hello"));
		assertTrue(getFilingForm("Hello--Hello").equals("hello 0000 hello"));
		assertTrue(getFilingForm("Hello -- Hello").equals("hello 0000 hello"));
		assertTrue(getFilingForm("--Hello").equals("hello"));
		assertTrue(getFilingForm("Hello---Hello").equals("hello 0000 hello"));
		assertTrue(getFilingForm("Hello, Jr., 1910-1997").equals("hello jr 1910 1997"));
		assertTrue(getFilingForm("Hello & Goodbye!").equals("hello goodbye 6&"));
		assertTrue(getFilingForm(" Alzheimer's disease ").equals("alzheimers disease"));
		assertTrue(getFilingForm("‘Abbāsah ukht al-Rashīd aww-nakbat al-Barāmikah").
				equals("abbasah ukht al rashid aww nakbat al baramikah"));
		assertTrue(getFilingForm("’Abd al-Rraḥmān al-Kawākibī").
				equals("abd al rrahman al kawakibi"));
		assertTrue(getFilingForm(" ⁻Adh⁻i b⁻at : ḍr⁻ame").equals("adhi bat drame"));
		assertTrue(getFilingForm("⁻Ac⁻arya, Pushpalat⁻a").equals("acarya pushpalata"));
		assertTrue(getFilingForm("-0-De, Boll Weevil Convention. New Orleans. Nov.").
				equals("0 de boll weevil convention new orleans nov"));
		assertTrue(getFilingForm("°Cómo funciona el Mercado de Seguros Médicos?").
				equals("como funciona el mercado de seguros medicos"));
		assertTrue(getFilingForm("σ and π Electrons in Organic Compounds").
				equals("sigma and pi electrons in organic compounds"));
		assertTrue(getFilingForm("α- and β- modifications of benzene hexabromid").
				equals("alpha and beta modifications of benzene hexabromid"));
		assertTrue(getFilingForm("£1,000,000 bank-note and other stories").
				equals("1000000 bank note and other stories"));
		assertTrue(getFilingForm("€Tudes de Centre de DV̈eloppement Financer le dV̈eloppment").
				equals("tudes de centre de dveloppement financer le dveloppment"));
		assertTrue(getFilingForm("†Wilhelm His").equals("wilhelm his"));
		assertTrue(getFilingForm("").equals(""));
		assertTrue(getFilingForm("         ").equals(""));
		// non-standard space characters \u3000 is common in CJK text
		assertTrue(getFilingForm("\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u200B\u202F\u205F\u3000\uFEFF").equals(""));
	    assertTrue(getFilingForm("sydsæter knut").equals("sydsaeter knut"));

	    // non-Roman scripts are not fully or well supported in filing
	    assertTrue(getFilingForm("قيرواني، محمد الطيب الطويلي").equals("قيرواني محمد الطيب الطويلي"));
	    // Orig. string has control characters (hex:200E,200F,202B,202C). Should be stripped.
	    assertTrue(getFilingForm("‎‫‏شماخ بن ضرار،‏‬‎").equals("شماخ بن ضرار"));
	    System.out.println(getFilingForm("ʼImārah, Abū al-ʼIlā. | Taftish al-nihā’ī"));
	}

	@Test
	public void removeTrailingPunctuationTest() {
		assertEquals("Hi",         removeTrailingPunctuation("Hi.....",      ". "));
		assertEquals("Hi",         removeTrailingPunctuation("Hi /",         "./ "));
		assertEquals("Smith, J.R.",removeTrailingPunctuation("Smith, J.R.,", ",. "));
		assertEquals("Smith, John",removeTrailingPunctuation("Smith, John.,",",. "));
		assertEquals("A, B, etc.", removeTrailingPunctuation("A, B, etc. /", "/,. "));
		assertEquals("etc.",       removeTrailingPunctuation("etc. /",       "/,. "));
		assertEquals("A",          removeTrailingPunctuation("A.",           "/,. "));
		assertEquals("",           removeTrailingPunctuation(".....",        ". "));
		assertEquals("",           removeTrailingPunctuation("",             ""));
		assertEquals("",           removeTrailingPunctuation("",             null));
		assertEquals("asdf.",      removeTrailingPunctuation("asdf.",        null));
		assertEquals("etc.",       removeTrailingPunctuation("etc /",        "/. "));
		assertEquals("etc.",       removeTrailingPunctuation("etc",          "/. "));
		assertEquals("Vance, A.",  removeTrailingPunctuation("Vance, A",     "/. "));
		assertEquals("Vance,A.",   removeTrailingPunctuation("Vance,A",      "/. "));
		assertEquals("363909803X", removeTrailingPunctuation("363909803X",   "/. "));
		assertEquals("12-X",       removeTrailingPunctuation("12-X",         "/. "));
		assertEquals("Jr.",        removeTrailingPunctuation("Jr",           "/. "));
		assertEquals("Herndon, James B., Jr.",  removeTrailingPunctuation("Herndon, James B., Jr.",    "/. "));
		assertEquals("Herndon, James B., Jr.",  removeTrailingPunctuation("Herndon, James B., Jr",    "/. "));
		assertEquals("NetLibrary, Inc.",        removeTrailingPunctuation("NetLibrary, Inc.",    "/. "));
		assertEquals("NetLibrary, Inc.",        removeTrailingPunctuation("NetLibrary, Inc",    "/. "));
		assertNull(removeTrailingPunctuation(null,"3"));
		assertEquals("Hi"+PDF_closeRTL,         removeTrailingPunctuation("Hi....."+PDF_closeRTL,      ". "));
		assertEquals("Hi"+PDF_closeRTL,         removeTrailingPunctuation("Hi /"+PDF_closeRTL,         "./ "));
		assertEquals("Smith, J.R."+PDF_closeRTL,removeTrailingPunctuation("Smith, J.R.,"+PDF_closeRTL, ",. "));
		assertEquals("Smith, John"+PDF_closeRTL,removeTrailingPunctuation("Smith, John.,"+PDF_closeRTL,",. "));
		assertEquals("A, B, etc."+PDF_closeRTL, removeTrailingPunctuation("A, B, etc. /"+PDF_closeRTL, "/,. "));
		assertEquals("etc."+PDF_closeRTL,       removeTrailingPunctuation("etc. /"+PDF_closeRTL,       "/,. "));
		assertEquals("A"+PDF_closeRTL,          removeTrailingPunctuation("A."+PDF_closeRTL,           "/,. "));
		assertEquals(""+PDF_closeRTL,           removeTrailingPunctuation("....."+PDF_closeRTL,        ". "));
		assertEquals(""+PDF_closeRTL,           removeTrailingPunctuation(""+PDF_closeRTL,             ""));
		assertEquals(""+PDF_closeRTL,           removeTrailingPunctuation(""+PDF_closeRTL,             null));
		assertEquals("asdf."+PDF_closeRTL,      removeTrailingPunctuation("asdf."+PDF_closeRTL,        null));
	}
}
