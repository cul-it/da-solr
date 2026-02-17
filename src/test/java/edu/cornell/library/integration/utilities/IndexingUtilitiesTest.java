package edu.cornell.library.integration.utilities;

import static edu.cornell.library.integration.marc.DataField.PDF_closeRTL;
import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.insertSpaceAfterCommas;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.*;

import java.text.Normalizer;

import org.junit.Test;

public class IndexingUtilitiesTest {

	@Test
	public void insertSpaceAfterCommasTest() {
	 assertTrue(true);
	 assertEquals("hello, world",insertSpaceAfterCommas("hello, world"));
	 assertEquals("hello, world",insertSpaceAfterCommas("hello,world"));
	 assertEquals("hello, world,",insertSpaceAfterCommas("hello, world,"));
	 assertEquals("hello, world",insertSpaceAfterCommas("hello, world"));
	 assertEquals("hello, world,  of, fishes",insertSpaceAfterCommas("hello,world,  of,fishes"));
	}

	@Test
	public void getSortHeadingTest() {
		assertEquals("hello",getFilingForm("hello"));
		assertEquals("hello",getFilingForm("Hello"));
		assertEquals("hello",getFilingForm("Hello,"));
		assertEquals("hello",getFilingForm("  hello     "));
		assertEquals("hello",getFilingForm("héllo!"));
		assertEquals("hello",getFilingForm("!!- :::hello::: -!!"));
		assertFalse(getFilingForm("hello").equals("goodbye"));
		assertEquals("hello hello",getFilingForm("hello-  -,:()@#$%^* hello"));
		assertEquals("hello 0000 hello",getFilingForm("Hello>hello"));
		assertEquals("hello 0000 hello",getFilingForm("Hello > hello"));
		assertEquals("hello 0000 hello",getFilingForm("Hello--Hello"));
		assertEquals("hello 0000 hello",getFilingForm("Hello -- Hello"));
		assertEquals("hello",getFilingForm("--Hello"));
		assertEquals("hello 0000 hello",getFilingForm("Hello---Hello"));
		assertEquals("hello jr 1910 1997",getFilingForm("Hello, Jr., 1910-1997"));
		assertEquals("hello goodbye 6&",getFilingForm("Hello & Goodbye!"));
		assertEquals("alzheimers disease",getFilingForm(" Alzheimer's disease "));
		assertEquals("abbasah ukht al rashid aww nakbat al baramikah",
				getFilingForm("‘Abbāsah ukht al-Rashīd aww-nakbat al-Barāmikah"));
		assertEquals("abd al rrahman al kawakibi",getFilingForm("’Abd al-Rraḥmān al-Kawākibī"));
		assertEquals("adhi bat drame",getFilingForm(" ⁻Adh⁻i b⁻at : ḍr⁻ame"));
		assertEquals("acarya pushpalata",getFilingForm("⁻Ac⁻arya, Pushpalat⁻a"));
		assertEquals("0 de boll weevil convention new orleans nov",
				getFilingForm("-0-De, Boll Weevil Convention. New Orleans. Nov."));
		assertEquals("como funciona el mercado de seguros medicos",
				getFilingForm("°Cómo funciona el Mercado de Seguros Médicos?"));
		assertEquals("sigma and pi electrons in organic compounds",
				getFilingForm("σ and π Electrons in Organic Compounds"));
		assertEquals("alpha and beta modifications of benzene hexabromid",
				getFilingForm("α- and β- modifications of benzene hexabromid"));
		assertEquals("1000000 bank note and other stories",getFilingForm("£1,000,000 bank-note and other stories"));
		assertEquals("tudes de centre de dveloppement financer le dveloppment",
				getFilingForm("€Tudes de Centre de DV̈eloppement Financer le dV̈eloppment"));
		assertEquals("wilhelm his",getFilingForm("†Wilhelm His"));
		assertEquals("",getFilingForm(""));
		assertEquals("",getFilingForm("         "));
		// non-standard space characters \u3000 is common in CJK text
		assertEquals("",
				getFilingForm("\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u200B\u202F\u205F\u3000\uFEFF"));
		assertEquals("sydsaeter knut",getFilingForm("sydsæter knut"));
		assertEquals("imarah abu al ila 0000 taftish al nihai",
				getFilingForm("ʼImārah, Abū al-ʼIlā. | Taftish al-nihā’ī"));
		assertEquals("anarkhiia evoliutsiia bez nasiliia",getFilingForm("Anarkhii︠a︡ ėvoli︠u︡t︠s︡ii︠a︡ bez nasilii︠a︡"));
		assertEquals("bhura kri cha ra to",getFilingForm("Bhurāʺ krīʺ, Cha rā toʻ"));

	    // non-Roman scripts are not fully or well supported in filing
		assertEquals("قيرواني محمد الطيب الطويلي",getFilingForm("قيرواني، محمد الطيب الطويلي"));
	    // Orig. string has control characters (hex:200E,200F,202B,202C). Should be stripped.
		assertEquals("شماخ بن ضرار",getFilingForm("‎‫‏شماخ بن ضرار،‏‬‎"));
	}

	@Test
	public void removeTrailingPunctuationTest() {
		assertEquals("Hi.....",    removeTrailingPunctuation("Hi.....",      ". "));
		assertEquals("Hi",         removeTrailingPunctuation("Hi /",         "./ "));
		assertEquals("Smith, J.R.",removeTrailingPunctuation("Smith, J.R.,", ",. "));
		assertEquals("Smith, John",removeTrailingPunctuation("Smith, John.,",",. "));
		assertEquals("A, B, etc.", removeTrailingPunctuation("A, B, etc. /", "/,. "));
		assertEquals("etc.",       removeTrailingPunctuation("etc. /",       "/,. "));
		assertEquals("A",          removeTrailingPunctuation("A.",           "/,. "));
		assertEquals(".....",      removeTrailingPunctuation(".....",        ". "));
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
		assertEquals("Social Security accountability report for ...",
				                   removeTrailingPunctuation("Social Security accountability report for ...","/. "));
		assertEquals("Herndon, James B., Jr.",  removeTrailingPunctuation("Herndon, James B., Jr.",    "/. "));
		assertEquals("Herndon, James B., Jr.",  removeTrailingPunctuation("Herndon, James B., Jr",    "/. "));
		assertEquals("NetLibrary, Inc.",        removeTrailingPunctuation("NetLibrary, Inc.",    "/. "));
		assertEquals("NetLibrary, Inc.",        removeTrailingPunctuation("NetLibrary, Inc",    "/. "));
		assertNull(removeTrailingPunctuation(null,"3"));
		assertEquals("Hi....."+PDF_closeRTL,    removeTrailingPunctuation("Hi....."+PDF_closeRTL,      ". "));
		assertEquals("Hi"+PDF_closeRTL,         removeTrailingPunctuation("Hi /"+PDF_closeRTL,         "./ "));
		assertEquals("Smith, J.R."+PDF_closeRTL,removeTrailingPunctuation("Smith, J.R.,"+PDF_closeRTL, ",. "));
		assertEquals("Smith, John"+PDF_closeRTL,removeTrailingPunctuation("Smith, John.,"+PDF_closeRTL,",. "));
		assertEquals("A, B, etc."+PDF_closeRTL, removeTrailingPunctuation("A, B, etc. /"+PDF_closeRTL, "/,. "));
		assertEquals("etc."+PDF_closeRTL,       removeTrailingPunctuation("etc. /"+PDF_closeRTL,       "/,. "));
		assertEquals("A"+PDF_closeRTL,          removeTrailingPunctuation("A."+PDF_closeRTL,           "/,. "));
		assertEquals("....."+PDF_closeRTL,      removeTrailingPunctuation("....."+PDF_closeRTL,        ". "));
		assertEquals(""+PDF_closeRTL,           removeTrailingPunctuation(""+PDF_closeRTL,             ""));
		assertEquals(""+PDF_closeRTL,           removeTrailingPunctuation(""+PDF_closeRTL,             null));
		assertEquals("asdf."+PDF_closeRTL,      removeTrailingPunctuation("asdf."+PDF_closeRTL,        null));
	}

	@Test
	public void nonStardardApostropheSorting() {
		// For each non-standard apostrophe unicode character, filing normalization should strip it as not sortable.
		assertEquals("imarah", getFilingForm("ʹImārah")); // 02B9
		assertEquals("imarah", getFilingForm("ʻImārah")); // 02BB
		assertEquals("imarah", getFilingForm("ʼImārah")); // 02BC
		assertEquals("imarah", getFilingForm("ʽImārah")); // 02BD
		assertEquals("imarah", getFilingForm("ʾImārah")); // 02BE
		assertEquals("imarah", getFilingForm("ʿImārah")); // 02BF
		assertEquals("imarah", getFilingForm("‘Imārah")); // 2018
		assertEquals("imarah", getFilingForm("’Imārah")); // 2019
		assertEquals("imarah", getFilingForm("′Imārah")); // 2032
	}

	@Test
	public void doublePrimeSorting() {
		assertEquals("doubleprime", getFilingForm("″Double″prime″")); // 2033 "double prime"
		assertEquals("doubleprime", getFilingForm("ʺDoubleʺprimeʺ")); // 02BA "modifier letter double prime"
		assertEquals("singleprime", getFilingForm("′Single′prime′")); // 2032
		// single prime sort also tested in nonStardardApostropheSorting()
		assertEquals("tripleprime", getFilingForm("‴Triple‴prime‴")); // 2034 "triple prime"
	}
}
