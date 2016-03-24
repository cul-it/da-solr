package edu.cornell.library.integration.utilities;

import static edu.cornell.library.integration.indexer.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.insertSpaceAfterCommas;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import org.junit.Test;

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
	}
	
}
