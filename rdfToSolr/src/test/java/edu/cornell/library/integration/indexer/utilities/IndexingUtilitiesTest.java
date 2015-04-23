package edu.cornell.library.integration.indexer.utilities;

import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.getSortHeading;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.insertSpaceAfterCommas;
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
		assertTrue(getSortHeading("hello").equals("hello"));
		assertTrue(getSortHeading("Hello").equals("hello"));
		assertTrue(getSortHeading("Hello,").equals("hello"));
		assertTrue(getSortHeading("  hello     ").equals("hello"));
		assertTrue(getSortHeading("héllo!").equals("hello"));
		assertTrue(getSortHeading("-- :::hello::: --").equals("hello"));
		assertFalse(getSortHeading("hello").equals("goodbye"));
		assertTrue(getSortHeading("hello-  -- hello").equals("hello hello"));
		assertTrue(getSortHeading("Hello > hello").equals("hello aaa hello"));
		assertTrue(getSortHeading("Hello, Jr., 1910-1997").equals("hello jr 1910 1997"));
		assertTrue(getSortHeading("‘Abbāsah ukht al-Rashīd aww-nakbat al-Barāmikah").
				equals("abbasah ukht al rashid aww nakbat al baramikah"));
		assertTrue(getSortHeading("’Abd al-Rraḥmān al-Kawākibī").
				equals("abd al rrahman al kawakibi"));
		assertTrue(getSortHeading(" ⁻Adh⁻i b⁻at : ḍr⁻ame").equals("adhi bat drame"));
		assertTrue(getSortHeading("⁻Ac⁻arya, Pushpalat⁻a").equals("acarya pushpalata"));
		assertTrue(getSortHeading("-0-De, Boll Weevil Convention. New Orleans. Nov.").
				equals("0 de boll weevil convention new orleans nov"));
		assertTrue(getSortHeading("°Cómo funciona el Mercado de Seguros Médicos?").
				equals("como funciona el mercado de seguros medicos"));
		assertTrue(getSortHeading("σ and π Electrons in Organic Compounds").
				equals("sigma and pi electrons in organic compounds"));
		assertTrue(getSortHeading("α- and β- modifications of benzene hexabromid").
				equals("alpha and beta modifications of benzene hexabromid"));
		assertTrue(getSortHeading("£1,000,000 bank-note and other stories").
				equals("1000000 bank note and other stories"));
		assertTrue(getSortHeading("€Tudes de Centre de DV̈eloppement Financer le dV̈eloppment").
				equals("tudes de centre de dveloppement financer le dveloppment"));
		assertTrue(getSortHeading("†Wilhelm His").equals("wilhelm his"));
		assertTrue(getSortHeading("").equals(""));
		assertTrue(getSortHeading("         ").equals(""));
		// non-standard space characters \u3000 is common in CJK text
		assertTrue(getSortHeading("\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u200B\u202F\u205F\u3000\uFEFF").equals(""));
	    assertTrue(getSortHeading("sydsæter knut").equals("sydsaeter knut"));

	    // non-Roman scripts are not fully or well supported in filing
	    assertTrue(getSortHeading("قيرواني، محمد الطيب الطويلي").equals("قيرواني محمد الطيب الطويلي"));
	}
	
}
