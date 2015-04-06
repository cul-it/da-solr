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
		assertTrue(getSortHeading("  hello ").equals("hello"));
		assertTrue(getSortHeading("héllo!").equals("hello"));
		assertTrue(getSortHeading("-- :::hello::: --").equals("hello"));
		assertFalse(getSortHeading("hello").equals("goodbye"));
		assertTrue(getSortHeading("hello- -- hello").equals("hello hello"));
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
//		assertTrue(getSortHeading("£ & the $; or, Gold debts & taxes").
//				equals("pound the dollar or gold debts taxes"));
	}
	
}
