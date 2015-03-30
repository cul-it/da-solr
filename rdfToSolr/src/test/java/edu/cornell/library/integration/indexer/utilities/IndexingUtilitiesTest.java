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
		assertFalse(getSortHeading("hello").equals("goodbye"));
		assertTrue(getSortHeading("héllo!").equals("hello"));
		assertTrue(getSortHeading("Hello > hello").equals("hello aaa hello"));
		assertTrue(getSortHeading("Hello, Jr., 1910-1997").equals("hello jr 1910 1997"));
		assertTrue(getSortHeading("‘Abbāsah ukht al-Rashīd aww-nakbat al-Barāmikah").
				equals("abbasah ukht al rashid aww nakbat al baramikah"));
		assertTrue(getSortHeading("’Abd al-Rraḥmān al-Kawākibī").
				equals("abd al rrahman al kawakibi"));
		assertTrue(getSortHeading(" ⁻Adh⁻i b⁻at : ḍr⁻ame").equals("adhi bat drame"));
	}
	
}
