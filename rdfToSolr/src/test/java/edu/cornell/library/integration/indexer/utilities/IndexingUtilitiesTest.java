package edu.cornell.library.integration.indexer.utilities;

import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.insertSpaceAfterCommas;
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
	
}
