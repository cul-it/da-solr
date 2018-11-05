package edu.cornell.library.integration.ilcommons.configuration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.cornell.library.integration.indexer.utilities.Config;

public class VoyagerToSolrConfigurationTest {

	@SuppressWarnings("static-method")
	@Test
	public void testInsertDate() {
		String test = "No replacement";
		assertTrue(Config.insertIterationContext(test).equals(test));
		test = "should/XXXX/replace";
		assertFalse(Config.insertIterationContext(test).equals(test));
		test = "Today is XXXX.";
		assertFalse(Config.insertIterationContext(test).equals(test));
		System.out.println(Config.insertIterationContext(test));
	}

}
