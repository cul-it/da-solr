package edu.cornell.library.integration.ilcommons.configuration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class VoyagerToSolrConfigurationTest {

	@Test
	public void testInsertDate() {
		String test = "No replacement";
		assertTrue(VoyagerToSolrConfiguration.insertDate(test).equals(test));
		test = "should/XXXX/replace";
		assertFalse(VoyagerToSolrConfiguration.insertDate(test).equals(test));
		test = "Today is XXXX.";
		assertFalse(VoyagerToSolrConfiguration.insertDate(test).equals(test));
		System.out.println(VoyagerToSolrConfiguration.insertDate(test));
	}

}
