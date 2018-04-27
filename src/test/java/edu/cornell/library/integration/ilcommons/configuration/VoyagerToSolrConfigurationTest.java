package edu.cornell.library.integration.ilcommons.configuration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class VoyagerToSolrConfigurationTest {

	@SuppressWarnings("static-method")
	@Test
	public void testInsertDate() {
		String test = "No replacement";
		assertTrue(SolrBuildConfig.insertIterationContext(test).equals(test));
		test = "should/XXXX/replace";
		assertFalse(SolrBuildConfig.insertIterationContext(test).equals(test));
		test = "Today is XXXX.";
		assertFalse(SolrBuildConfig.insertIterationContext(test).equals(test));
		System.out.println(SolrBuildConfig.insertIterationContext(test));
	}

}
