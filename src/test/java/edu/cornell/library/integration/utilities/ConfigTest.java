package edu.cornell.library.integration.utilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.cornell.library.integration.utilities.Config;

public class ConfigTest {

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
