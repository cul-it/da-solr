package edu.cornell.library.integration.marcXmlToRdf;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class MarcXmlToRdfTest {

	@Test
	public void escapeForNTriplesTest() {
		String s1 = "test \\ string";
		String s2 = MarcXmlToRdf.escapeForNTriples( s1 );
		assertTrue(s2.equals("test \\\\ string"));
		s1 = "testing \"quotes\".";
		s2 = MarcXmlToRdf.escapeForNTriples( s1 );
		assertTrue(s2.equals("testing \\\"quotes\\\"."));
	}

}
