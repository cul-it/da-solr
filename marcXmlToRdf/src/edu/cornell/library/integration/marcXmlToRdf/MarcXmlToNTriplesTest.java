package edu.cornell.library.integration.marcXmlToRdf;

import static org.junit.Assert.*;
import org.junit.Test;

public class MarcXmlToNTriplesTest {

	@Test
	public void escapeForNTriplesTest() {
		String s1 = "test \\ string";
		String s2 = MarcXmlToNTriples.escapeForNTriples( s1 );
		assertTrue(s2.equals("test \\\\ string"));
		s1 = "testing \"quotes\".";
		s2 = MarcXmlToNTriples.escapeForNTriples( s1 );
		assertTrue(s2.equals("testing \\\"quotes\\\"."));
	}

}
