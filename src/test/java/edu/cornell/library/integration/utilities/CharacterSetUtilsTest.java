package edu.cornell.library.integration.utilities;


import static edu.cornell.library.integration.utilities.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.isCJK;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.trimInternationally;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class CharacterSetUtilsTest {
	
	@Test
	public static void testIsCJK() {
		assertFalse(isCJK("Ø§Ù„Ù‚Ø§Ù‡Ø±Ø© : Ø´Ù…Ø³ Ù„Ù„Ù†Ø´Ø± ÙˆØ§Ù„ØªÙˆØ²ÙŠØ¹ØŒ 2012.â€�â€¬â€Ž")); // Arabic publication info
		//Japanese title with English Translation
		assertTrue(isCJK("è«–ç©¶ã‚¸ãƒ¥ãƒªã‚¹ãƒˆ = Quarterly jurist"));
		assertTrue(isCJK("ä¸€ä¹�äº”ã€‡å¹´ä»£çš„å�°ç�£")); // Chinese Title
		assertFalse(isCJK("Saint Antoine le Grand dans l'Orient chretien")); // French Title
		assertTrue(isCJK("ì œêµ­ ì�˜ ìœ„ì•ˆë¶€")); // Korean Title
		assertFalse(isCJK("Advanced linear algebra"));  // English Title
		//English text with Japanese character sometimes used as emoticon
		assertFalse(isCJK("Hi! How are you? ã‚·"));
	}
	
	/*
	 * We expect the same results from hasCJK() as isCJK(), except for the final test,
	 * which should return true instead of false.
	 */
	@Test
	public static void testHasCJK() {
		assertFalse(hasCJK("Ø§Ù„Ù‚Ø§Ù‡Ø±Ø© : Ø´Ù…Ø³ Ù„Ù„Ù†Ø´Ø± ÙˆØ§Ù„ØªÙˆØ²ÙŠØ¹ØŒ 2012.â€�â€¬â€Ž")); 
		assertTrue(hasCJK("è«–ç©¶ã‚¸ãƒ¥ãƒªã‚¹ãƒˆ = Quarterly jurist"));
		assertTrue(hasCJK("ä¸€ä¹�äº”ã€‡å¹´ä»£çš„å�°ç�£"));
		assertFalse(hasCJK("Saint Antoine le Grand dans l'Orient chretien"));
		assertTrue(hasCJK("ì œêµ­ ì�˜ ìœ„ì•ˆë¶€"));
		assertFalse(hasCJK("Advanced linear algebra"));
		assertTrue(hasCJK("Hi! How are you? ã‚·"));
	}
	
	@Test
	public static void testTrimInternationally() {
		// tests involving standard ASCII spacing
		assertTrue(trimInternationally(" abc ").equals("abc"));
		assertTrue(trimInternationally("   ").equals(""));
		assertTrue(trimInternationally("\tHello,   World !\n").equals("Hello,   World !"));
		assertTrue(trimInternationally("Hello, World.").equals("Hello, World."));
		// CJK spacing
		assertTrue(trimInternationally("ã€€ã€€ä¿„å›½ã€€ä¸œæ­£æ•™ã€€ä¾µã€€å�Žã€€å�²ç•¥ã€€").equals("ä¿„å›½ã€€ä¸œæ­£æ•™ã€€ä¾µã€€å�Žã€€å�²ç•¥"));
	}
}
