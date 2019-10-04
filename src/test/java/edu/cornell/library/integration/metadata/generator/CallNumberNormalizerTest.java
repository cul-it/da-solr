package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * <b>Changes to analysis need to be copied into test for test to be useful.</b><br/><br/>
 * Test normalization analysis logic from Solr schema's
 * &lt;fieldType name="callNumberNormalized"/&gt;
 */
@RunWith(Parameterized.class)
public class CallNumberNormalizerTest {

	@Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
			{"QA76 ",       "lccncode.QA 76"},
			{"QA76",        "lccncode.QA 76"},
			{"QA76.",       "lccncode.QA 76"},
			{"QA76 .R51",   "lccncode.QA 76 R 51"},
			{"1-1-1",       "lccncode.1 1 1"},
			{"HB4213.4 51.","lccncode.HB 4213.4 51"}
		});
	}

	private String callNo;
	private String expectedNormalized;

	public CallNumberNormalizerTest( String callNo , String expectedNormalized) {
		this.callNo = callNo;
		this.expectedNormalized = expectedNormalized;
	}

	@Test
	public void testCallNumberRegex() {
		assertEquals(this.expectedNormalized,
				String.join(" ",
				this.callNo
				.replaceAll("\\.(?!\\d)", " ")
				.replaceAll("([a-zA-Z])(\\d)", "$1 $2")
				.replaceAll("(\\d)([a-zA-Z])", "$1 $2")
				.replaceAll("^[^a-zA-Z\\d]*([a-zA-Z\\d])", "lccncode.$1")
				.split("[^a-zA-Z\\d\\.]+")
				));
	}

}
