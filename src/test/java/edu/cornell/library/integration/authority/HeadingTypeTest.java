package edu.cornell.library.integration.authority;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class HeadingTypeTest {

	@Test
	public void conversionsToOldHt() {
		assertEquals(
				edu.cornell.library.integration.metadata.support.HeadingType.PERSNAME,
				HeadingType.PERS.getOldHeadingType());
		assertEquals(
				edu.cornell.library.integration.metadata.support.HeadingType.CORPNAME,
				HeadingType.CORP.getOldHeadingType());
	}

}
