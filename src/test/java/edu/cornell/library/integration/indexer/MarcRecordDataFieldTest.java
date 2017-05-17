package edu.cornell.library.integration.indexer;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.cornell.library.integration.indexer.MarcRecord.DataField;

@SuppressWarnings("static-method")
public class MarcRecordDataFieldTest {

	@Test
	/*
	 * If we can successfully round-trip the field, converting the subfield separator to a
	 * different character, then we demonstrate that we are parsing the original string into
	 * appropriate subfields.
	 */
	public void createDataFieldFromSubfieldsStringTest() {
		assertEquals( "100 00 $a Field a $b Field b $a Field a #2",
				new DataField(1,"100",'0','0',"‡a Field a ‡b Field b ‡a Field a #2").toString('$') );
	}
}
