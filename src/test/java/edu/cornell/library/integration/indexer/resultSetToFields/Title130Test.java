package edu.cornell.library.integration.indexer.resultSetToFields;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

@SuppressWarnings("static-method")
public class Title130Test {

	@Test
	public void testSimple() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"130",'0',' ',"‡a Journal of hip hop studies (Online)"));
		String expected =
		"title_uniform_t: Journal of hip hop studies (Online)\n"+
		"title_uniform_t: Journal of hip hop studies (Online)\n"+
		"title_uniform_display: Journal of hip hop studies (Online)|Journal of hip hop studies (Online)\n";
		assertEquals( expected, Title130.generateSolrFields(rec,null).toString() );
	}

	@Test
	public void testArticle() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"130",'3',' ',"‡a El Fénix (Valencia) ‡p Indexes."));
		String expected =
		"title_uniform_t: El Fénix (Valencia) Indexes.\n"+
		"title_uniform_t: Fénix (Valencia) Indexes.\n"+
		"title_uniform_display: El Fénix (Valencia) Indexes.|El Fénix (Valencia) Indexes.\n";
		assertEquals( expected, Title130.generateSolrFields(rec,null).toString() );
	}
}
