package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class Title130Test {

	SolrFieldGenerator gen = new Title130();

	@Test
	public void testSimple() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"130",'0',' ',"‡a Journal of hip hop studies (Online)"));
		String expected =
		"title_uniform_t: Journal of hip hop studies (Online)\n"+
		"title_uniform_t: Journal of hip hop studies (Online)\n"+
		"title_uniform_display: Journal of hip hop studies (Online)|Journal of hip hop studies (Online)\n";
		assertEquals( expected, gen.generateSolrFields(rec,null).toString() );
	}

	@Test
	public void testArticle() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"130",'3',' ',"‡a El Fénix (Valencia) ‡p Indexes."));
		String expected =
		"title_uniform_t: El Fénix (Valencia) Indexes.\n"+
		"title_uniform_t: Fénix (Valencia) Indexes.\n"+
		"title_uniform_display: El Fénix (Valencia) Indexes.|El Fénix (Valencia) Indexes.\n";
		assertEquals( expected, gen.generateSolrFields(rec,null).toString() );
	}
}
