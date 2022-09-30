package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class MARCTest {

	SolrFieldGenerator gen = new MARC();

	@Test
	public void testSmallRecord() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "12345";
		rec.bib_id = "12345";
		rec.controlFields.add(new ControlField( 1, "005", "19940223151047.0"));
		rec.dataFields.add(new DataField(1,"100",'1',' ',"‡a Jones, Davy ‡d 1945-2012."));
		String expected =
		"marc_display: <?xml version='1.0' encoding='UTF-8'?>"
		+ "<record xmlns=\"http://www.loc.gov/MARC21/slim\">"
		+ "<leader> </leader>"
		+ "<controlfield tag=\"005\">19940223151047.0</controlfield>"
		+ "<datafield tag=\"100\" ind1=\"1\" ind2=\" \">"
		+ "<subfield code=\"a\">Jones, Davy</subfield>"
		+ "<subfield code=\"d\">1945-2012.</subfield></datafield>"
		+ "</record>\n" + 
		"id: 12345\n"+
		"bibid_display: 12345\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}
}
