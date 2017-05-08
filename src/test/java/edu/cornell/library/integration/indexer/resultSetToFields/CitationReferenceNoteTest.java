package edu.cornell.library.integration.indexer.resultSetToFields;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;

@SuppressWarnings("static-method")
public class CitationReferenceNoteTest {

	@Test
	public void testReferences() {
		DataField f = new DataField(3,"510");
		f.ind1 = '3';
		f.subfields.add(new MarcRecord.Subfield(1, 'a', "Described in DOCUMENTATION NEWSLETTER, Fall 1988."));
		MarcRecord rec = new MarcRecord();
		rec.dataFields.add(f);
		for (FieldSet fs : rec.matchAndSortDataFields()) {
			List<SolrField> sfs = CitationReferenceNote.generateSolrFields(fs).fields;
			assertEquals(1,
					sfs.size() );
			assertEquals("references_display",
					sfs.get(0).fieldName);
			assertEquals("Described in DOCUMENTATION NEWSLETTER, Fall 1988.",
					sfs.get(0).fieldValue);
		}
	}

	@Test
	public void testIndexedBy() {
		DataField f = new DataField(3,"510");
		f.ind1 = '0';
		f.subfields.add(new MarcRecord.Subfield(1, 'a', "Indexed by note."));
		MarcRecord rec = new MarcRecord();
		rec.dataFields.add(f);
		for (FieldSet fs : rec.matchAndSortDataFields()) {
			List<SolrField> sfs = CitationReferenceNote.generateSolrFields(fs).fields;
			assertEquals(1,                     sfs.size() );
			assertEquals("indexed_by_display",  sfs.get(0).fieldName);
			assertEquals("Indexed by note.",    sfs.get(0).fieldValue);
		}
	}

	@Test
	public void testIndexedSelectively880() {
		DataField f1 = new DataField(3,1,"510");
		f1.ind1 = '2';
		f1.subfields.add(new MarcRecord.Subfield(1, '6', "880-01"));
		f1.subfields.add(new MarcRecord.Subfield(2, 'a', "Indexed Selectively by XXXXX"));
		DataField f2 = new DataField(17,1,"510",true);
		f2.ind1 = '2';
		f2.subfields.add(new MarcRecord.Subfield(1, '6', "510-01"));
		f2.subfields.add(new MarcRecord.Subfield(2, 'a', "Non-Roman Indexed Selectively by XXXXX"));
		MarcRecord rec = new MarcRecord();
		rec.dataFields.add(f1);
		rec.dataFields.add(f2);
		for (FieldSet fs : rec.matchAndSortDataFields()) {
			List<SolrField> sfs = CitationReferenceNote.generateSolrFields(fs).fields;
			assertEquals(2,                                        sfs.size() );
			assertEquals("indexed_selectively_by_display",         sfs.get(0).fieldName);
			assertEquals("Non-Roman Indexed Selectively by XXXXX", sfs.get(0).fieldValue);
			assertEquals("indexed_selectively_by_display",         sfs.get(0).fieldName);
			assertEquals("Indexed Selectively by XXXXX",           sfs.get(0).fieldValue);
		}
	}

}
