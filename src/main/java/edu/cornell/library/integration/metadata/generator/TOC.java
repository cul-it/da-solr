package edu.cornell.library.integration.metadata.generator;

import static edu.cornell.library.integration.marc.DataField.PDF_closeRTL;
import static edu.cornell.library.integration.marc.DataField.RLE_openRTL;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.stream.XMLStreamException;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.Syndetics;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * processing into contents_display and partial_contents_display
 * 
 */
public class TOC implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("020", "505"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config config ) throws SQLException, IOException {

		SolrFields solrFields = new SolrFields();
		List<DataField> marcFields = rec.matchSortAndFlattenDataFields("505");

		Set<String> isbns = new TreeSet<>();
		for (DataField f : rec.dataFields) if ( f.tag.equals("020") )
			for (Subfield sf : f.subfields) if ( sf.code.equals('a') ) {
				String isbn = sf.value.trim();
				if (isbn.contains(" ")) isbn = isbn.substring(0, isbn.indexOf(' '));
				isbn = isbn.replaceAll("[^0-9]", "");
				isbns.add(isbn);
				if ( isbn.length() == 10 )
					isbns.add(ISBN.isbn10to13(isbn));
			}

		if ( ! isbns.isEmpty() )
			try ( Connection current = config.getDatabaseConnection("Current");
					PreparedStatement syndeticsMarc = current.prepareStatement(
							"SELECT marc FROM syndeticsData WHERE isbn = ?")) {
				for (String isbn : isbns) {
					syndeticsMarc.setString(1, isbn);
					try ( ResultSet rs = syndeticsMarc.executeQuery() ) {
						while ( rs.next() ) {
							try {
								MarcRecord tocMarc = Syndetics.readSyndeticsMarc(rs.getString("marc"));
								for ( DataField f : tocMarc.dataFields ) if ( f.tag.equals("970") )
									marcFields.add(f);
							} catch (XMLStreamException e) {
								// This can't really happen since we parse the provided MARC before pushing it to the
								// database, so any syntax issues would cause errors then. 
								e.printStackTrace();
							}
						}
					}
				}
			}

		for (DataField f: marcFields) {

			String value = f.concatenateSpecificSubfields("acgtr");

			if ( f.mainTag.equals("505") ) {
				// Populate display value(s)
				String relation = (f.ind1.equals('2')) ? "partial_contents_display" : "contents_display";
				solrFields.addAll(splitToc( relation,  value ));
			}

			// Populate search values
			boolean cjk = (f.tag.equals("880") && f.getScript().equals(DataField.Script.CJK));
			String titleField  = (cjk) ?  "title_addl_t_cjk" :  "title_addl_t";
			String authorField = (cjk) ? "author_addl_t_cjk" : "author_addl_t";
			String tocField    = (cjk) ?         "toc_t_cjk" :         "toc_t";
			for ( Subfield sf : f.subfields )
				switch (sf.code) {
				case 'r':
				case 'c':
					solrFields.add(new SolrField( authorField, sf.value )); break;
				case 't':
					solrFields.add(new SolrField( titleField, sf.value ));
				}
			solrFields.add(new SolrField(tocField,value));
		}
		return solrFields;
	}

	private static SolrFields splitToc(String relation, String value) {
		SolrFields sfs = new SolrFields();
		boolean rightToLeft = false;
		if (value.endsWith(PDF_closeRTL)) {
			rightToLeft = true;
			value = value.substring(RLE_openRTL.length(), value.length() - PDF_closeRTL.length());
		}
		for(String item: value.split(" *-- *")) {
			if (rightToLeft)
				item = RLE_openRTL + item + PDF_closeRTL;
			sfs.add(new SolrField(relation,item));
		}
		return sfs;
	}
}
