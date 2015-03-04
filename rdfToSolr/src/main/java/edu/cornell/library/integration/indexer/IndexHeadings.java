package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.removeAllPunctuation;

import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.solr.client.solrj.SolrServerException;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;

public class IndexHeadings {

	private Connection connection = null;
	private PreparedStatement ps_recordsByQuery = null;
	private PreparedStatement ps_recordsAboutQuery = null;
	private PreparedStatement ps_recordsQuery = null;
	private PreparedStatement ps_updateRecordsBy = null;
	private PreparedStatement ps_updateRecordsAbout = null;
	private PreparedStatement ps_updateRecords = null;
	private PreparedStatement ps_createHeadingWBy = null;
	private PreparedStatement ps_createHeadingWAbout = null;
	private PreparedStatement ps_createHeadingWWorksCount = null;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// load configuration for location of index, location of authorities
		Collection<String> requiredArgs = new HashSet<String>();
		requiredArgs.add("xmlDir");
		requiredArgs.add("blacklightSolrUrl");
		requiredArgs.add("solrUrl");
	            
		VoyagerToSolrConfiguration config = VoyagerToSolrConfiguration.loadConfig(args,requiredArgs);
		try {
			new IndexHeadings(config);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	
	public IndexHeadings(VoyagerToSolrConfiguration config) throws Exception {
        
		connection = config.getDatabaseConnection(1);
		Collection<BLField> blFields = new HashSet<BLField>();
		blFields.add(new BLField( HeadType.AUTHOR, HeadTypeDesc.PERSNAME, "author_100_exact" ));
		blFields.add(new BLField( HeadType.AUTHOR, HeadTypeDesc.CORPNAME, "author_110_exact" ));
		blFields.add(new BLField( HeadType.AUTHOR, HeadTypeDesc.EVENT,    "author_111_exact" ));
		blFields.add(new BLField( HeadType.AUTHOR, HeadTypeDesc.PERSNAME, "author_700_exact" ));
		blFields.add(new BLField( HeadType.AUTHOR, HeadTypeDesc.CORPNAME, "author_710_exact" ));
		blFields.add(new BLField( HeadType.AUTHOR, HeadTypeDesc.EVENT,    "author_711_exact" ));
		blFields.add(new BLField( HeadType.SUBJECT, HeadTypeDesc.PERSNAME, "subject_600_exact"));
		blFields.add(new BLField( HeadType.SUBJECT, HeadTypeDesc.CORPNAME, "subject_610_exact"));
		blFields.add(new BLField( HeadType.SUBJECT, HeadTypeDesc.EVENT, "subject_611_exact"));
		blFields.add(new BLField( HeadType.SUBJECT, HeadTypeDesc.TOPIC, "subject_650_exact"));
		blFields.add(new BLField( HeadType.SUBJECT, HeadTypeDesc.GEONAME, "subject_651_exact"));
		blFields.add(new BLField( HeadType.SUBJECT, HeadTypeDesc.CHRONTERM, "subject_648_exact"));
		blFields.add(new BLField( HeadType.SUBJECT, HeadTypeDesc.GENRE, "subject_655_exact"));
		blFields.add(new BLField( HeadType.SUBJECT, HeadTypeDesc.GEONAME, "subject_662_exact"));
		
		for (BLField blf : blFields) {
		
			System.out.printf("Poling Blacklight Solr field %s for %s values as %s\n",
						blf.fieldName(),blf.headingTypeDesc(),blf.headingType());
			URL queryUrl = new URL(config.getBlacklightSolrUrl() + "/terms?terms.fl=" +
					blf.fieldName() + "&terms.sort=index&terms.limit=100000000");
			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
			InputStream in = queryUrl.openStream();
			XMLStreamReader r  = inputFactory.createXMLStreamReader(in);
	
			// fast forward to response body
			FF: while (r.hasNext()) {
				String event = getEventTypeString(r.next());
				if (event.equals("START_ELEMENT")) {
					if (r.getLocalName().equals("lst"))
						for (int i = 0; i < r.getAttributeCount(); i++)
							if (r.getAttributeLocalName(i).equals("name")) {
								String name = r.getAttributeValue(i);
								if (name.equals("terms")) break FF;
							}
				}
			}
			
			// process actual results
			String heading = null;
			Integer recordCount = null;
			int headingCount = 0;
			while (r.hasNext()) {
				String event = getEventTypeString(r.next());
				if (event.equals("START_ELEMENT")) {
					if (r.getLocalName().equals("int")) {
						for (int i = 0; i < r.getAttributeCount(); i++)
							if (r.getAttributeLocalName(i).equals("name"))
								heading = r.getAttributeValue(i);
						recordCount = Integer.valueOf(r.getElementText());
						addCountToDB(blf,heading,recordCount);
						if (++headingCount % 10_000 == 0)
							System.out.printf("%s => %d\n",heading,recordCount);
					}
				}
			}
			in.close();		
		}

	}

	
	private void addCountToDB(BLField blf, String heading, Integer count) throws SolrServerException, SQLException {

		PreparedStatement pstmt = null;
		String count_field = null;
		if (blf.headingType().equals(HeadType.AUTHOR)) {
			count_field = "works_by";
			if (ps_recordsByQuery == null) 
				ps_recordsByQuery = connection.prepareStatement(
						"SELECT id, works_by as count FROM heading " +
						"WHERE type = ? AND type_desc = ? AND sort = ?");
			pstmt = ps_recordsByQuery;
		} else if (blf.headingType().equals(HeadType.SUBJECT)) {
			count_field = "works_about";
			if (ps_recordsAboutQuery == null) 
				ps_recordsAboutQuery = connection.prepareStatement(
						"SELECT id, works_about as count FROM heading " +
						"WHERE type = ? AND type_desc = ? AND sort = ?");
			pstmt = ps_recordsAboutQuery;
		} else {
			count_field = "works";
			if (ps_recordsQuery == null) 
				ps_recordsQuery = connection.prepareStatement(
						"SELECT id, works as count FROM heading " +
						"WHERE type = ? AND type_desc = ? AND sort = ?");
			pstmt = ps_recordsQuery;
		}
		
		String headingSort = getSortHeading( heading );

		pstmt.setInt(1, blf.headingType().ordinal());
		pstmt.setInt(2, blf.headingTypeDesc().ordinal());
		pstmt.setString(3, getSortHeading(heading));
		ResultSet rs = pstmt.executeQuery();
		Integer recordId = null;
		Integer oldCount = null;
		while (rs.next()) {
			recordId = rs.getInt("id");
			oldCount = rs.getInt("count");
		}

		if (recordId != null) {
			// Update record count
			if (count_field.equals("works_by")) {
				if (ps_updateRecordsBy == null)
					ps_updateRecordsBy = connection.prepareStatement(
							"UPDATE heading SET works_by = ? WHERE id = ?");
				pstmt = ps_updateRecordsBy;
			} else if (count_field.equals("works_about")) {
				if (ps_updateRecordsAbout == null)
					ps_updateRecordsAbout = connection.prepareStatement(
							"UPDATE heading SET works_about = ? WHERE id = ?");
				pstmt = ps_updateRecordsAbout;
			} else {
				if (ps_updateRecords == null)
					ps_updateRecords = connection.prepareStatement(
							"UPDATE heading SET works = ? WHERE id = ?");
				pstmt = ps_updateRecords;
			}
			pstmt.setInt(1, oldCount + count);
			pstmt.setInt(2, recordId);
			pstmt.executeUpdate();
		} else {
			// create new record
			if (count_field.equals("works_by")) {
				if (ps_createHeadingWBy == null)
					ps_createHeadingWBy = connection.prepareStatement(
							"INSERT INTO heading (heading, sort, type, type_desc, works_by) " +
							"VALUES (?, ?, ?, ?, ?)");
				pstmt = ps_createHeadingWBy;
			} else if (count_field.equals("works_about")) {
				if (ps_createHeadingWAbout == null)
					ps_createHeadingWAbout = connection.prepareStatement(
							"INSERT INTO heading (heading, sort, type, type_desc, works_about) " +
							"VALUES (?, ?, ?, ?, ?)");
				pstmt = ps_createHeadingWAbout;
			} else {
				if (ps_createHeadingWWorksCount == null)
					ps_createHeadingWWorksCount = connection.prepareStatement(
							"INSERT INTO heading (heading, sort, type, type_desc, works) " +
							"VALUES (?, ?, ?, ?, ?)");
				pstmt = ps_createHeadingWWorksCount;
			}
			pstmt.setString(1, heading);
			pstmt.setString(2, headingSort);
			pstmt.setInt(3, blf.headingType().ordinal());
			pstmt.setInt(4, blf.headingTypeDesc().ordinal());
			pstmt.setInt(5, count);
			int affectedCount = pstmt.executeUpdate();
			if (affectedCount < 1) 
				throw new SQLException("Creating Heading Record Failed.");
		}
	}

	
	private String getSortHeading(String heading) {
		// Remove all punctuation will strip punctuation. We replace hyphens with spaces
		// first so hyphenated words will sort as though the space were present.
		String sortHeading = removeAllPunctuation(heading.
				replaceAll("\\p{InCombiningDiacriticalMarks}+", "").
				toLowerCase().
				replaceAll("-", " "));
		return sortHeading.trim();
	}

	private final static String getEventTypeString(int  eventType)
	{
	  switch  (eventType)
	    {
	        case XMLEvent.START_ELEMENT:
	          return "START_ELEMENT";
	        case XMLEvent.END_ELEMENT:
	          return "END_ELEMENT";
	        case XMLEvent.PROCESSING_INSTRUCTION:
	          return "PROCESSING_INSTRUCTION";
	        case XMLEvent.CHARACTERS:
	          return "CHARACTERS";
	        case XMLEvent.COMMENT:
	          return "COMMENT";
	        case XMLEvent.START_DOCUMENT:
	          return "START_DOCUMENT";
	        case XMLEvent.END_DOCUMENT:
	          return "END_DOCUMENT";
	        case XMLEvent.ENTITY_REFERENCE:
	          return "ENTITY_REFERENCE";
	        case XMLEvent.ATTRIBUTE:
	          return "ATTRIBUTE";
	        case XMLEvent.DTD:
	          return "DTD";
	        case XMLEvent.CDATA:
	          return "CDATA";
	        case XMLEvent.SPACE:
	          return "SPACE";
	    }
	  return  "UNKNOWN_EVENT_TYPE ,   "+ eventType;
	}
	
	static class BLField {
		private HeadType _ht;
		private HeadTypeDesc _htd;
		private String _fieldName;
		
		public BLField(HeadType ht, HeadTypeDesc htd,String fieldName) {
			_ht = ht;
			_htd = htd;
			_fieldName = fieldName;
		}
		public HeadType headingType() { return _ht; }
		public HeadTypeDesc headingTypeDesc() { return _htd; }
		public String fieldName() { return _fieldName; }
		
	}

}
