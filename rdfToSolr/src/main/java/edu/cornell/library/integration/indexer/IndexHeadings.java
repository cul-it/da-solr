package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.indexer.utilities.BrowseUtils.getSortHeading;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.BlacklightField;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.RecordSet;

public class IndexHeadings {

	private Connection connection = null;
	// This structure should contain only up to six PreparedStatement objects at most.
	private Map<HeadType,Map<String,PreparedStatement>> statements =
			new HashMap<HeadType,Map<String,PreparedStatement>>();
	VoyagerToSolrConfiguration config;
	private Map<Integer,Integer> wrongHeadingCounts = new HashMap<Integer,Integer>();
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			new IndexHeadings(args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	
	public IndexHeadings(String[] args) throws Exception {
        
		// load configuration for location of index, location of authorities
		Collection<String> requiredArgs = new HashSet<String>();
		requiredArgs.add("xmlDir");
		requiredArgs.add("blacklightSolrUrl");
		requiredArgs.add("solrUrl");
	            
		config = VoyagerToSolrConfiguration.loadConfig(args,requiredArgs);		
		
		connection = config.getDatabaseConnection(1);
		Collection<BlacklightField> blFields = new HashSet<BlacklightField>();
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.AUTHOR, HeadTypeDesc.PERSNAME, "author_100_exact","author_facet" ));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.AUTHOR, HeadTypeDesc.CORPNAME, "author_110_exact","author_facet" ));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.AUTHOR, HeadTypeDesc.EVENT,    "author_111_exact","author_facet" ));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.AUTHOR, HeadTypeDesc.PERSNAME, "author_700_exact","author_facet" ));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.AUTHOR, HeadTypeDesc.CORPNAME, "author_710_exact","author_facet" ));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.AUTHOR, HeadTypeDesc.EVENT,    "author_711_exact","author_facet" ));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.SUBJECT, HeadTypeDesc.PERSNAME, "subject_600_exact","subject_topic_facet"));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.SUBJECT, HeadTypeDesc.CORPNAME, "subject_610_exact","subject_topic_facet"));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.SUBJECT, HeadTypeDesc.EVENT, "subject_611_exact","subject_topic_facet"));
		blFields.add(new BlacklightField(RecordSet.SUBJECT, HeadType.SUBJECT, HeadTypeDesc.TOPIC, "subject_650_exact","subject_topic_facet"));
		blFields.add(new BlacklightField(RecordSet.SUBJECT, HeadType.SUBJECT, HeadTypeDesc.GEONAME, "subject_651_exact","subject_geo_facet"));
		blFields.add(new BlacklightField(RecordSet.SUBJECT, HeadType.SUBJECT, HeadTypeDesc.CHRONTERM, "subject_648_exact","subject_era_facet"));
		blFields.add(new BlacklightField(RecordSet.SUBJECT, HeadType.SUBJECT, HeadTypeDesc.GENRE, "subject_655_exact","subject_topic_facet"));
		blFields.add(new BlacklightField(RecordSet.SUBJECT, HeadType.SUBJECT, HeadTypeDesc.GEONAME, "subject_662_exact","subject_topic_facet"));//TODO: switch to geo after reindex
		
		for (BlacklightField blf : blFields) {
		
			System.out.printf("Poling Blacklight Solr field %s for %s values as %s\n",
						blf.fieldName(),blf.headingTypeDesc(),blf.headingType());
			
			if ( ! statements.containsKey(blf.headingType()))
				statements.put(blf.headingType(), new HashMap<String,PreparedStatement>());
			
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
						addCountToDB(blf,statements.get(blf.headingType()),heading, recordCount);
						if (++headingCount % 10_000 == 0)
							System.out.printf("%s => %d\n",heading,recordCount);
					}
				}
			}
			in.close();		
		}

	}

	
	private void addCountToDB(BlacklightField blf, Map<String, PreparedStatement> stmts, String headingSort, Integer count) throws SolrServerException, SQLException {

		String count_field = blf.headingType().dbField();
		// update record count in db
		if ( ! stmts.containsKey("update")) {
			String query = String.format( "UPDATE heading SET %s = %s + ? "
					+ "WHERE record_set = ? AND type_desc = ? AND sort = ?", count_field, count_field);
			stmts.put("update", connection.prepareStatement(query));
		}
		PreparedStatement stmt = stmts.get("update");
		stmt.setInt(1, count);
		stmt.setInt(2, blf.recordSet().ordinal());
		stmt.setInt(3, blf.headingTypeDesc().ordinal());
		stmt.setString(4, headingSort);
		int rowsAffected = stmt.executeUpdate();
		
		// if no rows were affected, this heading is not yet in the database
		if ( rowsAffected == 0 ) {
			String headingDisplay;
			try {
				headingDisplay = getDisplayHeading( blf , headingSort );
				if ( ! stmts.containsKey("insert")) {
					String query = String.format(
							"INSERT INTO heading (heading, sort, record_set, type_desc, %s) " +
							"VALUES (?, ?, ?, ?, ?)", count_field);
					stmts.put("insert", connection.prepareStatement(query));
				}
				stmt = stmts.get("insert");
				stmt.setString(1, headingDisplay);
				stmt.setString(2, headingSort);
				stmt.setInt(3, blf.recordSet().ordinal());
				stmt.setInt(4, blf.headingTypeDesc().ordinal());
				stmt.setInt(5, count);
				stmt.executeUpdate();
			} catch (IOException | XMLStreamException | URISyntaxException e) {
				System.out.println("IO error retrieving heading display format from Blacklight. Count not recorded for: "+headingSort);
				e.printStackTrace();
				System.exit(1);
			}
		}

	}

	
	private String getDisplayHeading(BlacklightField blf, String headingSort) throws IOException, XMLStreamException, URISyntaxException {
		
		String facet = blf.facetField();
		if (facet == null)
			return headingSort;

		Collection<String> solrArgs = new HashSet<String>();
		solrArgs.add( "qt=standard" );
		solrArgs.add( "q="+URLEncoder.encode("*:*","UTF-8") );
		solrArgs.add( "fq="+blf.fieldName()+"%22"+
				URLEncoder.encode(headingSort,"UTF-8").replaceAll("%22", "%5C%22")+"%22");
		solrArgs.add( "rows=0" );
		solrArgs.add( "echoParams=none" );
		solrArgs.add( "facet=true" );
		solrArgs.add( "facet.field="+facet );
		solrArgs.add( "facet.limit=40" );
		solrArgs.add( "facet.mincount=1" );
		String query = config.getBlacklightSolrUrl() +"/select?" + StringUtils.join( solrArgs, "&");
//		System.out.println("**** Looking for display value for: "+headingSort+"****");
		

		URI uri = new URI(query);
		URL queryUrl = uri.toURL();
		XMLInputFactory inputFactory = XMLInputFactory.newInstance();
		InputStream in = queryUrl.openStream();
		XMLStreamReader r  = inputFactory.createXMLStreamReader(in);

		// fast forward to response body
		FF: while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("START_ELEMENT"))
				if (r.getLocalName().equals("lst"))
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("name"))
							if (r.getAttributeValue(i).equals(facet)) break FF;
		}
		
		// process actual results
		String heading = null;
		int wrongHeadingCount = 0;
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("START_ELEMENT")) {
				if (r.getLocalName().equals("int")) {
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("name"))
							heading = r.getAttributeValue(i);
					String sort = getSortHeading(heading);
//					System.out.println(heading + " => "+ sort);
					if (sort.equals(headingSort)) {
						in.close();
						recordWrongHeadingCount(wrongHeadingCount);
						return heading;
					} else {
						wrongHeadingCount++;
					}
				}
			}
		}
		in.close();
		recordWrongHeadingCount(wrongHeadingCount);
		System.out.println("Didn't find display form for: "+headingSort);
		System.out.println(query);
	//	System.exit(1);
		return headingSort;
	}
	
	private void recordWrongHeadingCount( int c ) {
		if (wrongHeadingCounts.containsKey(c))
			wrongHeadingCounts.put(c, wrongHeadingCounts.get(c)+1);
		else
			wrongHeadingCounts.put(c, 1);
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
	

}
