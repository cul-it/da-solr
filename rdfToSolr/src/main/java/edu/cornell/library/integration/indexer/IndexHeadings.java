package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.indexer.utilities.FilingNormalization.getSortHeading;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.getXMLEventTypeString;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.solr.client.solrj.SolrServerException;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.BlacklightField;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.RecordSet;

public class IndexHeadings {

	private Connection connection = null;
	// This structure should contain only up to six PreparedStatement objects at most.
	private Map<HeadType,Map<String,PreparedStatement>> statements =
			new HashMap<HeadType,Map<String,PreparedStatement>>();
	SolrBuildConfig config;
	private Map<Integer,Integer> wrongHeadingCounts = new HashMap<Integer,Integer>();
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			new IndexHeadings(args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}
	
	
	public IndexHeadings(String[] args) throws Exception {
        
		// load configuration for location of index, location of authorities
		Collection<String> requiredArgs = new ArrayList<String>();
		requiredArgs.add("xmlDir");
		requiredArgs.add("blacklightSolrUrl");
	            
		config = SolrBuildConfig.loadConfig(args,requiredArgs);		
		
		connection = config.getDatabaseConnection("Headings");
		deleteCountsFromDB();
		connection.setAutoCommit(false);

		Collection<BlacklightField> blFields = new ArrayList<BlacklightField>();
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.AUTHOR, HeadTypeDesc.PERSNAME, "author_100_filing","author_facet" ));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.AUTHOR, HeadTypeDesc.CORPNAME, "author_110_filing","author_facet" ));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.AUTHOR, HeadTypeDesc.EVENT,    "author_111_filing","author_facet" ));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.AUTHOR, HeadTypeDesc.PERSNAME, "author_700_filing","author_facet" ));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.AUTHOR, HeadTypeDesc.CORPNAME, "author_710_filing","author_facet" ));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.AUTHOR, HeadTypeDesc.EVENT,    "author_711_filing","author_facet" ));

		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.SUBJECT, HeadTypeDesc.PERSNAME, "subject_600_filing","subject_topic_facet"));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.SUBJECT, HeadTypeDesc.CORPNAME, "subject_610_filing","subject_topic_facet"));
		blFields.add(new BlacklightField(RecordSet.NAME, HeadType.SUBJECT, HeadTypeDesc.EVENT, "subject_611_filing","subject_topic_facet"));
		blFields.add(new BlacklightField(RecordSet.SUBJECT, HeadType.SUBJECT, HeadTypeDesc.TOPIC, "subject_650_filing","subject_topic_facet"));
		blFields.add(new BlacklightField(RecordSet.SUBJECT, HeadType.SUBJECT, HeadTypeDesc.GEONAME, "subject_651_filing","subject_geo_facet"));
		blFields.add(new BlacklightField(RecordSet.SUBJECT, HeadType.SUBJECT, HeadTypeDesc.CHRONTERM, "subject_648_filing","subject_era_facet"));
		blFields.add(new BlacklightField(RecordSet.SUBJECT, HeadType.SUBJECT, HeadTypeDesc.GENRE, "subject_655_filing","subject_topic_facet"));
		blFields.add(new BlacklightField(RecordSet.SUBJECT, HeadType.SUBJECT, HeadTypeDesc.GEONAME, "subject_662_filing","subject_geo_facet"));
		blFields.add(new BlacklightField(RecordSet.NAMETITLE, HeadType.SUBJECT, HeadTypeDesc.WORK, "authortitle_600_filing","subject_topic_facet"));
		blFields.add(new BlacklightField(RecordSet.NAMETITLE, HeadType.SUBJECT, HeadTypeDesc.WORK, "authortitle_610_filing","subject_topic_facet"));
		blFields.add(new BlacklightField(RecordSet.NAMETITLE, HeadType.SUBJECT, HeadTypeDesc.WORK, "authortitle_611_filing","subject_topic_facet"));

		blFields.add(new BlacklightField(RecordSet.NAMETITLE, HeadType.AUTHORTITLE, HeadTypeDesc.WORK, "authortitle_240_filing","authortitle_facet"));
		blFields.add(new BlacklightField(RecordSet.NAMETITLE, HeadType.AUTHORTITLE, HeadTypeDesc.WORK, "authortitle_245_filing","authortitle_facet"));
		blFields.add(new BlacklightField(RecordSet.NAMETITLE, HeadType.AUTHORTITLE, HeadTypeDesc.WORK, "authortitle_700_filing","authortitle_facet"));
		blFields.add(new BlacklightField(RecordSet.NAMETITLE, HeadType.AUTHORTITLE, HeadTypeDesc.WORK, "authortitle_710_filing","authortitle_facet"));
		blFields.add(new BlacklightField(RecordSet.NAMETITLE, HeadType.AUTHORTITLE, HeadTypeDesc.WORK, "authortitle_711_filing","authortitle_facet"));

		for (BlacklightField blf : blFields) {
			
			processBlacklightFieldHeaderData( blf );
			connection.commit();
		}
	}

	
	private void processBlacklightFieldHeaderData(BlacklightField blf) throws Exception {

		System.out.printf("Poling Blacklight Solr field %s for %s values as %s\n",
					blf.fieldName(),blf.headingTypeDesc(),blf.headingType());

		if ( ! statements.containsKey(blf.headingType()))
			statements.put(blf.headingType(), new HashMap<String,PreparedStatement>());

		// save terms info for field to temporary file.
		URL queryUrl = new URL(config.getBlacklightSolrUrl() + "/terms?terms.fl=" +
				blf.fieldName() + "&terms.sort=index&terms.limit=100000000");
		final Path tempPath = Files.createTempFile("indexHeadings-"+blf.fieldName()+"-", ".xml");
		tempPath.toFile().deleteOnExit();
		FileOutputStream fos = new FileOutputStream(tempPath.toString());
		ReadableByteChannel rbc = Channels.newChannel(queryUrl.openStream());
		fos.getChannel().transferFrom(rbc, 0, Integer.MAX_VALUE); //Integer.MAX_VALUE translates to 2 gigs max download
		fos.close();

		// then read the file back in to process it.
		FileInputStream fis = new FileInputStream(tempPath.toString());
		XMLInputFactory inputFactory = XMLInputFactory.newInstance();
		XMLStreamReader r  = inputFactory.createXMLStreamReader(fis);

		// fast forward to response body
		FF: while (r.hasNext()) {
			String event = getXMLEventTypeString(r.next());
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
			String event = getXMLEventTypeString(r.next());
			if (event.equals("START_ELEMENT")) {
				if (r.getLocalName().equals("int")) {
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("name"))
							heading = r.getAttributeValue(i);
					recordCount = Integer.valueOf(r.getElementText());
					addCountToDB(blf,statements.get(blf.headingType()),heading, recordCount);
					if (++headingCount % 10_000 == 0) {
						System.out.printf("%s => %d\n",heading,recordCount);
						connection.commit();
					}
				}
			}
		}
		fis.close();
		Files.delete(tempPath);
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

	private void deleteCountsFromDB() throws SQLException {

		int batchsize = 10_000;

		Statement stmt = connection.createStatement();
		stmt.executeQuery("SELECT MAX(id) FROM heading");
		int maxId = 0;
		ResultSet rs = stmt.getResultSet();
		while (rs.next())
			maxId = rs.getInt(1);

		PreparedStatement pstmt = connection.prepareStatement
				("UPDATE heading SET works = 0, works_by = 0, works_about = 0 "
						+ "WHERE id BETWEEN ? AND ?");
		for (int left = 0; left < maxId; left += batchsize) {
			pstmt.setInt(1, left + 1);
			pstmt.setInt(2, left + batchsize);
			pstmt.executeUpdate();
		}
		pstmt.close();

	}

	
	private String getDisplayHeading(BlacklightField blf, String headingSort) throws IOException, XMLStreamException, URISyntaxException {
		
		String facet = blf.facetField();
		if (facet == null)
			return headingSort;

		StringBuilder sb = new StringBuilder();
		sb.append(config.getBlacklightSolrUrl());
		sb.append("/select?&qt=standard&rows=0&echoParams=none" );
		// all records
		sb.append( "&q=" ); sb.append(URLEncoder.encode("*:*","UTF-8") );
		// filtered by filing value
		sb.append( "&fq=" ); sb.append(blf.fieldName() ); sb.append("%3A%22" ); // colon-start quotation
		         sb.append( URLEncoder.encode(headingSort,"UTF-8").replaceAll("%22", "%5C%22")); //escape quotes
		         sb.append("%22"); // end quotation
		// return display values from facet field
		sb.append( "&facet=true&facet.limit=40&facet.mincount=1&facet.field="); sb.append( facet );
		String query =  sb.toString();

		URI uri = new URI(query);
		URL queryUrl = uri.toURL();
		XMLInputFactory inputFactory = XMLInputFactory.newInstance();
		InputStream in = queryUrl.openStream();
		XMLStreamReader r  = inputFactory.createXMLStreamReader(in);

		// fast forward to response body
		FF: while (r.hasNext()) {
			String event = getXMLEventTypeString(r.next());
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
			String event = getXMLEventTypeString(r.next());
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

}
