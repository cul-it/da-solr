package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
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
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.solr.client.solrj.SolrServerException;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.BlacklightField;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;

public class IndexHeadings {

	private Connection connection = null;
	// This structure should contain only up to six PreparedStatement objects at most.
	private Map<HeadType,Map<String,PreparedStatement>> statements =
			new HashMap<HeadType,Map<String,PreparedStatement>>();
	SolrBuildConfig config;
	private XMLInputFactory inputFactory = XMLInputFactory.newInstance();


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
		Collection<String> requiredArgs = SolrBuildConfig.getRequiredArgsForWebdav();
		requiredArgs.add("blacklightSolrUrl");
	            
		config = SolrBuildConfig.loadConfig(null,requiredArgs);
		if (args.length > 0) {
			int currentHour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
			try {
				int minHour = Integer.parseInt(args[0]);
				if (minHour > currentHour)
					throw new IllegalStateException("Error: according to provided arguments, "
							+ "this method can't be run before "+minHour+":00 local time.");
				if (args.length > 1) {
					int maxHour = Integer.parseInt(args[1]);
					if (maxHour <= currentHour)
						throw new IllegalStateException("Error: according to provided arguments, "
								+ "this method can't be run after "+maxHour+":00 local time.");
				}
			} catch (NumberFormatException e) {
				System.out.println("Error: 1st argument (if provided) must be an integer 0-23 "
						+ "representing the minimum hour BEFORE which this job should fail.");
				System.out.println("\\t2nd argument (if provided) must be an integer 0-23 "
						+ "representing the maximum hour AFTER which this job should fail.");
				System.out.println("\\tTo provide a max hour and not a min, the 1st argument must be 0.");
				throw e;
			}
		}

		connection = config.getDatabaseConnection("Headings");
		deleteCountsFromDB();
		connection.setAutoCommit(false);

		Collection<BlacklightField> blFields = new ArrayList<BlacklightField>();
		blFields.add(new BlacklightField(HeadType.AUTHOR, HeadTypeDesc.PERSNAME));
		blFields.add(new BlacklightField(HeadType.AUTHOR, HeadTypeDesc.CORPNAME));
		blFields.add(new BlacklightField(HeadType.AUTHOR, HeadTypeDesc.EVENT));

		blFields.add(new BlacklightField(HeadType.SUBJECT, HeadTypeDesc.PERSNAME));
		blFields.add(new BlacklightField(HeadType.SUBJECT, HeadTypeDesc.CORPNAME));
		blFields.add(new BlacklightField(HeadType.SUBJECT, HeadTypeDesc.EVENT));
		blFields.add(new BlacklightField(HeadType.SUBJECT, HeadTypeDesc.TOPIC));
		blFields.add(new BlacklightField(HeadType.SUBJECT, HeadTypeDesc.GEONAME));
		blFields.add(new BlacklightField(HeadType.SUBJECT, HeadTypeDesc.CHRONTERM));
		blFields.add(new BlacklightField(HeadType.SUBJECT, HeadTypeDesc.GENRE));
		blFields.add(new BlacklightField(HeadType.SUBJECT, HeadTypeDesc.WORK));

		blFields.add(new BlacklightField(HeadType.AUTHORTITLE, HeadTypeDesc.WORK));

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
		FF: while (r.hasNext())
			if (r.next() == XMLEvent.START_ELEMENT)
				if (r.getLocalName().equals("lst"))
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("name")) {
							String name = r.getAttributeValue(i);
							if (name.equals("terms")) break FF;
						}

		// process actual results
		String heading = null;
		Integer recordCount = null;
		int headingCount = 0;
		while (r.hasNext())
			if (r.next() == XMLEvent.START_ELEMENT)
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
		fis.close();
		Files.delete(tempPath);
	}


	private void addCountToDB(BlacklightField blf, Map<String, PreparedStatement> stmts, String headingSort, Integer count)
			throws SolrServerException, SQLException, InterruptedException {

		String count_field = blf.headingType().dbField();
		// update record count in db
		if ( ! stmts.containsKey("update")) {
			String query = String.format( "UPDATE heading SET %s = %s + ? "
					+ "WHERE type_desc = ? AND sort = ?", count_field, count_field);
			stmts.put("update", connection.prepareStatement(query));
		}
		PreparedStatement stmt = stmts.get("update");
		stmt.setInt(1, count);
		stmt.setInt(2, blf.headingTypeDesc().ordinal());
		stmt.setString(3, headingSort);
		int rowsAffected = stmt.executeUpdate();
		
		// if no rows were affected, this heading is not yet in the database
		if ( rowsAffected == 0 ) {
			String headingDisplay;
			try {
				headingDisplay = getDisplayHeading( blf , headingSort );
				if (headingDisplay == null) return;
				if ( ! stmts.containsKey("insert")) {
					String query = String.format(
							"INSERT INTO heading (heading, sort, type_desc, %s) " +
							"VALUES (?, ?, ?, ?)", count_field);
					stmts.put("insert", connection.prepareStatement(query));
				}
				stmt = stmts.get("insert");
				stmt.setString(1, headingDisplay);
				stmt.setString(2, headingSort);
				stmt.setInt(3, blf.headingTypeDesc().ordinal());
				stmt.setInt(4, count);
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


	private String getDisplayHeading(BlacklightField blf, String headingSort)
			throws IOException, XMLStreamException, URISyntaxException, InterruptedException {

		String facet = blf.facetField();
		if (facet == null)
			return headingSort;

		// Get the top few facet values matching a search for headingSort
		String query = buildBLDisplayHeadingQuery
				(config.getBlacklightSolrUrl(),
				 blf.fieldName(),
				 headingSort,
				 facet, false);

		// Process top facet values, and identify one that matches sort heading.
		String heading = findHeadingInSolrResponse(query, headingSort, facet);
		if (heading != null) return heading;

		// If nothing was found, try again with a larger facet response from Solr
		query = buildBLDisplayHeadingQuery
				(config.getBlacklightSolrUrl(),
				 blf.fieldName(),
				 headingSort,
				 facet, true);
		heading = findHeadingInSolrResponse(query, headingSort, facet);
		if (heading != null) return heading;

		// If that still didn't work, print an error message for future investigation.
		System.out.println("Didn't find display form: "+query);
		return null;
	}

	private String findHeadingInSolrResponse(String query, String headingSort, String facet)
			throws URISyntaxException, MalformedURLException, XMLStreamException, InterruptedException {

		URI uri = new URI(query);
		URL queryUrl = uri.toURL();

		while (true) {
			try{
				InputStream in = queryUrl.openStream();
				XMLStreamReader r  = inputFactory.createXMLStreamReader(in);
		
				// fast forward to response body
				FF: while (r.hasNext())
					if (r.next() == XMLEvent.START_ELEMENT)
						if (r.getLocalName().equals("lst"))
							for (int i = 0; i < r.getAttributeCount(); i++)
								if (r.getAttributeLocalName(i).equals("name"))
									if (r.getAttributeValue(i).equals(facet)) break FF;
				
				// process actual results
				String heading = null;
				while (r.hasNext())
					if (r.next() == XMLEvent.START_ELEMENT)
						if (r.getLocalName().equals("int")) {
							for (int i = 0; i < r.getAttributeCount(); i++)
								if (r.getAttributeLocalName(i).equals("name"))
									heading = r.getAttributeValue(i);
							String sort = getFilingForm(heading);
							if (sort.equals(headingSort)) {
								in.close();
								return heading;
							}
						}
				in.close();
				return null;
			} catch (IOException e) {
				/* The only way the while(true) loop is repeated, is if an error is
				 * thrown and execution ends up in this block. In that case, we will just
				 * wait a few seconds and try again.
				 */
				System.out.println("IOException querying Solr at <"+query+">.");
				Thread.sleep( 3000 );
				System.out.println("retrying...");
			}
		}
	}


	private String buildBLDisplayHeadingQuery(String blacklightSolrUrl,
			String fieldName, String headingSort, String facet, Boolean fullFacetList) throws UnsupportedEncodingException {

		StringBuilder sb = new StringBuilder();
		sb.append(config.getBlacklightSolrUrl());
		sb.append("/select?&qt=standard&rows=0&echoParams=none" );
		// all records
		sb.append( "&q=*%3A*" ); // q=*:*
		// filtered by filing value
		sb.append( "&fq=" );
				sb.append(fieldName);
				sb.append("%3A%22" ); // colon-start quotation
				sb.append( URLEncoder.encode(headingSort,"UTF-8").replaceAll("%22", "%5C%22")); //escape quotes
		        sb.append("%22"); // end quotation
		// return display values from facet field
		if (fullFacetList)
			sb.append( "&facet=true&facet.limit=100000&facet.mincount=1&facet.field=");
		else
			sb.append( "&facet=true&facet.limit=4&facet.mincount=1&facet.field=");
		sb.append( facet );
		return sb.toString();
	}
}
