package edu.cornell.library.integration.processing;

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
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import edu.cornell.library.integration.metadata.support.HeadingCategory;
import edu.cornell.library.integration.utilities.BlacklightHeadingField;
import edu.cornell.library.integration.utilities.Config;

public class IndexHeadings {

	private Connection connection = null;
	// This structure should contain only up to six PreparedStatement objects at most.
	private Map<HeadingCategory,Map<String,String>> queries = new HashMap<>();
	Config config;
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
		Collection<String> requiredArgs = Config.getRequiredArgsForDB("Headings");
		requiredArgs.add("blacklightSolrUrl");
		this.config = Config.loadConfig(requiredArgs);
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

		this.connection = this.config.getDatabaseConnection("Headings");
		deleteCountsFromDB();
		this.connection.setAutoCommit(false);

		for (BlacklightHeadingField blf : BlacklightHeadingField.values()) {

			processBlacklightHeadingFieldHeaderData( blf );
			this.connection.commit();
		}
	}

	private void processBlacklightHeadingFieldHeaderData(BlacklightHeadingField blf) throws Exception {

		System.out.printf("Poling Blacklight Solr field %s for %s values as %s\n",
					blf.fieldName(),blf.headingType(),blf.headingCategory());

		if ( ! this.queries.containsKey(blf.headingCategory()))
			this.queries.put(blf.headingCategory(), new HashMap<String,String>());

		String blacklightSolrUrl = this.config.getBlacklightSolrUrl();

		int batchSize = 1_000_000;
		int numFound = 1;
		int currentOffset = 0;
		while (numFound > 0) {
			URL queryUrl = new URL(blacklightSolrUrl+
					"/select?qt=standard&q=type:Catalog&rows=0&facet=true&facet.sort=index&facet.mincount=1&facet.field=" +
					blf.fieldName() +"&facet.limit="+batchSize+"&facet.offset="+currentOffset+"&wt=xml");
			numFound = addCountsToDB( queryUrl, blf );
			currentOffset += batchSize;
		}
	}

	private int addCountsToDB(URL queryUrl, BlacklightHeadingField blf) throws Exception {

		// save terms info for field to temporary file.
		final Path tempPath = Files.createTempFile("indexHeadings-"+blf.fieldName()+"-", ".xml");
		tempPath.toFile().deleteOnExit();

		try (   FileOutputStream fos = new FileOutputStream(tempPath.toString());
				ReadableByteChannel rbc = Channels.newChannel(queryUrl.openStream())  ){

			fos.getChannel().transferFrom(rbc, 0, Integer.MAX_VALUE); //Integer.MAX_VALUE translates to 2 gigs max download
		}

		// then read the file back in to process it.
		int headingCount = 0;
		try (  FileInputStream fis = new FileInputStream(tempPath.toString())  ){

			XMLStreamReader r  = this.inputFactory.createXMLStreamReader(fis);

			// fast forward to response body
			FF: while (r.hasNext())
				if (r.next() == XMLStreamConstants.START_ELEMENT)
					if (r.getLocalName().equals("lst"))
						for (int i = 0; i < r.getAttributeCount(); i++)
							if (r.getAttributeLocalName(i).equals("name")) {
								String name = r.getAttributeValue(i);
								if (name.equals(blf.fieldName())) break FF;
							}

			// process actual results
			String heading = null;
			Integer recordCount = null;
			while (r.hasNext())
				if (r.next() == XMLStreamConstants.START_ELEMENT)
					if (r.getLocalName().equals("int")) {
						for (int i = 0; i < r.getAttributeCount(); i++)
							if (r.getAttributeLocalName(i).equals("name"))
								heading = r.getAttributeValue(i);
						recordCount = Integer.valueOf(r.getElementText());
						addCountToDB(blf,this.queries.get(blf.headingCategory()),heading, recordCount);
						if (++headingCount % 10_000 == 0) {
							System.out.printf("%s => %d\n",heading,recordCount);
							this.connection.commit();
						}
					}
			this.connection.commit();
		}
		Files.delete(tempPath);
		return headingCount;
	}


	private void addCountToDB(BlacklightHeadingField blf, Map<String,String> qs, String headingSort, Integer count)
			throws SQLException, InterruptedException {

		String count_field = blf.headingCategory().dbField();
		// update record count in db
		if ( ! qs.containsKey("update")) {
			qs.put("update", String.format("UPDATE heading SET %s = ? WHERE heading_type = ? AND sort = ?", count_field));
		}
		int rowsAffected;
		try ( PreparedStatement uStmt = this.connection.prepareStatement( qs.get("update") ) ) {
			uStmt.setInt(1, count);
			uStmt.setInt(2, blf.headingType().ordinal());
			uStmt.setString(3, headingSort);
			rowsAffected = uStmt.executeUpdate();

			// if no rows were affected, this heading is not yet in the database
			if ( rowsAffected == 0 ) {
				String headingDisplay;
				try {
					headingDisplay = getDisplayHeading( blf , headingSort );
					if (headingDisplay == null) return;
					if ( ! qs.containsKey("insert")) {
						qs.put("insert",String.format(
								"INSERT INTO heading (heading, sort, heading_type, %s) " +
										"VALUES (?, ?, ?, ?)", count_field));
					}
					try ( PreparedStatement iStmt = this.connection.prepareStatement( qs.get("insert") ) ) {
						iStmt.setString(1, headingDisplay);
						iStmt.setString(2, headingSort);
						iStmt.setInt(3, blf.headingType().ordinal());
						iStmt.setInt(4, count);
						iStmt.executeUpdate();
					}
				} catch (IOException | XMLStreamException | URISyntaxException e) {
					System.out.println("IO error retrieving heading display format from Blacklight. Count not recorded for: "+headingSort);
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
	}

	private void deleteCountsFromDB() throws SQLException {

		int batchsize = 10_000;
		int maxId = 0;

		try (   Statement stmt = this.connection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT MAX(id) FROM heading") ) {

				while (rs.next())
					maxId = rs.getInt(1);
		}

		try (  PreparedStatement pstmt = this.connection.prepareStatement
				("UPDATE heading SET works = 0, works_by = 0, works_about = 0 "
						+ "WHERE id BETWEEN ? AND ?")  ){
			for (int left = 0; left < maxId; left += batchsize) {
				pstmt.setInt(1, left + 1);
				pstmt.setInt(2, left + batchsize);
				pstmt.executeUpdate();
			}
		}

	}


	private String getDisplayHeading(BlacklightHeadingField blf, String headingSort)
			throws IOException, XMLStreamException, URISyntaxException, InterruptedException {

		String facet = blf.facetField();
		if (facet == null)
			return headingSort;

		// Get the top few facet values matching a search for headingSort
		String query = buildBLDisplayHeadingQuery
				(blf.fieldName(),
				 headingSort,
				 facet, false);

		// Process top facet values, and identify one that matches sort heading.
		String heading = findHeadingInSolrResponse(query, headingSort, facet);
		if (heading != null) return heading;

		// If nothing was found, try again with a larger facet response from Solr
		query = buildBLDisplayHeadingQuery
				(blf.fieldName(),
				 headingSort,
				 facet, true);
		heading = findHeadingInSolrResponse(query, headingSort, facet);
		if (heading != null) return heading;

		// If that still didn't work, print an error message for future investigation.
		System.out.println("Didn't find display form - "+blf.fieldName()+":"+headingSort);
		return null;
	}

	private String findHeadingInSolrResponse(String query, String headingSort, String facet)
			throws URISyntaxException, MalformedURLException, XMLStreamException, InterruptedException {

		URI uri = new URI(query);
		URL queryUrl = uri.toURL();

		while (true) {
			try (  InputStream in = queryUrl.openStream() ){

				XMLStreamReader r  = this.inputFactory.createXMLStreamReader(in);

				// fast forward to response body
				FF: while (r.hasNext())
					if (r.next() == XMLStreamConstants.START_ELEMENT)
						if (r.getLocalName().equals("lst"))
							for (int i = 0; i < r.getAttributeCount(); i++)
								if (r.getAttributeLocalName(i).equals("name"))
									if (r.getAttributeValue(i).equals(facet)) break FF;

				// process actual results
				String heading = null;
				while (r.hasNext())
					if (r.next() == XMLStreamConstants.START_ELEMENT)
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
			} catch (@SuppressWarnings("unused") IOException e) {
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


	private String buildBLDisplayHeadingQuery(
			String fieldName, String headingSort, String facet, Boolean fullFacetList) throws UnsupportedEncodingException {

		StringBuilder sb = new StringBuilder();
		sb.append(this.config.getBlacklightSolrUrl());
		sb.append("/select?&qt=standard&rows=0&echoParams=none&wt=xml" );
		// all records
		sb.append( "&q=type:Catalog" );
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
