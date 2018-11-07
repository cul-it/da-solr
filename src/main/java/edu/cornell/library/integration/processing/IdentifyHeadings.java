package edu.cornell.library.integration.processing;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.EnumSet;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import edu.cornell.library.integration.metadata.support.AuthorityData.BlacklightField;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.FilingNormalization;

/**
 * This is an unpolished utility class that will probably be retired before I need to really
 * clean it up.<br/><br/>
 * 
 * IdentifyHeadings deals with the inefficiencies of running IndexHeadings.java on
 * a database that doesn't yet contain any of the unauthorized headings. IndexHeadings.java
 * uses a facet query into the Blacklight index to identify the most popular display form 
 * for any new (normalized) headings. The selection of the most popular form is to increase
 * the likelihood that a error in the heading that still normalizes correctly (e.g. bad
 * punctuation, capitalization) won't become the form displayed in the interface. After the
 * initial pass with IndexHeadings, the database should contain the vast majority of headings
 * and during subsequent runs will only need to go to this effort for headings that are new
 * in the catalog.<br/><br/>
 * 
 * These facet queries are significantly slower in the SolrCloud environment than they were
 * in the stand-alone Solr, causing what used to be a slow, 2-day occasional task into a 
 * grueling task that ran for two weeks and was about half done before I implemented an
 * alternative. IdentifyHeadings short-circuits the population of the initial headings but
 * doesn't necessarily select the most popular form, so it's more likely to select the
 * erroneous headings. By running this without the <code>facet.sort=index</code> in the query
 * URL, the values are pulled in order of decreasing popularity, which <i>does</i> guarantee
 * that the most popular versions are found first and become the selected display forms, but
 * the job will crash before fully processing any of the larger facets due to the increased
 * effort Solr needs for interpolating the values in order to identify the overall heading
 * sequence.<br/><br/>
 * 
 * This workaround doesn't work correctly for <code>HeadTypeDesc.PERSNAME</code>,
 * <code>HeadTypeDesc.CORPNAME</code>, <code>HeadTypeDesc.EVENT</code>, because they don't
 * have separate display facets in the Blacklight index.<br/><br/>
 */
public class IdentifyHeadings {

	public static void main(String[] args) {
		try {
			new IdentifyHeadings();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public IdentifyHeadings() throws Exception {

		// load configuration for location of index, location of authorities
		Collection<String> requiredArgs = Config.getRequiredArgsForWebdav();
		requiredArgs.add("blacklightSolrUrl");
		Config	config = Config.loadConfig(null,requiredArgs);

		EnumSet<BlacklightField> blFields = EnumSet.of(

/*				BlacklightField.AUTHOR_PERSON,
				BlacklightField.AUTHOR_CORPORATE,
				BlacklightField.AUTHOR_EVENT,
				BlacklightField.SUBJECT_PERSON,
				BlacklightField.SUBJECT_CORPORATE,
				BlacklightField.SUBJECT_EVENT, */
				BlacklightField.AUTHORTITLE_WORK,
				BlacklightField.SUBJECT_WORK,
				BlacklightField.SUBJECT_TOPIC,
				BlacklightField.SUBJECT_PLACE,
				BlacklightField.SUBJECT_CHRON,
				BlacklightField.SUBJECT_GENRE  );

		for (BlacklightField blf : blFields) {
			processBlacklightFieldHeaderData( config, blf );
		}
	}

	private static void processBlacklightFieldHeaderData(Config config, BlacklightField blf) throws Exception {
		System.out.printf("Poling Blacklight Solr field %s for %s values as %s\n",
				blf.fieldName(),blf.headingTypeDesc(),blf.headingType());

		String blacklightSolrUrl = config.getBlacklightSolrUrl();

		int numFound = 1;
//		int batchSize = 1_000;
		int currentOffset = 0;
		int batchSize = 250_000;
//		int currentOffset = 2_250_000;
		while (numFound > 0) {
			URL queryUrl = new URL(blacklightSolrUrl+
					"/select?qt=standard&rows=0&q=*:*&echoParams=none&facet=true&facet.sort=index&facet.field="+blf.facetField()+
					"&facet.limit="+batchSize+"&facet.offset="+currentOffset);
			numFound = pollHeadings( config, queryUrl, blf );
			currentOffset += batchSize;
		}
	}

	private static int pollHeadings(Config config, URL queryUrl, BlacklightField blf) throws Exception {

		// save terms info for field to temporary file.
		final Path tempPath = Files.createTempFile("indexHeadings-"+blf.facetField()+"-", ".xml");
		tempPath.toFile().deleteOnExit();
		System.out.println(queryUrl);

		try (   FileOutputStream fos = new FileOutputStream(tempPath.toString());
				ReadableByteChannel rbc = Channels.newChannel(queryUrl.openStream())  ){

			fos.getChannel().transferFrom(rbc, 0, Integer.MAX_VALUE); //Integer.MAX_VALUE translates to 2 gigs max download
		}

		// then read the file back in to process it.
		int headingCount = 0;
		try (	Connection connection = config.getDatabaseConnection("Headings");
				FileInputStream fis = new FileInputStream(tempPath.toString())  ){

			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
			XMLStreamReader r  = inputFactory.createXMLStreamReader(fis);

			// fast forward to response body
			FF: while (r.hasNext())
				if (r.next() == XMLStreamConstants.START_ELEMENT)
					if (r.getLocalName().equals("lst"))
						for (int i = 0; i < r.getAttributeCount(); i++)
							if (r.getAttributeLocalName(i).equals("name")) {
								String name = r.getAttributeValue(i);
								if (name.equals(blf.facetField())) break FF;
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
						addToDB(connection,blf,heading);
//						headingCount++;
						if (++headingCount % 5000 == 0)
							System.out.printf("%s => %d\n",heading,recordCount);
					}
		}
		Files.delete(tempPath);
		return headingCount;
	}

	private static void addToDB(Connection connection, BlacklightField blf, String heading) throws SQLException {

		String sort = FilingNormalization.getFilingForm(heading);

		// return if heading already appears in database
		try ( PreparedStatement checkStmt = connection.prepareStatement(
				"SELECT * FROM heading WHERE type_desc = ? AND sort = ?")) {
			checkStmt.setInt(1, blf.headingTypeDesc().ordinal());
			checkStmt.setString(2, sort);
			try ( ResultSet rs = checkStmt.executeQuery() ) {
				while (rs.next())
					return;
			}
		}

		// add heading to database if new
		try ( PreparedStatement insertStmt = connection.prepareStatement(
				"INSERT INTO heading (heading, sort, type_desc) VALUES (?, ?, ?)")) {
			insertStmt.setString(1, heading);
			insertStmt.setString(2, sort);
			insertStmt.setInt(3, blf.headingTypeDesc().ordinal());
			insertStmt.executeUpdate();
		}
		
	}


}
