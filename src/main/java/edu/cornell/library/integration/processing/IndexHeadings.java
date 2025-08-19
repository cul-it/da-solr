package edu.cornell.library.integration.processing;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.json.JsonQueryRequest;
import org.apache.solr.client.solrj.request.json.TermsFacetMap;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;

import edu.cornell.library.integration.metadata.support.HeadingCategory;
import edu.cornell.library.integration.utilities.BlacklightHeadingField;
import edu.cornell.library.integration.utilities.Config;

public class IndexHeadings {

	// This structure should contain only up to six PreparedStatement objects at most.
	private Map<HeadingCategory,Map<String,String>> queries = new HashMap<>();
	Config config;

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
		this.config.setDatabasePoolsize("Headings", 2);
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


		try (Http2SolrClient solr = new Http2SolrClient
				.Builder(config.getBlacklightSolrUrl())
				.withBasicAuthCredentials(config.getSolrUser(),config.getSolrPassword()).build();) {

			for (BlacklightHeadingField blf : BlacklightHeadingField.values()) {

				deleteCountsFromDB();
				processBlacklightHeadingFieldHeaderData( solr, blf );
			}
			
		}

	}

	private void processBlacklightHeadingFieldHeaderData(Http2SolrClient solr, BlacklightHeadingField blf) throws Exception {

		System.out.printf("Poling Blacklight Solr field %s for %s values as %s\n",
					blf.fieldName(),blf.headingType(),blf.headingCategory());

		if ( ! this.queries.containsKey(blf.headingCategory()))
			this.queries.put(blf.headingCategory(), new HashMap<String,String>());

		int batchSize = 100_000;
		int numFound = 1;
		int currentOffset = 0;
		while (numFound > 0) {

			TermsFacetMap blFacet = new TermsFacetMap(blf.fieldName())
					.setLimit(batchSize).setBucketOffset(currentOffset).setSort("index");
			JsonQueryRequest request = new JsonQueryRequest()
					.setQuery("type:Catalog").setLimit(0).withFacet("headings", blFacet);
			QueryResponse qr = request.process(solr);

			Map<String,Object> m = new HashMap<>();
			qr.toMap(m);

			Map<String,Object> facets = (Map<String,Object>)m.get("facets");
			Map<String,Object> headings = (Map<String,Object>)facets.get("headings");
			List<Map<String,Object>> buckets = (List<Map<String,Object>>)headings.get("buckets");

			numFound = addCountsToDB( solr, buckets, blf );
			currentOffset += batchSize;
		}
	}

	private int addCountsToDB(Http2SolrClient solr, List<Map<String,Object>> buckets, BlacklightHeadingField blf)
			throws Exception {

		int headingCount = 0;
		for (Map<String,Object> bucket : buckets) {

			String heading = (String) bucket.get("val");
			Long recordCount = (Long) bucket.get("count");

			addCountToDB(solr, blf, heading, recordCount.intValue());
			if (++headingCount % 10_000 == 0) {
				System.out.printf("%s => %d\n",heading,recordCount);
			}
		}
		return headingCount;
	}


	private void addCountToDB(Http2SolrClient solr, BlacklightHeadingField blf, String headingSort, Integer count)
			throws SQLException, InterruptedException, SolrServerException {

		String count_field = blf.headingCategory().dbField();

		try (Connection conn = this.config.getDatabaseConnection("Headings");
			PreparedStatement uStmt = conn.prepareStatement(String.format(
					"UPDATE heading SET %s = ? WHERE heading_type = ? AND sort = ?", count_field))) {

			uStmt.setInt(1, count);
			uStmt.setInt(2, blf.headingType().ordinal());
			uStmt.setString(3, headingSort);
			int rowsAffected = uStmt.executeUpdate();

			// if no rows were affected, this heading is not yet in the database
			if ( rowsAffected == 0 ) {
				String headingDisplay;
				try {
					headingDisplay = getDisplayHeading( solr, blf , headingSort );
					if (headingDisplay == null) return;
					try ( PreparedStatement iStmt = conn.prepareStatement( String.format(
							"INSERT INTO heading (heading, sort, heading_type, %s) " +
									"VALUES (?, ?, ?, ?)", count_field)) ) {
						iStmt.setString(1, headingDisplay);
						iStmt.setString(2, headingSort);
						iStmt.setInt(3, blf.headingType().ordinal());
						iStmt.setInt(4, count);
						iStmt.executeUpdate();
					}
				} catch (IOException e) {
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

		try (Connection conn = this.config.getDatabaseConnection("Headings");
			 Statement stmt = conn.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT MAX(id) FROM heading");
			 PreparedStatement pstmt = conn.prepareStatement
				("UPDATE heading SET works = 0, works_by = 0, works_about = 0 "
				+ "WHERE id BETWEEN ? AND ?")  ){
			while (rs.next())
				maxId = rs.getInt(1);
			for (int left = 0; left < maxId; left += batchsize) {
				pstmt.setInt(1, left + 1);
				pstmt.setInt(2, left + batchsize);
				pstmt.executeUpdate();
			}
		}

	}


	private String getDisplayHeading(Http2SolrClient solr, BlacklightHeadingField blf, String headingSort)
			throws SolrServerException, IOException {

		String facet = blf.facetField();
		if (facet == null)
			return headingSort;

		// Get the top few facet values matching a search for headingSort
		SolrQuery query = buildBLDisplayHeadingQuery(blf.fieldName(), headingSort, facet, false);

		// Process top facet values, and identify one that matches sort heading.
		String heading = findHeadingInSolrResponse(solr.query(query), headingSort, facet);
		if (heading != null) return heading;

		// If nothing was found, try again with a larger facet response from Solr
		query = buildBLDisplayHeadingQuery(blf.fieldName(), headingSort, facet, true);
		heading = findHeadingInSolrResponse(solr.query(query), headingSort, facet);
		if (heading != null) return heading;

		// If that still didn't work, print an error message for future investigation.
		System.out.println("Didn't find display form - "+blf.fieldName()+":"+headingSort);
		System.out.println(query.toQueryString());
		return null;
	}

	private String findHeadingInSolrResponse(QueryResponse qr, String headingSort, String facet) {

		FacetField f = qr.getFacetField(facet);
		if (f == null) return null;

		for (Count c : f.getValues())
			if ( getFilingForm(c.getName()).equals(headingSort))
				return c.getName();

		return null;
	}


	private SolrQuery buildBLDisplayHeadingQuery(
			String fieldName, String headingSort, String facet, Boolean fullFacetList) throws UnsupportedEncodingException {

		SolrQuery q = new SolrQuery();
		q.setQuery("type:Catalog");
		q.setRows(0);
		q.setFilterQueries(String.format("%s:\"%s\"", fieldName, headingSort ));
		q.setFacet(true);
		q.setFacetLimit((fullFacetList)? 100_000 : 4 );
		q.addFacetField(facet);
		return q;
	}
}
