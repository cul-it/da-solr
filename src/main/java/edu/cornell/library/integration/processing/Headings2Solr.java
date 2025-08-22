package edu.cornell.library.integration.processing;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.metadata.support.AuthorityData.ReferenceType;
import edu.cornell.library.integration.metadata.support.HeadingCategory;
import edu.cornell.library.integration.metadata.support.HeadingType;
import edu.cornell.library.integration.utilities.BlacklightHeadingField;
import edu.cornell.library.integration.utilities.Config;

public class Headings2Solr {

	private Connection connection = null;
	private Config config;
	private List<Integer> authorTypes = Arrays.asList(HeadingType.PERSNAME.ordinal(),
			HeadingType.CORPNAME.ordinal(),HeadingType.EVENT.ordinal());
	private final HeadingType[] headingTypes = HeadingType.values();
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final Map<HeadingCategory,HashMap<HeadingType,String>> blacklightFields = new HashMap<>();
	private static DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

	static {
		for ( BlacklightHeadingField blf : BlacklightHeadingField.values() ) {
			if (! blacklightFields.containsKey(blf.headingCategory()))
				blacklightFields.put(blf.headingCategory(), new HashMap<HeadingType,String>());
			blacklightFields.get(blf.headingCategory()).put(blf.headingType(), blf.browseCtsName());
		}
	}

	public static void main(String[] args) {
		try {
			new Headings2Solr();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public Headings2Solr() throws SolrServerException, IOException, SQLException {
		Collection<String> requiredArgs = Config.getRequiredArgsForDB("Headings");
		requiredArgs.addAll( Arrays.asList("authorSolrUrl","subjectSolrUrl","authorTitleSolrUrl") );
		this.config = Config.loadConfig(requiredArgs);

		this.config.setDatabasePoolsize("Headings", 2);
		this.connection = this.config.getDatabaseConnection("Headings");

		try (Http2SolrClient solr = new Http2SolrClient
				.Builder(this.config.getSubjectSolrUrl())
				.withBasicAuthCredentials(this.config.getSolrUser(),this.config.getSolrPassword()).build()){
			findWorks(solr, HeadingCategory.SUBJECT);
		}
		try (Http2SolrClient solr = new Http2SolrClient
				.Builder(this.config.getAuthorSolrUrl())
				.withBasicAuthCredentials(this.config.getSolrUser(),this.config.getSolrPassword()).build()){
			findWorks(solr, HeadingCategory.AUTHOR);
		}
		try (Http2SolrClient solr = new Http2SolrClient
				.Builder(this.config.getAuthorTitleSolrUrl())
				.withBasicAuthCredentials(this.config.getSolrUser(),this.config.getSolrPassword()).build()){
			findWorks(solr, HeadingCategory.AUTHORTITLE);
		}
		this.connection.close();
	}

	private void findWorks(Http2SolrClient solr , HeadingCategory hc)
			throws SolrServerException, IOException, SQLException {
		String query =
			"SELECT h.* "
			+ "FROM heading as h"

			+ " LEFT JOIN (reference AS r2, heading AS h2)"
			+ "  ON (r2.from_heading = h.id"
			+ "    AND h2.id = r2.to_heading )"

			+ " WHERE h."+hc.dbField()+" > 0"
			+ "    OR h2."+hc.dbField()+" > 0"
			+ " GROUP BY h.id";
		Collection<SolrInputDocument> docs = new ArrayList<>();
		Date mostRecentDate = getMostRecentSolrDate( solr );
		if (mostRecentDate != null)
			System.out.println("Before processing, the most recent timestamp in Solr is "+
					formatter.format( mostRecentDate.toInstant() ));

		/* This method requires its own connection to the database so it can buffer results
		 * which keeps the connection used tied up and unavailable for other queries
		 */
		try (   Connection connectionFindWorks = this.config.getDatabaseConnection("Headings");
				Statement stmt = connectionFindWorks.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY) ) {
			stmt.setFetchSize(30_000);
			stmt.execute(query);
			try ( ResultSet rs = stmt.getResultSet() ) {
				while (rs.next()) {
					int id = rs.getInt("id");
					SolrInputDocument doc = new SolrInputDocument();
					doc.addField("id", id);
					doc.addField("heading", rs.getString("heading"));
					doc.addField("headingSort", rs.getString("sort"));
					References xrefs = getXRefs(id, hc);
					if (xrefs.seeJson != null)
						doc.addField("see", xrefs.seeJson);
					if (xrefs.seeAlsoJson != null)
						doc.addField("seeAlso", xrefs.seeAlsoJson);
					doc.addField("alternateForm", getAltForms(id));

					HeadingType ht = this.headingTypes[ rs.getInt("heading_type") ];
					if ( ! hc.equals(HeadingCategory.AUTHORTITLE))
						doc.addField("headingTypeDesc", ht.toString());
					doc.addField("blacklightField", blacklightFields.get(hc).get(ht));
					AuthorityStatus as = getIsAuthorized(id);
					doc.addField("authority", ! as.equals(AuthorityStatus.NONE) );
					doc.addField("mainEntry", as.equals(AuthorityStatus.MAIN) );
					doc.addField("notes", getNotes(this.connection, id));
					String rda = getRda(id);
					if (rda != null) doc.addField("rda_json", rda);
					doc.addField("count",rs.getInt(hc.dbField()));
					doc.addField("counts_json", countsJson(rs));
					docs.add(doc);
					if (docs.size() == 5_000) {
						System.out.printf("%d: %s\n",id,rs.getString("heading"));
						solr.add(docs);
						docs.clear();
					}
				}
				if ( ! docs.isEmpty() )
					solr.add(docs);
				if ( mostRecentDate != null )
					solr.deleteByQuery(String.format(
							"timestamp:[ * TO \"%s\"]",formatter.format( mostRecentDate.toInstant() )));
			}
		}
	}
	
	private static Date getMostRecentSolrDate(Http2SolrClient solr_q) throws SolrServerException, IOException {
		SolrQuery q = new SolrQuery("*:*");
		q.setRows(1);
		q.setSort("timestamp", SolrQuery.ORDER.desc);
		q.setFields("timestamp");
		for (SolrDocument doc : solr_q.query(q).getResults()) {
			return (Date) doc.getFieldValue("timestamp");
		}
		return null;
	}

	// TODO Expand this to include authority identifiers in Solr args
	private static PreparedStatement isAuth_pstmt = null;
	private AuthorityStatus getIsAuthorized(int id) throws SQLException {
		AuthorityStatus as = AuthorityStatus.NONE;
		if ( isAuth_pstmt == null )
			isAuth_pstmt = this.connection.prepareStatement(
					"SELECT main_entry FROM authority2heading"+
					" WHERE heading_id = ?" );
		isAuth_pstmt.setInt(1, id);
		isAuth_pstmt.execute();
		try ( ResultSet rs = isAuth_pstmt.getResultSet() ) {
			while (rs.next())
				if ( rs.getBoolean("main_entry") )
					as = AuthorityStatus.MAIN;
				else if ( as.equals(AuthorityStatus.NONE) )
					as = AuthorityStatus.XREF;
		}
		return as;
	}
	private enum AuthorityStatus{ MAIN, XREF, NONE; }


	private static PreparedStatement ref_pstmt = null;
	private References getXRefs(int id, HeadingCategory hc) throws SQLException, JsonProcessingException {
		Collection<String> seeRefs = new ArrayList<>();
		Map<String,Collection<Object>> seeAlsoRefs = new HashMap<>();
		if (ref_pstmt == null)
			ref_pstmt = this.connection.prepareStatement(
					" SELECT r.ref_type, r.ref_desc, h.* "
					+ " FROM reference AS r, heading AS h "
					+ "WHERE r.from_heading = ? "
					+ "  AND r.to_heading = h.id "
					+ "ORDER BY h.sort"	);
		ref_pstmt.setInt(1, id);
		ref_pstmt.execute();
		try (  ResultSet rs = ref_pstmt.getResultSet() ) {

			while (rs.next()) {
				int count = rs.getInt(hc.dbField());
				if (count == 0) continue;
				Map<String,Object> vals = new HashMap<>();
				vals.put("count", count);
				vals.put("worksAbout", rs.getInt("works_about"));
				vals.put("heading", rs.getString("heading"));
				int heading_type = rs.getInt("heading_type");
				if (this.authorTypes.contains(heading_type))
					vals.put("worksBy", rs.getInt("works_by"));
				if (HeadingType.WORK.ordinal() == heading_type)
					vals.put("works", rs.getInt("works"));
				vals.put("headingTypeDesc", this.headingTypes[  rs.getInt("heading_type") ].toString());
				String ref_desc = rs.getString("ref_desc");
				String relationship = null;
				if (ref_desc != null && ! ref_desc.isEmpty())
					relationship = ref_desc;
				else
					relationship = "";
				if (ReferenceType.FROM4XX.ordinal() == rs.getInt("ref_type")) {
					if (! relationship.isEmpty())
						vals.put("relationship", relationship);
					seeRefs.add(mapper.writeValueAsString(vals));
				} else {
					if (seeAlsoRefs.containsKey(relationship))
						seeAlsoRefs.get(relationship).add(vals);
					else {
						Collection<Object> thisRel = new ArrayList<>();
						thisRel.add(vals);
						seeAlsoRefs.put(relationship, thisRel);
					}
				}
			}
		}
		References r = new References();
		if ( ! seeRefs.isEmpty())
			r.seeJson = seeRefs;
		if ( ! seeAlsoRefs.isEmpty())
			r.seeAlsoJson = mapper.writeValueAsString(seeAlsoRefs);
		return r;
	}

	private String countsJson( ResultSet rs ) throws SQLException {
		int heading_type = rs.getInt("heading_type");
		if (this.authorTypes.contains(heading_type ))
			return String.format("{\"worksBy\":%d,\"worksAbout\":%d}",
					rs.getInt("works_by"),rs.getInt("works_about"));
		if (HeadingType.WORK.ordinal() == heading_type)
			return String.format("{\"worksAbout\":%d,\"works\":%d}",
					rs.getInt("works_about"),rs.getInt("works"));
		return String.format("{\"worksAbout\":%d}",rs.getInt("works_about"));
	}
	
	private static PreparedStatement note_pstmt = null;
	public static Collection<String> getNotes( Connection headings, int id ) throws SQLException {
		if (note_pstmt == null)
			note_pstmt = headings.prepareStatement("SELECT note FROM note WHERE heading_id = ?");
		note_pstmt.setInt(1, id);
		note_pstmt.execute();
		Collection<String> notes = new ArrayList<>();
		try ( ResultSet rs = note_pstmt.getResultSet() ){
			while (rs.next()) {
				String note = rs.getString("note");
				if (note.startsWith("[") && note.endsWith("]"))
					notes.add( note );
				else {
					try {
						notes.add(mapper.writeValueAsString(Arrays.asList(note)));
					} catch (JsonProcessingException e) { e.printStackTrace(); }
				}
			}
		}
		return notes;
	}

	private static PreparedStatement alt_pstmt = null;
	private Collection<String> getAltForms( int id ) throws SQLException {
		if (alt_pstmt == null)
			alt_pstmt = this.connection.prepareStatement(
					"SELECT heading FROM heading, reference "
					+ "WHERE to_heading = ? "
					+ "AND from_heading = heading.id "
					+ "AND ref_type = "+ReferenceType.FROM4XX.ordinal()
					+ " ORDER BY sort");
		alt_pstmt.setInt(1, id);
		alt_pstmt.execute();
		Collection<String> forms = new ArrayList<>();
		try ( ResultSet rs = alt_pstmt.getResultSet() ){
			while (rs.next())
				forms.add( rs.getString("heading") );
		}
		return forms;
	}

	private static PreparedStatement rda_pstmt = null;
	private String getRda( int id ) throws SQLException {
		if (rda_pstmt == null)
			rda_pstmt = this.connection.prepareStatement("SELECT rda FROM rda WHERE heading_id = ?");
		rda_pstmt.setInt(1, id);
		rda_pstmt.execute();
		String rda = null;
		try ( ResultSet rs = rda_pstmt.getResultSet() ) {
			if (rs.next())
				rda = rs.getString("rda");
		}
		return rda;
	}
	
	private class References {
		public References() { }
		public String seeAlsoJson = null;
		public Collection<String> seeJson = null;
	}
}
