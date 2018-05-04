package edu.cornell.library.integration.indexer;

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
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.BlacklightField;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.ReferenceType;

public class Headings2Solr {

	private Connection connection = null;
	private SolrBuildConfig config;
	private List<Integer> authorTypes = Arrays.asList(HeadTypeDesc.PERSNAME.ordinal(),
			HeadTypeDesc.CORPNAME.ordinal(),HeadTypeDesc.EVENT.ordinal());
	private final HeadTypeDesc[] HeadTypeDescs = HeadTypeDesc.values();
	private static final ObjectMapper mapper = new ObjectMapper();
	private final Map<HeadType,HashMap<HeadTypeDesc,String>> blacklightFields = new HashMap<>();
	private static DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;

	public static void main(String[] args) {
		try {
			new Headings2Solr(args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public Headings2Solr(String[] args) throws Exception {
		Collection<String> requiredArgs = SolrBuildConfig.getRequiredArgsForWebdav();
		requiredArgs.add("authorSolrUrl");
		requiredArgs.add("subjectSolrUrl");
		requiredArgs.add("authorTitleSolrUrl");
		config = SolrBuildConfig.loadConfig(args,requiredArgs);

		connection = config.getDatabaseConnection("Headings");

		try ( HttpSolrClient solr_q = new HttpSolrClient(config.getSubjectSolrUrl());
				ConcurrentUpdateSolrClient solr_u =
						new ConcurrentUpdateSolrClient(config.getSubjectSolrUrl(),1000,5) ){
			findWorks(solr_u, solr_q, HeadType.SUBJECT);
			solr_u.blockUntilFinished();
		}
		try ( HttpSolrClient solr_q = new HttpSolrClient(config.getAuthorSolrUrl());
				ConcurrentUpdateSolrClient solr_u =
						new ConcurrentUpdateSolrClient(config.getAuthorSolrUrl(),1000,5) ){
			findWorks(solr_u, solr_q, HeadType.AUTHOR);
			solr_u.blockUntilFinished();
		}
		try ( HttpSolrClient solr_q = new HttpSolrClient(config.getAuthorTitleSolrUrl());
				ConcurrentUpdateSolrClient solr_u =
						new ConcurrentUpdateSolrClient(config.getAuthorTitleSolrUrl(),1000,5) ){
			findWorks(solr_u, solr_q, HeadType.AUTHORTITLE);
			solr_u.blockUntilFinished();
		}
		connection.close();
	}

	private void findWorks(ConcurrentUpdateSolrClient solr_u, HttpSolrClient solr_q, HeadType ht) throws Exception  {
		String query =
			"SELECT h.* "
			+ "FROM heading as h"

			+ " LEFT JOIN (reference AS r2, heading AS h2)"
			+ "  ON (r2.from_heading = h.id"
			+ "    AND h2.id = r2.to_heading )"

			+ " WHERE h."+ht.dbField()+" > 0"
			+ "    OR h2."+ht.dbField()+" > 0"
			+ " GROUP BY h.id";
		Collection<SolrInputDocument> docs = new ArrayList<>();
		Date mostRecentDate = getMostRecentSolrDate( solr_q );
		if (mostRecentDate != null)
			System.out.println("Before processing, the most recent timestamp in Solr is "+
					formatter.format( mostRecentDate.toInstant() ));

		/* This method requires its own connection to the database so it can buffer results
		 * which keeps the connection used tied up and unavailable for other queries
		 */
		try (   Connection connectionFindWorks = config.getDatabaseConnection("Headings");
				Statement stmt = connectionFindWorks.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY) ) {
			stmt.setFetchSize(Integer.MIN_VALUE);
			stmt.execute(query);
			try ( ResultSet rs = stmt.getResultSet() ) {
				while (rs.next()) {
					int id = rs.getInt("id");
					SolrInputDocument doc = new SolrInputDocument();
					doc.addField("id", id);
					doc.addField("heading", rs.getString("heading"));
					doc.addField("headingSort", rs.getString("sort"));
					References xrefs = getXRefs(id, ht);
					if (xrefs.seeJson != null)
						doc.addField("see", xrefs.seeJson);
					if (xrefs.seeAlsoJson != null)
						doc.addField("seeAlso", xrefs.seeAlsoJson);
					doc.addField("alternateForm", getAltForms(id));
					HeadTypeDesc htd = HeadTypeDescs[ rs.getInt("type_desc") ];
					if ( ! ht.equals(HeadType.AUTHORTITLE))
						doc.addField("headingTypeDesc", htd.toString());
					if (! blacklightFields.containsKey(ht) || ! blacklightFields.get(ht).containsKey(htd)) {
						BlacklightField blf = new BlacklightField(ht,htd);
						if (! blacklightFields.containsKey(ht))
							blacklightFields.put(ht, new HashMap<HeadTypeDesc,String>());
						blacklightFields.get(ht).put(htd, blf.browseCtsName());
					}
					doc.addField("blacklightField", blacklightFields.get(ht).get(htd));
					doc.addField("authority", rs.getBoolean("authority"));
					doc.addField("mainEntry", rs.getBoolean("main_entry"));
					doc.addField("notes", getNotes(id));
					String rda = getRda(id);
					if (rda != null) doc.addField("rda_json", rda);
					doc.addField("count",rs.getInt(ht.dbField()));
					doc.addField("counts_json", countsJson(rs));
					docs.add(doc);
					if (docs.size() == 5_000) {
						System.out.printf("%d: %s\n",id,rs.getString("heading"));
						solr_u.add(docs);
						docs.clear();
					}
				}
				if ( ! docs.isEmpty() )
					solr_u.add(docs);
				if ( mostRecentDate != null )
					solr_u.deleteByQuery(String.format(
							"timestamp:[ * TO \"%s\"]",formatter.format( mostRecentDate.toInstant() )));
			}
		}
	}
	
	private static Date getMostRecentSolrDate(HttpSolrClient solr_q) throws SolrServerException, IOException {
		SolrQuery q = new SolrQuery("*:*");
		q.setRows(1);
		q.setSort("timestamp", SolrQuery.ORDER.desc);
		q.setFields("timestamp");
		for (SolrDocument doc : solr_q.query(q).getResults()) {
			return (Date) doc.getFieldValue("timestamp");
		}
		return null;
	}

	private static PreparedStatement ref_pstmt = null;
	private References getXRefs(int id, HeadType ht) throws SQLException, JsonProcessingException {
		Collection<String> seeRefs = new ArrayList<>();
		Map<String,Collection<Object>> seeAlsoRefs = new HashMap<>();
		if (ref_pstmt == null)
			ref_pstmt = connection.prepareStatement(
					" SELECT r.ref_type, r.ref_desc, h.* "
					+ " FROM reference AS r, heading AS h "
					+ "WHERE r.from_heading = ? "
					+ "  AND r.to_heading = h.id "
					+ "ORDER BY h.sort"	);
		ref_pstmt.setInt(1, id);
		ref_pstmt.execute();
		try (  ResultSet rs = ref_pstmt.getResultSet() ) {

			while (rs.next()) {
				int count = rs.getInt(ht.dbField());
				if (count == 0) continue;
				Map<String,Object> vals = new HashMap<>();
				vals.put("count", count);
				vals.put("worksAbout", rs.getInt("works_about"));
				vals.put("heading", rs.getString("heading"));
				int type_desc = rs.getInt("type_desc");
				if (authorTypes.contains(type_desc))
					vals.put("worksBy", rs.getInt("works_by"));
				if (HeadTypeDesc.WORK.ordinal() == type_desc)
					vals.put("works", rs.getInt("works"));
				vals.put("headingTypeDesc", HeadTypeDescs[  rs.getInt("type_desc") ].toString());
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
		int type_desc = rs.getInt("type_desc");
		if (authorTypes.contains(type_desc ))
			return String.format("{\"worksBy\":%d,\"worksAbout\":%d}",
					rs.getInt("works_by"),rs.getInt("works_about"));
		if (HeadTypeDesc.WORK.ordinal() == type_desc)
			return String.format("{\"worksAbout\":%d,\"works\":%d}",
					rs.getInt("works_about"),rs.getInt("works"));
		return String.format("{\"worksAbout\":%d}",rs.getInt("works_about"));
	}
	
	private static PreparedStatement note_pstmt = null;
	private Collection<String> getNotes( int id ) throws SQLException {
		if (note_pstmt == null)
			note_pstmt = connection.prepareStatement("SELECT note FROM note WHERE heading_id = ?");
		note_pstmt.setInt(1, id);
		note_pstmt.execute();
		Collection<String> notes = new ArrayList<>();
		try ( ResultSet rs = note_pstmt.getResultSet() ){
			while (rs.next())
				notes.add( rs.getString("note") );
		}
		return notes;
	}

	private static PreparedStatement alt_pstmt = null;
	private Collection<String> getAltForms( int id ) throws SQLException {
		if (alt_pstmt == null)
			alt_pstmt = connection.prepareStatement(
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
			rda_pstmt = connection.prepareStatement("SELECT rda FROM rda WHERE heading_id = ?");
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
		public String seeAlsoJson = null;
		public Collection<String> seeJson = null;
	}
}
