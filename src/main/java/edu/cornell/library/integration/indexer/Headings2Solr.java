package edu.cornell.library.integration.indexer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.ReferenceType;

public class Headings2Solr {

	private Connection connection = null;
	private SolrBuildConfig config;
	private List<Integer> authorTypes = Arrays.asList(HeadTypeDesc.PERSNAME.ordinal(),
			HeadTypeDesc.CORPNAME.ordinal(),HeadTypeDesc.EVENT.ordinal());
	private final HeadTypeDesc[] HeadTypeDescs = HeadTypeDesc.values();
	static final ObjectMapper mapper = new ObjectMapper();

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

		ConcurrentUpdateSolrServer solr =
				new ConcurrentUpdateSolrServer(config.getSubjectSolrUrl(),1000,5);
		findWorks(solr, HeadType.SUBJECT);
		solr.shutdown();
		solr = new ConcurrentUpdateSolrServer(config.getAuthorSolrUrl(),1000,5);
		findWorks(solr, HeadType.AUTHOR);
		solr.shutdown();
		solr = new ConcurrentUpdateSolrServer(config.getAuthorTitleSolrUrl(),1000,5);
		findWorks(solr, HeadType.AUTHORTITLE);
		solr.shutdown();
		connection.close();
	}

	private void findWorks(ConcurrentUpdateSolrServer solr, HeadType ht) throws Exception  {
		String query =
			"SELECT h.* "
			+ "FROM heading as h"

			+ " LEFT JOIN (reference AS r2, heading AS h2)"
			+ "  ON (r2.from_heading = h.id"
			+ "    AND h2.id = r2.to_heading )"

			+ " WHERE h."+ht.dbField()+" > 0"
			+ "    OR h2."+ht.dbField()+" > 0"
			+ " GROUP BY h.id";
		/* This method requires its own connection to the database so it can buffer results
		 * which keeps the connection used tied up and unavailable for other queries
		 */
		Connection connectionFindWorks = config.getDatabaseConnection("Headings");
		Statement stmt = connectionFindWorks.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
				java.sql.ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(Integer.MIN_VALUE);
		stmt.execute(query);
		ResultSet rs = stmt.getResultSet();
		int addsSinceCommit= 0;
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
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
			if ( ! ht.equals(HeadType.AUTHORTITLE))
				doc.addField("headingTypeDesc", HeadTypeDescs[ rs.getInt("type_desc") ]);
			doc.addField("authority", rs.getBoolean("authority"));
			doc.addField("mainEntry", rs.getBoolean("main_entry"));
			doc.addField("notes", getNotes(id));
			String rda = getRda(id);
			if (rda != null) doc.addField("rda_json", rda);
			doc.addField("count",rs.getInt(ht.dbField()));
			doc.addField("counts_json", countsJson(rs));
			docs.add(doc);
			if (docs.size() == 5_000) {
				System.out.printf("%d: %s\n", rs.getInt("id"),rs.getString("heading"));
				solr.add(docs);
				docs.clear();
				addsSinceCommit += 5_000;
				if (addsSinceCommit >= 200_000) {
					solr.commit();
					addsSinceCommit = 0;
				}
			}
		}
		stmt.close();
		if ( ! docs.isEmpty() )
			solr.add(docs);
		solr.blockUntilFinished();
		solr.commit();
		connectionFindWorks.close();
	}
	
	private static PreparedStatement ref_pstmt = null;
	private References getXRefs(int id, HeadType ht) throws SQLException, JsonProcessingException {
		Collection<String> seeRefs = new ArrayList<String>();
		Map<String,Collection<Object>> seeAlsoRefs = new HashMap<String,Collection<Object>>();
		if (ref_pstmt == null)
			ref_pstmt = connection.prepareStatement(
					" SELECT r.ref_type, r.ref_desc, h.* "
					+ " FROM reference AS r, heading AS h "
					+ "WHERE r.from_heading = ? "
					+ "  AND r.to_heading = h.id "
					+ "ORDER BY h.sort"	);
		ref_pstmt.setInt(1, id);
		ref_pstmt.execute();
		ResultSet rs = ref_pstmt.getResultSet();
		while (rs.next()) {
			int count = rs.getInt(ht.dbField());
			if (count == 0) continue;
			Map<String,Object> vals = new HashMap<String,Object>();
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
					Collection<Object> thisRel = new ArrayList<Object>();
					thisRel.add(vals);
					seeAlsoRefs.put(relationship, thisRel);
				}
			}
		}
		rs.close();
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
		ResultSet rs = note_pstmt.getResultSet();
		Collection<String> notes = new ArrayList<String>();
		while (rs.next())
			notes.add( rs.getString("note") );
		rs.close();
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
		ResultSet rs = alt_pstmt.getResultSet();
		Collection<String> forms = new ArrayList<String>();
		while (rs.next())
			forms.add( rs.getString("heading") );
		rs.close();
		return forms;
	}

	private static PreparedStatement rda_pstmt = null;
	private String getRda( int id ) throws SQLException {
		if (rda_pstmt == null)
			rda_pstmt = connection.prepareStatement("SELECT rda FROM rda WHERE heading_id = ?");
		rda_pstmt.setInt(1, id);
		rda_pstmt.execute();
		ResultSet rs = rda_pstmt.getResultSet();
		String rda = null;
		if (rs.next())
			rda = rs.getString("rda");
		rs.close();
		return rda;
	}
	
	private class References {
		public String seeAlsoJson = null;
		public Collection<String> seeJson = null;
	}
}