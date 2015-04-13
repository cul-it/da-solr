package edu.cornell.library.integration.indexer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
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
	private Collection<Integer> authorTypes = new HashSet<Integer>();
	private ReferenceType[] referenceTypes = ReferenceType.values(); 
	private HeadTypeDesc[] HeadTypeDescs = HeadTypeDesc.values();
	static final ObjectMapper mapper = new ObjectMapper();
	SolrServer solr = null;

	public static void main(String[] args) {
		try {
			new Headings2Solr(args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public Headings2Solr(String[] args) throws Exception {
		Collection<String> requiredArgs = new HashSet<String>();
		requiredArgs.add("xmlDir");
		requiredArgs.add("blacklightSolrUrl");
		requiredArgs.add("solrUrl");
	            
		config = SolrBuildConfig.loadConfig(args,requiredArgs);	
		solr = new HttpSolrServer(config.getSolrUrl());

		authorTypes.add(HeadTypeDesc.PERSNAME.ordinal());
		authorTypes.add(HeadTypeDesc.CORPNAME.ordinal());
		authorTypes.add(HeadTypeDesc.EVENT.ordinal());

		connection = config.getDatabaseConnection("Headings");
		findWorks(HeadType.AUTHOR);

	}
	
	private void findWorks(HeadType ht) throws Exception  {
		String query =
			"SELECT h.* "
			+ "FROM heading as h"

			+ " LEFT JOIN (reference AS r2, heading AS h2)"
			+ "  ON (r2.from_heading = h.id"
			+ "    AND h2.id = r2.to_heading )"

			+ " WHERE h."+ht.dbField()+" > 0"
			+ "    OR h2."+ht.dbField()+" > 0"
			+ " GROUP BY h.id";
		Statement stmt = connection.createStatement();
		stmt.execute(query);
		ResultSet rs = stmt.getResultSet();
//		int docCount = 0;
		Collection<SolrInputDocument> docs = new HashSet<SolrInputDocument>();
		while (rs.next()) {
			int id = rs.getInt("id");
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField("id", id);
			doc.addField("heading", rs.getString("heading"));
			doc.addField("headingSort", rs.getString("sort"));
			Collection<Reference> xrefs = getXRefs(id);
			// preferedForm && seeAlso xrefs
			for (Reference r : xrefs) doc.addField(r.type.toString(), r.json);
			for (String alt : getAltForms(id)) doc.addField("alternateForm", alt);
			doc.addField("headingTypeDesc", HeadTypeDescs[  rs.getInt("type_desc") ]);
			doc.addField("authority", rs.getBoolean("authority"));
			doc.addField("mainEntry", rs.getBoolean("main_entry"));
			for (String note : getNotes(id)) doc.addField("notes", note);
			String rda = getRda(id);
			if (rda != null) doc.addField("rda_json", rda);
			doc.addField("count",rs.getInt(ht.dbField()));
			doc.addField("counts_json", countsJson(rs));
	//		System.out.println( IndexingUtilities.prettyXMLFormat( ClientUtils.toXML( doc ) ) );
	//		if ( ++docCount == 20 )System.exit(0);
			docs.add(doc);
			if (docs.size() == 1000) {
				System.out.printf("%d: %s (%s)\n", rs.getInt("id"),rs.getString("heading"),rs.getString("sort"));
				solr.add(docs);
				solr.commit();
				docs.clear();
			}
		}
		solr.add(docs);
		solr.commit();
	}
	
	private PreparedStatement ref_pstmt = null;
	private Collection<Reference> getXRefs(int id) throws SQLException, JsonProcessingException {
		Collection<Reference> refs = new HashSet<Reference>();
		if (ref_pstmt == null)
			ref_pstmt = connection.prepareStatement(
					" SELECT r.ref_type, h.* "
					+ " FROM reference AS r, heading AS h "
					+ "WHERE r.from_heading = ? "
					+ "  AND r.to_heading = h.id"	);
		ref_pstmt.setInt(1, id);
		ref_pstmt.execute();
		ResultSet rs = ref_pstmt.getResultSet();
		while (rs.next()) {
			Map<String,Object> vals = new HashMap<String,Object>();
			vals.put("worksAbout", rs.getInt("works_about"));
			vals.put("heading", rs.getString("heading"));
			if (authorTypes.contains(rs.getInt("type_desc")))
				vals.put("worksBy", rs.getInt("works_by"));
			vals.put("headingTypeDesc", HeadTypeDescs[  rs.getInt("type_desc") ].toString());
			Reference r = new Reference(referenceTypes[ rs.getInt("ref_type") ]);
			r.json = mapper.writeValueAsString(vals);
			refs.add(r);
		}
		return refs;
	}

	private String countsJson( ResultSet rs ) throws SQLException {
		if (authorTypes.contains(rs.getInt("type_desc")))
			return String.format("{\"worksBy\":\"%d\",\"worksAbout\":\"%d\"}",
					rs.getInt("works_by"),rs.getInt("works_about"));
		return String.format("{\"worksAbout\":\"%d\"}",rs.getInt("works_about"));
	}
	
	private PreparedStatement note_pstmt = null;
	private Collection<String> getNotes( int id ) throws SQLException {
		if (note_pstmt == null)
			note_pstmt = connection.prepareStatement("SELECT note FROM note WHERE heading_id = ?");
		note_pstmt.setInt(1, id);
		note_pstmt.execute();
		ResultSet rs = note_pstmt.getResultSet();
		Collection<String> notes = new HashSet<String>();
		while (rs.next())
			notes.add( rs.getString("note") );
		return notes;
	}

	private PreparedStatement alt_pstmt = null;
	private Collection<String> getAltForms( int id ) throws SQLException {
		if (alt_pstmt == null)
			alt_pstmt = connection.prepareStatement("SELECT form FROM alt_form WHERE heading_id = ?");
		alt_pstmt.setInt(1, id);
		alt_pstmt.execute();
		ResultSet rs = alt_pstmt.getResultSet();
		Collection<String> forms = new HashSet<String>();
		while (rs.next())
			forms.add( rs.getString("form") );
		return forms;
	}

	private PreparedStatement rda_pstmt = null;
	private String getRda( int id ) throws SQLException {
		if (rda_pstmt == null)
			rda_pstmt = connection.prepareStatement("SELECT rda FROM rda WHERE heading_id = ?");
		rda_pstmt.setInt(1, id);
		rda_pstmt.execute();
		ResultSet rs = rda_pstmt.getResultSet();
		while (rs.next())
			return rs.getString("rda");
		return null;
	}
	
	private class Reference {
		ReferenceType type = null;
		String json = null;
		
		public Reference(ReferenceType refType) {
			type = refType;
		}
	}
}
