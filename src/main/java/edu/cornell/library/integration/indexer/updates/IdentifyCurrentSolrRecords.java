package edu.cornell.library.integration.indexer.updates;

import static edu.cornell.library.integration.indexer.utilities.Config.getRequiredArgsForDB;
import static edu.cornell.library.integration.marc.MarcRecord.MARC_DATE_FORMAT;
import static edu.cornell.library.integration.utilities.IndexingUtilities.pullReferenceFields;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.utilities.IndexingUtilities.TitleMatchReference;

/**
 * Pull lists of current (unsuppressed) bib, holding, and item records along with
 * their modified dates to populate in a set of dated database tables. The contents
 * of these tables can then be compared with the contents of the Solr index.
 */
public class IdentifyCurrentSolrRecords {

	private Connection current = null;
	private int bibCount = 0;
	private int mfhdCount = 0;
	private int itemCount = 0;
	private int workCount = 0;
	private Map<String,PreparedStatement> pstmts = new HashMap<>();
	private SimpleDateFormat marcDateFormat;

	public static void main(String[] args)  {

		List<String> requiredArgs = getRequiredArgsForDB("Current");
		requiredArgs.add("solrUrl");

		try{        
			new IdentifyCurrentSolrRecords( Config.loadConfig(requiredArgs));
		}catch( Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}

	@SuppressWarnings("unchecked")
	public IdentifyCurrentSolrRecords(Config config) throws Exception {

		marcDateFormat = new SimpleDateFormat( MARC_DATE_FORMAT );
	    current = config.getDatabaseConnection("Current");
	    current.setAutoCommit(false);
	    setUpTables();

	    int fetchsize = 5000;
	    long offset = 0;

	    try ( HttpSolrClient solr = new HttpSolrClient(config.getSolrUrl()) ){
	    SolrQuery query = new SolrQuery();
	    query.setQuery("id:*");
	    query.setFields("bibid_display","online","location_facet","url_access_json",
	    		"format","title_display","title_vern_display","title_uniform_display",
	    		"language_facet","edition_display","pub_date_display","timestamp",
	    		"type","other_id_display","holdings_display","item_display");
	    query.set("defType", "lucene");
	    query.setRows(0);
	    Long totalRecords = solr.query(query).getResults().getNumFound();
	    query.setRows(fetchsize);

	    while (offset < totalRecords) {
	    	query.setStart((int) offset);
		    for (SolrDocument doc : solr.query(query).getResults()) {
		    	int bibid = processSolrBibData(doc);

		    	if (((String) doc.getFieldValue("type")).equals("Catalog"))
		    		processWorksData((ArrayList<Object>) doc.getFieldValue("other_id_display"),bibid);
	
		    	processSolrHoldingsData((ArrayList<Object>) doc.getFieldValue("holdings_display"),bibid);
	
		    	processSolrItemData((ArrayList<Object>) doc.getFieldValue("item_display"));	    	
		    }
			offset += fetchsize;
			current.commit();
	    }
		for (PreparedStatement pstmt : pstmts.values()) {
			pstmt.executeBatch();
			pstmt.close();
		}
		makeDBKeys();
		current.commit();
	    }
	}

	private void setUpTables() throws SQLException {
		try ( Statement stmt = current.createStatement() ){
		
		stmt.execute("drop table if exists bibRecsSolr");
		stmt.execute("create table bibRecsSolr ( "
				+ "bib_id int(10) unsigned not null, "
				+ "record_date timestamp null, "
				+ "active int(1) default 1, "
				+ "index_date timestamp default now(), "
				+ "format varchar(256), "
				+ "sites text, "
				+ "libraries text, "
				+ "edition text, "
				+ "pub_date text, "
				+ "language text, "
				+ "title text, "
				+ "linking_mod_date timestamp, "
				+ "solr_document longtext)");

		stmt.execute("drop table if exists mfhdRecsSolr");
		stmt.execute("create table mfhdRecsSolr ( "
				+ "bib_id int(10) unsigned not null, "
				+ "mfhd_id int(10) unsigned not null, "
				+ "record_date timestamp null, "
				+ "active int(1) default 1)");

		stmt.execute("drop table if exists itemRecsSolr");
		stmt.execute("create table itemRecsSolr ( "
				+ "mfhd_id int(10) unsigned not null, "
				+ "item_id int(10) unsigned not null, "
				+ "record_date timestamp null, "
				+ "active int(1) default 1)");

		stmt.execute("drop table if exists bib2work");
		stmt.execute("create table bib2work ( "
				+ "bib_id int(10) unsigned not null, "
				+ "oclc_id int(10) unsigned not null, "
				+ "work_id int(10) unsigned not null, "
				+ "active int(1) default 1, "
				+ "mod_date timestamp not null default current_timestamp)");
		}
		current.commit();

	}

	private void makeDBKeys() throws SQLException {
		try ( Statement stmt = current.createStatement() ) {
		stmt.execute("alter table bibRecsSolr  add primary key (bib_id),  add key (index_date)");
		stmt.execute("alter table mfhdRecsSolr add         key (bib_id),  add key (mfhd_id)");
		stmt.execute("alter table itemRecsSolr add         key (item_id), add key (mfhd_id)");
		stmt.execute("alter table bib2work     add         key (work_id), add key (bib_id)");
		}
		current.commit();
	}

	@SuppressWarnings("resource") // PreparedStatement being left open
	private int processSolrBibData(SolrDocument doc) throws SQLException, ParseException {
		bibCount++;
		TitleMatchReference ref = pullReferenceFields(doc);

		if ( ! pstmts.containsKey("bib_insert"))
			pstmts.put("bib_insert",current.prepareStatement(
					"INSERT INTO bibRecsSolr"+
					" (bib_id, record_date, format, sites,libraries,index_date,edition,pub_date,language,title) "
							+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));

		// Attempt to reflect Solr date in table. If fails, record not in Voyager.
		// note: date comparison comes later.
		PreparedStatement pstmt = pstmts.get("bib_insert");
		pstmt.setInt(1, ref.id);
		pstmt.setTimestamp(2, ref.timestamp);
		pstmt.setString(3, ref.format);
		pstmt.setString(4, ref.sites);
		pstmt.setString(5, ref.libraries);
		pstmt.setTimestamp(6, new Timestamp( ((Date) doc.getFieldValue("timestamp")).getTime() ));
		pstmt.setString(7, ref.edition);
		pstmt.setString(8, ref.pub_date);
		pstmt.setString(9, ref.language);
		pstmt.setString(10, ref.title);
		pstmt.addBatch();
		if (++bibCount % 1000 == 0)
			pstmt.executeBatch();
		return ref.id;
	}

	private void processWorksData(ArrayList<Object> ids, int bibid) throws SQLException {

		if (ids == null)
			return;

		Set<Integer> oclcIds = new HashSet<>();
		for (Object obj : ids) {
			String id = (String) obj;
			if (id.startsWith("(OCoLC)")) {
				try {
					oclcIds.add(Integer.valueOf(id.substring(7)));
				} catch (@SuppressWarnings("unused") NumberFormatException e) {
					// Ignore the value if it's invalid
				}
			}
		}

		if (oclcIds.isEmpty())
			return;
		
		try ( PreparedStatement pstmt = current.prepareStatement(
				"SELECT work_id FROM workids.work2oclc WHERE oclc_id = ?") ){
		if (! pstmts.containsKey("work_insert"))
			pstmts.put("work_insert", current.prepareStatement(
				"INSERT INTO bib2work (bib_id, oclc_id, work_id) VALUES (?, ?, ?)"));
		@SuppressWarnings("resource") // PreparedStatement being left open
		PreparedStatement insertStmt = pstmts.get("work_insert");
		for (int oclcId : oclcIds) {
			pstmt.setInt(1, oclcId);
			try ( ResultSet rs = pstmt.executeQuery() ){
				while (rs.next()) {
					long workid = rs.getLong(1);
					insertStmt.setInt(1, bibid);
					insertStmt.setInt(2, oclcId);
					insertStmt.setLong(3, workid);
					insertStmt.addBatch();
					workCount++;
				}
			}
		}
		if (workCount % 1000 == 0)
			insertStmt.executeBatch();
		}
	}

	private void processSolrHoldingsData(ArrayList<Object> solrHoldings, int bibid) throws SQLException, ParseException {

		if (solrHoldings == null)
			return;

		if ( ! pstmts.containsKey("mfhd_insert"))
			pstmts.put("mfhd_insert",current.prepareStatement(
					"INSERT INTO mfhdRecsSolr (bib_id, mfhd_id, record_date) VALUES (?, ?, ?)"));
		@SuppressWarnings("resource") // PreparedStatement being left open
		PreparedStatement pstmt = pstmts.get("mfhd_insert");
		for (int i = 0; i < solrHoldings.size(); i++) {
			String holding = (String) solrHoldings.get(i);
			if (holding.isEmpty()) continue;
			int holdingsId;
			Timestamp modified = null;
			if (holding.contains("|")) {
				String[] parts = holding.split("\\|",2);
				holdingsId = Integer.valueOf(parts[0]);
				modified = new Timestamp( marcDateFormat.parse(parts[1]).getTime() );
			} else if (! holding.isEmpty()) {
				holdingsId = Integer.valueOf(holding);
			} else {
				continue;
			}
			mfhdCount++;
			pstmt.setInt(1, bibid);
			pstmt.setInt(2, holdingsId);
			pstmt.setTimestamp(3, modified);
			pstmt.addBatch();
			if (++mfhdCount % 1000 == 0)
				pstmt.executeBatch();
		}
	}

	@SuppressWarnings("resource") // PreparedStatement being left open
	private void processSolrItemData(ArrayList<Object> solrItems) throws SQLException, ParseException {

		if (solrItems == null)
			return;

		if ( ! pstmts.containsKey("item_insert"))
			pstmts.put("item_insert", current.prepareStatement(
					"INSERT INTO itemRecsSolr (mfhd_id, item_id, record_date) VALUES (?, ?, ?)"));
		PreparedStatement pstmt = pstmts.get("item_insert");
		for (int i = 0; i < solrItems.size(); i++) {
			String item = (String) solrItems.get(i);
			if (item.isEmpty()) continue;
			itemCount++;
			Timestamp modified = null;
			String[] parts = item.split("\\|");
			int itemId = 0, mfhdId = 0;
			try {
				itemId = Integer.valueOf(parts[0]);
				mfhdId = Integer.valueOf(parts[1]);
			} catch (NumberFormatException e) {
				StringBuilder sb = new StringBuilder();
				sb.append("solrItems ").append(solrItems.toString());
				sb.append("\nsolrItems.size ").append(solrItems.size());
				sb.append("\ni ").append(i);
				sb.append("\nsolrItems.get(i) ").append(solrItems.get(i));
				sb.append("\nparts[0] ").append(parts[0]);
				sb.append("\nparts[1] ").append(parts[1]);
				System.out.println(sb.toString());
				throw e;
			}
			if (parts.length > 2)
				modified = new Timestamp( marcDateFormat.parse(parts[2]).getTime() );
			pstmt.setInt(1, mfhdId);
			pstmt.setInt(2, itemId);
			pstmt.setTimestamp(3, modified);
			pstmt.addBatch();
			if (++itemCount % 1000 == 0)
				pstmt.executeBatch();
		}
	}		

	
}