package edu.cornell.library.integration.indexer.updates;

import static edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig.getRequiredArgsForDB;
import static edu.cornell.library.integration.utilities.IndexingUtilities.marcDateFormat;
import static edu.cornell.library.integration.utilities.IndexingUtilities.pullReferenceFields;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.utilities.IndexingUtilities.TitleMatchReference;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;

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
	private Map<String,PreparedStatement> pstmts = new HashMap<String,PreparedStatement>();

	public static void main(String[] args)  {

		List<String> requiredArgs = getRequiredArgsForDB("Current");
		requiredArgs.add("solrUrl");

		try{        
			new IdentifyCurrentSolrRecords( SolrBuildConfig.loadConfig(args, requiredArgs));
		}catch( Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}

	@SuppressWarnings("unchecked")
	public IdentifyCurrentSolrRecords(SolrBuildConfig config) throws Exception {

	    current = config.getDatabaseConnection("Current");
	    current.setAutoCommit(false);
	    setUpTables();

	    int fetchsize = 5000;
	    long offset = 0;

	    HttpSolrServer solr = new HttpSolrServer(config.getSolrUrl());
	    SolrQuery query = new SolrQuery();
	    query.setQuery("id:*");
	    query.setFields("bibid_display","online","location_facet","url_access_display",
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
	
		    	processSolrItemData((ArrayList<Object>) doc.getFieldValue("item_display"),bibid);	    	
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

	private void setUpTables() throws SQLException {
		Statement stmt = current.createStatement();
		
		stmt.execute("drop table if exists "+CurrentDBTable.BIB_SOLR.toString());
		stmt.execute("create table "+CurrentDBTable.BIB_SOLR.toString()+" ( "
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
				+ ") ENGINE=InnoDB");

		stmt.execute("drop table if exists "+CurrentDBTable.MFHD_SOLR.toString());
		stmt.execute("create table "+CurrentDBTable.MFHD_SOLR.toString()+" ( "
				+ "bib_id int(10) unsigned not null, "
				+ "mfhd_id int(10) unsigned not null, "
				+ "record_date timestamp null, "
				+ "active int(1) default 1 "
				+ ") ENGINE=InnoDB");

		stmt.execute("drop table if exists "+CurrentDBTable.ITEM_SOLR.toString());
		stmt.execute("create table "+CurrentDBTable.ITEM_SOLR.toString()+" ( "
				+ "mfhd_id int(10) unsigned not null, "
				+ "item_id int(10) unsigned not null, "
				+ "record_date timestamp null, "
				+ "active int(1) default 1 "
				+ ") ENGINE=InnoDB");

		stmt.execute("drop table if exists "+CurrentDBTable.BIB2WORK.toString());
		stmt.execute("create table "+CurrentDBTable.BIB2WORK.toString()+" ( "
				+ "bib_id int(10) unsigned not null, "
				+ "oclc_id int(10) unsigned not null, "
				+ "work_id int(10) unsigned not null, "
				+ "active int(1) default 1, "
				+ "mod_date timestamp not null default current_timestamp "
				+ ") ENGINE=InnoDB");
		current.commit();

	}

	private void makeDBKeys() throws SQLException {
		Statement stmt = current.createStatement();
		stmt.execute("alter table "+CurrentDBTable.BIB_SOLR.toString()+
				" add primary key (bib_id)");
		stmt.execute("alter table "+CurrentDBTable.MFHD_SOLR.toString()+
				" add key (bib_id), add key (mfhd_id)");
		stmt.execute("alter table "+CurrentDBTable.ITEM_SOLR.toString()+
				" add key (item_id), add key (mfhd_id)");
		stmt.execute("alter table "+CurrentDBTable.BIB2WORK.toString()+
				" add key (work_id), add key (bib_id)");
		current.commit();
	}

	private int processSolrBibData(SolrDocument doc) throws SQLException, ParseException {
		bibCount++;
		TitleMatchReference ref = pullReferenceFields(doc);

		if ( ! pstmts.containsKey("bib_insert"))
			pstmts.put("bib_insert",current.prepareStatement(
					"INSERT INTO "+CurrentDBTable.BIB_SOLR.toString()+
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

		Set<Integer> oclcIds = new HashSet<Integer>();
		for (Object obj : ids) {
			String id = (String) obj;
			if (id.startsWith("(OCoLC)")) {
				try {
					oclcIds.add(Integer.valueOf(id.substring(7)));
				} catch (NumberFormatException e) {
					// Ignore the value if it's invalid
				}
			}
		}

		if (oclcIds.isEmpty())
			return;
		
		PreparedStatement pstmt = current.prepareStatement(
				"SELECT work_id FROM workids.work2oclc WHERE oclc_id = ?");
		if (! pstmts.containsKey("work_insert"))
			pstmts.put("work_insert", current.prepareStatement(
				"INSERT INTO "+CurrentDBTable.BIB2WORK.toString()+
				" (bib_id, oclc_id, work_id) VALUES (?, ?, ?)"));
		PreparedStatement insertStmt = pstmts.get("work_insert");
		for (int oclcId : oclcIds) {
			pstmt.setInt(1, oclcId);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				long workid = rs.getLong(1);
				insertStmt.setInt(1, bibid);
				insertStmt.setInt(2, oclcId);
				insertStmt.setLong(3, workid);
				insertStmt.addBatch();
				workCount++;
			}
		}
		if (workCount % 1000 == 0)
			insertStmt.executeBatch();
		pstmt.close();
	}

	private void processSolrHoldingsData(ArrayList<Object> solrHoldings, int bibid) throws SQLException, ParseException {

		if (solrHoldings == null)
			return;

		if ( ! pstmts.containsKey("mfhd_insert"))
			pstmts.put("mfhd_insert",current.prepareStatement(
					"INSERT INTO "+CurrentDBTable.MFHD_SOLR.toString()+
					" (bib_id, mfhd_id, record_date) VALUES (?, ?, ?)"));
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

	private void processSolrItemData(ArrayList<Object> solrItems, int bibid) throws SQLException, ParseException {

		if (solrItems == null)
			return;

		if ( ! pstmts.containsKey("item_insert"))
			pstmts.put("item_insert", current.prepareStatement(
					"INSERT INTO "+CurrentDBTable.ITEM_SOLR.toString()+
					" (mfhd_id, item_id, record_date) VALUES (?, ?, ?)"));
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