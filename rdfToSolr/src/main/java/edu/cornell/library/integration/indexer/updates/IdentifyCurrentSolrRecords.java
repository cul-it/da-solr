package edu.cornell.library.integration.indexer.updates;

import static edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig.getRequiredArgsForDB;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.identifyOnlineServices;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
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
	private Map<String,PreparedStatement> pstmts = new HashMap<String,PreparedStatement>();
	private final SimpleDateFormat marcDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

	public static void main(String[] args)  {

		List<String> requiredArgs = new ArrayList<String>();
		requiredArgs.addAll(getRequiredArgsForDB("Current"));
		requiredArgs.add("solrUrl");

		try{        
			new IdentifyCurrentSolrRecords( SolrBuildConfig.loadConfig(args, requiredArgs));
		}catch( Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}

	public IdentifyCurrentSolrRecords(SolrBuildConfig config) throws Exception {

	    current = config.getDatabaseConnection("Current");
	    current.setAutoCommit(false);
	    setUpTables();

	    int fetchsize = 1000;
	    long offset = 0;

	    HttpSolrServer solr = new HttpSolrServer(config.getSolrUrl());
	    SolrQuery query = new SolrQuery();
	    query.setQuery("id:*");
	    query.setFields("bibid_display","online","location_facet","url_access_display",
	    		"format","title_display","title_vern_display","title_uniform_display",
	    		"language_facet","edition_display","pub_date_display","timestamp",
	    		"type","other_id_display","holdings_display","item_display");
	    query.set("defType", "lucene");

	    while (offset < fetchsize) {
	    	query.setStart((int) offset);
		    query.setRows(fetchsize);
		    QueryResponse response = solr.query(query);
		    for (SolrDocument doc : response.getResults()) {
				int bibid = processSolrBibData(
						(ArrayList) doc.getFieldValue("bibid_display"),
						(ArrayList) doc.getFieldValue("location_facet"),
						(ArrayList) doc.getFieldValue("url_access_display"),
						(ArrayList) doc.getFieldValue("format"),
						(String) doc.getFieldValue("title_display"),
						(String) doc.getFieldValue("title_vern_display"),
						(ArrayList) doc.getFieldValue("title_uniform_display"),
						(ArrayList) doc.getFieldValue("language_facet"),
						(ArrayList) doc.getFieldValue("edition_display"),
						(ArrayList) doc.getFieldValue("pub_date_display"),
						(Date) doc.getFieldValue("timestamp"));
				if (bibCount % 10_000 == 0)
					current.commit();
				
				if (((String) doc.getFieldValue("type")).equals("Catalog"))
					processWorksData((ArrayList) doc.getFieldValue("other_id_display"),bibid);
	
				processSolrHoldingsData((ArrayList) doc.getFieldValue("holdings_display"),bibid);
	
				processSolrItemData((ArrayList) doc.getFieldValue("item_display"),bibid);	    	
		    }
	    }
		for (PreparedStatement pstmt : pstmts.values()) {
			pstmt.executeBatch();
			pstmt.close();
		}
		reactivateDBKeys();
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
				+ "needs_update int(1) default 0, "
				+ "key (bib_id) ) ENGINE=InnoDB");
		stmt.execute("alter table "+CurrentDBTable.BIB_SOLR.toString()+" disable keys");

		stmt.execute("drop table if exists "+CurrentDBTable.MFHD_SOLR.toString());
		stmt.execute("create table "+CurrentDBTable.MFHD_SOLR.toString()+" ( "
				+ "bib_id int(10) unsigned not null, "
				+ "mfhd_id int(10) unsigned not null, "
				+ "record_date timestamp null, "
				+ "active int(1) default 1, "
				+ "key (mfhd_id), "
				+ "key (bib_id) ) ENGINE=InnoDB");
		stmt.execute("alter table "+CurrentDBTable.MFHD_SOLR.toString()+" disable keys");

		stmt.execute("drop table if exists "+CurrentDBTable.ITEM_SOLR.toString());
		stmt.execute("create table "+CurrentDBTable.ITEM_SOLR.toString()+" ( "
				+ "mfhd_id int(10) unsigned not null, "
				+ "item_id int(10) unsigned not null, "
				+ "record_date timestamp null, "
				+ "active int(1) default 1, "
				+ "key (item_id), "
				+ "key (mfhd_id) ) ENGINE=InnoDB");
		stmt.execute("alter table "+CurrentDBTable.ITEM_SOLR.toString()+" disable keys");

		stmt.execute("drop table if exists "+CurrentDBTable.BIB2WORK.toString());
		stmt.execute("create table "+CurrentDBTable.BIB2WORK.toString()+" ( "
				+ "bib_id int(10) unsigned not null, "
				+ "oclc_id int(10) unsigned not null, "
				+ "work_id int(10) unsigned not null, "
				+ "active int(1) default 1, "
				+ "mod_date timestamp not null default current_timestamp, "
				+ "key (work_id), key (bib_id) ) ENGINE=InnoDB");
		stmt.execute("alter table "+CurrentDBTable.BIB2WORK.toString()+" disable keys");
		current.commit();

	}

	private void reactivateDBKeys() throws SQLException {
		Statement stmt = current.createStatement();
		stmt.execute("alter table "+CurrentDBTable.BIB_SOLR.toString()+" enable keys");
		stmt.execute("alter table "+CurrentDBTable.MFHD_SOLR.toString()+" enable keys");
		stmt.execute("alter table "+CurrentDBTable.ITEM_SOLR.toString()+" enable keys");
		stmt.execute("alter table "+CurrentDBTable.BIB2WORK.toString()+" enable keys");
		current.commit();
	}

	private int processSolrBibData(
			ArrayList<Object> solrBib,
			ArrayList<Object>  location_facet,
			ArrayList<Object>  url,
			ArrayList<Object>  format,
			String title_display,
			String title_vern_display,
			ArrayList<Object>  title_uniform_display,
			ArrayList<Object>  language,
			ArrayList<Object>  edition,
			ArrayList<Object>  pubdate,
			Date  timestamp
			) throws SQLException, ParseException {
		bibCount++;
		String[] parts = ((String)solrBib.get(0)).split("\\|", 2);
		int bibid = Integer.valueOf(parts[0]);
		if ( ! pstmts.containsKey("bib_insert"))
			pstmts.put("bib_insert",current.prepareStatement(
					"INSERT INTO "+CurrentDBTable.BIB_SOLR.toString()+
					" (bib_id, record_date, format, sites,libraries,index_date,edition,pub_date,language,title) "
							+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));
		String sites = (url == null) ? null : identifyOnlineServices(url);
		if ( url != null && ! url.isEmpty() )
			if (sites == null)
				sites = "Online";
		String libraries = eliminateDuplicateLocations(location_facet);
		String title = null;
		if (title_uniform_display != null && ! title_uniform_display.isEmpty()) {
			String first = (String) title_uniform_display.get(0);
			int pipePos = first.indexOf('|');
			if (pipePos == -1)
				title = first;
			else
				title = first.substring(0, pipePos);
		}
		if (title == null)
			if (title_vern_display != null && ! title_vern_display.isEmpty())
				title = title_vern_display;
		if (title == null)
			title = title_display;

		// Attempt to reflect Solr date in table. If fails, record not in Voyager.
		// note: date comparison comes later.
		PreparedStatement pstmt = pstmts.get("bib_insert");
		try {
		pstmt.setInt(1, bibid);
		pstmt.setTimestamp(2, new Timestamp( marcDateFormat.parse(parts[1]).getTime() ));
		pstmt.setString(3, StringUtils.join(format, ','));
		pstmt.setString(4, sites);
		pstmt.setString(5, libraries);
		pstmt.setTimestamp(6, new Timestamp( timestamp.getTime() ));
		pstmt.setString(7, edition != null && edition.isEmpty()
				? null : (String) edition.get(0));
		pstmt.setString(8, pubdate != null && pubdate.isEmpty()
				? null : StringUtils.join(pubdate, ", "));
		pstmt.setString(9, language != null && language.isEmpty()
				? null : StringUtils.join(language, ", "));
		pstmt.setString(10, title);
		pstmt.addBatch();
		} catch (IllegalArgumentException | ParseException e) {
			StringBuilder sb = new StringBuilder();
			sb.append("solrBib ").append(solrBib);
			sb.append("\nlocation_facet ").append(location_facet);
			sb.append("\nurl ").append(url);
			sb.append("\nformat ").append(format);
			sb.append("\ntitle_display ").append(title_display);
			sb.append("\ntitle_vern_display ").append(title_vern_display);
			sb.append("\ntitle_uniform_display ").append(title_uniform_display);
			sb.append("\nlanguage ").append(language);
			sb.append("\nedition ").append(edition);
			sb.append("\npubdate ").append(pubdate);
			sb.append("\ntimestamp ").append(timestamp);
			System.out.println(sb.toString());
			throw e;
		}
		if (++bibCount % 1000 == 0)
			pstmt.executeBatch();
		return bibid;
	}

	private String eliminateDuplicateLocations(ArrayList<Object> location_facet) {
		if (location_facet == null) return "";
		StringBuilder sb = new StringBuilder();
		Collection<Object> foundValues = new HashSet<Object>();
		boolean first = true;
		for (Object val : location_facet) {
			if (foundValues.contains(val))
				continue;
			foundValues.add(val);
			if (first)
				first = false;
			else
				sb.append(", ");
			sb.append(val.toString());
		}
		return sb.toString();
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
		PreparedStatement insertStmt = current.prepareStatement(
				"INSERT INTO "+CurrentDBTable.BIB2WORK.toString()+
				" (bib_id, oclc_id, work_id) VALUES (?, ?, ?)");
		for (int oclcId : oclcIds) {
			pstmt.setInt(1, oclcId);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				long workid = rs.getLong(1);
				insertStmt.setInt(1, bibid);
				insertStmt.setInt(2, oclcId);
				insertStmt.setLong(3, workid);
				insertStmt.addBatch();
			}
		}
		insertStmt.executeBatch();
		insertStmt.close();
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