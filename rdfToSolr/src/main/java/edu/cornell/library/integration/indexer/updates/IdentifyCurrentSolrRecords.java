package edu.cornell.library.integration.indexer.updates;

import static edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig.getRequiredArgsForDB;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * Pull lists of current (unsuppressed) bib, holding, and item records along with
 * their modified dates to populate in a set of dated database tables. The contents
 * of these tables can then be compared with the contents of the Solr index.
 */
public class IdentifyCurrentSolrRecords {
	
	private SolrBuildConfig config;
	private Connection current = null;
	private int bibCount = 0;
	private int mfhdCount = 0;
	private int itemCount = 0;
	private String bibTable = null;
	private String mfhdTable = null;
	private String itemTable = null;
	private Map<String,PreparedStatement> pstmts = new HashMap<String,PreparedStatement>();
	private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");


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
	    this.config = config;

	    current = config.getDatabaseConnection("Current");
	    current.setAutoCommit(false);
	    setUpTables();

	    URL queryUrl = new URL(config.getSolrUrl() +
				"/select?qt=standard&q=id%3A*&wt=csv&fl=bibid_display,holdings_display,item_display&rows=50000000");

	    // Save Solr data to a temporary file
	    final Path tempPath = Files.createTempFile("identifyCurrentSolrRecords-", ".csv");
		tempPath.toFile().deleteOnExit();
		FileOutputStream fos = new FileOutputStream(tempPath.toString());
		ReadableByteChannel rbc = Channels.newChannel(queryUrl.openStream());
		fos.getChannel().transferFrom(rbc, 0, Integer.MAX_VALUE); //Integer.MAX_VALUE translates to 2 gigs max download
		fos.close();

		// Then read the file back in to process it
		BufferedReader reader = Files.newBufferedReader(tempPath , StandardCharsets.UTF_8);
		String line = null;
		while ((line = reader.readLine()) != null ) {
			if (line.startsWith("bibid_display")) continue;
			
			int holdings_index = line.indexOf(',');
			int bibid = processSolrBibData(line.substring(0, holdings_index));
			if (holdings_index + 1 == line.length()) continue;
			
			int item_index;
			if (line.charAt(holdings_index + 1) == '"') {
				item_index = line.indexOf('"', holdings_index+2)+1;
				processSolrHoldingsData(line.substring(holdings_index+2, item_index-1), bibid);
			} else {
				item_index = line.indexOf(',',holdings_index+1);
				processSolrHoldingsData(line.substring(holdings_index+1, item_index), bibid);
			}
			if (item_index + 1 == line.length()) continue;
			if (line.charAt(item_index + 1) == '"') {
				processSolrItemData(line.substring(item_index + 2, line.length()-1),bibid);
			} else {
				processSolrItemData(line.substring(item_index + 1),bibid);
			}
			if (bibCount % 100_000 == 0)
				current.commit();
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
		String solrUrl = config.getSolrUrl();
		String solrIndexName = solrUrl.substring(solrUrl.lastIndexOf('/')+1);
		bibTable = "bib_solr_"+solrIndexName;
		mfhdTable = "mfhd_solr_"+solrIndexName;
		itemTable = "item_solr_"+solrIndexName;
		
		stmt.execute("drop table if exists "+bibTable);
		stmt.execute("create table "+bibTable+" ( "
				+ "bib_id int(10) unsigned not null, "
				+ "solr_date timestamp null, "
				+ "key (bib_id) ) ENGINE=InnoDB");
		stmt.execute("alter table "+bibTable+" disable keys");

		stmt.execute("drop table if exists "+mfhdTable);
		stmt.execute("create table "+mfhdTable+" ( "
				+ "bib_id int(10) unsigned not null, "
				+ "mfhd_id int(10) unsigned not null, "
				+ "solr_date timestamp null, "
				+ "key (mfhd_id) ) ENGINE=InnoDB");
		stmt.execute("alter table "+mfhdTable+" disable keys");

		stmt.execute("drop table if exists "+itemTable);
		stmt.execute("create table "+itemTable+" ( "
				+ "mfhd_id int(10) unsigned not null, "
				+ "item_id int(10) unsigned not null, "
				+ "solr_date timestamp null, "
				+ "key (item_id) ) ENGINE=InnoDB");
		stmt.execute("alter table "+itemTable+" disable keys");
		current.commit();

	}

	private void reactivateDBKeys() throws SQLException {
		Statement stmt = current.createStatement();
		stmt.execute("alter table "+bibTable+" enable keys");
		stmt.execute("alter table "+mfhdTable+" enable keys");
		stmt.execute("alter table "+itemTable+" enable keys");
		current.commit();
	}

	private int processSolrBibData(String solrBib) throws SQLException, ParseException {
		bibCount++;
		String[] parts = solrBib.split("\\|", 2);
		int bibid = Integer.valueOf(parts[0]);
		if ( ! pstmts.containsKey("bib_insert"))
			pstmts.put("bib_insert",current.prepareStatement(
					"INSERT INTO "+bibTable+" (bib_id, solr_date) VALUES (?, ?)"));
		
		// Attempt to reflect Solr date in table. If fails, record not in Voyager.
		// note: date comparison comes later.
		PreparedStatement pstmt = pstmts.get("bib_insert");
		pstmt.setInt(1, bibid);
		pstmt.setTimestamp(2, new Timestamp( dateFormat.parse(parts[1]).getTime() ));
		pstmt.addBatch();
		if (++bibCount % 1000 == 0)
			pstmt.executeBatch();
		return bibid;
	}

	private void processSolrHoldingsData(String solrHoldings, int bibid) throws SQLException, ParseException {
		String[] solrHoldingsList = solrHoldings.split(",");
		if ( ! pstmts.containsKey("mfhd_insert"))
			pstmts.put("mfhd_insert",current.prepareStatement(
					"INSERT INTO "+mfhdTable+" (bib_id, mfhd_id, solr_date) VALUES (?, ?, ?)"));
		PreparedStatement pstmt = pstmts.get("mfhd_insert");
		for (int i = 0; i < solrHoldingsList.length; i++) {
			int holdingsId;
			Timestamp modified = null;
			if (solrHoldingsList[i].contains("|")) {
				String[] parts = solrHoldingsList[i].split("\\|",2);
				holdingsId = Integer.valueOf(parts[0]);
				modified = new Timestamp( dateFormat.parse(parts[1]).getTime() );
			} else if (! solrHoldingsList[i].isEmpty()) {
				holdingsId = Integer.valueOf(solrHoldingsList[i]);
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

	private void processSolrItemData(String solrItems, int bibid) throws SQLException, ParseException {
		String[] solrItemList = solrItems.split(",");
		if ( ! pstmts.containsKey("item_insert"))
			pstmts.put("item_insert", current.prepareStatement(
					"INSERT INTO "+itemTable+" (mfhd_id, item_id, solr_date) VALUES (?, ?, ?)"));
		PreparedStatement pstmt = pstmts.get("item_insert");
		for (int i = 0; i < solrItemList.length; i++) {
			itemCount++;
			Timestamp modified = null;
			String[] parts = solrItemList[i].split("\\|");
			int itemId = Integer.valueOf(parts[0]);
			int mfhdId = Integer.valueOf(parts[1]);
			if (parts.length > 2)
				modified = new Timestamp( dateFormat.parse(parts[2]).getTime() );
			pstmt.setInt(1, mfhdId);
			pstmt.setInt(2, itemId);
			pstmt.setTimestamp(3, modified);
			pstmt.addBatch();
			if (++itemCount % 1000 == 0)
				pstmt.executeBatch();
		}
	}		

	
}