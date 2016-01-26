package edu.cornell.library.integration.indexer.updates;

import static edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig.getRequiredArgsForDB;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.identifyOnlineServices;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;
import org.apache.commons.lang.StringUtils;

import au.com.bytecode.opencsv.CSVReader;
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

	    URL queryUrl = new URL(config.getSolrUrl() +
				"/select?qt=standard&q=id%3A*&wt=csv&rows=50000000&"
				+ "fl=bibid_display,online,location_facet,url_access_display,format,"
		//                 0          1          2               3             4    
				+ "edition_display,pub_date_display,timestamp,type,other_id_display,"
			    //	       5              6              7      8         9
				+ "holdings_display,item_display");
	    //                10           11

	    // Save Solr data to a temporary file
	    final Path tempPath = Files.createTempFile("identifyCurrentSolrRecords-", ".csv");
		tempPath.toFile().deleteOnExit();
		FileOutputStream fos = new FileOutputStream(tempPath.toString());
		ReadableByteChannel rbc = Channels.newChannel(queryUrl.openStream());
		fos.getChannel().transferFrom(rbc, 0, Integer.MAX_VALUE); //Integer.MAX_VALUE translates to 2 gigs max download
		fos.close();
		
		CSVReader reader = new CSVReader(new FileReader(tempPath.toString()));

		// Then read the file back in to process it

		String[] nextLine = null;
		while ((nextLine = reader.readNext()) != null ) {
			if (nextLine[0].startsWith("bibid")) continue;
			int bibid = processSolrBibData(nextLine[0],nextLine[1],nextLine[2],
					nextLine[3],nextLine[4],nextLine[5],nextLine[6],nextLine[7]);
			if (bibCount % 10_000 == 0)
				current.commit();
			if (nextLine.length == 9) continue;
			
			if (nextLine[8].equals("Catalog"))
				processWorksData( nextLine[9], bibid );
			if (nextLine.length == 10) continue;

			processSolrHoldingsData(nextLine[10],bibid);
			if (nextLine.length == 11) continue;

			processSolrItemData(nextLine[11],bibid);
		}
		reader.close();
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
				+ "location_label text, "
				+ "edition text, "
				+ "pub_date text, "
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

	private int processSolrBibData(String solrBib,String online, String location_facet,String url,
			String format,String edition, String pubdate, String timestamp) throws SQLException, ParseException {
		bibCount++;
		String[] parts = solrBib.split("\\|", 2);
		int bibid = Integer.valueOf(parts[0]);
		if ( ! pstmts.containsKey("bib_insert"))
			pstmts.put("bib_insert",current.prepareStatement(
					"INSERT INTO "+CurrentDBTable.BIB_SOLR.toString()+
					" (bib_id, record_date, format, location_label,index_date,edition,pub_date) "
							+ "VALUES (?, ?, ?, ?, ?, ?, ?)"));
		List<String> locations = new ArrayList<String>();
		Collection<Object> urls = new HashSet<Object>();
		for (String s : url.split(","))
			urls.add(s);
		String sites = identifyOnlineServices(urls);
		if ( ! url.isEmpty() ) {
			if (sites == null)
				locations.add("Online");
			else
				locations.add("Online: "+sites);
		}
		if ( ! location_facet.isEmpty() )
			locations.add("At the Library: "+eliminateDuplicateLocations(location_facet));

		// Attempt to reflect Solr date in table. If fails, record not in Voyager.
		// note: date comparison comes later.
		PreparedStatement pstmt = pstmts.get("bib_insert");
		pstmt.setInt(1, bibid);
		pstmt.setTimestamp(2, new Timestamp( marcDateFormat.parse(parts[1]).getTime() ));
		pstmt.setString(3, format);
		pstmt.setString(4, StringUtils.join(locations," / "));
		pstmt.setTimestamp(5, new Timestamp( DatatypeConverter.parseDateTime(timestamp).getTimeInMillis() ));
		pstmt.setString(6, edition.isEmpty() ? null : edition);
		pstmt.setString(7, pubdate.isEmpty() ? null : pubdate);
		pstmt.addBatch();
		if (++bibCount % 1000 == 0)
			pstmt.executeBatch();
		return bibid;
	}

	private String eliminateDuplicateLocations(String location_facet) {
		String[] fieldValues = location_facet.split(",");
		StringBuilder sb = new StringBuilder();
		Collection<Object> foundValues = new HashSet<Object>();
		boolean first = true;
		for (Object val : fieldValues) {
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

	private void processWorksData(String ids, int bibid) throws SQLException {

		Set<Integer> oclcIds = new HashSet<Integer>();
		for (String id : ids.split(",")) {
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

	private void processSolrHoldingsData(String solrHoldings, int bibid) throws SQLException, ParseException {
		String[] solrHoldingsList = solrHoldings.split(",");
		if ( ! pstmts.containsKey("mfhd_insert"))
			pstmts.put("mfhd_insert",current.prepareStatement(
					"INSERT INTO "+CurrentDBTable.MFHD_SOLR.toString()+
					" (bib_id, mfhd_id, record_date) VALUES (?, ?, ?)"));
		PreparedStatement pstmt = pstmts.get("mfhd_insert");
		for (int i = 0; i < solrHoldingsList.length; i++) {
			if (solrHoldingsList[i].isEmpty()) continue;
			int holdingsId;
			Timestamp modified = null;
			if (solrHoldingsList[i].contains("|")) {
				String[] parts = solrHoldingsList[i].split("\\|",2);
				holdingsId = Integer.valueOf(parts[0]);
				modified = new Timestamp( marcDateFormat.parse(parts[1]).getTime() );
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
					"INSERT INTO "+CurrentDBTable.ITEM_SOLR.toString()+
					" (mfhd_id, item_id, record_date) VALUES (?, ?, ?)"));
		PreparedStatement pstmt = pstmts.get("item_insert");
		for (int i = 0; i < solrItemList.length; i++) {
			if (solrItemList[i].isEmpty()) continue;
			itemCount++;
			Timestamp modified = null;
			String[] parts = solrItemList[i].split("\\|");
			int itemId = Integer.valueOf(parts[0]);
			int mfhdId = Integer.valueOf(parts[1]);
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