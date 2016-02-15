package edu.cornell.library.integration.indexer.updates;

import static edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig.getRequiredArgsForDB;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.identifyOnlineServices;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.Reader;
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
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

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
				"/select?qt=standard&q=id%3A*&wt=csv&rows=50000000&csv.escape=\\&"
				+ "fl=bibid_display,online,location_facet,url_access_display,format,"
				+ "title_display,title_vern_display,title_uniform_display,language_facet,"
				+ "edition_display,pub_date_display,timestamp,type,other_id_display,"
				+ "holdings_display,item_display");

	    // Save Solr data to a temporary file
	    final Path tempPath = Files.createTempFile("identifyCurrentSolrRecords-", ".csv");
		tempPath.toFile().deleteOnExit();
		FileOutputStream fos = new FileOutputStream(tempPath.toString());
		ReadableByteChannel rbc = Channels.newChannel(queryUrl.openStream());
		fos.getChannel().transferFrom(rbc, 0, 100_000_000_000L);
		fos.close();

		Reader reader = new FileReader(tempPath.toString());
		Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader().parse(reader);

		// Then read the file back in to process it

		for (CSVRecord record : records) {
			try {
				int bibid = processSolrBibData
						(record.get("bibid_display"),record.get("online"),record.get("location_facet"),
						record.get("url_access_display"),record.get("format"),record.get("title_display"),
						record.get("title_vern_display"),record.get("title_uniform_display"),
						record.get("language_facet"),record.get("edition_display"),
						record.get("pub_date_display"),record.get("timestamp"));
				if (bibCount % 10_000 == 0)
					current.commit();
				
				if (record.get("type").equals("Catalog"))
					processWorksData( record.get("other_id_display"), bibid );
	
				processSolrHoldingsData(record.get("holdings_display"),bibid);
	
				processSolrItemData(record.get("item_display"),bibid);
			} catch (NumberFormatException e ) {
				Map<String,String> valueMap = record.toMap();
				StringBuilder sb = new StringBuilder();
				sb.append("CSV row value map:\n");
				for (Entry<String, String> entry : valueMap.entrySet()) {
					sb.append(entry.getKey()).append(' ').append(entry.getValue()).append('\n');
				}
				System.out.print(sb.toString());
				System.out.flush();
				throw e;
			}
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
			String solrBib, //0
			String online,  //1
			String location_facet, //2
			String url, //3
			String format, //4
			String title_display, //5
			String title_vern_display, //6
			String title_uniform_display, //7
			String language, //8
			String edition, //9
			String pubdate, //10
			String timestamp //11
			) throws SQLException, ParseException {
		bibCount++;
		String[] parts = solrBib.split("\\|", 2);
		int bibid = Integer.valueOf(parts[0]);
		if ( ! pstmts.containsKey("bib_insert"))
			pstmts.put("bib_insert",current.prepareStatement(
					"INSERT INTO "+CurrentDBTable.BIB_SOLR.toString()+
					" (bib_id, record_date, format, sites,libraries,index_date,edition,pub_date,language,title) "
							+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));
		Collection<Object> urls = new HashSet<Object>();
		for (String s : url.split(","))
			urls.add(s);
		String sites = identifyOnlineServices(urls);
		if ( ! url.isEmpty() )
			if (sites == null)
				sites = "Online";
		String libraries = eliminateDuplicateLocations(location_facet);
		String title = null;
		if ( ! title_uniform_display.isEmpty()) {
			int pipePos = title_uniform_display.indexOf('|');
			if (pipePos == -1)
				title = title_uniform_display;
			else
				title = title_uniform_display.substring(0, pipePos);
		}
		if (title == null)
			if (! title_vern_display.isEmpty())
				title = title_vern_display;
		if (title == null)
			title = title_display;

		// Attempt to reflect Solr date in table. If fails, record not in Voyager.
		// note: date comparison comes later.
		PreparedStatement pstmt = pstmts.get("bib_insert");
		try {
		pstmt.setInt(1, bibid);
		pstmt.setTimestamp(2, new Timestamp( marcDateFormat.parse(parts[1]).getTime() ));
		pstmt.setString(3, format);
		pstmt.setString(4, sites);
		pstmt.setString(5, libraries);
		pstmt.setTimestamp(6, new Timestamp( DatatypeConverter.parseDateTime(timestamp).getTimeInMillis() ));
		pstmt.setString(7, edition.isEmpty() ? null : edition);
		pstmt.setString(8, pubdate.isEmpty() ? null : pubdate);
		pstmt.setString(9, language.isEmpty() ? null : language);
		pstmt.setString(10, title);
		pstmt.addBatch();
		} catch (IllegalArgumentException | ParseException e) {
			StringBuilder sb = new StringBuilder();
			sb.append("solrBib ").append(solrBib);
			sb.append("\nonline ").append(online);
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
			int itemId = 0, mfhdId = 0;
			try {
				itemId = Integer.valueOf(parts[0]);
				mfhdId = Integer.valueOf(parts[1]);
			} catch (NumberFormatException e) {
				StringBuilder sb = new StringBuilder();
				sb.append("solrItems ").append(solrItems);
				sb.append("\nsolrItemList.length ").append(solrItemList.length);
				sb.append("\ni ").append(i);
				sb.append("\nsolrItemList[i] ").append(solrItemList[i]);
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