package edu.cornell.library.integration.voyager;

import static edu.cornell.library.integration.indexer.utilities.Config.getRequiredArgsForDB;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.indexer.queues.AddToQueue;
import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;

/**
 * Pull lists of current (unsuppressed) bib, holding, and item records along with
 * their modified dates to populate in a set of dated database tables. The contents
 * of these tables can then be compared with the contents of the Solr index. Any existing
 * set of tables will be replaced.
 */
public class IdentifyCurrentVoyagerRecords {

	public static void main(String[] args)  {

		List<String> requiredArgs = new ArrayList<>();
		requiredArgs.addAll(getRequiredArgsForDB("Current"));
		requiredArgs.addAll(getRequiredArgsForDB("Voy"));

		try{        
			new IdentifyCurrentVoyagerRecords( Config.loadConfig(args, requiredArgs) );
		}catch( Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * 
	 * Pull lists of current (unsuppressed) bib, holding, and item records along with
	 * their modified dates to populate in a set of dated database tables. The contents
	 * of these tables can then be compared with the contents of the Solr index. Any existing
	 * set of tables will be replaced.
	 * @param config
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 * 
	 */
	public IdentifyCurrentVoyagerRecords(Config config)
			throws ClassNotFoundException, SQLException, IOException, InterruptedException{

		config.setDatabasePoolsize("Current", 2);
		config.setDatabasePoolsize("Voy", 2);

		try (   Connection voyager = config.getDatabaseConnection("Voy");
	    		Connection current = config.getDatabaseConnection("Current") ) {

	    	current.setAutoCommit(false);

	    	buildBibVoyTable  ( config, voyager, current );
	    	buildMfhdVoyTable ( config, voyager, current );
//	    	buildItemVoyTable ( voyager, current );

	    	current.commit();
	    }
	}

	private static void buildBibVoyTable(Config config, Connection voyager,Connection current)
			throws SQLException, ClassNotFoundException, IOException, InterruptedException {
		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement()  ){

			setRecordCheckDate(c_stmt, RecordType.BIBLIOGRAPHIC);
			c_stmt.execute("SET unique_checks=0");

			// Starting with bibs, create the destination table, then populated it from Voyager
			c_stmt.execute("drop table if exists bibRecsVoyager");
			c_stmt.execute("create table bibRecsVoyager ( "
					+ "bib_id int(10) unsigned not null, "
					+ "record_date timestamp not null, "
					+ "active int not null, "
					+ "marc_xml blob null, "
					+ "key (bib_id) ) "
					+ "ENGINE=MyISAM");
			c_stmt.execute("alter table bibRecsVoyager disable keys");
			current.commit();
			DownloadMARC getMARC = new DownloadMARC( config );

			try (   ResultSet rs = v_stmt.executeQuery
						("select BIB_ID, UPDATE_DATE, SUPPRESS_IN_OPAC "
								+ " from BIB_MASTER");
					PreparedStatement pstmt = current.prepareStatement
							("insert into bibRecsVoyager (bib_id, record_date, active) values ( ? , ? , ? )")  ) {

				int i = 0;
				while (rs.next()) {
					int bib_id = rs.getInt(1);
					Timestamp mod_date= rs.getTimestamp(2);
					Boolean active = rs.getString(3).equals("N") ;
					String marc_xml = getMARC.downloadXml(RecordType.BIBLIOGRAPHIC, bib_id);
					pstmt.setInt(1, bib_id );
					pstmt.setTimestamp(2, mod_date);
					pstmt.setBoolean(3, active);
					pstmt.setString(4, marc_xml);
					pstmt.addBatch();
					if (active)
					AddToQueue.newBib( config, bib_id, mod_date );
					if ((++i % 256) == 0) {
						pstmt.executeBatch();
						System.out.println(i +" bibs pulled.");
						current.commit();
					}
				}
				pstmt.executeBatch();
				System.out.println("Bib count: "+i);
			}
		    c_stmt.execute("alter table bibRecsVoyager enable keys");
		    current.commit();
		}
	}



	private static void buildMfhdVoyTable(Config config, Connection voyager,Connection current)
			throws SQLException, ClassNotFoundException, IOException, InterruptedException {
		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement()  ){

			setRecordCheckDate(c_stmt, RecordType.HOLDINGS);

			c_stmt.execute("drop table if exists mfhdRecsVoyager");
			c_stmt.execute("create table mfhdRecsVoyager ( "
					+ "bib_id int(10) unsigned not null, "
					+ "mfhd_id int(10) unsigned not null, "
					+ "record_date timestamp null, "
					+ "marc_xml blob null, "
					+ "key (mfhd_id), "
					+ "key (bib_id) ) "
					+ "ENGINE=MyISAM");
			c_stmt.execute("alter table mfhdRecsVoyager disable keys");

			current.commit();
			DownloadMARC getMARC = new DownloadMARC( config );

			try (   ResultSet rs = v_stmt.executeQuery
					( "select BIB_MFHD.BIB_ID, MFHD_MASTER.MFHD_ID, UPDATE_DATE"
	    				+"  from BIB_MFHD, MFHD_MASTER"
	    				+" where BIB_MFHD.MFHD_ID = MFHD_MASTER.MFHD_ID"
	    				+ "  and SUPPRESS_IN_OPAC = 'N'");
					PreparedStatement pstmt = current.prepareStatement
	    				("insert into mfhdRecsVoyager (bib_id, mfhd_id, record_date) values (?, ?, ?)");
					PreparedStatement bibConfirm = current.prepareStatement(
	    				"select bib_id from bibRecsVoyager where bib_id = ?") ){
				int i = 0;
				while (rs.next()) {
					int bib_id = rs.getInt(1);
					int mfhd_id = rs.getInt(2);
					if ( ! isUnsuppressed(bibConfirm, bib_id) )
						continue;
					String marc_xml = getMARC.downloadXml(RecordType.HOLDINGS, mfhd_id);
					pstmt.setInt(1, bib_id);
					pstmt.setInt(2, mfhd_id);
					pstmt.setTimestamp(3, rs.getTimestamp(3));
					pstmt.setString(4, marc_xml);
					pstmt.addBatch();
					if ((++i % 256) == 0) {
						pstmt.executeBatch();
						System.out.println(i +" mfhds pulled.");
						current.commit();
					}
				}
				pstmt.executeBatch();
				current.commit();
				c_stmt.execute("alter table mfhdRecsVoyager enable keys");
				System.out.println("Mfhd count: "+i);
			}
		}
	}

	private static void buildItemVoyTable(Connection voyager, Connection current) throws SQLException {
		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement()  ){

			c_stmt.execute("drop table if exists itemRecsVoyager");
			c_stmt.execute("create table itemRecsVoyager ( "
					+ "mfhd_id int(10) unsigned not null, "
					+ "item_id int(10) unsigned not null, "
					+ "record_date timestamp null, "
					+ "key (item_id) ) "
					+ "ENGINE=MyISAM");
			c_stmt.execute("alter table itemRecsVoyager disable keys");

			current.commit();

			try (   ResultSet rs = v_stmt.executeQuery
					("select MFHD_ITEM.MFHD_ID, ITEM.ITEM_ID, ITEM.MODIFY_DATE"
							+"  from MFHD_ITEM, ITEM"
							+" where MFHD_ITEM.ITEM_ID = ITEM.ITEM_ID");
					PreparedStatement pstmt = current.prepareStatement
							("insert into itemRecsVoyager (mfhd_id, item_id, record_date) values (?, ?, ?)");
					PreparedStatement mfhdConfirm = current.prepareStatement(
		    				"select mfhd_id from mfhdRecsVoyager where mfhd_id = ?")) {

				int i = 0;
				while (rs.next()) {
					int mfhd_id = rs.getInt(1);
					if ( ! isUnsuppressed(mfhdConfirm, mfhd_id) )
						continue;
					pstmt.setInt(1, mfhd_id);
					pstmt.setInt(2, rs.getInt(2));
					pstmt.setTimestamp(3, rs.getTimestamp(3));
					pstmt.addBatch();
					if ((++i % 2048) == 0) {
						pstmt.executeBatch();
						if ((i % 262_144) == 0) {
							System.out.println(i +" items pulled.");
							current.commit();
						}
					}
				}
				pstmt.executeBatch();
				pstmt.close();
				System.out.println("Item count: "+i);
				c_stmt.execute("alter table itemRecsVoyager enable keys");
				current.commit();
			}
			c_stmt.execute("SET unique_checks=1");
		}
	}

	private static void setRecordCheckDate(Statement c_stmt, RecordType recType) throws SQLException {
		c_stmt.execute("create table if not exists recordCheckDate ( "
				+ "type varchar(15) not null, "
				+ "check_date timestamp not null,"
				+ "primary key ( type ) ) "
				+ "ENGINE=MyISAM");
		c_stmt.execute("REPLACE INTO recordCheckDate (type, check_date) values ( '"
				+recType.name()+"', NOW())");
	}

	private static boolean isUnsuppressed(PreparedStatement pstmt, int bib_id) throws SQLException {
		boolean bibInTable = false;
		pstmt.setInt(1, bib_id);
		try ( ResultSet rs = pstmt.executeQuery() ) {
			while (rs.next())
				bibInTable = true;
		}
		return bibInTable;
	}

}