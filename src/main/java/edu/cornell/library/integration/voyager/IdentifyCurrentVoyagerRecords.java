package edu.cornell.library.integration.voyager;

import static edu.cornell.library.integration.indexer.utilities.Config.getRequiredArgsForDB;

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
import edu.cornell.library.integration.voyager.IdentifyChangedRecords.DataChangeUpdateType;

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
			new IdentifyCurrentVoyagerRecords( Config.loadConfig(requiredArgs) );
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
	 * 
	 */
	public IdentifyCurrentVoyagerRecords(Config config) throws ClassNotFoundException, SQLException{

		config.setDatabasePoolsize("Current", 2);
		config.setDatabasePoolsize("Voy", 2);

		try (   Connection voyager = config.getDatabaseConnection("Voy");
	    		Connection current = config.getDatabaseConnection("Current") ) {

	    	current.setAutoCommit(false);
	    	try (   Statement c_stmt = current.createStatement() ) {
	    		c_stmt.execute("drop table if exists generationQueue");
	    		c_stmt.execute("CREATE TABLE generationQueue (" 
	    				+ " id int(12) NOT NULL PRIMARY KEY AUTO_INCREMENT," 
	    				+ " bib_id int(10) unsigned NOT NULL," 
	    				+ " priority tinyint(1) unsigned NOT NULL," 
	    				+ " cause varchar(256) DEFAULT NULL,"
	    				+ " record_date timestamp NOT NULL ) " 
	    				+"ENGINE=MyISAM");
	    	}

	    	buildBibVoyTable  ( voyager, current );
	    	buildMfhdVoyTable ( voyager, current );
	    	buildItemVoyTable ( voyager, current );

	    	try (   Statement c_stmt = current.createStatement() ) {
	    		c_stmt.execute("alter table generationQueue add key ( bib_id ) ");
	    		c_stmt.execute("alter table generationQueue add key ( priority, record_date ) ");
	    	}
	    	current.commit();
	    }
	}

	private static void buildBibVoyTable(Connection voyager, Connection current) throws SQLException {

		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement()  ){

			c_stmt.execute("SET unique_checks=0");

			// Starting with bibs, create the destination table, then populated it from Voyager
			c_stmt.execute("drop table if exists bibRecsVoyager");
			c_stmt.execute("create table bibRecsVoyager ( "
					+ "bib_id int(10) unsigned not null, "
					+ "record_date timestamp not null, "
					+ "active int not null, "
					+ "marc_xml blob null ) "
					+ "ENGINE=MyISAM");
			current.commit();

			try (   ResultSet rs = v_stmt.executeQuery
							("select BIB_ID, CREATE_DATE, UPDATE_DATE, SUPPRESS_IN_OPAC from BIB_MASTER");
					PreparedStatement invStmt = current.prepareStatement
							("insert into bibRecsVoyager ( bib_id, record_date, active ) values ( ? , ? , ? )");
					PreparedStatement qStmt = AddToQueue.generationQueueStmt(current) ) {

				int i = 0;
				while (rs.next()) {
					int bib_id = rs.getInt(1);
					Timestamp mod_date= rs.getTimestamp(3);
					boolean newBib = false;
					if ( mod_date == null ) {
						mod_date= rs.getTimestamp(2);
						newBib = true;
					}
					boolean active = rs.getString(4).equals("N") ;

					add2BibInventory( invStmt, bib_id, mod_date, active );
					if (active)
						AddToQueue.add2QueueBatch( qStmt, bib_id, mod_date,
								(newBib)?DataChangeUpdateType.BIB_ADD:DataChangeUpdateType.BIB_UPDATE);

					if ((++i % 2048) == 0) {
						invStmt.executeBatch();
						qStmt.executeBatch();
						System.out.println(i +" bibs pulled.");
						current.commit();
					}
				}
				invStmt.executeBatch();
				qStmt.executeBatch();
				System.out.println("Bib count: "+i);
			}
		    c_stmt.execute("alter table bibRecsVoyager add primary key ( bib_id ) ");
		    current.commit();
		}
	}

	private static void buildMfhdVoyTable(Connection voyager,Connection current) throws SQLException {

		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement()  ){

			c_stmt.execute("drop table if exists mfhdRecsVoyager");
			c_stmt.execute("create table mfhdRecsVoyager ( "
					+ "bib_id int(10) unsigned not null, "
					+ "mfhd_id int(10) unsigned not null, "
					+ "record_date timestamp null, "
					+ "marc_xml blob null ) "
					+ "ENGINE=MyISAM");

			current.commit();

			try (   ResultSet rs = v_stmt.executeQuery
						( "select BIB_MFHD.BIB_ID, MFHD_MASTER.MFHD_ID, CREATE_DATE, UPDATE_DATE"
						+ "  from BIB_MFHD, MFHD_MASTER"
	    				+ " where BIB_MFHD.MFHD_ID = MFHD_MASTER.MFHD_ID"
	    				+ "   and SUPPRESS_IN_OPAC = 'N'");
					PreparedStatement invStmt = current.prepareStatement
	    				("insert into mfhdRecsVoyager ( bib_id, mfhd_id, record_date) values (?, ?, ?)");
					PreparedStatement qStmt = AddToQueue.generationQueueStmt(current);
					PreparedStatement bibConfirm = current.prepareStatement
						("select bib_id from bibRecsVoyager where bib_id = ? AND active = 1") ){
				int i = 0;
				while (rs.next()) {

					int bib_id = rs.getInt(1);
					if ( ! isUnsuppressed(bibConfirm, bib_id) )	continue;
					int mfhd_id = rs.getInt(2);
					Timestamp mod_date= rs.getTimestamp(4);
					boolean newMfhd = false;
					if ( mod_date == null ) {
						mod_date= rs.getTimestamp(3);
						newMfhd = true;
					}

					add2MfhdInventory( invStmt, bib_id, mfhd_id, mod_date );
					AddToQueue.add2QueueBatch( qStmt, bib_id, mod_date,
							(newMfhd)?DataChangeUpdateType.MFHD_ADD:DataChangeUpdateType.MFHD_UPDATE );

					if ((++i % 2048) == 0) {
						qStmt.executeBatch();
						invStmt.executeBatch();
						System.out.println(i +" mfhds pulled.");
						current.commit();
					}
				}
				qStmt.executeBatch();
				invStmt.executeBatch();
				current.commit();
				c_stmt.execute("alter table mfhdRecsVoyager add key ( bib_id )");
				c_stmt.execute("alter table mfhdRecsVoyager add key ( mfhd_id )");
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
					+ "record_date timestamp null ) "
					+ "ENGINE=MyISAM");

			current.commit();

			try (   ResultSet rs = v_stmt.executeQuery
							("select BIB_MFHD.BIB_ID, MFHD_ITEM.MFHD_ID, ITEM.ITEM_ID, ITEM.CREATE_DATE, ITEM.MODIFY_DATE"
							+"  from BIB_MFHD, MFHD_ITEM, ITEM"
							+" where MFHD_ITEM.ITEM_ID = ITEM.ITEM_ID and BIB_MFHD.MFHD_ID = MFHD_ITEM.MFHD_ID");
					PreparedStatement invStmt = current.prepareStatement
							("insert into itemRecsVoyager (mfhd_id, item_id, record_date) values (?, ?, ?)");
					PreparedStatement qStmt = AddToQueue.generationQueueStmt(current);
					PreparedStatement mfhdConfirm = current.prepareStatement
							("select mfhd_id from mfhdRecsVoyager where mfhd_id = ?")) {

				int i = 0;
				while (rs.next()) {
					int mfhd_id = rs.getInt(2);
					if ( ! isUnsuppressed(mfhdConfirm, mfhd_id) )
						continue;
					int bib_id = rs.getInt(1);
					int item_id = rs.getInt(3);
					Timestamp mod_date= rs.getTimestamp(5);
					boolean newItem = false;
					if ( mod_date == null ) {
						mod_date= rs.getTimestamp(4);
						newItem = true;
					}

					add2ItemInventory( invStmt, mfhd_id, item_id, mod_date );
					AddToQueue.add2QueueBatch( qStmt, bib_id, mod_date,
							(newItem)?DataChangeUpdateType.ITEM_ADD:DataChangeUpdateType.ITEM_UPDATE );

					if ((++i % 2048) == 0) {
						qStmt.executeBatch();
						invStmt.executeBatch();
						System.out.println(i +" items pulled.");
						current.commit();
					}
				}
				invStmt.executeBatch();
				qStmt.executeBatch();
				c_stmt.execute("alter table itemRecsVoyager add key (item_id)");
				c_stmt.execute("alter table itemRecsVoyager add key (mfhd_id)");
				current.commit();
				System.out.println("Item count: "+i);
			}
			c_stmt.execute("SET unique_checks=1");
		}
	}

	private static void add2BibInventory(PreparedStatement invStmt, int bib_id, Timestamp mod_date, boolean active) throws SQLException {
		invStmt.setInt(1, bib_id );
		invStmt.setTimestamp(2, mod_date);
		invStmt.setBoolean(3, active);
		invStmt.addBatch();
	}

	private static void add2MfhdInventory(PreparedStatement invStmt, int bib_id, int mfhd_id, Timestamp mod_date) throws SQLException {
		invStmt.setInt(1, bib_id);
		invStmt.setInt(2, mfhd_id);
		invStmt.setTimestamp(3, mod_date);
		invStmt.addBatch();
	}

	private static void add2ItemInventory(PreparedStatement invStmt, int mfhd_id, int item_id, Timestamp mod_date) throws SQLException {
		invStmt.setInt(1, mfhd_id);
		invStmt.setInt(2, item_id);
		invStmt.setTimestamp(3, mod_date);
		invStmt.addBatch();
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