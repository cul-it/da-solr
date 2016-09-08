package edu.cornell.library.integration.voyager;

import static edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig.getRequiredArgsForDB;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;

/**
 * Pull lists of current (unsuppressed) bib, holding, and item records along with
 * their modified dates to populate in a set of dated database tables. The contents
 * of these tables can then be compared with the contents of the Solr index. Any existing
 * set of tables will be replaced.
 */
public class IdentifyCurrentVoyagerRecords {

	public static void main(String[] args)  {

		List<String> requiredArgs = new ArrayList<String>();
		requiredArgs.addAll(getRequiredArgsForDB("Current"));
		requiredArgs.addAll(getRequiredArgsForDB("Voy"));

		try{        
			new IdentifyCurrentVoyagerRecords( SolrBuildConfig.loadConfig(args, requiredArgs) );
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
	public IdentifyCurrentVoyagerRecords(SolrBuildConfig config) throws ClassNotFoundException, SQLException{

	    try (   Connection voyager = config.getDatabaseConnection("Voy");
	    		Connection current = config.getDatabaseConnection("Current") ) {

	    	current.setAutoCommit(false);

	    	buildBibVoyTable  ( voyager, current );
	    	buildMfhdVoyTable ( voyager, current );
	    	buildItemVoyTable ( voyager, current );

	    	current.commit();
	    }
	}

	private static void buildBibVoyTable(Connection voyager, Connection current) throws SQLException {
		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement()  ){

			c_stmt.execute("SET unique_checks=0");

			// Starting with bibs, create the destination table, then populated it from Voyager
			c_stmt.execute("drop table if exists "+CurrentDBTable.BIB_VOY);
			c_stmt.execute("create table "+CurrentDBTable.BIB_VOY+"( "
					+ "bib_id int(10) unsigned not null, "
					+ "record_date timestamp null, "
					+ "active int not null default 1, "
					+ "key (bib_id) ) "
					+ "ENGINE=InnoDB");
			c_stmt.execute("alter table "+CurrentDBTable.BIB_VOY+" disable keys");
			current.commit();

			try (   ResultSet rs = v_stmt.executeQuery
						("select BIB_ID, UPDATE_DATE, SUPPRESS_IN_OPAC "
								+ " from BIB_MASTER");
					PreparedStatement pstmt = current.prepareStatement
							("insert into "+CurrentDBTable.BIB_VOY+
									" (bib_id, record_date, active) values ( ? , ? , ? )")  ) {

				int i = 0;
				while (rs.next()) {
					pstmt.setInt(1, rs.getInt(1) );
					pstmt.setTimestamp(2, rs.getTimestamp(2) );
					pstmt.addBatch();
					if ((++i % 2048) == 0) {
						pstmt.executeBatch();
						if ((i % 262_144) == 0) {
							System.out.println(i +" bibs pulled.");
							current.commit();
						}
					}
				}
				pstmt.executeBatch();
				System.out.println("Bib count: "+i);
			}
		    c_stmt.execute("alter table "+CurrentDBTable.BIB_VOY+" disable keys");
		    current.commit();
		}
	}


	private static void buildMfhdVoyTable(Connection voyager, Connection current) throws SQLException {
		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement()  ){

			c_stmt.execute("drop table if exists "+CurrentDBTable.MFHD_VOY.toString());
			c_stmt.execute("create table "+CurrentDBTable.MFHD_VOY.toString()+"( "
					+ "bib_id int(10) unsigned not null, "
					+ "mfhd_id int(10) unsigned not null, "
					+ "record_date timestamp null, "
					+ "key (mfhd_id), "
					+ "key (bib_id) ) "
					+ "ENGINE=InnoDB");
			c_stmt.execute("alter table "+CurrentDBTable.MFHD_VOY.toString()+" disable keys");
			current.commit();

			try (   ResultSet rs = v_stmt.executeQuery
					( "select BIB_MFHD.BIB_ID, MFHD_MASTER.MFHD_ID, UPDATE_DATE"
	    				+"  from BIB_MFHD, MFHD_MASTER"
	    				+" where BIB_MFHD.MFHD_ID = MFHD_MASTER.MFHD_ID"
	    				+ "  and SUPPRESS_IN_OPAC = 'N'");
					PreparedStatement pstmt = current.prepareStatement
	    				("insert into "+CurrentDBTable.MFHD_VOY.toString()+
	    						" (bib_id, mfhd_id, record_date) values (?, ?, ?)");
					PreparedStatement bibConfirm = current.prepareStatement(
	    				"select bib_id from "+CurrentDBTable.BIB_VOY.toString()+" where bib_id = ?") ){
				int i = 0;
				while (rs.next()) {
					int bib_id = rs.getInt(1);
					if ( ! isUnsuppressed(bibConfirm, bib_id) )
						continue;
					pstmt.setInt(1, bib_id);
					pstmt.setInt(2, rs.getInt(2));
					pstmt.setTimestamp(3, rs.getTimestamp(3));
					pstmt.addBatch();
					if ((++i % 2048) == 0) {
						pstmt.executeBatch();
						if ((i % 262_144) == 0) {
							System.out.println(i +" mfhds pulled.");
							current.commit();
						}
					}
				}
				pstmt.executeBatch();
				current.commit();
				c_stmt.execute("alter table "+CurrentDBTable.MFHD_VOY.toString()+" enable keys");
				System.out.println("Mfhd count: "+i);
			}
		}
	}

	private static void buildItemVoyTable(Connection voyager, Connection current) throws SQLException {
		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement()  ){

			c_stmt.execute("drop table if exists "+CurrentDBTable.ITEM_VOY.toString());
			c_stmt.execute("create table "+CurrentDBTable.ITEM_VOY.toString()+"( "
					+ "mfhd_id int(10) unsigned not null, "
					+ "item_id int(10) unsigned not null, "
					+ "record_date timestamp null, "
					+ "key (item_id) ) "
					+ "ENGINE=InnoDB");
			c_stmt.execute("alter table "+CurrentDBTable.ITEM_VOY.toString()+" disable keys");
			current.commit();

			try (   ResultSet rs = v_stmt.executeQuery
					("select MFHD_ITEM.MFHD_ID, ITEM.ITEM_ID, ITEM.MODIFY_DATE"
							+"  from MFHD_ITEM, ITEM"
							+" where MFHD_ITEM.ITEM_ID = ITEM.ITEM_ID");
					PreparedStatement pstmt = current.prepareStatement
							("insert into "+CurrentDBTable.ITEM_VOY.toString()+
									" (mfhd_id, item_id, record_date) values (?, ?, ?)");
					PreparedStatement mfhdConfirm = current.prepareStatement(
		    				"select mfhd_id from "+CurrentDBTable.MFHD_VOY.toString()+" where mfhd_id = ?")) {

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
				c_stmt.execute("alter table "+CurrentDBTable.ITEM_VOY.toString()+" enable keys");
				current.commit();
			}
			c_stmt.execute("SET unique_checks=1");
		}
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