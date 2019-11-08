package edu.cornell.library.integration.processing;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collection;

import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.voyager.DownloadMARC;

public class UpdateAuthorityRecords {

	public static void main(String[] args) throws SQLException, IOException, InterruptedException {

		Collection<String> requiredArgs = Config.getRequiredArgsForDB("Headings");
		requiredArgs.addAll( Config.getRequiredArgsForDB("Voy"));
		Config config = Config.loadConfig(requiredArgs);
		config.setDatabasePoolsize("Voy", 2);
		DownloadMARC marc = new DownloadMARC(config);

		try ( Connection voyager = config.getDatabaseConnection("Voy");
				Connection headings = config.getDatabaseConnection("Headings");
				PreparedStatement voyRecords = voyager.prepareStatement(
						"SELECT auth_id, COALESCE(update_date,create_date) mod_date"+
						"  FROM auth_master"+
						" WHERE auth_id BETWEEN ? AND ? "+
						" ORDER BY auth_id");
				PreparedStatement headingsRecords = headings.prepareStatement(
						"SELECT authId, modDate"+
						"  FROM authority"+
						" WHERE authId BETWEEN ? AND ? "+
						" ORDER BY authId");
				){
			int maxId = getMaxAuthorityId( voyager, headings );
			int cursor = 0;
			int batchSize = 50_000;
			voyRecords.setFetchSize(batchSize);
			headingsRecords.setFetchSize(batchSize);

			while ( cursor < maxId ) {

				voyRecords.setInt(1, cursor+1);
				voyRecords.setInt(2, cursor+batchSize);
				headingsRecords.setInt(1, cursor+1);
				headingsRecords.setInt(2, cursor+batchSize);
				try (ResultSet v_rs = voyRecords.executeQuery();
					ResultSet h_rs = headingsRecords.executeQuery()) {

					v_rs.next();
					h_rs.next();

					while ( ! v_rs.isAfterLast() && ! h_rs.isAfterLast() ) {
						int v_id = v_rs.getInt(1);
						int h_id = h_rs.getInt(1);
						Timestamp v_date = v_rs.getTimestamp(2);

						if ( v_id == h_id ) {
							if ( ! v_date.equals(h_rs.getTimestamp(2)) )
								processChangedAuthorityRecord( voyager, headings, marc, v_id, v_date );
							v_rs.next();
							h_rs.next();
							continue;
						}
						if ( v_id < h_id ) {
							processNewAuthorityRecord( voyager, headings, marc, v_id, v_date );
							v_rs.next();
							continue;
						}
						processDeletedAuthorityRecord( voyager, headings, h_id );
						h_rs.next();
					}

					while ( ! v_rs.isAfterLast() ) {
						processNewAuthorityRecord( voyager, headings, marc,v_rs.getInt(1), v_rs.getTimestamp(2) );
						v_rs.next();
					}

					while ( ! h_rs.isAfterLast() ) {
						processDeletedAuthorityRecord( voyager, headings, h_rs.getInt(1) );
						h_rs.next();
					}

				}

				cursor += batchSize;
//				System.exit(0);
			}
		}
	}

	private static void processDeletedAuthorityRecord(Connection voyager, Connection headings, int authId) {
		System.out.printf("a%d deleted\n",authId);
		// TODO Auto-generated method stub
		
	}

	private static void processNewAuthorityRecord(
			Connection voyager, Connection headings, DownloadMARC marc, int authId, Timestamp modDate)
					throws SQLException, IOException, InterruptedException {
		System.out.printf("a%d new %s\n",authId,modDate);
		MarcRecord rec = marc.getMarc(RecordType.AUTHORITY, authId);
		// TODO Auto-generated method stub
		
	}

	private static void processChangedAuthorityRecord(
			Connection voyager, Connection headings, DownloadMARC marc, int authId, Timestamp modDate)
					throws SQLException, IOException, InterruptedException {
//		System.out.printf("a%d updated %s\n",authId,modDate);
		MarcRecord rec = marc.getMarc(RecordType.AUTHORITY, authId);
		// TODO Auto-generated method stub
		
	}

	private static int getMaxAuthorityId(Connection voyager, Connection headings) throws SQLException {
		int maxId = 0;
		try ( Statement voy = voyager.createStatement();
				ResultSet rsVoy = voy.executeQuery("select max(auth_id) from auth_data");
				Statement head = headings.createStatement();
				ResultSet rsHead = head.executeQuery("select max(authId) from authority");) {
			while ( rsVoy.next() )
				maxId = rsVoy.getInt(1);
			while ( rsHead.next() )
				if ( rsHead.getInt(1) > maxId ) maxId = rsHead.getInt(1);
		}
		if ( maxId > 0 ) return maxId;
		throw new RuntimeException("Max authority record id not determined.");
	}


}
