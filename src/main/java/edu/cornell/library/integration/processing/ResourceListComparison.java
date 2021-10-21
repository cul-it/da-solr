package edu.cornell.library.integration.processing;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.utilities.Config;

public class ResourceListComparison {

	public static void main(String[] args) throws SQLException {

		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		Config config = Config.loadConfig(requiredArgs);
		try (Connection current = config.getDatabaseConnection("Current")) {

			compareLists( current, "instanceFolio", "instanceLDP", "hrid");
		}
	}

	private static void compareLists(
			Connection current, String folioCacheTable, String ldpListTable, String idField)
					throws SQLException {

		List<String> newerInLDP = new ArrayList<>();
		List<String> onlyInLDP = new ArrayList<>();
		List<String> onlyInFC = new ArrayList<>();

		try ( Statement s1 = current.createStatement();
				Statement s2 = current.createStatement();
				ResultSet fRs = s1.executeQuery(String.format(
						"SELECT %s, moddate FROM %s ORDER BY %s", idField, folioCacheTable, idField));
				ResultSet lRs = s2.executeQuery(String.format(
						"SELECT %s, moddate FROM %s ORDER BY %s", idField, ldpListTable, idField))) {

			if ( ! fRs.next() )
				throw new SQLException("Error: folio cache table must not have zero records.");
			if ( ! lRs.next() )
				throw new SQLException("Error: ldp list table must not have zero records.");

			String fId = null;
			String lId = null;
			Timestamp recentEnuf = Timestamp.valueOf("2021-08-01 00:00:00");
			while ( ! fRs.isAfterLast() && ! lRs.isAfterLast() ) {

				fId = fRs.getString(1);
				lId = lRs.getString(1);
				Timestamp fMod = fRs.getTimestamp(2);
				Timestamp lMod = lRs.getTimestamp(2);

				int compare = fId.compareTo(lId);

				if ( compare == 0 ) { // fId = lId
					if ( fMod == null ) {
						// This is probably pre-load migration data. Flag as changed if lMod recent enuf
						if ( lMod.after(recentEnuf) ) {
							System.out.println( fId+" M => "+lMod);
							newerInLDP.add(fId);
						}
					} else if ( lMod.after(fMod) ) {
						System.out.println( fId+" M "+fMod+" => "+lMod);
						newerInLDP.add(fId);
					} // else unchanged
					lRs.next();
					fRs.next();

				} else if ( compare > 0 ) { // fId > lId (lId not in folio cache - new? missed?)

					System.out.println(lId+" N");
					onlyInLDP.add(lId);
					lRs.next();

				} else { // fId < lId (fId not in ldp - deleted?)

					System.out.println(fId+" D");
					onlyInFC.add(fId);
					fRs.next();

				}
			}

			while ( ! lRs.isAfterLast() ) {
				System.out.println(lId+" N");
				onlyInLDP.add(lId);
				lRs.next();
			}

			while ( ! fRs.isAfterLast() ) {
				System.out.println(fId+" D");
				onlyInFC.add(fId);
				fRs.next();
			}

		}


	}
}
