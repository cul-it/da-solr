package edu.cornell.library.integration.processing;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import edu.cornell.library.integration.utilities.Config;

public class ResourceListComparison {

	public static void main(String[] args) throws SQLException {

		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		Config config = Config.loadConfig(requiredArgs);
		try (Connection current = config.getDatabaseConnection("Current")) {

			compareLists( current, "instanceFolio", "instanceLDP", "hrid" );
			compareLists( current, "bibFolio", "bibLDP", "instanceHrid" );
			compareLists( current, "holdingFolio", "holdingLDP", "hrid" );
			compareLists( current, "itemFolio", "itemLDP", "hrid" );
			compareLists( current, "loanFolio", "loanLDP", "id" );
			compareLists( current, "orderFolio", "orderLDP", "id" );
			compareLists( current, "orderLineFolio", "orderLineLDP", "id" );
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
			Timestamp fMod = null;
			Timestamp lMod = null;

			Timestamp recentEnuf = Timestamp.valueOf("2021-08-01 00:00:00");
			Long i = Instant.now().getEpochSecond();
			i = i - ( i % 86_400 );
			Timestamp midnightThisMorningUTC = Timestamp.from(Instant.ofEpochSecond(i));

			while ( ! fRs.isAfterLast() && ! lRs.isAfterLast() ) {

				fId = fRs.getString(1);
				lId = lRs.getString(1);
				fMod = fRs.getTimestamp(2);
				lMod = lRs.getTimestamp(2);

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

					if ( fMod == null || fMod.before(midnightThisMorningUTC) ) {
						System.out.println(fId+" D");
						onlyInFC.add(fId);
					} else
						System.out.println(fId+" Looks deleted but ignoring due to recency.");
					fRs.next();					

				}
			}

			while ( ! lRs.isAfterLast() ) {
				System.out.println(lId+" N");
				onlyInLDP.add(lId);
				lRs.next();
			}

			while ( ! fRs.isAfterLast() ) {
				if ( fMod != null && fMod.before(midnightThisMorningUTC) ) {
					System.out.println(fId+" D");
					onlyInFC.add(fId);
				} else
					System.out.println(fId+" Looks deleted but ignoring due to recency.");
				fRs.next();
			}

		}
		System.out.printf("%s/%s\n",folioCacheTable,ldpListTable);
		System.out.printf("newerInLDP: %d bibs (%s,%s,%s)\n", newerInLDP.size(),
				random(newerInLDP),random(newerInLDP),random(newerInLDP));
		System.out.printf("onlyInLDP: %d bibs (%s,%s,%s)\n", onlyInLDP.size(),
				random(onlyInLDP),random(onlyInLDP),random(onlyInLDP));
		System.out.printf("onlyInFC: %d bibs (%s,%s,%s)\n", onlyInFC.size(),
				random(onlyInFC),random(onlyInFC),random(onlyInFC));


	}

	private static Random rand = null;
	private static Object random(List<String> list) {
		if ( list == null || list.isEmpty() ) return null;
		if ( rand == null ) rand = new Random();
		return list.get(rand.nextInt(list.size()));
	}
}
