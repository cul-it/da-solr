package edu.cornell.library.integration.processing;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Random;

import edu.cornell.library.integration.utilities.ComparisonLists;
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

	@SuppressWarnings("resource")
	public static ComparisonLists compareLists(
			Connection current, String folioCacheTable, String ldpListTable, String idField)
					throws SQLException {

		ComparisonLists c = new ComparisonLists();
		int batchSize = 100_000; // 10616312 after 12 minutes at 1k, 11? after 12 minutes at 10k

		try ( Statement fS = current.createStatement();
				Statement lS = current.createStatement() ) {
	
			ResultSet fRs = fS.executeQuery(String.format(
					"SELECT %s, moddate FROM %s ORDER BY %s LIMIT %d",
					idField, folioCacheTable, idField, batchSize));
			ResultSet lRs = lS.executeQuery(String.format(
					"SELECT %s, moddate FROM %s ORDER BY %s LIMIT %d",
					idField, ldpListTable, idField, batchSize));

			if ( ! fRs.next() )
				throw new SQLException("Error: folio cache table must not have zero records.");
			if ( ! lRs.next() )
				throw new SQLException("Error: ldp list table must not have zero records.");

			String fId = null;
			String lId = null;
			Timestamp fMod = null;
			Timestamp lMod = null;
	
			Timestamp recentEnuf = Timestamp.valueOf("2021-08-01 00:00:00");
			Long now = Instant.now().getEpochSecond();
			now = now - ( now % 86_400 );
			Timestamp midnightThisMorningUTC = Timestamp.from(Instant.ofEpochSecond(now));
	
			while ( fRs != null && lRs != null && ! fRs.isAfterLast() && ! lRs.isAfterLast() ) {
	
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
							c.newerInLDP.add(fId);
						}
					} else if ( lMod.after(fMod) ) {
						System.out.println( fId+" M "+fMod+" => "+lMod);
						c.newerInLDP.add(fId);
					} // else unchanged

					lRs = iterateResultSet(lRs, idField, ldpListTable, lId, batchSize, lS);
					fRs = iterateResultSet(fRs, idField, folioCacheTable, fId, batchSize, fS);
	
				} else if ( compare > 0 ) { // fId > lId (lId not in folio cache - new? missed?)
	
					System.out.println(lId+" N");
					c.onlyInLDP.add(lId);
					lRs = iterateResultSet(lRs, idField, ldpListTable, lId, batchSize, lS);
	
				} else { // fId < lId (fId not in ldp - deleted?)
	
					if ( fMod == null || fMod.before(midnightThisMorningUTC) ) {
						System.out.println(fId+" D");
						c.onlyInFC.add(fId);
					}
					fRs = iterateResultSet(fRs, idField, folioCacheTable, fId, batchSize, fS);
	
				}
			}
	
			while ( lRs != null && ! lRs.isAfterLast() ) {
				System.out.println(lId+" N");
				c.onlyInLDP.add(lId);
				lRs = iterateResultSet(lRs, idField, ldpListTable, lId, batchSize, lS);
			}
	
			while ( fRs != null && ! fRs.isAfterLast() ) {
				if ( fMod != null && fMod.before(midnightThisMorningUTC) ) {
					System.out.println(fId+" D");
					c.onlyInFC.add(fId);
				}
				fRs = iterateResultSet(fRs, idField, folioCacheTable, fId, batchSize, fS);
			}
			if ( lRs != null ) lRs.close();
			if ( fRs != null ) fRs.close();
		}
		System.out.printf("%s/%s\n",folioCacheTable,ldpListTable);
		System.out.printf("newerInLDP: %d records (%s,%s,%s)\n", c.newerInLDP.size(),
				random(c.newerInLDP),random(c.newerInLDP),random(c.newerInLDP));
		System.out.printf("onlyInLDP: %d records (%s,%s,%s)\n", c.onlyInLDP.size(),
				random(c.onlyInLDP),random(c.onlyInLDP),random(c.onlyInLDP));
		System.out.printf("onlyInFC: %d records (%s,%s,%s)\n", c.onlyInFC.size(),
				random(c.onlyInFC),random(c.onlyInFC),random(c.onlyInFC));
		return c;

	}

	private static ResultSet iterateResultSet(
			ResultSet rs, String idField, String tableName, String id, int batchSize, Statement s)
					throws SQLException {
		if ( ! rs.next() ) {
			rs.close();
			String sql = String.format(
					"SELECT %s, moddate FROM %s WHERE %s > '%s' ORDER BY %s LIMIT %d",
					idField, tableName, idField, id, idField, batchSize);
			ResultSet newRs = s.executeQuery(sql);
			if ( ! newRs.next() ) {
				System.out.println("no results for "+sql);
				return null; }
			return newRs;
		}
		return rs;
	}

	private static Random rand = null;
	private static Object random(List<String> list) {
		if ( list == null || list.isEmpty() ) return null;
		if ( rand == null ) rand = new Random();
		return list.get(rand.nextInt(list.size()));
	}

}
