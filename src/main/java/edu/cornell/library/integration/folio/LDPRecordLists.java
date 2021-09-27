package edu.cornell.library.integration.folio;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.cornell.library.integration.utilities.Config;

public class LDPRecordLists {

	public static void main(String[] args) throws SQLException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(Config.getRequiredArgsForDB("LDP"));
		Config config = Config.loadConfig(requiredArgs);
		try (Connection ldp = config.getDatabaseConnection("LDP");
			  Connection current = config.getDatabaseConnection("Current")) {

			populateBibLDPList(current,ldp);
			populateInstanceLDPList(current,ldp);
			populateHoldingLDPList(current,ldp);
			populateLoanLDPList(current,ldp);
			populateOrderLDPList(current,ldp);
			populateOrderLineLDPList(current,ldp);
			populateItemLDPList(current,ldp);
		}
	}

	private static void populateInstanceLDPList( Connection current, Connection ldp )
			throws SQLException {

		try ( 
				PreparedStatement toEmpty = current.prepareStatement("DELETE FROM instanceLDP");
				PreparedStatement to = current.prepareStatement(
						"INSERT INTO instanceLDP ( hrid, moddate ) VALUES (?,?)");
				PreparedStatement from = ldp.prepareStatement(
						"SELECT hrid, JSON_EXTRACT_PATH_TEXT(data,'metadata','updatedDate') AS moddate"
						+" FROM inventory_instances WHERE hrid > ? ORDER BY hrid LIMIT 10000")){

			toEmpty.executeUpdate();
			syphonData( from, to, false );
		}
	}

	private static void populateBibLDPList( Connection current, Connection ldp )
			throws SQLException {

		try ( 
				PreparedStatement toEmpty = current.prepareStatement("DELETE FROM bibLDP");
				PreparedStatement to = current.prepareStatement(
						"INSERT INTO bibLDP ( instanceHrid, moddate ) VALUES (?,?)");
				PreparedStatement from = ldp.prepareStatement(
						"SELECT instance_hrid, updated_date"
						+" FROM srs_records "
						+"WHERE instance_hrid > ? AND state = 'ACTUAL' "
						+"ORDER BY instance_hrid LIMIT 10000")){

			toEmpty.executeUpdate();
			syphonData( from, to, true );
		}
	}

	private static void populateHoldingLDPList( Connection current, Connection ldp )
			throws SQLException {

		try ( 
				PreparedStatement toEmpty = current.prepareStatement("DELETE FROM holdingLDP");
				PreparedStatement to = current.prepareStatement(
						"INSERT INTO holdingLDP ( hrid, moddate ) VALUES (?,?)");
				PreparedStatement from = ldp.prepareStatement(
						"SELECT hrid, JSON_EXTRACT_PATH_TEXT(data,'metadata','updatedDate') AS moddate"
						+" FROM inventory_holdings WHERE hrid > ? ORDER BY hrid LIMIT 10000")){

			toEmpty.executeUpdate();
			syphonData( from, to, false );
		}
	}

	private static void populateItemLDPList( Connection current, Connection ldp )
			throws SQLException {

		try ( 
				PreparedStatement toEmpty = current.prepareStatement("DELETE FROM itemLDP");
				PreparedStatement to = current.prepareStatement(
						"INSERT INTO itemLDP ( hrid, moddate ) VALUES (?,?)");
				PreparedStatement from = ldp.prepareStatement(
						"SELECT hrid, JSON_EXTRACT_PATH_TEXT(data,'metadata','updatedDate') AS moddate"
						+" FROM inventory_items WHERE hrid > ? ORDER BY hrid LIMIT 10000")){

			toEmpty.executeUpdate();
			syphonData( from, to, false );
		}
	}

	private static void populateLoanLDPList( Connection current, Connection ldp )
			throws SQLException {

		try ( 
				PreparedStatement toEmpty = current.prepareStatement("DELETE FROM loanLDP");
				PreparedStatement to = current.prepareStatement(
						"INSERT INTO loanLDP ( id, moddate ) VALUES (?,?)");
				PreparedStatement from = ldp.prepareStatement(
						"SELECT id, JSON_EXTRACT_PATH_TEXT(data,'metadata','updatedDate') AS moddate"
						+" FROM circulation_loans WHERE id > ? ORDER BY id LIMIT 10000")){

			toEmpty.executeUpdate();
			syphonData( from, to, false );
		}
	}

	private static void populateOrderLDPList( Connection current, Connection ldp )
			throws SQLException {

		try ( 
				PreparedStatement toEmpty = current.prepareStatement("DELETE FROM orderLDP");
				PreparedStatement to = current.prepareStatement(
						"INSERT INTO orderLDP ( id, moddate ) VALUES (?,?)");
				PreparedStatement from = ldp.prepareStatement(
						"SELECT id, JSON_EXTRACT_PATH_TEXT(data,'metadata','updatedDate') AS moddate"
						+" FROM po_purchase_orders WHERE id > ? ORDER BY id LIMIT 10000")){

			toEmpty.executeUpdate();
			syphonData( from, to, false );
		}
	}

	private static void populateOrderLineLDPList( Connection current, Connection ldp )
			throws SQLException {

		try ( 
				PreparedStatement toEmpty = current.prepareStatement("DELETE FROM orderLineLDP");
				PreparedStatement to = current.prepareStatement(
						"INSERT INTO orderLineLDP ( id, moddate ) VALUES (?,?)");
				PreparedStatement from = ldp.prepareStatement(
						"SELECT id, JSON_EXTRACT_PATH_TEXT(data,'metadata','updatedDate') AS moddate"
						+" FROM po_lines WHERE id > ? ORDER BY id LIMIT 10000")){

			toEmpty.executeUpdate();
			syphonData( from, to, false );
		}
	}

	private static void syphonData(
			PreparedStatement from, PreparedStatement to, Boolean srs)
					throws SQLException {
		from.setFetchSize(1_000);
		String cursor = "0";
		boolean done = false;
		while ( ! done ) {
			from.setString(1, cursor);
			try (ResultSet fromRS = from.executeQuery(
					) ) {
				int i = 0;
				while ( fromRS.next() ) {
					cursor = fromRS.getString(1);
					to.setString(1, cursor);
					String date = fromRS.getString(2);
					if ( srs ) {
						Matcher m = srsRecDTP.matcher(date);
						if ( m.matches() ) date = m.group(1)+"+00";
						to.setTimestamp(2,Timestamp.from(srsRecDT.parse(date,Instant::from)));
					} else
						to.setTimestamp(2,Timestamp.from(isoDT.parse(date,Instant::from)));
					to.addBatch();
					if (++i % 1000 == 0) {
						to.executeBatch();
					}
				}
				if ( i % 1000 != 0 ) to.executeBatch();
				System.out.println(cursor);
				if ( i < 10_000 ) done = true;
			}
		}
	}

	private static DateTimeFormatter isoDT = DateTimeFormatter.ISO_DATE_TIME;
	private static DateTimeFormatter srsRecDT =
			DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSx");
	private static Pattern srsRecDTP = Pattern.compile("(.*\\.\\d{3})\\d*\\+00");
}
