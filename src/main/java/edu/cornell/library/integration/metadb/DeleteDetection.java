package edu.cornell.library.integration.metadb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.folio.OkapiClient;
import edu.cornell.library.integration.utilities.Config;

public class DeleteDetection {

	public static void main(String[] args) throws SQLException, IOException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("MetaDB");
		requiredArgs.addAll(Config.getRequiredArgsForDB("Current"));
		Config config = Config.loadConfig(requiredArgs);
		try (Connection metadb = config.getDatabaseConnection("MetaDB");
				Connection inventory = config.getDatabaseConnection("Current");) {
//			DatabaseMetaData dbmd = metadb.getMetaData();
//			try (ResultSet tables = dbmd.getTables(null, "folio_inventory", "%", new String[] { "TABLE" })) {
//				while (tables.next()) {
//					System.out.println(tables.getString("TABLE_NAME"));
//				}
//			}
			Timestamp cursor = getUpdateCursor(inventory);

			Map<String,String> deletedHoldings= getDeletedHoldings(metadb, inventory, config.getOkapi("Folio"), cursor);
			System.out.format("%d deleted holdings (%s)\n",
					deletedHoldings.size(), String.join(", ", deletedHoldings.keySet()));

			Map<String,String> deletedItems= getDeletedItems(metadb, inventory, config.getOkapi("Folio"), cursor);
			System.out.format("%d deleted items (%s)\n",
					deletedItems.size(), String.join(", ", deletedItems.keySet()));
		}
	}

	private static Map<String,String> getDeletedHoldings(
			Connection metadb, Connection inventory, OkapiClient folio, Timestamp cursor)
					throws SQLException, IOException {
		/**
		 * Get holding record versions that have ended since cursor and don't have a more current version.
		 * 
		 * For those that are identified, check the folio cache. If absent, deletion is already done. If present,
		 * use cache to identify instance hrid.
		 * 
		 * For those remaining, check that Folio doesn't actually still contain the holding record.
		 */

		Map<String,String> deletedHoldings = new HashMap<>();
		try (PreparedStatement deletedHoldingsStmt = metadb.prepareStatement(
				"select hr1.hrid, hr1.id"+
				"  from folio_inventory.holdings_record__t__ hr1"+
				"  left join folio_inventory.holdings_record__t__ hr2"+
				"    on hr1.id = hr2.id and hr1.__end = hr2.__start"+
				" where hr1.__end > ?"+
				"   and hr1.__current = false\r\n"+
				"   and hr2.__id is null");
			 PreparedStatement instanceByHoldingStmt = inventory.prepareStatement(
				"select instanceHrid from holdingFolio where hrid = ?")) {
			deletedHoldingsStmt.setTimestamp(1, cursor);
			try (ResultSet rs = deletedHoldingsStmt.executeQuery()) {
				while (rs.next()) {
					String holdingHrid = rs.getString(1);
					String holdingId = rs.getString(2);
					instanceByHoldingStmt.setString(1, holdingHrid);
					String instanceHrid = null;
					try (ResultSet rs2 = instanceByHoldingStmt.executeQuery()) {
						while (rs2.next()) instanceHrid = rs2.getString(1);
					}
					if (instanceHrid == null) continue; //holding not in cache

					List<Map<String,Object>> holdings =
							folio.queryAsList("/holdings-storage/holdings","id=="+holdingId);
					if (holdings.size() == 1) continue;// holding still in folio

					deletedHoldings.put(holdingHrid, instanceHrid);
				}
			}
		}
		return deletedHoldings;
	}

	private static Map<String,String> getDeletedItems(
			Connection metadb, Connection inventory, OkapiClient folio, Timestamp cursor)
					throws SQLException, IOException {
		/**
		 * Get item versions that have ended since cursor and don't have a more current version.
		 * 
		 * For those that are identified, check the folio cache. If absent, deletion is already done. If present,
		 * attempt use cache to identify instance hrid.
		 * 
		 * For those remaining, check that Folio doesn't actually still contain the item.
		 */

		Map<String,String> deletedItems = new HashMap<>();
		try (PreparedStatement deletedItemsStmt = metadb.prepareStatement(
				"select i1.hrid, i1.id from folio_inventory.item__t__ i1"+
				"  left join folio_inventory.item__t__ i2"+
				"    on i1.id = i2.id and i1.__end = i2.__start"+
				" where i1.__end > ?"+
				"   and i1.__current = false"+
				"   and i2.__id is null");
			 PreparedStatement holdingByItemStmt = inventory.prepareStatement(
				"select holdingHrid from itemFolio where hrid = ?");
			 PreparedStatement instanceByHoldingStmt = inventory.prepareStatement(
				"select instanceHrid from holdingFolio where hrid = ?")) {
			deletedItemsStmt.setTimestamp(1, cursor);
			try (ResultSet rs = deletedItemsStmt.executeQuery()) {
				while (rs.next()) {
					String itemHrid = rs.getString(1);
					String itemId = rs.getString(2);
					String holdingHrid = null;
					holdingByItemStmt.setString(1, itemHrid);
					try (ResultSet rs2 = holdingByItemStmt.executeQuery()) {
						while (rs2.next()) holdingHrid = rs2.getString(1);
					}
					if (holdingHrid == null) continue; // item not in cache

					List<Map<String,Object>> items = folio.queryAsList("/item-storage/items","id=="+itemId);
					if (items.size() == 1) continue; // item still in folio

					String instanceHrid = null;
					instanceByHoldingStmt.setString(1, holdingHrid);
					try (ResultSet rs2 = instanceByHoldingStmt.executeQuery()) {
						while (rs2.next()) instanceHrid = rs2.getString(1);
					}
					deletedItems.put(itemHrid, instanceHrid);
				}
			}
		}
		return deletedItems;
	}

	private static Timestamp getUpdateCursor(Connection inventory) throws SQLException {
		try (PreparedStatement pstmt = inventory.prepareStatement(
						"SELECT current_to_date FROM updateCursor WHERE cursor_name = ?")) {
			pstmt.setString(1, "deletes");
			try (ResultSet rs = pstmt.executeQuery()) {
				while (rs.next())
					return rs.getTimestamp(1);
			}
		}
		// default to two days ago
		return new Timestamp(Calendar.getInstance().getTime().getTime()-(48*60*60*1000));
	}

}
