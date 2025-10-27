package edu.cornell.library.integration.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CopyCachedFolioRecordsToTestCache {

	public static void main(String[] args) throws SQLException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("InventoryProd");
		requiredArgs.addAll(Config.getRequiredArgsForDB("InventoryTest"));

		Config config = Config.loadConfig(requiredArgs);

		List<String> instanceHrids = Arrays.asList("5043174");

		try (Connection inventoryProd = config.getDatabaseConnection("InventoryProd");
				Connection inventoryTest = config.getDatabaseConnection("InventoryTest"); ) {

			copyInstances( inventoryProd, inventoryTest, instanceHrids);
			copyBibs( inventoryProd, inventoryTest, instanceHrids);
			List<String> holdingHrids = copyHoldings(  inventoryProd, inventoryTest, instanceHrids);
			List<String> itemHrids = copyItems(  inventoryProd, inventoryTest, holdingHrids);
			copyLoans(  inventoryProd, inventoryTest, itemHrids);
			copyRequests(  inventoryProd, inventoryTest, itemHrids);
		}


	}


	private static void copyRequests(Connection inventoryProd, Connection inventoryTest, List<String> itemHrids) throws SQLException {
		try(PreparedStatement getRequests = inventoryProd.prepareStatement("SELECT * FROM requestFolio WHERE itemHrid = ?");
			PreparedStatement putRequests = inventoryTest.prepareStatement(
					"REPLACE INTO requestFolio(id, itemId, itemHrid, moddate, content) VALUES (?, ?, ?, ?, ?)")){
			for (String itemHrid : itemHrids) {
				getRequests.setString(1, itemHrid);
				try (ResultSet rs = getRequests.executeQuery()) {
					while (rs.next()) {
						putRequests.setString(1, rs.getString("id"));
						putRequests.setString(2, rs.getString("itemId"));
						putRequests.setString(3, rs.getString("itemHrid"));
						putRequests.setTimestamp(4, rs.getTimestamp("moddate"));
						putRequests.setString(5, rs.getString("content"));
						putRequests.addBatch();
						System.out.format("REQUEST %s (for item %s)\n",rs.getString("id"), rs.getString("itemHrid"));
					}
				}
			}
			putRequests.executeBatch();
		}
	}


	private static void copyLoans(Connection inventoryProd, Connection inventoryTest, List<String> itemHrids) throws SQLException {
		try(PreparedStatement getLoan = inventoryProd.prepareStatement("SELECT * FROM loanFolio WHERE itemHrid = ?");
			PreparedStatement putLoan = inventoryTest.prepareStatement(
					"REPLACE INTO loanFolio(id, holdingId, itemHrid, moddate, content) VALUES (?, ?, ?, ?, ?)")){
			for (String itemHrid : itemHrids) {
				getLoan.setString(1, itemHrid);
				try (ResultSet rs = getLoan.executeQuery()) {
					while (rs.next()) {
						putLoan.setString(1, rs.getString("id"));
						putLoan.setString(2, rs.getString("holdingId"));
						putLoan.setString(3, rs.getString("itemHrid"));
						putLoan.setTimestamp(4, rs.getTimestamp("moddate"));
						putLoan.setString(5, rs.getString("content"));
						putLoan.addBatch();
						System.out.format("LOAN %s (for item %s)\n",rs.getString("id"), rs.getString("itemHrid"));
					}
				}
			}
			putLoan.executeBatch();
		}
	}


	private static List<String> copyItems(Connection inventoryProd, Connection inventoryTest, List<String> instanceHrids) throws SQLException {
		List<String> itemHrids = new ArrayList<>();
		try(PreparedStatement getItem = inventoryProd.prepareStatement("SELECT * FROM itemFolio WHERE holdingHrid = ?");
			PreparedStatement putItem = inventoryTest.prepareStatement(
					"REPLACE INTO itemFolio(id, hrid, holdingId, holdingHrid, sequence, barcode, moddate, content)"
					+ " VALUES (?, ?, ?, ?, ?, ?, ? , ?)")){

			for (String instanceHrid : instanceHrids) {
				getItem.setString(1, instanceHrid);
				try (ResultSet rs = getItem.executeQuery()) {
					while (rs.next()) {
						putItem.setString(1, rs.getString("id"));
						putItem.setString(2, rs.getString("hrid"));
						putItem.setString(3, rs.getString("holdingId"));
						putItem.setString(4, rs.getString("holdingHrid"));
						putItem.setInt(5, rs.getInt("sequence"));
						putItem.setString(6, rs.getString("barcode"));
						putItem.setTimestamp(7, rs.getTimestamp("moddate"));
						putItem.setString(8, rs.getString("content"));
						putItem.addBatch();
						itemHrids.add(rs.getString("hrid"));
						System.out.format("ITEM %s (for holding %s)\n",rs.getString("id"), rs.getString("holdingHrid"));
					}
				}
			}
			putItem.executeBatch();
		}
		return itemHrids;
	}

	private static List<String> copyHoldings(Connection inventoryProd, Connection inventoryTest, List<String> instanceHrids) throws SQLException {
		List<String> holdingHrids = new ArrayList<>();
		try(PreparedStatement getHolding = inventoryProd.prepareStatement("SELECT * FROM holdingFolio WHERE instanceHrid = ?");
			PreparedStatement putHolding = inventoryTest.prepareStatement(
					"REPLACE INTO holdingFolio(id, hrid, instanceId, instanceHrid, active, moddate, content, podCurrent)"
					+ " VALUES (?, ?, ?, ?, ?, ?, ? , ?)")){

			for (String instanceHrid : instanceHrids) {
				getHolding.setString(1, instanceHrid);
				try (ResultSet rs = getHolding.executeQuery()) {
					while (rs.next()) {
						putHolding.setString(1, rs.getString("id"));
						putHolding.setString(2, rs.getString("hrid"));
						putHolding.setString(3, rs.getString("instanceId"));
						putHolding.setString(4, rs.getString("instanceHrid"));
						putHolding.setInt(5, rs.getInt("active"));
						putHolding.setTimestamp(6, rs.getTimestamp("moddate"));
						putHolding.setString(7, rs.getString("content"));
						putHolding.setInt(8, rs.getInt("podCurrent"));
						putHolding.addBatch();
						holdingHrids.add(rs.getString("hrid"));
						System.out.format("HOLDING %s (for instance %s)\n",rs.getString("id"), rs.getString("instanceHrid"));
					}
				}
			}
			putHolding.executeBatch();
		}
		return holdingHrids;
	}

	private static void copyInstances(Connection inventoryProd, Connection inventoryTest, List<String> instanceHrids) throws SQLException {
		try(PreparedStatement getInstance = inventoryProd.prepareStatement("SELECT * FROM instanceFolio WHERE hrid = ?");
			PreparedStatement putInstance = inventoryTest.prepareStatement(
					"REPLACE INTO instanceFolio (id, hrid, active, source, moddate, content) VALUES (?, ?, ?, ?, ?, ?) ")){

			for (String instanceHrid : instanceHrids) {
				getInstance.setString(1, instanceHrid);
				try (ResultSet rs = getInstance.executeQuery()) {
					while (rs.next()) {
						putInstance.setString(1, rs.getString("id"));
						putInstance.setString(2, rs.getString("hrid"));
						putInstance.setInt(3, rs.getInt("active"));
						putInstance.setString(4, rs.getString("source"));
						putInstance.setTimestamp(5, rs.getTimestamp("moddate"));
						putInstance.setString(6, rs.getString("content"));
						putInstance.addBatch();
						System.out.format("INSTANCE %s\n",rs.getString("id"));
					}
				}
			}
			putInstance.executeBatch();
		}
	}

	private static void copyBibs(Connection inventoryProd, Connection inventoryTest, List<String> instanceHrids) throws SQLException {
		try(PreparedStatement getBib = inventoryProd.prepareStatement("SELECT * FROM bibFolio WHERE instanceHrid = ?");
			PreparedStatement putBib = inventoryTest.prepareStatement(
					"REPLACE INTO bibFolio (instanceHrid, moddate, content) VALUES (?, ?, ?) ")){

			for (String instanceHrid : instanceHrids) {
				getBib.setString(1, instanceHrid);
				try (ResultSet rs = getBib.executeQuery()) {
					while (rs.next()) {
						putBib.setString(1, rs.getString("instanceHrid"));
						putBib.setTimestamp(2, rs.getTimestamp("moddate"));
						putBib.setString(3, rs.getString("content"));
						putBib.addBatch();
						System.out.format("BIB (for instance %s)\n", rs.getString("instanceHrid"));
					}
				}
			}
			putBib.executeBatch();
		}
	}

}
