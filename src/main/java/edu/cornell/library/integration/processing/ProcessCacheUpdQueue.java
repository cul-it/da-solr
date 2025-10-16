package edu.cornell.library.integration.processing;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.OkapiClient;
import edu.cornell.library.integration.utilities.Config;

public class ProcessCacheUpdQueue {

	public static void main(String[] args) throws SQLException, IOException {

		Config config = Config.loadConfig(Config.getRequiredArgsForDB("Current"));

		OkapiClient folio = config.getOkapi("Folio");
		try (Connection inventory = config.getDatabaseConnection("Current")) {

			Update upd = getUpdate(inventory);
			while (upd != null) {
				System.out.format("%s %s (%s)\n", upd.type.toUpperCase(), upd.uuid, upd.cause);
				switch (upd.type) {
				case "instance":
					updateInstance(inventory, folio, upd);
					break;
				case "holding":
					updateHolding(inventory, folio, upd);
					break;
				case "item":
					updateItem(inventory, folio, upd);
					break;
				default:
					System.out.println("Still need to implement handling of record type: "+upd.type);
					System.exit(0);
				}
				deleteUpdateFromQueue(inventory, upd);
				upd = getUpdate(inventory);
			}
		}
	}

	private static void updateInstance(Connection inventory, OkapiClient folio, Update upd)
			throws SQLException, IOException {

		String hrid = null;
		if (! upd.cause.equals("ADDED"))
			hrid = getHridFromInventory(inventory, upd);

		try {
			Map<String,Object> instance = mapper.readValue(folio.query("/instance-storage/instances/"+upd.uuid), Map.class);
			hrid = (String)instance.get("hrid");
			String source = (String)instance.get("source");
			try (PreparedStatement rH = inventory.prepareStatement(
					"REPLACE INTO instanceFolio (id, hrid, active, source, moddate, content) VALUES (?,?,?,?,?,?)"); ){
						rH.setString(1, (String)instance.get("id"));
						rH.setString(2, hrid);
						rH.setBoolean(3, ! (Boolean)instance.getOrDefault("discoverySuppress",false) 
										&& ! (Boolean)instance.getOrDefault("staffSuppress",false));
						rH.setString(4, source);
						Map<String,String> meta = Map.class.cast(instance.get("metadata"));
						rH.setTimestamp(5, Timestamp.from(Instant.parse(meta.get("updatedDate").replace("+00:00","Z"))));
						rH.setString(6, mapper.writeValueAsString(instance));
						rH.executeUpdate();
					}
			if (source.equals("MARC"))
				updateBib(inventory, folio, upd);
			try(PreparedStatement iGQ = inventory.prepareStatement(
					"INSERT INTO generationQueue (hrid, priority, cause) VALUES (?, 7, 'metadb instance')");){
				iGQ.setString(1, hrid);  iGQ.executeUpdate();
			}
		} catch (IOException e) {
			if (e.getMessage().equals("Not Found")) {
				if (hrid == null) return;
				try (PreparedStatement dIF = inventory.prepareStatement("DELETE FROM instanceFolio WHERE hrid = ?");
					 PreparedStatement dHF = inventory.prepareStatement("DELETE FROM bibFolio WHERE instanceHrid = ?");
					 PreparedStatement dPMD = inventory.prepareStatement("DELETE FROM processedMarcData WHERE hrid = ?");
					 PreparedStatement iGQ = inventory.prepareStatement(
						"INSERT INTO deleteQueue (hrid, priority, cause) VALUES (?, 1, 'metadb instance')")){
					dIF.setString(1, hrid);  dIF.executeUpdate();
					dHF.setString(1, hrid);  dHF.executeUpdate();
					dPMD.setString(1, hrid);  dPMD.executeUpdate();
					iGQ.setString(1, hrid);  iGQ.executeUpdate();
				}
			}
			else throw(e);
		}

	}


	private static void updateHolding(Connection inventory, OkapiClient folio, Update upd)
			throws SQLException, IOException {

		String hrid = null;
		if (! upd.cause.equals("ADDED"))
			hrid = getHridFromInventory(inventory, upd);

		try {
			Map<String,Object> holding = mapper.readValue(folio.query("/holdings-storage/holdings/"+upd.uuid), Map.class);
			hrid = (String)holding.get("hrid");
			String instanceId = (String)holding.get("instanceId");
			Map<String,Object> instance = getInstanceFromInventory(inventory, null, null, instanceId);
			if (instance == null) {
				updateInstance(inventory, folio, new Update("instance", instanceId, "ADDED"));
				instance = getInstanceFromInventory(inventory, null, null, instanceId);
			}
			if (instance == null) { System.out.println("Impossible. Please diagnose. (holding)");  System.exit(0); }
			String instanceHrid = (String)instance.get("hrid");
			try (PreparedStatement rH = inventory.prepareStatement(
					"REPLACE INTO holdingFolio (id,hrid,instanceId,instanceHrid,active,moddate,content) "+
					" VALUES (?,?,?,?,?,?,?)"); ){
						rH.setString(1, (String)holding.get("id"));
						rH.setString(2, hrid);
						rH.setString(3, instanceId);
						rH.setString(4, instanceHrid);
						rH.setBoolean(5, ! (Boolean)holding.getOrDefault("discoverySuppress",false));
						Map<String,String> meta = Map.class.cast(holding.get("metadata"));
						rH.setTimestamp(6, Timestamp.from(Instant.parse(meta.get("updatedDate").replace("+00:00","Z"))));
						rH.setString(7, mapper.writeValueAsString(holding));
						rH.executeUpdate();
					}
			try(PreparedStatement iGQ = inventory.prepareStatement(
					"INSERT INTO generationQueue (hrid, priority, cause) VALUES (?, 7, 'metadb holding')");
				PreparedStatement iAQ = inventory.prepareStatement(
					"INSERT INTO availQueue (hrid, priority, cause) VALUES (?, 7, 'metadb holding')");){
				iGQ.setString(1, instanceHrid);  iGQ.executeUpdate();
				iAQ.setString(1, instanceHrid);  iAQ.executeUpdate();
			}
		} catch (IOException e) {
			if (e.getMessage().equals("Not Found")) {
				if (hrid == null) return;
				String instanceHrid = checkForInstanceHridInCachedHolding(inventory, upd.uuid);
				try (PreparedStatement dHF = inventory.prepareStatement("DELETE FROM holdingFolio WHERE hrid = ?");
					 PreparedStatement iGQ = inventory.prepareStatement(
						"INSERT INTO generationQueue (hrid, priority, cause) VALUES (?, 7, 'metadb holding')");
					 PreparedStatement iAQ = inventory.prepareStatement(
						"INSERT INTO availQueue (hrid, priority, cause) VALUES (?, 7, 'metadb holding')");) {
					dHF.setString(1, hrid);  dHF.executeUpdate();
					if (instanceHrid == null) return;

					iGQ.setString(1, instanceHrid);  iGQ.executeUpdate();
					iAQ.setString(1, instanceHrid);  iAQ.executeUpdate();
				}
			}
			else throw(e);
		}
	}

	private static void updateItem(Connection inventory, OkapiClient folio, Update upd)
			throws SQLException, IOException {

		System.out.format("%s %s (%s)\n", upd.type, upd.uuid, upd.cause);
		String hrid = null;
		if (! upd.cause.equals("ADDED"))
			hrid = getHridFromInventory(inventory, upd);

		try {
			Map<String,Object> item = mapper.readValue(folio.query("/item-storage/items/"+upd.uuid), Map.class);
			hrid = (String)item.get("hrid");
			String holdingId = (String)item.get("holdingsRecordId");
			Map<String,Object> holding = getHoldingFromInventory(inventory, null, holdingId);
			if (holding == null) {
				updateHolding(inventory, folio, new Update("holding", holdingId, "ADDED"));
				holding = getHoldingFromInventory(inventory, null, holdingId);
			}
			if (holding == null) { System.out.println("Impossible. Please diagnose. (item)");  System.exit(0); }
			Map<String,Object> instance = getInstanceFromInventory(inventory, null, (String)holding.get("instanceHrid"), null);
			String instanceHrid = (String)instance.get("hrid");
			String barcode = (String)item.getOrDefault("barcode", null);
			if (barcode != null && barcode.length() > 14) barcode = null;
			try (PreparedStatement rI = inventory.prepareStatement(
					"REPLACE INTO itemFolio (id, hrid, holdingId, holdingHrid, moddate, barcode, content)" +
					" VALUES (?,?,?,?,?,?,?)"); ){
						rI.setString(1, (String)item.get("id"));
						rI.setString(2, hrid);
						rI.setString(3, holdingId);
						rI.setString(4, (String)holding.get("hrid"));
						Map<String,String> meta = Map.class.cast(item.get("metadata"));
						rI.setTimestamp(5, Timestamp.from(Instant.parse(meta.get("updatedDate").replace("+00:00","Z"))));
						rI.setString(6, barcode);
						rI.setString(7, mapper.writeValueAsString(item));
						rI.executeUpdate();
					}
			try(PreparedStatement iAQ = inventory.prepareStatement(
					"INSERT INTO availQueue (hrid, priority, cause) VALUES (?, 7, 'metadb holding')");){
				iAQ.setString(1, instanceHrid);  iAQ.executeUpdate();
			}
		} catch (IOException e) {
			if (e.getMessage().equals("Not Found")) {
				if (hrid == null) return;
				String instanceHrid = checkForInstanceHridInCachedItem(inventory, upd.uuid);
				try (PreparedStatement dIF = inventory.prepareStatement("DELETE FROM itemFolio WHERE hrid = ?");
					 PreparedStatement iAQ = inventory.prepareStatement(
						"INSERT INTO availQueue (hrid, priority, cause) VALUES (?, 7, 'metadb holding')");) {

					dIF.setString(1, hrid);  dIF.executeUpdate();
					if (instanceHrid != null) {
						iAQ.setString(1, instanceHrid);  iAQ.executeUpdate();
					}
				}
			}
			else throw(e);
		}
	}


	private static void updateBib(Connection inventory, OkapiClient folio, Update upd) throws SQLException, IOException {
		// this presumes that the uuid in the Update record for a bib will be the instance uuid.
		String hrid = getHridFromInventory(inventory, upd);
		if (hrid == null && ! upd.type.equals("DELETE")) {
			updateInstance(inventory, folio, upd);
			hrid = getHridFromInventory(inventory, upd);
		}
		String srsQuery = "/source-storage/records/"+upd.uuid+"/formatted?idType=INSTANCE";
		try {
			String marc = folio.query(srsQuery).replaceAll("\\s*\\n\\s*", " ");
			try (PreparedStatement rBF = inventory.prepareStatement(
					"REPLACE INTO bibFolio (instanceHrid,moddate,content) VALUES (?,?,?)");
				PreparedStatement iGQ = inventory.prepareStatement(
					"INSERT INTO generationQueue (hrid, priority, cause) VALUES (?, 7, 'metadb bib')")) {
				Matcher m = modDateP.matcher(marc);
				Timestamp marcTimestamp = (m.matches())
						? Timestamp.from(Instant.parse(m.group(1).replace("+00:00","Z"))): null;

				rBF.setString(1, hrid);
				rBF.setTimestamp(2, marcTimestamp);
				rBF.setString(3, marc);
				rBF.executeUpdate();
				iGQ.setString(1, hrid);  iGQ.executeUpdate();
			}
		} catch (IOException e) {
			if (e.getMessage().equals("Not Found")) {
				if (hrid == null) return;
				try (PreparedStatement dBF = inventory.prepareStatement("DELETE FROM bibFolio WHERE instanceHrid = ?");
					PreparedStatement iGQ = inventory.prepareStatement(
							"INSERT INTO generationQueue (hrid, priority, cause) VALUES (?, 7, 'metadb bib')")) {
					dBF.setString(1, hrid);  dBF.executeUpdate();
					iGQ.setString(1, hrid);  iGQ.executeUpdate();
				}
			}
			else throw(e);
		}
	}

	private static Map<String,Object> getInstanceFromInventory(Connection inventory, Update upd,
			String instanceHrid, String instanceId)
			throws SQLException {

		if (instanceHrid != null)
			try (PreparedStatement stmt = inventory.prepareStatement(
					"SELECT * FROM instanceFolio WHERE hrid = ?")) {
				stmt.setString(1, instanceHrid);
				try (ResultSet rs = stmt.executeQuery()) { while (rs.next()) { return rsToInstance(rs); } }
			}

		if (instanceId != null)
			try (PreparedStatement stmt = inventory.prepareStatement(
					"SELECT * FROM instanceFolio WHERE id = ?")) {
				stmt.setString(1, instanceId);
				try (ResultSet rs = stmt.executeQuery()) { while (rs.next()) { return rsToInstance(rs); } }
			}

		if (upd != null && upd.type.equals("holding"))
			try (PreparedStatement stmt = inventory.prepareStatement(
					"SELECT i.* FROM instanceFolio i, holdingFolio h WHERE h.id = ? and h.instanceHrid = i.hrid")) {
				stmt.setString(1, upd.uuid);
				try (ResultSet rs = stmt.executeQuery()) { while (rs.next()) { return rsToInstance(rs); } }
			}
		return null;
	}


	private static Map<String,Object> getHoldingFromInventory(Connection inventory, Update upd, String holdingId)
			throws SQLException {

		if (holdingId != null)
			try (PreparedStatement stmt = inventory.prepareStatement(
					"SELECT * FROM holdingFolio WHERE id = ?")) {
				stmt.setString(1, holdingId);
				try (ResultSet rs = stmt.executeQuery()) { while (rs.next()) { return rsToHolding(rs); } }
			}

		if (upd != null && upd.type.equals("holding"))
			try (PreparedStatement stmt = inventory.prepareStatement(
					"SELECT i.* FROM instanceFolio i, holdingFolio h WHERE h.id = ? and h.instanceHrid = i.hrid")) {
				stmt.setString(1, upd.uuid);
				try (ResultSet rs = stmt.executeQuery()) { while (rs.next()) { return rsToHolding(rs); } }
			}
		return null;
	}


	private static Map<String,Object> rsToInstance(ResultSet rs) throws SQLException {
		Map<String,Object> instance = new HashMap<>();
		instance.put("id", rs.getString("id"));
		instance.put("hrid", rs.getString("hrid"));
		instance.put("active", rs.getBoolean("active"));
		instance.put("source", rs.getString("source"));
		instance.put("moddate", rs.getTimestamp("moddate"));
		instance.put("content", rs.getString("content"));
		return instance;

	}

	private static Map<String,Object> rsToHolding(ResultSet rs) throws SQLException {
		Map<String,Object> holding = new HashMap<>();
		holding.put("id", rs.getString("id"));
		holding.put("hrid", rs.getString("hrid"));
		holding.put("instanceId", rs.getString("instanceId"));
		holding.put("instanceHrid", rs.getString("instanceHrid"));
		holding.put("active", rs.getBoolean("active"));
		holding.put("moddate", rs.getTimestamp("moddate"));
		holding.put("content", rs.getString("content"));
		return holding;
	}

	private static String getHridFromInventory(Connection inventory, Update upd) throws SQLException {
		// this presumes that the uuid in the Update record for a bib will be the instance uuid.
		String table = (upd.type.equals("bib"))?"instanceFolio":upd.type+"Folio";
		try(PreparedStatement stmt = inventory.prepareStatement(
				"SELECT hrid FROM "+table+" WHERE id = ?")) {
			stmt.setString(1, upd.uuid);
			try(ResultSet rs = stmt.executeQuery()) { while (rs.next()) return rs.getString("hrid"); }
		}
		return null;
	}

	private static String checkForInstanceHridInCachedHolding(Connection inventory,String holdingId) throws SQLException {
		try(PreparedStatement stmt = inventory.prepareStatement(
				"SELECT instanceHrid FROM holdingFolio WHERE id = ?")) {
			stmt.setString(1, holdingId);
			try(ResultSet rs = stmt.executeQuery()) { while (rs.next()) return rs.getString("instanceHrid"); }
		}
		return null;
	}

	private static String checkForInstanceHridInCachedItem(Connection inventory, String itemId) throws SQLException {
		try(PreparedStatement stmt = inventory.prepareStatement(
				"SELECT instanceHrid FROM holdingFolio h, itemFolio i WHERE h.hrid = i.holdingHrid and i.id = ?")) {
			stmt.setString(1,itemId);
			try(ResultSet rs = stmt.executeQuery()) { while (rs.next()) return rs.getString("instanceHrid"); }
		}
		return null;
	}

	private static Update getUpdate(Connection inventory) throws SQLException {
		try (Statement stmt = inventory.createStatement();
			 ResultSet rs = stmt.executeQuery("SELECT * FROM cacheUpdateQueue ORDER BY record_date LIMIT 1")) {
			while (rs.next())
				return new Update(rs.getString("type"),rs.getString("uuid"),rs.getString("cause"));
		}
		return null;
	}

	private static Update deleteUpdateFromQueue(Connection inventory, Update upd) throws SQLException {
		try (PreparedStatement pstmt = inventory.prepareStatement(
				"DELETE FROM cacheUpdateQueue WHERE type = ? and uuid = ?")) {
			pstmt.setString(1,upd.type);
			pstmt.setString(2, upd.uuid);
			pstmt.executeUpdate();
		}
		return null;
	}

	private static class Update {
		String type;
		String uuid;
		String cause;
		Update(String type, String uuid, String cause) {
			this.type = type;
			this.uuid = uuid;
			this.cause = cause;
		}
	}

	static Pattern modDateP = Pattern.compile("^.*\"updatedDate\" *: *\"([^\"]+)\".*$");

	static ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.setSerializationInclusion(Include.NON_EMPTY);
	}

}
