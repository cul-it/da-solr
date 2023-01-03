package edu.cornell.library.integration.processing;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.LDPRecordLists;
import edu.cornell.library.integration.folio.OkapiClient;
import edu.cornell.library.integration.utilities.ComparisonLists;
import edu.cornell.library.integration.utilities.Config;

public class LDPChangeDetection {

	public static void main(String[] args) throws SQLException, IOException, InterruptedException {

		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(Config.getRequiredArgsForDB("LDP"));
		Config config = Config.loadConfig(requiredArgs);
		try (Connection ldp = config.getDatabaseConnection("LDP");
			  Connection inventory = config.getDatabaseConnection("Current");
			  PreparedStatement queueDelete = inventory.prepareStatement
				("INSERT INTO deleteQueue (hrid,priority,cause,record_date) VALUES (?,6,'LDP',NOW())");
			  PreparedStatement queueAvail = inventory.prepareStatement
				("INSERT INTO availabilityQueue (hrid,priority,cause,record_date) VALUES (?,6,'LDP',?)");
			  PreparedStatement queueGen = inventory.prepareStatement
				("INSERT INTO generationQueue (hrid,priority,cause,record_date) VALUES (?,6,'LDP',?)")) {

			OkapiClient folio = config.getOkapi("Folio");

			LDPRecordLists.populateInstanceLDPList(inventory, ldp);
			LDPRecordLists.populateBibLDPList(inventory, ldp);
			LDPRecordLists.populateHoldingLDPList(inventory, ldp);
			LDPRecordLists.populateItemLDPList(inventory, ldp);


			{ // INSTANCES
				ComparisonLists c = ResourceListComparison.compareLists(
						inventory,"instanceFolio", "instanceLDP", "hrid" );
				processInstanceDiffs(inventory, folio, c, queueGen, queueDelete);
			}

			{ // BIBS
				ComparisonLists c = ResourceListComparison.compareLists(
						inventory,"bibFolio", "bibLDP", "instanceHrid" );
				processBibDiffs(inventory, folio, c, queueGen);
			}

			{ // HOLDINGS
				ComparisonLists c = ResourceListComparison.compareLists(
						inventory,"holdingFolio", "holdingLDP", "hrid" );
				processHoldingDiffs(inventory, folio, c);
			}

			{ // ITEMS
				ComparisonLists c = ResourceListComparison.compareLists(
						inventory,"itemFolio", "itemLDP", "hrid" );
				processItemDiffs(inventory, folio, c);
			}

		}
	}

	private static void processBibDiffs(
			Connection inventory,
			OkapiClient folio,
			ComparisonLists c,
			PreparedStatement queueGen
			) throws SQLException, IOException, InterruptedException {
		System.out.printf("newerInLDP: %d bibs\n", c.newerInLDP.size());
		System.out.printf("onlyInLDP: %d bibs\n", c.onlyInLDP.size());
		System.out.printf("onlyInFC: %d bibs\n", c.onlyInFC.size());

		if ( ! c.newerInLDP.isEmpty() ) {
			System.out.println("Out of date bibs in Folio cache:");
			try (
			PreparedStatement instanceIdByHridStmt = inventory.prepareStatement(
					"SELECT id FROM instanceFolio WHERE hrid = ?");
			PreparedStatement cacheReplaceStmt = inventory.prepareStatement(
					"REPLACE INTO bibFolio (instanceHrid,moddate,content) VALUES (?,?,?)")){
				for (String hrid : c.newerInLDP ) {
					System.out.println(hrid);
					instanceIdByHridStmt.setString(1, hrid);
					String id = null;
					try ( ResultSet rs = instanceIdByHridStmt.executeQuery() ) {
						while (rs.next()) id = rs.getString(1);
					}
					if ( id == null ) {
						System.out.printf("Instance %s not in folio cache.\n",hrid);
						continue;
					}
					String marc = folio.query("/source-storage/records/"+id+"/formatted?idType=INSTANCE")
							.replaceAll("\\s*\\n\\s*", " ");
					Matcher m = modDateP.matcher(marc);
					Timestamp marcTimestamp = (m.matches())
							? Timestamp.from(Instant.parse(m.group(1).replace("+00:00","Z"))): null;
					cacheReplaceStmt.setString(1, hrid);
					cacheReplaceStmt.setTimestamp(2, marcTimestamp);
					cacheReplaceStmt.setString(3, marc);
					cacheReplaceStmt.executeUpdate();
					queueGen.setString(1, hrid);
					queueGen.setTimestamp(2, marcTimestamp);
					queueGen.executeUpdate();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
						throw new InterruptedException(e.getMessage());
					}
				}
			}
		}

		int falseDeletes = 0;
		if ( ! c.onlyInFC.isEmpty() ) {
			System.out.println("Apparently deleted bibs in Folio cache:");
			try ( PreparedStatement instanceIdByHridStmt = inventory.prepareStatement(
					"SELECT id FROM instanceFolio WHERE hrid = ?") ){
				for (String hrid : c.onlyInFC ) {
					System.out.println(hrid);
					instanceIdByHridStmt.setString(1, hrid);
					String id = null;
					try ( ResultSet rs = instanceIdByHridStmt.executeQuery() ) {
						while (rs.next()) id = rs.getString(1);
					}
					if ( id == null ) {
						System.out.printf("Instance %s not in folio cache.\n",hrid);
						continue;
					}
					try {
						String marc = folio.query("/source-storage/records/"+id+"/formatted?idType=INSTANCE");
						System.out.printf("%s is not deleted. (%d)\n",hrid,++falseDeletes);
						if ( falseDeletes > 400 ) {
							System.out.println(
									"High number of records falsely appearing deleted may indicate "
									+ "LDP issues. Quitting.");
							System.exit(1);
						}
					} catch ( IOException e ) {
						if ( e.getMessage().equals("Not Found") ) {
							System.out.println("Yes, this record seems to be deleted.");
						} else {
							e.printStackTrace();
							throw new IOException( e.getMessage() );
						}
					}
				}
			}
		}

		if ( ! c.onlyInLDP.isEmpty() ) {
			System.out.println("Missing bibs in Folio cache:");
			try (
			PreparedStatement instanceIdByHridStmt = inventory.prepareStatement(
					"SELECT id FROM instanceFolio WHERE hrid = ?");
			PreparedStatement cacheReplaceStmt = inventory.prepareStatement(
					"REPLACE INTO bibFolio (instanceHrid,moddate,content) VALUES (?,?,?)")){
				for (String hrid : c.onlyInLDP ) {
					System.out.println(hrid);
					instanceIdByHridStmt.setString(1, hrid);
					String id = null;
					try ( ResultSet rs = instanceIdByHridStmt.executeQuery() ) {
						while (rs.next()) id = rs.getString(1);
					}
					if ( id == null ) {
						System.out.printf("Instance %s not in folio cache.\n",hrid);
						continue;
					}
					String marc = folio.query("/source-storage/records/"+id+"/formatted?idType=INSTANCE")
							.replaceAll("\\s*\\n\\s*", " ");
					Matcher m = modDateP.matcher(marc);
					Timestamp marcTimestamp = (m.matches())
							? Timestamp.from(Instant.parse(m.group(1).replace("+0000","Z"))): null;
					cacheReplaceStmt.setString(1, hrid);
					cacheReplaceStmt.setTimestamp(2, marcTimestamp);
					cacheReplaceStmt.setString(3, marc);
					cacheReplaceStmt.executeUpdate();
					queueGen.setString(1, hrid);
					queueGen.setTimestamp(2, marcTimestamp);
					queueGen.executeUpdate();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
						throw new InterruptedException(e.getMessage());
					}
				}
			}
		}
	}

	private static void processInstanceDiffs(
			Connection inventory,
			OkapiClient folio,
			ComparisonLists c,
			PreparedStatement queueGen,
			PreparedStatement queueDelete
			) throws SQLException, IOException, InterruptedException {
		System.out.printf("newerInLDP: %d instances\n", c.newerInLDP.size());
		System.out.printf("onlyInLDP: %d instances\n", c.onlyInLDP.size());
		System.out.printf("onlyInFC: %d instances\n", c.onlyInFC.size());

		if ( ! c.newerInLDP.isEmpty() ) {
			try (
			PreparedStatement instanceIdByHridStmt = inventory.prepareStatement(
					"SELECT id FROM instanceFolio WHERE hrid = ?");
			PreparedStatement cacheReplaceStmt = inventory.prepareStatement(
					"REPLACE INTO instanceFolio (id, hrid, active, source, moddate, content) "+
					" VALUES (?,?,?,?,?,?)") ){
				for ( String hrid : c.newerInLDP ) {
					System.out.println(hrid);
					instanceIdByHridStmt.setString(1, hrid);
					String id = null;
					try ( ResultSet rs = instanceIdByHridStmt.executeQuery() ) {
						while (rs.next()) id = rs.getString(1);
					}
					if ( id == null ) {
						System.out.printf(
								"Instance %s not in folio cache, though it was a minute ago.\n",hrid);
						System.exit(1);
					}
					List<Map<String,Object>> instances =
							folio.queryAsList("/instance-storage/instances", "id=="+id);
					for (Map<String,Object> instance : instances) {
						Object o1 = instance.get("discoverySuppress");
						boolean active1 = (o1 == null)?true
								:String.class.isInstance(o1)?!Boolean.valueOf((String)o1):!(boolean)o1;
						Object o2 = instance.get("staffSuppress");
						boolean active2 = (o2 == null)?true
								:String.class.isInstance(o2)?!Boolean.valueOf((String)o2):!(boolean)o2;
						boolean active = active1 && active2;
						String source = (String)instance.get("source");
						Map<String,String> metadata = (Map<String,String>) instance.get("metadata");
						Timestamp moddate = Timestamp.from(Instant.parse(
								metadata.get("updatedDate").replace("+00:00","Z")));
						cacheReplaceStmt.setString(1, id);
						cacheReplaceStmt.setString(2, hrid);
						cacheReplaceStmt.setBoolean(3, active);
						cacheReplaceStmt.setString(4, source);
						cacheReplaceStmt.setTimestamp(5, moddate);
						cacheReplaceStmt.setString(6, mapper.writeValueAsString(instance));
						int i = cacheReplaceStmt.executeUpdate();
						System.out.println(i);
						queueGen.setString(1, hrid);
						queueGen.setTimestamp(2, moddate);
						queueGen.executeUpdate();
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
							throw new InterruptedException(e.getMessage());
						}
					}
				}
			}
		} // END update processing

		int falseDeletes = 0;
		if ( ! c.onlyInFC.isEmpty() ) {
			try (
			PreparedStatement instanceIdByHridStmt = inventory.prepareStatement(
					"SELECT id FROM instanceFolio WHERE hrid = ?") ){
				for ( String hrid : c.onlyInFC ) {
					System.out.println(hrid);
					instanceIdByHridStmt.setString(1, hrid);
					String id = null;
					try ( ResultSet rs = instanceIdByHridStmt.executeQuery() ) {
						while (rs.next()) id = rs.getString(1);
					}
					if ( id == null ) {
						System.out.printf(
								"Instance %s not in folio cache, though it was a minute ago.\n",hrid);
						System.exit(1);
					}
					List<Map<String,Object>> instances =
							folio.queryAsList("/instance-storage/instances", "id=="+id);
					if ( instances.isEmpty() ) {
						queueDelete.setString(1, hrid);
						queueDelete.executeUpdate();
					} else {
						System.out.printf("%s is not deleted. (%d)\n",hrid,++falseDeletes);
						if ( falseDeletes > 400 ) {
							System.out.println(
									"High number of records falsely appearing deleted may indicate "
									+ "LDP issues. Quitting.");
							System.exit(1);
						}
					}
				}
			}
		} // END delete processing
		
	}

	private static void processHoldingDiffs(
			Connection inventory,
			OkapiClient folio,
			ComparisonLists c
			) throws SQLException, IOException, InterruptedException {
		System.out.printf("newerInLDP: %d holdings\n", c.newerInLDP.size());
		System.out.printf("onlyInLDP: %d holdings\n", c.onlyInLDP.size());
		System.out.printf("onlyInFC: %d holdings\n", c.onlyInFC.size());

		if ( ! c.newerInLDP.isEmpty() ) {
			System.out.println("Out of date holdings in Folio cache:");
			try (
			PreparedStatement holdingIdByHridStmt = inventory.prepareStatement(
					"SELECT id, instanceHrid FROM holdingFolio WHERE hrid = ?");
			PreparedStatement cacheReplaceStmt = inventory.prepareStatement(
					"REPLACE INTO holdingFolio (id,hrid,instanceId,instanceHrid,active,moddate,content) "+
					" VALUES (?,?,?,?,?,?,?)");
			PreparedStatement queueGen = inventory.prepareStatement
					("INSERT INTO generationQueue (hrid,priority,cause,record_date) VALUES (?,6,'LDP',?)")){
				for (String hrid : c.newerInLDP ) {
					System.out.println(hrid);
					holdingIdByHridStmt.setString(1, hrid);
					String id = null;
					String instanceHrid = null;
					try ( ResultSet rs = holdingIdByHridStmt.executeQuery() )
					{ while (rs.next()) { id = rs.getString(1); instanceHrid = rs.getString(2); } }
					if ( id == null ) {
						System.out.printf("Holding %s not in folio cache.\n",hrid);
						continue;
					}
					List<Map<String,Object>> holdings =
							folio.queryAsList("/holdings-storage/holdings","id=="+id);
					for ( Map<String,Object> holding : holdings ) {
						Object o1 = holding.get("discoverySuppress");
						boolean active1 = (o1 == null)?true
								:String.class.isInstance(o1)?!Boolean.valueOf((String)o1):!(boolean)o1;
						Object o2 = holding.get("staffSuppress");
						boolean active2 = (o2 == null)?true
								:String.class.isInstance(o2)?!Boolean.valueOf((String)o2):!(boolean)o2;
						boolean active = active1 && active2;
						Map<String,String> metadata = (Map<String,String>) holding.get("metadata");
						Timestamp moddate = Timestamp.from(Instant.parse(
								metadata.get("updatedDate").replace("+00:00","Z")));
						String instanceId = (String)holding.get("instanceId");

						cacheReplaceStmt.setString(1, id);
						cacheReplaceStmt.setString(2, hrid);
						cacheReplaceStmt.setString(3, instanceId);
						cacheReplaceStmt.setString(4, (instanceHrid==null)?"":instanceHrid);
						cacheReplaceStmt.setBoolean(5, active);
						cacheReplaceStmt.setTimestamp(6, moddate);
						cacheReplaceStmt.setString(7,mapper.writeValueAsString(holding));
						cacheReplaceStmt.executeUpdate();
						if ( instanceHrid != null ) {
							queueGen.setString(1, instanceHrid);
							queueGen.setTimestamp(2, moddate);
							queueGen.executeUpdate();
						}
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
							throw new InterruptedException(e.getMessage());
						}
					}
				}
			}
		}

		int falseDeletes = 0;
		if ( ! c.onlyInFC.isEmpty() ) {
			System.out.println("Apparently deleted holdings in Folio cache:");
			try ( PreparedStatement holdingIdByHridStmt = inventory.prepareStatement(
					"SELECT id, instanceHrid FROM holdingFolio WHERE hrid = ?");
					PreparedStatement deleteCache = inventory.prepareStatement(
							"DELETE FROM holdingFolio WHERE hrid = ?");
					PreparedStatement queueAvail = inventory.prepareStatement
							("INSERT INTO availabilityQueue (hrid,priority,cause,record_date)"
									+ " VALUES (?,6,'LDP',NOW())");
					PreparedStatement queueGen = inventory.prepareStatement
							("INSERT INTO generationQueue (hrid,priority,cause,record_date)"
									+ " VALUES (?,6,'LDP',NOW())")){
				for (String hrid : c.onlyInFC ) {
					System.out.println(hrid);
					holdingIdByHridStmt.setString(1, hrid);
					String id = null;
					String instanceHrid = null;
					try ( ResultSet rs = holdingIdByHridStmt.executeQuery() ) {
						while (rs.next()) { id = rs.getString(1); instanceHrid = rs.getString(2); }
					}
					if ( id == null ) {
						System.out.printf("Holding %s not in folio cache.\n",hrid);
						continue;
					}
					List<Map<String,Object>> holdings =
							folio.queryAsList("/holdings-storage/holdings","id=="+id);
					if ( holdings.isEmpty() ) {
						deleteCache.setString(1, hrid);
						deleteCache.executeUpdate();
						queueGen.setString(1, instanceHrid);
						queueGen.executeUpdate();
						queueAvail.setString(1, instanceHrid);
						queueAvail.executeUpdate();
					} else {
						System.out.printf("%s is not deleted. (%d)\n",hrid,++falseDeletes);
						if ( falseDeletes > 400 ) {
							System.out.println(
									"High number of records falsely appearing deleted may indicate "
									+ "LDP issues. Quitting.");
							System.exit(1);
						}
					}
				}
			}
		}
	}

	private static void processItemDiffs(
			Connection inventory,
			OkapiClient folio,
			ComparisonLists c
			) throws SQLException, IOException, InterruptedException {
		System.out.printf("newerInLDP: %d items\n", c.newerInLDP.size());
		System.out.printf("onlyInLDP: %d items\n", c.onlyInLDP.size());
		System.out.printf("onlyInFC: %d items\n", c.onlyInFC.size());

		if ( ! c.newerInLDP.isEmpty() ) {
			System.out.println("Out of date holdings in Folio cache:");
			try (
			PreparedStatement itemIdByHridStmt = inventory.prepareStatement(
					"SELECT id, holdingHrid FROM itemFolio WHERE hrid = ?");
			PreparedStatement instanceHridByHoldingHrid = inventory.prepareStatement(
					"SELECT instanceHrid FROM holdingFolio WHERE hrid = ?");
			PreparedStatement cacheReplaceStmt = inventory.prepareStatement(
					"REPLACE INTO itemFolio (id, hrid, holdingId, holdingHrid, moddate, barcode, content) "+
					" VALUES (?,?,?,?,?,?,?)");
			PreparedStatement availGen = inventory.prepareStatement
					("INSERT INTO availabilityQueue (hrid,priority,cause,record_date) VALUES (?,6,'LDP',?)")){
				ITEM: for (String hrid : c.newerInLDP ) {
					System.out.println(hrid);
					itemIdByHridStmt.setString(1, hrid);
					String id = null;
					String holdingHrid = null;
					try ( ResultSet rs = itemIdByHridStmt.executeQuery() )
					{ while (rs.next()) { id = rs.getString(1); holdingHrid = rs.getString(2); } }
					if ( id == null ) {
						System.out.printf("Item %s not in folio cache.\n",hrid);
						continue;
					}
					List<Map<String,Object>> items =
							folio.queryAsList("/item-storage/items","id=="+id);
					for ( Map<String,Object> item : items ) {
						Object o1 = item.get("discoverySuppress");
						boolean active = (o1 == null)?true
								:String.class.isInstance(o1)?!Boolean.valueOf((String)o1):!(boolean)o1;
						Map<String,String> metadata = (Map<String,String>) item.get("metadata");
						Timestamp moddate = Timestamp.from(Instant.parse(
								metadata.get("updatedDate").replace("+00:00","Z")));

						String barcode = (item.containsKey("barcode"))?((String)item.get("barcode")).trim():null;
						if (barcode != null && barcode.length()>14) {
							System.out.println("Barcode too long. Omitting ["+hrid+"/"+barcode+"]");
							barcode = null;
						}
						String holdingId = (String)item.get("holdingsRecordId");

						cacheReplaceStmt.setString(1, id);
						cacheReplaceStmt.setString(2, hrid);
						cacheReplaceStmt.setString(3, holdingId);
						cacheReplaceStmt.setString(4, holdingHrid);
						cacheReplaceStmt.setTimestamp(5, moddate);
						cacheReplaceStmt.setString(6, barcode);
						cacheReplaceStmt.setString(7,mapper.writeValueAsString(item));
						cacheReplaceStmt.executeUpdate();

						instanceHridByHoldingHrid.setString(1, holdingHrid);
						String instanceHrid = null;
						try ( ResultSet rs = instanceHridByHoldingHrid.executeQuery() )
						{ while ( rs.next() ) instanceHrid = rs.getString(1); }

						if ( instanceHrid == null ) {
							System.out.printf(
								"Holding %s already deleted, so no need to update for item.\n",holdingHrid);
							continue ITEM;
						}
						availGen.setString(1, instanceHrid);
						availGen.setTimestamp(2, moddate);
						availGen.executeUpdate();
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							e.printStackTrace();
							throw new InterruptedException(e.getMessage());
						}
					}
				}
			}
		}

		int falseDeletes = 0;
		if ( ! c.onlyInFC.isEmpty() ) {
			System.out.println("Apparently deleted items in Folio cache:");
			try ( PreparedStatement itemIdByHridStmt = inventory.prepareStatement(
						"SELECT id, holdingHrid FROM itemFolio WHERE hrid = ?");
					PreparedStatement instanceHridByHoldingHrid = inventory.prepareStatement(
							"SELECT instanceHrid FROM holdingFolio WHERE hrid = ?");
					PreparedStatement deleteCache = inventory.prepareStatement(
							"DELETE FROM itemFolio WHERE hrid = ?");
					PreparedStatement queueAvail = inventory.prepareStatement(
							"INSERT INTO availabilityQueue (hrid,priority,cause,record_date)"
									+ " VALUES (?,6,'LDP',NOW())")){
				for (String hrid : c.onlyInFC ) {
					System.out.println(hrid);
					itemIdByHridStmt.setString(1, hrid);
					String id = null;
					String holdingHrid = null;
					try ( ResultSet rs = itemIdByHridStmt.executeQuery() ) {
						while (rs.next()) { id = rs.getString(1); holdingHrid = rs.getString(2); }
					}
					if ( id == null ) {
						System.out.printf("Item %s not in folio cache.\n",hrid);
						continue;
					}
					List<Map<String,Object>> items =
							folio.queryAsList("/item-storage/items","id=="+id);
					if ( items.isEmpty() ) {
						deleteCache.setString(1, hrid);
						deleteCache.executeUpdate();

						instanceHridByHoldingHrid.setString(1, holdingHrid);
						String instanceHrid = null;
						try ( ResultSet rs = instanceHridByHoldingHrid.executeQuery() )
						{ while ( rs.next() ) instanceHrid = rs.getString(1); }

						if ( instanceHrid != null ) {
							queueAvail.setString(1, instanceHrid);
							queueAvail.executeUpdate();
						}
					} else {
						System.out.printf("%s is not deleted. (%d)\n",hrid,++falseDeletes);
						if ( falseDeletes > 400 ) {
							System.out.println(
									"High number of records falsely appearing deleted may indicate "
									+ "LDP issues. Quitting.");
							System.exit(1);
						}
					}
				}
			}
		}
	}


	static ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.setSerializationInclusion(Include.NON_EMPTY);
	}
	static Pattern modDateP = Pattern.compile("^.*\"updatedDate\" *: *\"([^\"]+)\".*$");

}
