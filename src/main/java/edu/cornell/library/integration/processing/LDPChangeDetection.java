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

			// INSTANCES
//			LDPRecordLists.populateInstanceLDPList(inventory, ldp);
			ComparisonLists c = ResourceListComparison.compareLists(
					inventory,"instanceFolio", "instanceLDP", "hrid" );

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
							boolean active1 = (o1 == null)?false
									:String.class.isInstance(o1)?Boolean.valueOf((String)o1):(boolean)o1;
							Object o2 = instance.get("staffSuppress");
							boolean active2 = (o2 == null)?false
									:String.class.isInstance(o2)?Boolean.valueOf((String)o2):(boolean)o2;
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
			}

			int falseDeletes = 0;
			if ( ! c.onlyInFC.isEmpty() ) {
				try (
				PreparedStatement instanceIdByHridStmt = inventory.prepareStatement(
						"SELECT id FROM instanceFolio WHERE hrid = ?");
				PreparedStatement cacheReplaceStmt = inventory.prepareStatement(
						"REPLACE INTO instanceFolio (id, hrid, active, source, moddate, content) "+
						" VALUES (?,?,?,?,?,?)") ){
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
							if ( falseDeletes > 1000 ) {
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
	}
	static ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.setSerializationInclusion(Include.NON_EMPTY);
	}

}
