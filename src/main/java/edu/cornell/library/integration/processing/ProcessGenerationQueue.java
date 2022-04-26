package edu.cornell.library.integration.processing;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.catalog.Catalog;
import edu.cornell.library.integration.folio.Locations;
import edu.cornell.library.integration.folio.Locations.Location;
import edu.cornell.library.integration.folio.OkapiClient;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.metadata.support.StatisticalCodes;
import edu.cornell.library.integration.metadata.support.SupportReferenceData;
import edu.cornell.library.integration.processing.GenerateSolrFields.BibChangeSummary;
import edu.cornell.library.integration.utilities.AddToQueue;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.Generator;
import edu.cornell.library.integration.utilities.IndexingUtilities;

public class ProcessGenerationQueue {

	public static void main(String[] args) throws Exception {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(Config.getRequiredArgsForDB("Hathi"));
		requiredArgs.addAll(Config.getRequiredArgsForDB("CallNos"));
		requiredArgs.addAll(Config.getRequiredArgsForDB("Headings"));
		requiredArgs.add("catalogClass");
		Config config = Config.loadConfig(requiredArgs);

		new ProcessGenerationQueue(config);
	}

	public ProcessGenerationQueue(Config config)
			throws SQLException, JsonProcessingException, IOException, XMLStreamException, InterruptedException {

		config.setDatabasePoolsize("Current", 5);
		GenerateSolrFields gen = new GenerateSolrFields(
				EnumSet.allOf(Generator.class),
				EnumSet.of(Generator.AUTHORTITLE,Generator.RECORDTYPE,Generator.CALLNUMBER,
						Generator.LANGUAGE, Generator.MARC, Generator.URL), "processedMarc" );

		Catalog.DownloadMARC marc = Catalog.getMarcDownloader(config);

		try (	Connection current = config.getDatabaseConnection("Current");
				PreparedStatement nextBibStmt = current.prepareStatement
						("SELECT generationQueue.hrid, priority" +
						 "  FROM generationQueue "+
						 "  LEFT JOIN bibLock ON generationQueue.hrid = bibLock.hrid"+
						 " WHERE bibLock.date IS NULL"+
						 " ORDER BY priority LIMIT 1");
				PreparedStatement allForBibStmt = current.prepareStatement
						("SELECT id, cause, record_date FROM generationQueue WHERE hrid = ?");
				PreparedStatement createLockStmt = current.prepareStatement
						("INSERT INTO bibLock (hrid) values (?)",Statement.RETURN_GENERATED_KEYS);
				PreparedStatement unlockStmt = current.prepareStatement
						("DELETE FROM bibLock WHERE id = ?");
				PreparedStatement oldLocksCleanupStmt = current.prepareStatement
						("DELETE FROM bibLock WHERE date < DATE_SUB( NOW(), INTERVAL 5 MINUTE)");
				PreparedStatement deqStmt = current.prepareStatement
						("DELETE FROM generationQueue WHERE id = ?");
				PreparedStatement deqByBibStmt = current.prepareStatement
						("DELETE FROM generationQueue WHERE hrid = ?");
				PreparedStatement bibRecsVoyUpdateStmt = current.prepareStatement
						("UPDATE bibRecsVoyager SET active = 0 WHERE bib_id = ?");
				PreparedStatement queueDeleteStmt = current.prepareStatement
						("INSERT INTO deleteQueue (priority, cause, hrid, record_date)"
								+ " VALUES ( 5, 'Discovered gone by generation proc', ?, now())");
				PreparedStatement oldestSolrFieldsData = current.prepareStatement
						("SELECT hrid, visit_date FROM processedMarcData ORDER BY visit_date LIMIT 1000");
				PreparedStatement instanceByHrid = current.prepareStatement
						("SELECT * FROM instanceFolio WHERE hrid = ?");
				PreparedStatement holdingsByInstanceHrid = current.prepareStatement(
						"SELECT * FROM holdingFolio WHERE instanceHrid = ?");
				PreparedStatement availabilityQueueStmt = AddToQueue.availabilityQueueStmt(current);
				PreparedStatement headingsQueueStmt = AddToQueue.headingsQueueStmt(current);
				PreparedStatement generationQueueStmt = AddToQueue.generationQueueStmt(current);
				) {

			Connection voyager = null;
			OkapiClient folio = null;
			if ( config.isOkapiConfigured("Folio")) {
				folio = config.getOkapi("Folio");
				Locations locations = new Locations(folio);
				Location online = locations.getByCode("serv,remo");
				if (online == null ) {
					System.out.println("Something has changed with the online location.");
					System.out.println("An adjustment will be necessary to correctly identify online holdings.");
					System.exit(1);
				}
				StatisticalCodes.initializeCodes(folio);
				SupportReferenceData.initialize(folio);
			} else if ( config.isDatabaseConfigured("Voy"))
				voyager = config.getDatabaseConnection("Voy");
			else {
				System.out.println("Either Voyager or Folio connection must be configured.");
			}

			oldestSolrFieldsData.setFetchSize(1000);
			int ninesToProcess = 0;

			BIB: for ( int i = 0 ; i < 10_000_000; i++ ) {
				// Identify Bib to generate data for
				String bib = null;
				Integer priority = null;
				try (ResultSet rs = nextBibStmt.executeQuery()){
					while (rs.next()) { bib = rs.getString(1); priority = rs.getInt(2); }
				}

				if (bib == null || priority == null) {
					queueRecordsNotRecentlyVisited( oldestSolrFieldsData, generationQueueStmt );
					oldLocksCleanupStmt.executeUpdate();
					continue;
				}

				if ( priority.equals(9) ) {
					if (ninesToProcess == 0)
						ninesToProcess = determineNinesToProcess( current );
					if ( ninesToProcess > 0 ) {
						ninesToProcess--;
					} else {
						ninesToProcess++;
						Thread.sleep(1000);
						continue;
					}
				}

				allForBibStmt.setString(1,bib);
				List<String> recordChanges = new ArrayList<>();
				Set<Integer> queueIds = new HashSet<>();
				Timestamp minChangeDate = null;
				EnumSet<Generator> forcedGenerators = EnumSet.noneOf(Generator.class);
				int lockId = 0;
				try ( ResultSet rs = allForBibStmt.executeQuery() ) {
					while (rs.next()) {
						Integer id = rs.getInt("id");
						Timestamp recordDate = rs.getTimestamp("record_date");
						String cause = rs.getString("cause");
						recordChanges.add(cause+" "+recordDate);
						if ( cause.startsWith("ALL" ))
							forcedGenerators = EnumSet.allOf(Generator.class);
						else
							forcedGenerators.addAll(
									Arrays.stream(Generator.values())
									.filter(e -> cause.contains(e.name()))
									.collect(Collectors.toSet()));
						if (minChangeDate == null || minChangeDate.after(recordDate))
							minChangeDate = recordDate;
						queueIds.add(id);
					}
				}
				createLockStmt.setString(1,bib);
				try {
					createLockStmt.executeUpdate();
				} catch (SQLException e) {
					System.out.println("Tried to lock instance "+bib);
					continue BIB;
				}
				try ( ResultSet generatedKeys = createLockStmt.getGeneratedKeys() ) {
					if (generatedKeys.next()) lockId = generatedKeys.getInt(1);
				}
				System.out.println("** "+bib+": "+recordChanges.toString());

				Versions v = null;
				MarcRecord rec = null;
				Map<String,Object> instance = null;

				try {

					if ( voyager != null ) {
						v = new Versions( getBibRecordModDate( voyager, Integer.valueOf(bib) ) );
						if (v.bib == null) {
							throw new IOException("Record appears to be deleted. Dequeuing. "+bib);
						}
						v.mfhds = getMhfdRecordModDates(voyager,Integer.valueOf(bib));
	
						// Retrieve records
						rec = marc.getMarc(MarcRecord.RecordType.BIBLIOGRAPHIC, bib);
						for (String mfhdId : v.mfhds.keySet())
							rec.marcHoldings.add(marc.getMarc(MarcRecord.RecordType.HOLDINGS, mfhdId));

					} else if ( folio != null ) {
						String instanceId = null;
						instanceByHrid.setString(1, bib);
						try ( ResultSet rs = instanceByHrid.executeQuery() ) {
							while (rs.next()) {
								instanceId = rs.getString("id");
								instance = mapper.readValue( rs.getString("content"), Map.class);
							}
						}

						if ( instance == null ) {
							System.out.println("Instance hrid absent from Folio: "+bib);
							IndexingUtilities.queueBibDelete(current, bib);
							continue BIB;
						}

						v = new Versions(getModificationTimestamp( instance ));
						rec = marc.getMarc(MarcRecord.RecordType.BIBLIOGRAPHIC,instanceId);
						List<Map<String,Object>> folioHoldings = new ArrayList<>();
						holdingsByInstanceHrid.setString(1, bib);
						try (ResultSet rs = holdingsByInstanceHrid.executeQuery() ) {
							while (rs.next())
								folioHoldings.add(mapper.readValue(rs.getString("content"),Map.class));
						}
						Map<String,Timestamp> holdingTimestamps = new HashMap<>();
						for ( Map<String,Object> holding : folioHoldings ) {
							Timestamp t = getModificationTimestamp( holding );
							String holdingHrid = (String)holding.get("id");
							holdingTimestamps.put(holdingHrid, t);
						}
						v.mfhds = holdingTimestamps;
						if ( rec != null ) {
							rec.instance = instance;
							rec.bib_id = (String)instance.get("hrid");
							rec.folioHoldings = folioHoldings;
						} else {
							instance.put("holdings", folioHoldings);
						}

					}
				} catch (IOException e) {
					System.out.println(e.getMessage());
					e.printStackTrace();
					deqByBibStmt.setString(1, bib);
					deqByBibStmt.executeUpdate();
					bibRecsVoyUpdateStmt.setInt(1, Integer.valueOf(bib));
					bibRecsVoyUpdateStmt.executeUpdate();
					queueDeleteStmt.setString(1, bib);
					queueDeleteStmt.executeUpdate();
					continue;
				}

				BibChangeSummary solrChanges;
				if ( rec == null ) 
					solrChanges = gen.generateNonMarcSolr(
							instance, config, mapper.writeValueAsString(v));
				else
					solrChanges = gen.generateSolr(
							rec, config, mapper.writeValueAsString(v),forcedGenerators);
				if (solrChanges.changedSegments != null) {
					AddToQueue.add2Queue(availabilityQueueStmt, bib, priority, minChangeDate, solrChanges.changedSegments);
					if (solrChanges.changedHeadingsSegments != null)
						AddToQueue.add2Queue(headingsQueueStmt, bib, priority, minChangeDate,
								solrChanges.changedHeadingsSegments);
				}

				for (Integer id : queueIds) {
					deqStmt.setInt(1, id);
					deqStmt.addBatch();
				}
				deqStmt.executeBatch();
				unlockStmt.setInt(1, lockId);
				unlockStmt.executeUpdate();

			}
		}
	}

	private int determineNinesToProcess(Connection current) throws SQLException {
		try (
			Statement s = current.createStatement();
			ResultSet r = s.executeQuery("SELECT COUNT(*) FROM availabilityQueue WHERE priority = 9")){
			while ( r.next() ) {
				int queued = r.getInt(1);
				if (queued < 10_000)
					return 11_000 - queued;
				return -500;
			}
		}
		return 0 ; // really not possible unless db error
	}

	private static Timestamp getModificationTimestamp(Map<String, Object> folioObject) {
		if (! folioObject.containsKey("metadata")) return null;
		Instant modDate = null;
		@SuppressWarnings("unchecked")
		Map<String,String> metadata = (Map<String, String>) folioObject.get("metadata");

		if ( metadata.containsKey("UpdatedDate") && metadata.get("UpdatedDate") != null )
			modDate = Instant.parse(metadata.get("UpdatedDate"));
		else if ( metadata.containsKey("CreatedDate") && metadata.get("CreatedDate") != null )
			modDate = Instant.parse(metadata.get("CreatedDate"));

		if (modDate == null)
			return null;
		return Timestamp.from(modDate);
	}

	private static Map<String,Timestamp> getMhfdRecordModDates( Connection voyager, Integer bibId ) throws SQLException {
		try ( PreparedStatement pstmt = voyager.prepareStatement
				("SELECT mfhd_master.mfhd_id, create_date, update_date"
				+ " FROM mfhd_master, bib_mfhd "
				+ "WHERE BIB_MFHD.MFHD_ID = mfhd_master.mfhd_id"
				+ "  AND bib_id = ?"
				+ "  AND suppress_in_opac = 'N'")) {
			pstmt.setInt(1, bibId);
			try ( ResultSet rs = pstmt.executeQuery()) {
				Map<String,Timestamp> mfhds = new HashMap<>();
				while (rs.next()) {
					Timestamp mod_date = rs.getTimestamp(3);
					if (mod_date == null)
						mod_date = rs.getTimestamp(2);
					mfhds.put(String.valueOf(rs.getInt(1)), mod_date);
				}
				return mfhds;
			}
		}
	}

	private static Timestamp getBibRecordModDate( Connection voyager, Integer bibId ) throws SQLException {
		try ( PreparedStatement pstmt = voyager.prepareStatement
				("SELECT COALESCE(update_date,create_date) FROM bib_master WHERE bib_id = ?")) {
			pstmt.setInt(1, bibId);
			try ( ResultSet rs = pstmt.executeQuery()) {
				if ( ! rs.next()) return null; // deleted
				return rs.getTimestamp(1);
			}
		}
	}

	private static void queueRecordsNotRecentlyVisited(PreparedStatement oldestSolrFieldsData,
			PreparedStatement generationQueueStmt) throws SQLException {

		try (ResultSet rs = oldestSolrFieldsData.executeQuery()) {
			while(rs.next()) AddToQueue.add2Queue(
					generationQueueStmt,rs.getString(1),9,rs.getTimestamp(2),"Age of Record");
		}
		
	}

	@JsonAutoDetect(fieldVisibility = Visibility.ANY)
	private class Versions {
		@JsonProperty("bib")      Timestamp bib;
		@JsonProperty("holdings") Map<String,Timestamp> mfhds;
		public Versions ( Timestamp bibTime ) {
			this.bib = bibTime;
		}
	}
	static ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.setSerializationInclusion(Include.NON_NULL);
	}

}
