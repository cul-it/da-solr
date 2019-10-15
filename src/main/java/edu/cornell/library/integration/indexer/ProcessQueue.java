package edu.cornell.library.integration.indexer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.indexer.queues.AddToQueue;
import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.indexer.utilities.Generator;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.voyager.DownloadMARC;
import edu.cornell.library.integration.voyager.VoyagerUtilities;

public class ProcessQueue {

	public static void main(String[] args) throws Exception {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(Config.getRequiredArgsForDB("Voy"));
		requiredArgs.addAll(Config.getRequiredArgsForDB("Hathi"));
		requiredArgs.addAll(Config.getRequiredArgsForDB("CallNos"));
		requiredArgs.addAll(Config.getRequiredArgsForDB("Headings"));
		Config config = Config.loadConfig(requiredArgs);

		new ProcessQueue(config);
	}

	public ProcessQueue(Config config) throws Exception {

		config.setDatabasePoolsize("Current", 3);
		config.setDatabasePoolsize("Voy", 3);
		GenerateSolrFields gen = new GenerateSolrFields( EnumSet.allOf(Generator.class),"solrFields" );
		DownloadMARC marc = new DownloadMARC(config);

		try (	Connection current = config.getDatabaseConnection("Current");
				Statement stmt = current.createStatement();
				PreparedStatement nextBibStmt = current.prepareStatement
						("SELECT bib_id, priority FROM generationQueue ORDER BY priority LIMIT 1");
				PreparedStatement allForBibStmt = current.prepareStatement
						("SELECT id, cause, record_date FROM generationQueue WHERE bib_id = ?");
				PreparedStatement deprioritizeStmt = current.prepareStatement
						("UPDATE generationQueue SET priority = 9 WHERE id = ?");
				PreparedStatement deqStmt = current.prepareStatement
						("DELETE FROM generationQueue WHERE id = ?");
				PreparedStatement deqByBibStmt = current.prepareStatement
						("DELETE FROM generationQueue WHERE bib_id = ?");
				PreparedStatement bibRecsVoyUpdateStmt = current.prepareStatement
						("UPDATE bibRecsVoyager SET active = 0 WHERE bib_id = ?");
				PreparedStatement queueDeleteStmt = current.prepareStatement
						("INSERT INTO deleteQueue (priority, cause, bib_id, record_date)"
								+ " VALUES ( 5, 'Discovered gone by generation proc', ?, now())");
				PreparedStatement oldestSolrFieldsData = current.prepareStatement
						("SELECT bib_id, visit_date FROM solrFieldsData ORDER BY visit_date LIMIT 50");
				PreparedStatement availabilityQueueStmt = AddToQueue.availabilityQueueStmt(current);
				PreparedStatement generationQueueStmt = AddToQueue.generationQueueStmt(current);
				Connection voyager = config.getDatabaseConnection("Voy");
				
				) {

			do {
				// Identify Bib to generate data for
				Integer bib = null;
				Integer priority = null;
				stmt.execute("LOCK TABLES generationQueue WRITE");
				try (ResultSet rs = nextBibStmt.executeQuery()){
					while (rs.next()) { bib = rs.getInt(1); priority = rs.getInt(2); }
				}

				if (bib == null || priority == null) {
					stmt.execute("UNLOCK TABLES");
					queueRecordsNotRecentlyVisited( oldestSolrFieldsData, generationQueueStmt );
					continue;
				}

				allForBibStmt.setInt(1,bib);
				List<String> recordChanges = new ArrayList<>();
				Set<Integer> queueIds = new HashSet<>();
				Timestamp minChangeDate = null;
				EnumSet<Generator> forcedGenerators = EnumSet.noneOf(Generator.class);
				try ( ResultSet rs = allForBibStmt.executeQuery() ) {
					while (rs.next()) {
						Integer id = rs.getInt("id");
						Timestamp recordDate = rs.getTimestamp("record_date");
						String cause = rs.getString("cause");
						recordChanges.add(cause+" "+recordDate);
						forcedGenerators.addAll(
								Arrays.stream(Generator.values())
								.filter(e -> cause.contains(e.name()))
								.collect(Collectors.toSet()));
						deprioritizeStmt.setInt(1,id);
						deprioritizeStmt.addBatch();
						if (minChangeDate == null || minChangeDate.after(recordDate))
							minChangeDate = recordDate;
						queueIds.add(id);
					}
					deprioritizeStmt.executeBatch();
				}
				stmt.execute("UNLOCK TABLES");
				System.out.println("** "+bib+": "+recordChanges.toString());

				Versions v = new Versions( VoyagerUtilities.confirmBibRecordActive( voyager, bib) );
				if (v.bib == null) {
					System.out.println("Record appears to be deleted or suppressed. Dequeuing.");
					deqByBibStmt.setInt(1, bib);
					deqByBibStmt.executeUpdate();
					bibRecsVoyUpdateStmt.setInt(1, bib);
					bibRecsVoyUpdateStmt.executeUpdate();
					queueDeleteStmt.setInt(1, bib);
					queueDeleteStmt.executeUpdate();
					continue;
				}
				v.mfhds = VoyagerUtilities.confirmActiveMfhdRecords(voyager,bib);

				// Retrieve records
				MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC,
						marc.downloadXml(MarcRecord.RecordType.BIBLIOGRAPHIC, bib));
				for (Integer mfhdId : v.mfhds.keySet()) {
					rec.holdings.add(new MarcRecord( MarcRecord.RecordType.HOLDINGS,
						marc.downloadXml(MarcRecord.RecordType.HOLDINGS, mfhdId)));
				}

				String solrChanges = gen.generateSolr(
						rec, config, mapper.writeValueAsString(v),forcedGenerators);
				if (solrChanges != null)
					AddToQueue.add2Queue(availabilityQueueStmt, bib, priority, minChangeDate, solrChanges);

				for (Integer id : queueIds) {
					deqStmt.setInt(1, id);
					deqStmt.addBatch();
				}
				deqStmt.executeBatch();
				
			} while (true);
		}
	}

	private static void queueRecordsNotRecentlyVisited(PreparedStatement oldestSolrFieldsData,
			PreparedStatement generationQueueStmt) throws SQLException {

		try (ResultSet rs = oldestSolrFieldsData.executeQuery()) {
			while(rs.next())
				AddToQueue.add2Queue(generationQueueStmt, rs.getInt(1), 8, rs.getTimestamp(2), "Age of Record");
		}
		
	}

	@JsonAutoDetect(fieldVisibility = Visibility.ANY)
	private class Versions {
		@JsonProperty("bib")      Timestamp bib;
		@JsonProperty("holdings") Map<Integer,Timestamp> mfhds;
		public Versions ( Timestamp bibTime ) {
			this.bib = bibTime;
		}
	}
	static ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.setSerializationInclusion(Include.NON_NULL);
	}

}
