package edu.cornell.library.integration.indexer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.indexer.utilities.Generator;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

/**
 * Generate Solr fields based on a MARC bibliographic record with optional holdings, using a configured
 * set of field generators. Requires a Config containing the necessary database
 * connection information to push the results to the configured DB.
 * For example:<br/><br/>
 * <pre> MarcRecord rec = ...;
 * Config config = ...;
 * GenerateSolrFields gen = new GenerateSolrFields(EnumSet.of(
 *		Generator.AUTHORTITLE, Generator.SUBJECT));
 * gen.generateSolr(rec, config);</pre> *
 */
class GenerateSolrFields {

	// INSTANCE VARIABLES
	private final EnumSet<Generator> activeGenerators; // provided to constructor
	private final String tableNamePrefix; // provided to constructor
	private final Map<String,List<Generator>> fieldsSupported; // generated by constructor
	private Map<Generator,Timestamp> generatorTimestamps = null; // generated on first generateSolr() call

	// CONSTRUCTOR
	GenerateSolrFields( EnumSet<Generator> activeGenerators, String tableNamePrefix ) {
		this.activeGenerators = activeGenerators;
		this.tableNamePrefix = tableNamePrefix;
		this.fieldsSupported = constructFieldsSupported( activeGenerators );
	}

	// BEGIN INSTANCE METHODS

	/**
	 * Process a bibliographic MARC record with optional attached holdings MARC records into fields for
	 * indexing in Solr, using the set of SolrFieldGenerator classes provided to the GenerateSolrFields
	 * constructor. Config is used to gain access to various configuration options, the Current database,
	 * and a variety of other databases used by particular SolrFieldGenerator classes.<br/><br/>
	 * The results are written to the database table <pre>${Current}.${tableNamePrefix}Data</pre>
	 * On the first call of this method for an instance of GenerateSolrFields (if running in production
	 * mode), information about generators may be updated in the table
	 * <pre>${Current}.${tableNamePrefix}Generators</pre><br/><br/>
	 * @param rec Input bibliographic MARC with possible attached holdings records.
	 * @param config
	 * @return 	The return value is the number of SolrFieldGenerators which produced Solr fields sets are different
	 *     than those previously produced for the bibliographic record with this id. This counts equally the
	 *     cases where no previous Solr data existed, the record has changed and resulted in changed Solr
	 *     fields, the record has not changed, but outside data that is involved in the Solr field generator
	 *     has changed, the generator itself has updated logic that resulted in a change, or (though it's to
	 *     be avoided as best as possible,) the results of the generator are non-deterministic given unchanged
	 *     input data.<br/><br/>
	 *     If the return value is greater than zero, the updated Solr record data should be pushed to Solr.
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	String generateSolr( MarcRecord rec, Config config, String recordVersions )
			throws SQLException {
		return generateSolr( rec, config, recordVersions, EnumSet.noneOf(Generator.class));
	}
	String generateSolr( MarcRecord rec, Config config, String recordVersions, EnumSet<Generator> forcedGenerators )
			throws SQLException {

		Map<Generator,MarcRecord> recordChunks = createMARCChunks(rec,activeGenerators,this.fieldsSupported);
		Map<Generator,BibGeneratorData> originalValues = pullPreviousFieldDataFromDB(
				activeGenerators,tableNamePrefix,rec.id,config);
		LocalDateTime now = LocalDateTime.now();
		if (generatorTimestamps == null)
			generatorTimestamps = getGeneratorTimestamps(
					activeGenerators, Timestamp.valueOf(now), this.tableNamePrefix, config );

		List<BibGeneratorData> newValues = recordChunks.entrySet()
			.parallelStream()
			.map(entry -> processRecordChunkWithGenerator(
					entry.getKey(), generatorTimestamps.get(entry.getKey()),entry.getValue(),
					forcedGenerators.contains(entry.getKey()),
					originalValues.get(entry.getKey()), now, config))
			.collect(Collectors.toList());

		List<BibGeneratorData> changedOutputs = new ArrayList<>();
		List<BibGeneratorData> generatedNotChanged = new ArrayList<>();
		for (BibGeneratorData newGeneratorData : newValues) {
			if (newGeneratorData == null) continue;
			if (newGeneratorData.solrStatus.equals(Status.NEW) ||
					newGeneratorData.solrStatus.equals(Status.CHANGED)) {
				changedOutputs.add(newGeneratorData);
				if ( ! newGeneratorData.solrStatus.equals(Status.UNGENERATED)) {
					generatedNotChanged.add(newGeneratorData);
				}
			}
		}

		if ( ! changedOutputs.isEmpty() || ! generatedNotChanged.isEmpty() )
		System.out.printf(
				"%s: %d changed (%s)%s\n",rec.id,changedOutputs.size(),
				((changedOutputs.size() == activeGenerators.size())?"all":formatBGDList(changedOutputs)),
				((generatedNotChanged.size() > 0)
						? ("; also generated "+generatedNotChanged.size()+" ("+formatBGDList(generatedNotChanged))+")":""));
		if (generatedNotChanged.size() > 0)
			pushNewFieldDataToDB(activeGenerators,newValues,tableNamePrefix,rec.id,recordVersions, config);
		else 
			touchBibVisitDate(tableNamePrefix,rec.id, config);
		if (changedOutputs.size() > 0)
			return (changedOutputs.size() == activeGenerators.size())
					?"all Solr field segments":changedOutputs.toString();
		return null;
	}

	/**
	 * Create a table with the tableName specified in the constructor, in the Current database
	 * accessed through the Config.
	 * @param config
	 * @throws SQLException
	 */
	public void setUpDatabase( Config config ) throws SQLException {

		StringBuilder sbMainTableCreate = new StringBuilder();
		sbMainTableCreate.append("CREATE TABLE IF NOT EXISTS ").append(this.tableNamePrefix).append("Data (\n");
		sbMainTableCreate.append("bib_id  INT(10) UNSIGNED NOT NULL PRIMARY KEY,\n");
		sbMainTableCreate.append("record_dates text,\n");
		sbMainTableCreate.append("visit_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n");
		for ( Generator gen : activeGenerators ) {
			String genName = gen.name().toLowerCase();
			sbMainTableCreate.append(genName).append("_marc_segment          LONGTEXT DEFAULT NULL,\n");
			sbMainTableCreate.append(genName).append("_solr_fields           LONGTEXT DEFAULT NULL,\n");
			sbMainTableCreate.append(genName).append("_solr_fields_gen_date  TIMESTAMP NULL,\n");
		}
		sbMainTableCreate.setCharAt(sbMainTableCreate.length()-2, ')');
		String generatorListTableCreate =
				"CREATE TABLE IF NOT EXISTS "+this.tableNamePrefix+"Generators (\n"+
				" name     VARCHAR(24) NOT NULL PRIMARY KEY,\n"+
				" version  VARCHAR(24) NOT NULL,\n"+
				" mod_date TIMESTAMP   NOT NULL )";
		try ( Connection conn = config.getDatabaseConnection("Current");
				Statement stmt = conn.createStatement()) {
			stmt.execute(sbMainTableCreate.toString());
			stmt.execute(generatorListTableCreate);
		}

	}

	// BEGIN PRIVATE RESOURCES

	private static String formatBGDList( List<BibGeneratorData> list ) {
		StringBuilder sb = new StringBuilder();
		for (BibGeneratorData bgd : list) {
			if ( sb.length() != 0 )
				sb.append(' ');
			sb.append(bgd.gen).append('(').append(bgd.marcStatus.name().toLowerCase()).append(')');
		}
		return sb.toString();
	}

	private static void pushNewFieldDataToDB(
			EnumSet<Generator> activeGenerators,
			List<BibGeneratorData> newValues,
			String tableNamePrefix,
			String bibId,
			String recordVersions,
			Config config) throws SQLException {

		if (sql == null) {
			StringBuilder sbSql = new StringBuilder();
			sbSql.append("REPLACE INTO ").append(tableNamePrefix).append("Data (");
			sbSql.append("bib_id, record_dates, \n");
			for (Generator gen : activeGenerators) {
				String genName = gen.name().toLowerCase();
				sbSql.append(genName).append("_marc_segment,\n");
				sbSql.append(genName).append("_solr_fields,\n");
				sbSql.append(genName).append("_solr_fields_gen_date,\n");		
			}
			sbSql.setCharAt(sbSql.length()-2, ')');
			sbSql.append("VALUES ( ");
			int questionMarksNeeded = activeGenerators.size()*3+2;
			for (int i = 1 ; i <= questionMarksNeeded; i++) sbSql.append("?,");
			sbSql.setCharAt(sbSql.length()-1, ')');
			sql = sbSql.toString();
		}

		Map<Generator,BibGeneratorData> newValuesMap = new HashMap<>();
		for (BibGeneratorData data : newValues) newValuesMap.put(data.getGenerator(), data);
		try ( Connection conn = config.getDatabaseConnection("Current");
				PreparedStatement pstmt = conn.prepareStatement(sql)) {
			int parameterIndex = 1;
			pstmt.setString(parameterIndex++, bibId);
			pstmt.setString(parameterIndex++, recordVersions);
			for (Generator gen : activeGenerators) {
				BibGeneratorData data = newValuesMap.get(gen);
				pstmt.setString(parameterIndex++, data.marcSegment);
				pstmt.setString(parameterIndex++, data.solrSegment);
				pstmt.setTimestamp(parameterIndex++, data.solrGenDate);
			}
			pstmt.executeUpdate();
		}
		
	}
	private static String sql = null;


	private static void touchBibVisitDate(String tableNamePrefix, String bibId, Config config) throws SQLException {
		try ( Connection conn = config.getDatabaseConnection("Current");
				PreparedStatement pstmt = conn.prepareStatement
						("UPDATE "+tableNamePrefix+"Data SET visit_date = NOW() WHERE bib_id = ?")) {
			pstmt.setString(1, bibId);
			pstmt.executeUpdate();
		}		
	}


	private static Map<Generator, BibGeneratorData> pullPreviousFieldDataFromDB
	(EnumSet<Generator> activeGenerators,String tableNamePrefix, String bibId, Config config)
			throws SQLException {

		Map<Generator,BibGeneratorData> allData = new HashMap<>();
		try ( Connection conn = config.getDatabaseConnection("Current");
				PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM "+tableNamePrefix+"Data WHERE bib_id = ?") ){
			pstmt.setInt(1, Integer.valueOf(bibId));
			try ( ResultSet rs = pstmt.executeQuery() ) {
				while (rs.next()) {
					for ( Generator gen : activeGenerators) {
						String genName = gen.name().toLowerCase();
						BibGeneratorData d = new BibGeneratorData(
								rs.getString(genName+"_marc_segment"),
								rs.getString(genName+"_solr_fields"),
								rs.getTimestamp(genName+"_solr_fields_gen_date"));
						allData.put(gen,d);
					}
					return allData;
				}
			}
		}

		// No pre-existing data exists
		BibGeneratorData d = new BibGeneratorData( null, null, null );
		for ( Generator gen : activeGenerators )
			allData.put(gen,d);
		return allData;
	}

	private static Map<String, List<Generator>> constructFieldsSupported(EnumSet<Generator> activeGenerators) {
		Map<String,List<Generator>> fieldsSupported = new HashMap<>();
		for (Generator gen : activeGenerators) {
			List<String> classFieldsSupported = gen.getInstance().getHandledFields();
			for (String field : classFieldsSupported) {
				if ( ! fieldsSupported.containsKey(field) )
					fieldsSupported.put(field, new ArrayList<Generator>());
				fieldsSupported.get(field).add(gen);
			}
		}
		return fieldsSupported;
	}

	private static Random random = new Random();
	private static int randomCountDown = random.nextInt(400);
	private static BibGeneratorData processRecordChunkWithGenerator(
			Generator gen,Timestamp genModDate, MarcRecord recChunk, boolean forced,
			BibGeneratorData origData, LocalDateTime now, Config config){

		String marcSegment = recChunk.toString();
		Status marcStatus;
		if  (origData.marcSegment == null)
			marcStatus = Status.NEW;
		else if (marcSegment.equals(origData.marcSegment))
			marcStatus = Status.UNCHANGED;
		else
			marcStatus = Status.CHANGED;

		if (marcStatus.equals(Status.UNCHANGED)) {
			if (Timestamp.valueOf(now.minus(gen.getInstance().resultsShelfLife())).after(origData.solrGenDate)
					|| genModDate.after(origData.solrGenDate))
				marcStatus = Status.STALE;
			else if ( forced )
				marcStatus = Status.FORCED;
			else if (randomCountDown-- == 0 ) {
				marcStatus = Status.RANDOM;
				randomCountDown = random.nextInt(400);
			}
			else {
				origData.marcStatus = Status.UNCHANGED;
				origData.solrStatus = Status.UNGENERATED;
				origData.gen = gen;
				return origData;
			}
		}

		String solrFields;
		try {
			solrFields = gen.getInstance().generateSolrFields(recChunk,config).toString();
		} catch (ClassNotFoundException | SQLException | IOException e) {
			System.out.println("Generator "+gen+" failed on bib"+recChunk.id+"\n");
			e.printStackTrace();
			return null;
		}
		BibGeneratorData newData = new BibGeneratorData( marcSegment, solrFields, Timestamp.valueOf(now) );
		newData.marcStatus = marcStatus;
		newData.solrStatus = (origData.solrSegment == null) ? Status.NEW :
			( solrFields.equals(origData.solrSegment) ) ? Status.UNCHANGED : Status.CHANGED;
		newData.gen = gen;
		return newData;
	}

	private static Map<Generator, Timestamp> getGeneratorTimestamps(
			EnumSet<Generator> activeGenerators, Timestamp now, String tableNamePrefix, Config config)
			throws SQLException {
		String getQuery = "SELECT version, mod_date FROM "+tableNamePrefix+"Generators WHERE name = ?";
		String setQuery =
				"REPLACE INTO "+tableNamePrefix+"Generators ( name, version, mod_date ) VALUES ( ?, ?, ? )";

		try (Connection conn = config.getDatabaseConnection("Current");
				PreparedStatement queryStmt = conn.prepareStatement(getQuery);
				PreparedStatement writeStmt = conn.prepareStatement(setQuery)) {

			Map<Generator,Timestamp> generatorTimestamps = new HashMap<>();

			for (Generator gen : activeGenerators) {
				String genName = gen.name().toLowerCase();
				queryStmt.setString(1,genName);

				try (ResultSet rs = queryStmt.executeQuery()) {

					Boolean isChanged = true;
					if (rs.next()) {
						// generator has been seen before, check for version change.
						if (rs.getString(1).equals(gen.getInstance().getVersion())) {
							generatorTimestamps.put(gen,rs.getTimestamp(2));
							isChanged = false;
						}
						// new generator... automatically new.
					}

					if (isChanged) {
						generatorTimestamps.put(gen,now);
						if (config.isProduction()) {
							writeStmt.setString(1, genName);
							writeStmt.setString(2, gen.getInstance().getVersion());
							writeStmt.setTimestamp(3, now);
							writeStmt.executeUpdate();
						}
					}
				}
			}
			return generatorTimestamps;
		}
	}

	private static Map<Generator,MarcRecord> createMARCChunks(
			MarcRecord rec, EnumSet<Generator> activeGenerators, Map<String,List<Generator>> fieldsSupported) {
		Map<Generator,MarcRecord> recordChunks = new HashMap<>();

		for (Generator gen : activeGenerators) {
			recordChunks.put(gen, new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC ));
			recordChunks.get(gen).id = rec.id;
			recordChunks.get(gen).modifiedDate = rec.modifiedDate;
		}

		if (fieldsSupported.containsKey("leader"))
			for (Generator supportingClass : fieldsSupported.get("leader"))
				recordChunks.get(supportingClass).leader = rec.leader;

		for (ControlField f : rec.controlFields)
			if (fieldsSupported.containsKey(f.tag))
				for( Generator supportingClass : fieldsSupported.get(f.tag))
					recordChunks.get(supportingClass).controlFields.add(f);

		for (DataField f : rec.dataFields)
			if (fieldsSupported.containsKey(f.mainTag))
				for( Generator supportingClass : fieldsSupported.get(f.mainTag))
					recordChunks.get(supportingClass).dataFields.add(f);
			else
				System.out.println( "Unrecognized field "+f.mainTag+" in record "+rec.id );

		if (fieldsSupported.containsKey("holdings"))
			for (Generator supportingClass : fieldsSupported.get("holdings"))
				recordChunks.get(supportingClass).holdings = rec.holdings;

		return recordChunks;
	}

	private static class BibGeneratorData {
		final String marcSegment;
		final String solrSegment;
		final Timestamp solrGenDate;
		Status marcStatus = null;
		Status solrStatus = null;
		Generator gen = null;
		public BibGeneratorData( String marcSegment, String solrSegment, Timestamp solrGenDate) {
			this.marcSegment = marcSegment;
			this.solrSegment = solrSegment;
			this.solrGenDate = solrGenDate;
		}
		public Generator getGenerator() { return this.gen; }
	}

	private enum Status { UNGENERATED,NEW,CHANGED,UNCHANGED,STALE,FORCED,RANDOM; }
}
