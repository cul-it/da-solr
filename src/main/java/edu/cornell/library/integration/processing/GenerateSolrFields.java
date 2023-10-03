package edu.cornell.library.integration.processing;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.Generator;
import edu.cornell.library.integration.utilities.SolrFields;

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
	private final EnumSet<Generator> activeMarcGenerators; // provided to constructor
	private final EnumSet<Generator> activeNonMarcGenerators; // provided to constructor
	private final String tableNamePrefix; // provided to constructor
	private final Map<String,List<Generator>> fieldsSupported; // generated by constructor
	private Map<Generator,Timestamp> generatorTimestamps = null; // generated on first generate call

	// CONSTRUCTOR
	GenerateSolrFields( EnumSet<Generator> activeMarcGenerators,
			EnumSet<Generator> activeNonMarcGenerators, String tableNamePrefix ) {
		this.activeMarcGenerators = activeMarcGenerators;
		this.activeNonMarcGenerators = activeNonMarcGenerators;
		this.tableNamePrefix = tableNamePrefix;
		this.fieldsSupported = constructFieldsSupported( activeMarcGenerators );
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
	 * @throws XMLStreamException 
	 * @throws IOException 
	 */
	BibChangeSummary generateSolr( MarcRecord rec, Config config, String recordVersions )
			throws SQLException, IOException, XMLStreamException {
		return generateSolr( rec, config, recordVersions, EnumSet.noneOf(Generator.class));
	}
	BibChangeSummary generateNonMarcSolr(
			Map<String,Object> instance, Config config, String recordVersions )
			throws SQLException, IOException, XMLStreamException {

		sanitizeCarriageReturnsInInstance(instance);
		Map<Generator,BibGeneratorData> originalValues = pullPreviousFieldDataFromDB(
				this.activeNonMarcGenerators,this.tableNamePrefix,(String)instance.get("hrid"),config);
		LocalDateTime now = LocalDateTime.now();
		if (this.generatorTimestamps == null)
			this.generatorTimestamps = getGeneratorTimestamps(
					this.activeMarcGenerators, Timestamp.valueOf(now), this.tableNamePrefix, config );

		List<BibGeneratorData> newValues = this.activeNonMarcGenerators.stream()
				.map(gen -> processInstanceWithGenerator(
						gen, instance, originalValues.get(gen), now, config))
				.collect(Collectors.toList());
		return summarizeAndLogChanges(newValues,(String)instance.get("hrid"),
				recordVersions,config, false);
	}

	BibChangeSummary generateSolr(
			MarcRecord rec, Config config, String recordVersions, EnumSet<Generator> forcedGenerators )
			throws SQLException, IOException, XMLStreamException {

		sanitizeCarriageReturnsEtAlInMarc( rec );
		Map<Generator,MarcRecord> recordChunks = createMARCChunks(
				rec,this.activeMarcGenerators,this.fieldsSupported);
		Map<Generator,BibGeneratorData> originalValues = pullPreviousFieldDataFromDB(
				this.activeMarcGenerators,this.tableNamePrefix,rec.bib_id,config);
		LocalDateTime now = LocalDateTime.now();
		if (this.generatorTimestamps == null)
			this.generatorTimestamps = getGeneratorTimestamps(
					this.activeMarcGenerators, Timestamp.valueOf(now), this.tableNamePrefix, config );

		List<BibGeneratorData> newValues = recordChunks.entrySet()
			.parallelStream()
			.map(entry -> processRecordChunkWithGenerator(
					entry.getKey(), this.generatorTimestamps.get(entry.getKey()),entry.getValue(),
					forcedGenerators.contains(entry.getKey()),
					originalValues.get(entry.getKey()), now, config))
			.collect(Collectors.toList());

		return summarizeAndLogChanges(newValues,rec.bib_id,recordVersions,config, true);

	}

	private BibChangeSummary summarizeAndLogChanges(
			List<BibGeneratorData> newValues,String id,String recordVersions,
			Config config, boolean isMarc)
					throws IOException, SQLException, XMLStreamException {
		List<BibGeneratorData> changedOutputs = new ArrayList<>();
		List<BibGeneratorData> generatedNotChanged = new ArrayList<>();
		List<String> changedHeadingsBlocks = new ArrayList<>();
		for (BibGeneratorData newGeneratorData : newValues) {
			if (newGeneratorData == null) continue;
			if (newGeneratorData.solrStatus.equals(Status.NEW) ||
					newGeneratorData.solrStatus.equals(Status.CHANGED)) {
				changedOutputs.add(newGeneratorData);
				if ( newGeneratorData.triggerHeadingsUpdate )
					changedHeadingsBlocks.add(newGeneratorData.gen.name());
				if ( newGeneratorData.marcStatus.equals(Status.RANDOM) )
					writeInformationAboutChangesToLog( newGeneratorData );
			}
			else if ( ! newGeneratorData.solrStatus.equals(Status.UNGENERATED))
				generatedNotChanged.add(newGeneratorData);
		}

		if ( ! changedOutputs.isEmpty() || ! generatedNotChanged.isEmpty() )
		System.out.printf(
				"%s: %d changed (%s)%s\n",id,changedOutputs.size(),
				((changedOutputs.size() == this.activeMarcGenerators.size())
						?"all":formatBGDList(changedOutputs)),
				((generatedNotChanged.size() > 0)
						? ("; also generated "+generatedNotChanged.size()
						+" ("+formatBGDList(generatedNotChanged))+")":""));
		if (changedOutputs.size() > 0 || generatedNotChanged.size() > 0)
			pushNewFieldDataToDB(isMarc,newValues,
					id,recordVersions,config);
		else
			touchBibVisitDate(this.tableNamePrefix,id, config);
		if (changedOutputs.size() > 0) {
			String changeSummary = (changedOutputs.size() == this.activeMarcGenerators.size())
					?"all Solr field segments":formatBGDList(changedOutputs);
			if (changedHeadingsBlocks.size() > 0)
				return new BibChangeSummary(changeSummary,changedHeadingsBlocks.toString());
			return new BibChangeSummary(changeSummary);
		}
		return new BibChangeSummary(null);
	}

	private static void sanitizeCarriageReturnsEtAlInMarc(MarcRecord rec) {
		for (ControlField f : rec.controlFields)
			if (f.value.indexOf('\n')>-1 || f.value.indexOf('\r')>-1)
				f.value = f.value.replaceAll("[\n\r]+", " ");
		for (DataField f : rec.dataFields) for (Subfield sf : f.subfields)
			if (sf.value.indexOf('\n')>-1 || sf.value.indexOf('\r')>-1
					|| sf.value.indexOf('\u0001')>-1 || sf.value.indexOf('\u001B')>-1)
				if (f.tag.equals("010"))
					sf.value = sf.value.replaceAll("[\n\r\u0001\u001B]+", " ");
				else
					sf.value = sf.value.replaceAll("[\n\r\u0001\u001B]+", " ").trim();
	}

	// this is not recursive, which may need to change if we have carriage returns deeper.
	static Map<String,Object> sanitizeCarriageReturnsInInstance(Map<String,Object> instance) {
		for (Entry<String, Object> e : instance.entrySet()) {
			Object value = e.getValue();
			if ( value == null )
				continue;
			else if (String.class.isInstance(value))
				instance.put(e.getKey(),String.class.cast(value)
						.replaceAll("\\\\n"," ").replaceAll("\\s+"," ").trim() );
			else if (ArrayList.class.isInstance(value)) {
				List<Object> list = ArrayList.class.cast(value);
				for ( int i = 0; i < list.size(); i++ ) {
					Object item = list.get(i);
					if ( String.class.isInstance(item) )
						list.set(i, String.class.cast(item).replaceAll("\\\\n"," ").replaceAll("\\s+"," ").trim());
					else if (LinkedHashMap.class.isInstance(item)) {
						sanitizeCarriageReturnsInInstance(Map.class.cast(item));
					}
				}
			} else if (LinkedHashMap.class.isInstance(value)) {
				sanitizeCarriageReturnsInInstance( Map.class.cast(value) );
			} else if ( Integer.class.isInstance(value)
					|| Boolean.class.isInstance(value)) {
				// nothing needs doing
			} else {
				System.out.printf("Unexpected key type in instance hash: %s (%s)\n",
						value.getClass().getName(),e.getKey());
				Thread.dumpStack();
			}
		}
		return instance;
	}

	private static void writeInformationAboutChangesToLog(BibGeneratorData newGeneratorData)
			throws IOException, XMLStreamException {
		System.out.printf("Randomly regenerated segment %s produced changed output:\n",
				newGeneratorData.gen.name());
		Set<String> newFields = Arrays.stream( newGeneratorData.solrSegment.split("\n") )
				.collect(Collectors.toSet());
		Set<String> oldFields = Arrays.stream( newGeneratorData.oldData.solrSegment.split("\n") )
				.collect(Collectors.toSet());
		Set<String> commonFields = new HashSet<>();
		for (String f : newFields) if (oldFields.contains(f)) commonFields.add(f);
		newFields.removeAll(commonFields);
		oldFields.removeAll(commonFields);
		List<String> newMarc = newFields.stream().filter(f -> f.startsWith("marc_display: "))
				.map(f -> f.replaceAll("^marc_display: ", "")).collect(Collectors.toList());
		List<String> oldMarc = oldFields.stream().filter(f -> f.startsWith("marc_display: "))
				.map(f -> f.replaceAll("^marc_display: ", "")).collect(Collectors.toList());
		if ( ! newMarc.isEmpty() && ! oldMarc.isEmpty() ) {
			System.out.println("MARC record compare!");
			Set<String> newMarcFields = Arrays.stream((
					new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC,newMarc.get(0),false))
					.toString().split("\n")).collect(Collectors.toSet());
			Set<String> oldMarcFields = Arrays.stream((
					new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC,oldMarc.get(0),false))
					.toString().split("\n")).collect(Collectors.toSet());
			Set<String> commonMarcFields = new HashSet<>();
			for (String f : newMarcFields) if ( oldMarcFields.contains(f) ) commonMarcFields.add(f);
			newMarcFields.removeAll(commonMarcFields);
			oldMarcFields.removeAll(commonMarcFields);
			for ( String f : oldMarcFields ) System.out.printf("-marcfield- %s\n", f);
			for ( String f : newMarcFields ) System.out.printf("+marcfield+ %s\n", f);
			oldFields = oldFields.stream().filter(f -> ! f.startsWith("marc_display: "))
					.collect(Collectors.toSet());
			newFields = newFields.stream().filter(f -> ! f.startsWith("marc_display: "))
					.collect(Collectors.toSet());
		}
		for (String f : oldFields) System.out.printf("- %s\n", f);
		for (String f : newFields) System.out.printf("+ %s\n", f);
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
		sbMainTableCreate.append("hrid  VARCHAR(15) NOT NULL PRIMARY KEY,\n");
		sbMainTableCreate.append("record_dates text,\n");
		sbMainTableCreate.append("visit_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n");
		for ( Generator gen : this.activeMarcGenerators ) {
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

	private void pushNewFieldDataToDB(
			boolean isMarc,
			List<BibGeneratorData> newValues,
			String bibId,
			String recordVersions,
			Config config) throws SQLException {

		String sql;
		EnumSet<Generator> generators;
		if (isMarc) {
			generators = this.activeMarcGenerators;
			if (sqlMarc == null) sqlMarc = generateInsertQuery(generators);
			sql = sqlMarc;
		} else {
			generators = this.activeNonMarcGenerators;
			if (sqlNonMarc == null) sqlNonMarc = generateInsertQuery(generators);
			sql = sqlNonMarc;
		}

		Map<Generator,BibGeneratorData> newValuesMap = new HashMap<>();
		for (BibGeneratorData data : newValues) newValuesMap.put(data.getGenerator(), data);
		try ( Connection conn = config.getDatabaseConnection("Current");
				PreparedStatement pstmt = conn.prepareStatement(sql)) {
			int parameterIndex = 1;
			pstmt.setString(parameterIndex++, bibId);
			pstmt.setString(parameterIndex++, recordVersions);
			for (Generator gen : generators) {
				BibGeneratorData data = newValuesMap.get(gen);
				pstmt.setString(parameterIndex++, data.inputHash);
				pstmt.setString(parameterIndex++, data.solrSegment);
				pstmt.setTimestamp(parameterIndex++, data.solrGenDate);
			}
			pstmt.executeUpdate();
		}
		
	}
	private String generateInsertQuery(EnumSet<Generator> generators) {

		StringBuilder sbSql = new StringBuilder();
		sbSql.append("REPLACE INTO ").append(this.tableNamePrefix).append("Data (");
		sbSql.append("hrid, record_dates, \n");
		for (Generator gen : generators) {
			String genName = gen.name().toLowerCase();
			sbSql.append(genName).append("_marc_segment,\n");
			sbSql.append(genName).append("_solr_fields,\n");
			sbSql.append(genName).append("_solr_fields_gen_date,\n");		
		}
		sbSql.setCharAt(sbSql.length()-2, ')');
		sbSql.append("VALUES ( ");
		int questionMarksNeeded = generators.size()*3+2;
		for (int i = 1 ; i <= questionMarksNeeded; i++) sbSql.append("?,");
		sbSql.setCharAt(sbSql.length()-1, ')');
		return sbSql.toString();
	}

	private static String crc32(String marcSegment) {
		CRC32 crc32 = new CRC32();
		crc32.update(marcSegment.getBytes());
		return String.format("[CRC32:%d]", crc32.getValue());
	}
	private static String sqlMarc = null;
	private static String sqlNonMarc = null;


	private static void touchBibVisitDate(String tableNamePrefix, String bibId, Config config)
			throws SQLException {
		try ( Connection conn = config.getDatabaseConnection("Current");
				PreparedStatement pstmt = conn.prepareStatement
						("UPDATE "+tableNamePrefix+"Data SET visit_date = NOW() WHERE hrid = ?")) {
			pstmt.setString(1, bibId);
			pstmt.executeUpdate();
		}		
	}


	private static Map<Generator, BibGeneratorData> pullPreviousFieldDataFromDB
	(EnumSet<Generator> activeGenerators,String tableNamePrefix, String bibId, Config config)
			throws SQLException {

		Map<Generator,BibGeneratorData> allData = new HashMap<>();
		try ( Connection conn = config.getDatabaseConnection("Current");
				PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM "+tableNamePrefix+"Data WHERE hrid = ?") ){
			pstmt.setString(1, bibId);
			try ( ResultSet rs = pstmt.executeQuery() ) {
				while (rs.next()) {
					for ( Generator gen : activeGenerators) {
						String genName = gen.name().toLowerCase();
						BibGeneratorData d = new BibGeneratorData(
								rs.getString(genName+"_marc_segment"),//TODO rename field to input_hash
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

	private static Map<String, List<Generator>> constructFieldsSupported(
			EnumSet<Generator> activeGenerators) {
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

		String inputHash = crc32( recChunk.toXML(true) );
		Status marcStatus;
		if  (origData.inputHash == null)
			marcStatus = Status.NEW;
		else if (inputHash.equals(origData.inputHash))
			marcStatus = Status.UNCHANGED;
		else
			marcStatus = Status.CHANGED;

		if (marcStatus.equals(Status.UNCHANGED)) {
			if (Timestamp.valueOf(now.minus(gen.getInstance().resultsShelfLife())).after(origData.solrGenDate)
					|| genModDate.after(origData.solrGenDate))
				marcStatus = Status.STALE;
			else if ( forced )
				marcStatus = Status.FORCED;
			else if (randomCountDown-- <= 0 ) {
				marcStatus = Status.RANDOM;
				randomCountDown = random.nextInt(config.getRandomGeneratorWavelength());
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
		} catch (SQLException | IOException e) {
			System.out.println("Generator "+gen+" failed on bib"+recChunk.id+"\n");
			e.printStackTrace();
			return null;
		}
		BibGeneratorData newData = new BibGeneratorData(inputHash, solrFields, Timestamp.valueOf(now) );
		newData.oldData = origData;
		newData.marcStatus = marcStatus;
		newData.solrStatus = (origData.solrSegment == null) ? Status.NEW :
			( solrFields.equals(origData.solrSegment) ) ? Status.UNCHANGED : Status.CHANGED;
		newData.gen = gen;
		if ( ! newData.solrStatus.equals(Status.UNCHANGED) && gen.getInstance().providesHeadingBrowseData() )
			newData.triggerHeadingsUpdate = true;
		return newData;
	}
	private static BibGeneratorData processInstanceWithGenerator(
			Generator gen,Map<String,Object> instance,
			BibGeneratorData origData, LocalDateTime now, Config config){

		String instanceJson;
		try { instanceJson = mapper.writeValueAsString(instance); }
		catch (JsonProcessingException e) { e.printStackTrace(); return null; }
		String inputHash = crc32( instanceJson );
		Status instanceStatus;
		if ( origData.inputHash == null )
			instanceStatus = Status.NEW;
		else if ( inputHash.equals(origData.inputHash) )
			instanceStatus = Status.UNCHANGED;
		else
			instanceStatus = Status.CHANGED;

		String solrFields;
		SolrFields s = null;
		try {
			s = gen.getInstance().generateNonMarcSolrFields(instance, config);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		solrFields = ( s == null ) ? null : s.toString();
		BibGeneratorData newData = new BibGeneratorData(inputHash, solrFields, Timestamp.valueOf(now));
		newData.oldData = origData;
		newData.marcStatus = instanceStatus;
		if ( origData.solrSegment == null )
			newData.solrStatus = ( newData.solrSegment == null )?Status.UNCHANGED:Status.CHANGED;
		else
			newData.solrStatus = (origData.solrSegment.equals(newData.solrSegment))?Status.UNCHANGED:Status.CHANGED;
		newData.gen = gen;
		if ( ! newData.solrStatus.equals(Status.UNCHANGED) && gen.getInstance().providesHeadingBrowseData() )
			newData.triggerHeadingsUpdate = true;
		return newData;

	}
	static ObjectMapper mapper = new ObjectMapper();

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
			recordChunks.get(gen).bib_id = rec.bib_id;
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
			for (Generator supportingClass : fieldsSupported.get("holdings")) {
				recordChunks.get(supportingClass).marcHoldings = rec.marcHoldings;
				recordChunks.get(supportingClass).folioHoldings = rec.folioHoldings;
			}

		if ( fieldsSupported.containsKey("instance"))
			for (Generator supportingClass : fieldsSupported.get("instance"))
				recordChunks.get(supportingClass).instance = rec.instance;

		return recordChunks;
	}

	public static class BibChangeSummary {
		final String changedSegments;
		final String changedHeadingsSegments;
		BibChangeSummary(String segments) {
			this.changedSegments = segments;
			this.changedHeadingsSegments = null;
		}
		BibChangeSummary(String segments, String changedHeadingsSegments) {
			this.changedSegments = segments;
			this.changedHeadingsSegments = changedHeadingsSegments;
		}
	}
	private static class BibGeneratorData {
		final String inputHash;
		final String solrSegment;
		final Timestamp solrGenDate;
		Status marcStatus = null;
		Status solrStatus = null;
		Generator gen = null;
		BibGeneratorData oldData = null;
		boolean triggerHeadingsUpdate = false;
		public BibGeneratorData( String inputHash, String solrSegment, Timestamp solrGenDate) {
			this.inputHash = inputHash;
			this.solrSegment = solrSegment;
			this.solrGenDate = solrGenDate;
		}
		public Generator getGenerator() { return this.gen; }
	}

	private enum Status { UNGENERATED,NEW,CHANGED,UNCHANGED,STALE,FORCED,RANDOM; }
}
