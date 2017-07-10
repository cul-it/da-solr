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
import java.util.Calendar;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.Generator;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

/**
 * Generate Solr fields based on a MARC bibliographic record with optional holdings, using a configured
 * set of field generators. Requires a SolrBuildConfig containing the necessary database
 * connection information to push the results to the configured DB.
 * For example:<br/><br/>
 * <pre>MarcRecord rec = ...;
 * SolrBuildConfig config = ...;
 * GenerateSolrFields gen = new GenerateSolrFields(EnumSet.of(
 *		Generator.AUTHORTITLE, Generator.SUBJECT));
 * gen.generateSolr(rec, config);</pre> *
 */
public class GenerateSolrFields {

	private final EnumSet<Generator> activeGenerators;
	// keyed by MARC field
	private final Map<String,List<Generator>> fieldsSupported = new HashMap<>();

	public GenerateSolrFields( EnumSet<Generator> activeGenerators ) {
		this.activeGenerators = activeGenerators;
		for (Generator gen : this.activeGenerators) {
			List<String> classFieldsSupported = gen.getInstance().getHandledFields();
			for (String field : classFieldsSupported) {
				if ( ! fieldsSupported.containsKey(field) )
					fieldsSupported.put(field, new ArrayList<Generator>());
				fieldsSupported.get(field).add(gen);
			}
		}
	}

	public void generateSolr( MarcRecord rec, SolrBuildConfig config )
			throws ClassNotFoundException, SQLException, IOException {
		Map<Generator,MarcRecord> recordChunks = createMARCChunks(rec);
		for (Entry<Generator,MarcRecord> e : recordChunks.entrySet())
			processRecordChunkWithGenerator( e.getKey(), e.getValue(), config );
	}

	public void setUpDatabase( SolrBuildConfig config ) throws ClassNotFoundException, SQLException {

		try ( Connection conn = config.getDatabaseConnection("Current");
				Statement stmt = conn.createStatement()) {
			for ( Generator gen : activeGenerators )
				stmt.executeUpdate(createTableSQL.replace("$segmentTable", gen.getDbTable()));
		}
	}
	private static final String createTableSQL =
	"CREATE TABLE IF NOT EXISTS $segmentTable ( \n"+
	" bib_id                INT(10) UNSIGNED NOT NULL PRIMARY KEY,\n"+
	" marc_segment          TEXT DEFAULT NULL,\n"+
	" marc_segment_mod_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"+
	" solr_fields_json      LONGTEXT DEFAULT NULL,\n"+
	" solr_fields_mod_date  TIMESTAMP NULL,\n"+
	" solr_fields_gen_date  TIMESTAMP NULL )";
	private static ObjectMapper mapper = new ObjectMapper();

	private static void processRecordChunkWithGenerator(Generator gen, MarcRecord rec, SolrBuildConfig config)
			throws ClassNotFoundException, SQLException, IOException {
		// Is the segment already existent? If so, is it changed? stale?
		Status segStat = updateMarcSegmentInDBAndSeeIfChanged( gen, rec, config );
		if (segStat.equals(Status.UNCHANGED)) {
			System.out.println("There's no need to generate Solr fields for Generator "+gen);
			return;
		}
		Status fieldsStat = generateSolrFieldsThroughGenerator( gen, rec, config );
	}

	private static final String solrJsonQuerySQL =
	"SELECT solr_fields_json, solr_fields_mod_date FROM $segmentTable WHERE bib_id = ?";
	private static final String solrJsonUpdateSQL =
	"UPDATE $segmentTable "
	+ "SET solr_fields_json = ?, solr_fields_mod_date = ?, solr_fields_gen_date = ? "
	+ "WHERE bib_id = ?";
	private static Status generateSolrFieldsThroughGenerator(Generator gen, MarcRecord rec, SolrBuildConfig config)
			throws ClassNotFoundException, SQLException, IOException {
		SolrFields sfs = gen.getInstance().generateSolrFields(rec, config);
		String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(sfs.getFields());
		Status stat = null;
		try ( Connection conn = config.getDatabaseConnection("Current") ) {
			Timestamp prevModDate = null;
			try ( PreparedStatement pstmt = conn.prepareStatement(
					solrJsonQuerySQL.replace("$segmentTable", gen.getDbTable())) ){
				pstmt.setInt(1,Integer.valueOf(rec.id));
				try ( ResultSet rs = pstmt.executeQuery() ) {
					while (rs.next()) {
						prevModDate = rs.getTimestamp(2);
						if (json.equals(rs.getString(1)))
							stat = Status.UNCHANGED;
						else
							stat = Status.CHANGED;
					}
				}
			}
			try ( PreparedStatement pstmt = conn.prepareStatement(
					solrJsonUpdateSQL.replace("$segmentTable", gen.getDbTable())) ){
				Timestamp now = new Timestamp( Calendar.getInstance().getTime().getTime() );
				pstmt.setString(    1, json );
				pstmt.setTimestamp( 2, Status.CHANGED.equals(stat)?now:prevModDate );
				pstmt.setTimestamp( 3, now);
				pstmt.setInt(       4, Integer.valueOf(rec.id));
				pstmt.executeUpdate();
			}
		}
		return stat;
	}
	private static final String marcSegmentQuerySQL = // java.sql.Time.valueOf( LocalDateTime.now().plus( duration ) )
	"SELECT marc_segment, (solr_fields_gen_date > ?) AS is_fresh FROM $segmentTable WHERE bib_id = ?";
	private static final String marcSegmentInsertSQL =
	"INSERT INTO $segmentTable ( bib_id, marc_segment ) VALUES ( ? , ? )";
	private static final String marcSegmentUpdateSQL =
	"UPDATE $segmentTable SET marc_segment =?, marc_segment_mod_date = NOW() WHERE bib_id = ?";
	private static Status updateMarcSegmentInDBAndSeeIfChanged(Generator gen, MarcRecord rec, SolrBuildConfig config)
			throws NumberFormatException, SQLException, ClassNotFoundException {
		try ( Connection conn = config.getDatabaseConnection("Current") ) {
			Status stat = null;
			try ( PreparedStatement pstmt = conn.prepareStatement(
					marcSegmentQuerySQL.replace("$segmentTable", gen.getDbTable())) ){
				pstmt.setTimestamp(1,Timestamp.valueOf(LocalDateTime.now().plus(gen.getInstance().resultsShelfLife())));
				pstmt.setInt(2, Integer.valueOf(rec.id));
				try ( ResultSet rs = pstmt.executeQuery() ) {
					while (rs.next()) {
						if ( rs.getString(1).equals(rec.toString()) ) {
							if ( rs.getBoolean(2) == true ) {
								System.out.println( "MARC segment unchanged.");
								return Status.UNCHANGED;
							}
							System.out.println( "JSON stale." );
							return Status.STALE;
						}
						System.out.println( "Marc segment changed." );
						stat = Status.CHANGED;
					}
				}
			}
			if (stat == null) // Status.NEW
				try ( PreparedStatement pstmt = conn.prepareStatement(
						marcSegmentInsertSQL.replace("$segmentTable", gen.getDbTable())) ){
					pstmt.setInt(1, Integer.valueOf(rec.id));
					pstmt.setString(2, rec.toString());
					pstmt.executeUpdate();
					System.out.println( "Bib "+rec.id+" is not yet represented in "+gen.getDbTable());
					return Status.NEW;
				}
			if (Status.CHANGED.equals(stat))
				try ( PreparedStatement pstmt = conn.prepareStatement(
						marcSegmentUpdateSQL.replace("$segmentTable", gen.getDbTable())) ){
					pstmt.setString(1, rec.toString());
					pstmt.setInt(2, Integer.valueOf(rec.id));
					pstmt.executeUpdate();
					return stat;
				}
		}
		return null;
	}

	private Map<Generator, MarcRecord> createMARCChunks(MarcRecord rec) {
		Map<Generator,MarcRecord> recordChunks = new HashMap<>();
		for (ControlField f : rec.controlFields)
			if (fieldsSupported.containsKey(f.tag)) {
				for( Generator supportingClass : fieldsSupported.get(f.tag)) {
					if ( ! recordChunks.containsKey(supportingClass) ) {
						recordChunks.put(supportingClass, new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC ));
						recordChunks.get(supportingClass).id = rec.id;
					}
					recordChunks.get(supportingClass).controlFields.add(f);
				}
			}
		for (DataField f : rec.dataFields)
			if (fieldsSupported.containsKey(f.tag)) {
				for( Generator supportingClass : fieldsSupported.get(f.tag)) {
					if ( ! recordChunks.containsKey(supportingClass) ) {
						recordChunks.put(supportingClass, new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC ));
						recordChunks.get(supportingClass).id = rec.id;
					}
					recordChunks.get(supportingClass).dataFields.add(f);
				}
			}
		return recordChunks;
	}

	private enum Status { NEW,CHANGED,UNCHANGED,STALE; }
}
