package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.addDashesTo_YYYYMMDD_Date;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.RecordSet;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.ReferenceType;
import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.FieldValues;
import edu.cornell.library.integration.utilities.NameUtils;

public class IndexAuthorityRecords {

	private Connection connection = null;
	private static List<Integer> authorTypes = Arrays.asList(
			HeadTypeDesc.PERSNAME.ordinal(),
			HeadTypeDesc.CORPNAME.ordinal(),
			HeadTypeDesc.EVENT.ordinal());

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// load configuration for location of index, location of authorities
		Collection<String> requiredArgs = Config.getRequiredArgsForDB("Headings");
		requiredArgs.add("authorityMarcDirectory");

		Config config = Config.loadConfig(requiredArgs);
		try {
			new IndexAuthorityRecords(config);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public IndexAuthorityRecords(Config config)
			throws SQLException, FileNotFoundException, IOException, XMLStreamException {

		this.connection = config.getDatabaseConnection("Headings");
		//set up database (including populating description maps)
		setUpDatabase();

		String mrcDir = config.getAuthorityMarcDirectory();
		System.out.println("Looking for authority MARC in directory: "+mrcDir);
		File[] srcList = (new File(mrcDir)).listFiles();
		if ( srcList == null ) {
			System.out.printf( "'%s' is not a valid directory.\n",mrcDir);
			return;
		}
		if (srcList.length == 0) {
			System.out.printf("No files available to process in '%s'.\n",mrcDir);
			return;
		}
		System.out.println("Found: "+srcList.length+" files.");
		for (File srcFile : srcList) {
			System.out.println(srcFile);
			processFile( srcFile );
		}
		this.connection.close();
	}

	private void processFile( File srcFile )
			throws FileNotFoundException, IOException, XMLStreamException, SQLException {
		try (  InputStream is = new FileInputStream( srcFile );
				Scanner s1 = new Scanner(is);
				Scanner s2 = s1.useDelimiter("\\A")) {
			String marc21OrMarcXml = s2.hasNext() ? s2.next() : "";
			List<MarcRecord> recs = MarcRecord.getMarcRecords(RecordType.AUTHORITY, marc21OrMarcXml);
			System.out.println(recs.size() + " records found in file.");
			for (MarcRecord rec : recs)
				createHeadingRecordsFromAuthority(rec);
		}
	}

	private void setUpDatabase() throws SQLException {
		try ( Statement stmt = this.connection.createStatement() ) {

		stmt.execute("DROP TABLE IF EXISTS `heading`");
		stmt.execute("CREATE TABLE `heading` ("
				+ "`id` int(10) unsigned NOT NULL auto_increment, "
				+ "`heading` text,   `sort` mediumtext NOT NULL, "
				+ "`type_desc` tinyint(3) unsigned NOT NULL, "
				+ "`authority` tinyint(1) NOT NULL default '0', "
				+ "`main_entry` tinyint(1) NOT NULL default '0', "
				+ "`undifferentiated` tinyint(1) NOT NULL default '0', "
				+ "`works_by` mediumint(8) unsigned NOT NULL default '0', "
				+ "`works_about` mediumint(8) unsigned NOT NULL default '0', "
				+ "`works` mediumint(8) unsigned NOT NULL default '0', "
				+ "PRIMARY KEY  (`id`), "
				+ "KEY `uk` (`type_desc`,`sort`(100))) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

		stmt.execute("DROP TABLE IF EXISTS `note`");
		stmt.execute("CREATE TABLE `note` ( "
				+ "`heading_id` int(10) unsigned NOT NULL, "
				+ "`note` text NOT NULL, "
				+ "KEY (`heading_id`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

		stmt.execute("DROP TABLE IF EXISTS `authority`");
		stmt.execute("CREATE TABLE `authority` ( "
				+ "`heading_id` int(10) unsigned NOT NULL, "
				+ "`authority_id` text NOT NULL, "
				+ "KEY (`heading_id`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

		stmt.execute("DROP TABLE IF EXISTS `ref_type`");
		stmt.execute("CREATE TABLE `ref_type` ( "
				+ "`id` tinyint(3) unsigned NOT NULL, "
				+ "`name` varchar(256) NOT NULL, "
				+ "PRIMARY KEY  (`id`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=latin1");

		stmt.execute("DROP TABLE IF EXISTS `reference`");
		stmt.execute("CREATE TABLE `reference` ( "
				+ "`from_heading` int(10) unsigned NOT NULL, "
				+ "`to_heading` int(10) unsigned NOT NULL, "
				+ "`ref_type` tinyint(3) unsigned NOT NULL, "
				+ "`ref_desc` varchar(256) NOT NULL DEFAULT '', "
				+ " PRIMARY KEY (`from_heading`,`to_heading`,`ref_type`,`ref_desc`), "
				+ " KEY (`to_heading`) ) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=latin1");

		stmt.execute("DROP TABLE IF EXISTS `type_desc`");
		stmt.execute("CREATE TABLE `type_desc` ( "
				+ "`id` tinyint(3) unsigned NOT NULL, "
				+ "`name` varchar(256) NOT NULL, "
				+ "PRIMARY KEY  (`id`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=latin1");

		stmt.execute("DROP TABLE IF EXISTS `rda`");
		stmt.execute("CREATE TABLE `rda` ( "
				+ "`heading_id` int(10) unsigned NOT NULL, "
				+ "`rda` text NOT NULL, "
				+ "KEY `heading_id` (`heading_id`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");
		}

		try ( PreparedStatement insertDesc = this.connection.prepareStatement(
				"INSERT INTO type_desc (id,name) VALUES (? , ?)") ) {
		for ( HeadTypeDesc ht : HeadTypeDesc.values()) {
			insertDesc.setInt(1, ht.ordinal());
			insertDesc.setString(2, ht.toString());
			insertDesc.executeUpdate();
		}}

		try ( PreparedStatement insertRefType = this.connection.prepareStatement(
				"INSERT INTO ref_type (id,name) VALUES (? , ?)") ) {
		for ( ReferenceType rt : ReferenceType.values()) {
			insertRefType.setInt(1, rt.ordinal());
			insertRefType.setString(2, rt.toString());
			insertRefType.executeUpdate();
		}}
	}

	private void createHeadingRecordsFromAuthority( MarcRecord rec ) throws SQLException, JsonProcessingException  {
		String heading = null;
		String headingSort = null;
		String lccn = null;
		HeadTypeDesc htd = null;
		Collection<Relation> sees = new HashSet<>();
		Collection<Relation> seeAlsos = new HashSet<>();
		Collection<String> expectedNotes = new HashSet<>();
		Collection<String> foundNotes = new HashSet<>();
		Collection<String> notes = new HashSet<>();
		RdaData rdaData = new RdaData();

		Boolean isUndifferentiated = false;

		for (ControlField f : rec.controlFields) {
			if (f.tag.equals("008")) {
				if (f.value.length() >= 10) {
					Character recordType = f.value.charAt(9);
					if (recordType.equals('b')) 
						expectedNotes.add("666");
					else if (recordType.equals('c'))
						expectedNotes.add("664");
					Character undifferentiated = f.value.charAt(32);
					if (undifferentiated.equals('b'))
						isUndifferentiated = true;
				}
			}
		}
		// iterate through fields. Look for main heading and alternate forms.
		for (DataField f : rec.dataFields) {
			FieldValues nameFieldVals = null;
			if (f.tag.equals("010")) {
				for (Subfield sf : f.subfields) 
					if (sf.code.equals('a')) {
						if (sf.value.startsWith("sj")) {
							// this is a Juvenile subject authority heading, which
							// we will not represent in the headings browse.
							System.out.println("Skipping Juvenile subject authority heading: "+rec.id);
							return;
						}
						lccn = sf.value;
					}
			} else if (f.tag.startsWith("1")) { // main heading
				switch (f.tag) {
				case "100":
				case "110":
				case "111":
					nameFieldVals = NameUtils.authorAndOrTitleValues(f);
					if (nameFieldVals.type.equals(HeadType.AUTHORTITLE))
						htd = HeadTypeDesc.WORK;
					else if (f.tag.equals("100"))
						htd = HeadTypeDesc.PERSNAME;
					else if (f.tag.equals("110"))
						htd = HeadTypeDesc.CORPNAME;
					else
						htd = HeadTypeDesc.EVENT;
					break;
				case "130":
					htd = HeadTypeDesc.WORK;
					break;
				case "148":
					htd = HeadTypeDesc.CHRONTERM;
					break;
				case "150":
					htd = HeadTypeDesc.TOPIC;
					break;
				case "151":
					htd = HeadTypeDesc.GEONAME;
					break;
				case "155":
					htd = HeadTypeDesc.GENRE;
					break;
				case "162":
					htd = HeadTypeDesc.MEDIUM;
					break;
				default:
					// If the record is for a subdivision (main entry >=180),
					// we won't do anything with it.
					System.out.println("Not deriving heading browse entries from record. "+rec.id);
					return;
				}
				heading = dashedHeading(f, htd, nameFieldVals);

			} else if (f.tag.equals("260") || f.tag.equals("360")) {
				notes.add("Search under: "+f.concatenateSubfieldsOtherThan(""));
			} else if (f.tag.startsWith("3")) {

				String fieldName = null;

				MAIN: switch (f.tag) {
				case "370":
					for (Subfield sf : f.subfields)
						switch (sf.code) {
						case 'a': rdaData.add("Birth Place", sf.value);		break;
						case 'b': rdaData.add("Place of Death",sf.value);	break;
						case 'c': rdaData.add("Country",sf.value);			break;
						}
					break MAIN;

				case "372":
				case "373":
				case "374":
//				case "375":
				{
					String start = null, end = null, field = null;
					List<String> values = new ArrayList<>();
					switch (f.tag) {
					case "372": field = "Field"; break;
					case "373": field = "Group/Organization"; break;
					case "374": field = "Occupation"; break;
//					case "375": field = "Gender"; break;
					}
					for (Subfield sf : f.subfields)
						switch (sf.code) {
						case 'a': values.add( sf.value ); break;
						case 's': start = addDashesTo_YYYYMMDD_Date(sf.value); break;
						case 't': end = addDashesTo_YYYYMMDD_Date(sf.value); break;
						}
					if (values.isEmpty()) break MAIN;
					for (String value : values)
						if (start != null) {
							if (end != null) {
								if (start.equals(end))
									rdaData.add(field, String.format("%s (%s)", value,start));
								else
									rdaData.add(field, String.format("%s (%s through %s)", value,start,end));
							} else {
								rdaData.add(field, String.format("%s (starting %s)", value,start));
							}
						} else {
							if (end != null)
								rdaData.add(field, String.format("%s (until %s)", value,end));
							else
								rdaData.add(field, value);
						}
					}
					break MAIN;
				case "380": fieldName = "Form of Work";		break MAIN;
				case "382": fieldName = "Instrumentation";
				} //end MAIN

				if (fieldName != null)
					for (Subfield sf : f.subfields)
						if (sf.code.equals('a'))
							rdaData.add(fieldName, sf.value);

			} else if (f.tag.startsWith("4")) {
				// equivalent values
				Relation r = determineRelationship(f);
				if (r != null) {
					expectedNotes.addAll(r.expectedNotes);
					buildXRefHeading(r,f,heading);
					r.headingSort = getFilingForm( r.heading );
					for (Relation s : sees) 
						if (s.headingSort.equals(r.headingSort))
							r.display = false;
					sees.add(r);
				}
			} else if (f.tag.startsWith("5")) {
				// see alsos
				Relation r = determineRelationship(f);
				if (r != null) {
					expectedNotes.addAll(r.expectedNotes);
					buildXRefHeading(r,f,heading);
					r.headingSort = getFilingForm( r.heading );
					for (Relation s : seeAlsos) 
						if (s.headingSort.equals(r.headingSort))
							r.display = false;
					seeAlsos.add(r);
				}
			} else if (f.tag.equals("663")) {
				foundNotes.add("663");
				if (expectedNotes.contains("663")) {
					notes.add(buildJsonNote(f));
				} else {
					System.out.println("Field 663 found, but no matching 4XX or 5XX subfield w. "+rec.id);
				}
			} else if (f.tag.equals("664")) {
				foundNotes.add("664");
				if (expectedNotes.contains("664")) {
					notes.add(buildJsonNote(f));
				} else {
					System.out.println("Field 664 found, but no matching 4XX or 5XX subfield w or matching record type: c. "+rec.id);
				}
			} else if (f.tag.equals("665")) {
				foundNotes.add("665");
				if (expectedNotes.contains("665")) {
					notes.add(buildJsonNote(f));
				} else {
					System.out.println("Field 665 found, but no matching 4XX or 5XX subfield w. "+rec.id);
				}
			} else if (f.tag.equals("666")) {
				foundNotes.add("666");
				if (expectedNotes.contains("666")) {
					notes.add(buildJsonNote(f));
				} else {
					System.out.println("Field 666 found, but no matching record type: b. "+rec.id);
				}
			}
		}
		headingSort = getFilingForm(heading);

		// Populate Main Entry Heading Record
		Integer heading_id = getMainHeadingRecordId(heading,headingSort,htd,isUndifferentiated);
		for (String note : notes)
			insertNote(heading_id, note);
		if (lccn != null)
			insertAuthorityId(heading_id, lccn);

		expectedNotes.removeAll(foundNotes);
		if ( ! expectedNotes.isEmpty())
			System.out.println("Expected notes based on 4XX and/or 5XX subfield ws that didn't appear. "+rec.id);


		// Populate incoming 4XX cross references
		for (Relation r: sees) {
			if ( ! r.display) continue;
			if ( r.headingSort.equals(headingSort)) continue;
			crossRef(heading_id,r,ReferenceType.FROM4XX);
		}

		for (Relation r: seeAlsos) {
			// Populate incoming 5XX cross references
			if ( ! r.display) continue;
			if ( r.headingSort.equals(headingSort)) continue;
			crossRef(heading_id,r,ReferenceType.FROM5XX);

			// Where appropriate, populate outgoing 5XX cross references
			if (r.reciprocalRelationship != null)
				directRef(heading_id,r,ReferenceType.TO5XX);
		}

		populateRdaInfo(heading_id, rdaData);

		return;
	}

	private static String dashedHeading(DataField f, HeadTypeDesc htd, FieldValues nameFieldVals) {
		String dashed_terms = f.concatenateSpecificSubfields(" > ", "vxyz");
		String heading = null;
		if (htd.equals(HeadTypeDesc.WORK)) {
			if (f.tag.endsWith("30"))
				heading = f.concatenateSpecificSubfields("abcdeghjklmnopqrstu");
			else {
				if (nameFieldVals == null)
					nameFieldVals = NameUtils.authorAndOrTitleValues(f);
				heading = nameFieldVals.author+" | "+nameFieldVals.title;
			}
		} else if (authorTypes.contains(htd.ordinal())){
			heading = NameUtils.facetValue(f);
		} else {
			heading = f.concatenateSpecificSubfields("abcdefghjklmnopqrstu");
		}
		if ( ! heading.isEmpty() && ! dashed_terms.isEmpty() )
			heading += " > "+dashed_terms;
		return heading;
	}

	private static String buildJsonNote(DataField f) throws JsonProcessingException {
		List<Object> textBlocks = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		for (Subfield sf : f.subfields) {
			if (sf.code.equals('b')) {
				if (sb.length() > 0) {
					textBlocks.add(sb.toString());
					sb.setLength(0);
				}
				Map<String,String> header = new HashMap<>();
				header.put("header", sf.value);
				textBlocks.add(header);
			} else {
				if (sb.length() > 0) sb.append(' ');
				sb.append(sf.value);
			}
		}
		if (sb.length() > 0)
			textBlocks.add(sb.toString());
		return mapper.writeValueAsString(textBlocks);
	}

	private void populateRdaInfo(Integer heading_id, RdaData data) throws SQLException, JsonProcessingException {
		String json = data.json();
		if (json == null) return;
		try ( PreparedStatement stmt = this.connection.prepareStatement(
				"INSERT INTO rda (heading_id, rda) VALUES (?, ?)") ){
			stmt.setInt(1, heading_id);
			stmt.setString(2, json);
			stmt.executeUpdate();
		}
	}

	private void insertNote(Integer heading_id, String note) throws SQLException {
		try ( PreparedStatement stmt = this.connection.prepareStatement(
				"INSERT INTO note (heading_id, note) VALUES (? , ?)") ){
			stmt.setInt(1, heading_id);
			stmt.setString(2, note);
			stmt.executeUpdate();
		}
	}

	private void insertAuthorityId(Integer heading_id, String auth_id) throws SQLException {
		try ( PreparedStatement stmt = this.connection.prepareStatement(
				"INSERT INTO authority (heading_id, authority_id) VALUES (? , ?)") ){
			stmt.setInt(1, heading_id);
			stmt.setString(2, auth_id);
			stmt.executeUpdate();
		}
	}

	private Integer getMainHeadingRecordId(String heading, String headingSort,
			HeadTypeDesc htd, Boolean isUndifferentiated) throws SQLException {

		Integer recordId = null;
		boolean undifferentiated = false;

		try ( PreparedStatement pstmt = this.connection.prepareStatement(
				"SELECT id, undifferentiated FROM heading " +
				"WHERE type_desc = ? AND sort = ?") ){
			pstmt.setInt(1, htd.ordinal());
			pstmt.setString(2, headingSort);
			try ( ResultSet resultSet = pstmt.executeQuery() ) {
				while (resultSet.next()) {
					recordId = resultSet.getInt(1);
					undifferentiated = resultSet.getBoolean(2);
				}
			}
		}
		if (recordId != null) {
			// update sql record to make sure it's a main entry now; heading overrides
			// possibly different heading form populated from xref
			try ( PreparedStatement pstmt = this.connection.prepareStatement(
					"UPDATE heading SET main_entry = 1, heading = ? WHERE id = ?") ){
				pstmt.setString(1, heading); 
				pstmt.setInt(2, recordId);
				pstmt.executeUpdate();
			}
			// if the record was inserted as differentiated, but this one is undifferentiated
			// the update it.
			if (isUndifferentiated && ! undifferentiated) {
				try ( PreparedStatement pstmt = this.connection.prepareStatement(
						"UPDATE heading SET undifferentiated = 1 WHERE id = ?") ) {
					pstmt.setInt(1, recordId);
					pstmt.executeUpdate();
				}
			}
		} else {

			// create new record
			try (PreparedStatement pstmt = this.connection.prepareStatement(
					"INSERT INTO heading (heading, sort, type_desc, authority, main_entry, undifferentiated) " +
					"VALUES (?, ?, ?, 1, 1, ?)",
                    Statement.RETURN_GENERATED_KEYS) ) {

				pstmt.setString(1, Normalizer.normalize(heading, Normalizer.Form.NFC));
				pstmt.setString(2, headingSort);
				pstmt.setInt(3, htd.ordinal());
				pstmt.setBoolean(4, isUndifferentiated);
				int affectedCount = pstmt.executeUpdate();
				if (affectedCount < 1) 
					throw new SQLException("Creating Heading Record Failed.");
				try ( ResultSet generatedKeys = pstmt.getGeneratedKeys() ) {
					if (generatedKeys.next())
						recordId = generatedKeys.getInt(1); }
			}
		}

		return recordId;
	}

	/* If there are no more than 5 non-period characters in the heading,
	 * and all of those are capital letters, then this is an acronym.
	 */
	private static void buildXRefHeading( Relation r, DataField f , String mainHeading ) {
		String heading = dashedHeading(f, r.headingTypeDesc, null);
		String headingWOPeriods = heading.replaceAll("\\.", "");
		if (headingWOPeriods.length() > 5) {
			r.heading = heading;
			return;
		}
		boolean upperCase = true;
		for (char c : headingWOPeriods.toCharArray()) {
			if ( ! Character.isUpperCase(c)) {
				upperCase = false;
				break;
			}
		}
		if (upperCase)
			r.heading = heading + " (" + mainHeading + ")";
		else
			r.heading = heading;

	}

	private void crossRef(Integer heading_id, Relation r, ReferenceType rt) throws SQLException {
		int from_heading_id = getRelationshipHeadingId( r );
		insertRef(from_heading_id, heading_id, rt, r.relationship);
	}

	private void directRef(int heading_id, Relation r, ReferenceType rt) throws SQLException {
		int dest_heading_id = getRelationshipHeadingId( r );
		insertRef(heading_id, dest_heading_id, rt, r.reciprocalRelationship );
	}

	private void insertRef(int from_id, int to_id,
			ReferenceType rt, String relationshipDescription) throws SQLException {

		try ( PreparedStatement pstmt = this.connection.prepareStatement(
				"REPLACE INTO reference (from_heading, to_heading, ref_type, ref_desc)"
				+ " VALUES (?, ?, ?, ?)")  ){
			pstmt.setInt(1, from_id);
			pstmt.setInt(2, to_id);
			pstmt.setInt(3, rt.ordinal());
			if (relationshipDescription == null)
				pstmt.setString(4, "");
			else
				pstmt.setString(4, relationshipDescription);
			pstmt.executeUpdate();
		}
	}


	private Integer getRelationshipHeadingId(Relation r ) throws SQLException {

		Integer recordId = null;

		try ( PreparedStatement pstmt = this.connection.prepareStatement(
				"SELECT id FROM heading " +
				"WHERE type_desc = ? and sort = ?") ) {

			pstmt.setInt(1, r.headingTypeDesc.ordinal());
			pstmt.setString(2, r.headingSort);
			try ( ResultSet resultSet = pstmt.executeQuery() ) {
				while (resultSet.next())
					recordId = resultSet.getInt("id");
			}
		}
		if (recordId == null) {
			// create new record
			try ( PreparedStatement pstmt = this.connection.prepareStatement(
					"INSERT INTO heading (heading, sort, type_desc, authority) " +
					"VALUES (?, ?, ?, 1)",
                    Statement.RETURN_GENERATED_KEYS) ) {
				pstmt.setString(1, r.heading);
				pstmt.setString(2, r.headingSort);
				pstmt.setInt(3, r.headingTypeDesc.ordinal());
				int affectedCount = pstmt.executeUpdate();
				if (affectedCount < 1) 
					throw new SQLException("Creating Heading Record Failed.");
				try ( ResultSet generatedKeys = pstmt.getGeneratedKeys() ) {
				if (generatedKeys.next())
					recordId = generatedKeys.getInt(1); }
			}
		}

		return recordId;
	}


	private static Relation determineRelationship( DataField f ) {
		// Is there a subfield w? The relationship note in subfield w
		// describes the 4XX or 5XX heading, and must be reversed for the
		// from tracing.
		Relation r = new Relation();
		boolean hasW = false;

		switch( f.tag.substring(1) ) {
		case "00":
			r.headingTypeDesc = HeadTypeDesc.PERSNAME;
			for (Subfield sf : f.subfields)
				if (sf.code.equals('t'))
					r.headingTypeDesc = HeadTypeDesc.WORK;
			break;
		case "10":
			r.headingTypeDesc = HeadTypeDesc.CORPNAME;
			for (Subfield sf : f.subfields)
				if (sf.code.equals('t'))
					r.headingTypeDesc = HeadTypeDesc.WORK;
			break;
		case "11":
			r.headingTypeDesc = HeadTypeDesc.EVENT;
			for (Subfield sf : f.subfields)
				if (sf.code.equals('t'))
					r.headingTypeDesc = HeadTypeDesc.WORK;
			break;
		case "30":
			r.headingTypeDesc = HeadTypeDesc.WORK;	break;
		case "50":
			r.headingTypeDesc = HeadTypeDesc.TOPIC;		break;
		case "48":
			r.headingTypeDesc = HeadTypeDesc.CHRONTERM;	break;
		case "51":
			r.headingTypeDesc = HeadTypeDesc.GEONAME;	break;
		case "55":
			r.headingTypeDesc = HeadTypeDesc.GENRE;		break;
		case "62":
			r.headingTypeDesc = HeadTypeDesc.MEDIUM;	break;
		default: return null;
		}


		for (Subfield sf : f.subfields) {
			if (sf.code.equals('w')) {
				hasW = true;

				switch (sf.value.charAt(0)) {
				case 'a':
					//earlier heading
					r.relationship = "Later Heading";	break;
				case 'b':
					//later heading
					r.relationship = "Earlier Heading";	break;
				case 'd':
					//acronym
					r.relationship = "Full Heading";	break;
				case 'f':
					//musical composition
					r.relationship = "Musical Composition Based on this Work";
														break;
				case 'g':
					//broader term
					r.relationship = "Narrower Term";	break;
				case 'h':
					//narrower term
					r.relationship = "Broader Term";	break;
				case 'i':	
				case 'r':
					// get relationship name from subfield i 
					break;
				case 't':
					//parent body
					r.relationship = "Parent Body";
				}

				if (sf.value.length() >= 2) {
					switch (sf.value.charAt(1)) {
					case 'a':
						r.applicableContexts.add(RecordSet.NAME);		break;
					case 'b':
						r.applicableContexts.add(RecordSet.SUBJECT);	break;
					case 'c':
						r.applicableContexts.add(RecordSet.SERIES);		break;
					case 'd':
						r.applicableContexts.add(RecordSet.NAME);
						r.applicableContexts.add(RecordSet.SUBJECT);	break;
					case 'e':
						r.applicableContexts.add(RecordSet.NAME);
						r.applicableContexts.add(RecordSet.SERIES);		break;
					case 'f':
						r.applicableContexts.add(RecordSet.SUBJECT);
						r.applicableContexts.add(RecordSet.SERIES);		break;
					case 'g':
					default:
						r.applicableContexts.add(RecordSet.NAME);
						r.applicableContexts.add(RecordSet.SUBJECT);
						r.applicableContexts.add(RecordSet.SERIES);
						// 'g' refers to all three contexts. Any other values will not
						// be interpreted as limiting to applicable contexts.
					}
				} else {
					r.applicableContexts.add(RecordSet.NAME);
					r.applicableContexts.add(RecordSet.SUBJECT);
					r.applicableContexts.add(RecordSet.SERIES);
				}

				if (sf.value.length() >= 3) {
					Character offset2 = sf.value.charAt(2);
					if (offset2.equals('a')) {
						r.relationship = "Later Form of Heading";
					}
				}

				if (sf.value.length() >= 4) {
					switch (sf.value.charAt(3)) {
					case 'a':
						r.display = false;			break;
					case 'b':
						r.display = false;
						r.expectedNotes.add("664");	break;
					case 'c':
						r.display = false;
						r.expectedNotes.add("663");	break;
					case 'd':
						r.display = false;
						r.expectedNotes.add("665");	break;
					}
				}
			} else if (sf.code.equals('i')) {
				String rel = removeTrailingPunctuation(sf.value,": ").trim().toLowerCase();
				switch (rel) {
				case "alternate identity":
					r.relationship = "Real Identity";		break;
				case "real identity":
					r.relationship = "Alternate Identity";	break;
				case "family member":
					r.relationship = "Family";				break;
				case "family":
					r.relationship = "Family Member";		break;
				case "progenitor":
					r.relationship = "Descendants";			break;
				case "descendants":
					r.relationship = "Progenitor";			break;
				case "employee": 
					r.relationship = "Employer";			break;
				case "employer":
					r.relationship = "Employee";
					r.reciprocalRelationship = "Employer";	break;

				// The reciprocal relationship to descendant family is missing
				// from the RDA spec (appendix K), so it's unlikely that
				// "Progenitive Family" will actually appear. Of the three
				// adjective forms of "Progenitor" I found in the OED (progenital,
				// progenitive, progenitorial), progenitive has the highest level
				// of actual use according to my Google NGrams search
				case "descendant family": 
					r.relationship = "Progenitive Family";	break;
				case "progenitive family":
					r.relationship = "Descendant Family";
				}
			}
		}
		if ( ! hasW ) {
			r.applicableContexts.add(RecordSet.NAME);
			r.applicableContexts.add(RecordSet.SUBJECT);
			r.applicableContexts.add(RecordSet.SERIES);
		}
		return r;
	}

	static final ObjectMapper mapper = new ObjectMapper();
	private static class RdaData {

		Map<String,Collection<String>> data = new HashMap<>();

		public RdaData() { }

		public void add(String field, String value) {
			if ( ! this.data.containsKey(field))
				this.data.put(field, new HashSet<String>());
			this.data.get(field).add(value);
		}

		public String json() throws JsonProcessingException {
			if (this.data.isEmpty()) return null;
			return mapper.writeValueAsString(this.data);
		}
	}

	private static class Relation {	
		public Relation() { }
		public String relationship = null;
		public String reciprocalRelationship = null;
		public String heading = null;
		public String headingSort = null;
		public HeadTypeDesc headingTypeDesc = null;
		public Collection<RecordSet> applicableContexts = new HashSet<>();
		public Collection<String> expectedNotes = new HashSet<>();
		boolean display = true;
	}

}
