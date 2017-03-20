package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.addDashesTo_YYYYMMDD_Date;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.http.ConnectionClosedException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.MarcRecord.ControlField;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldValues;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.RecordSet;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.ReferenceType;

public class IndexAuthorityRecords {

	private Connection connection = null;
	private DavService davService;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// load configuration for location of index, location of authorities
		Collection<String> requiredArgs = SolrBuildConfig.getRequiredArgsForWebdav();
		requiredArgs.add("xmlDir");

		SolrBuildConfig config = SolrBuildConfig.loadConfig(args,requiredArgs);
		try {
			new IndexAuthorityRecords(config);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public IndexAuthorityRecords(SolrBuildConfig config) throws Exception {
        this.davService = DavServiceFactory.getDavService(config);

		connection = config.getDatabaseConnection("Headings");
		//set up database (including populating description maps)
		setUpDatabase();

		String xmlDir = config.getWebdavBaseUrl() + "/" + config.getXmlDir();
		System.out.println("Looking for authority xml in directory: "+xmlDir);
        List<String> authXmlFiles = davService.getFileUrlList(xmlDir);
        System.out.println("Found: "+authXmlFiles.size());
        Iterator<String> i = authXmlFiles.iterator();
        while (i.hasNext()) {
			String srcFile = i.next();
			System.out.println(srcFile);
			boolean processedFile = false;
			while ( ! processedFile ) {
				try {
					processFile( srcFile );
					processedFile = true;
				} catch (ConnectionClosedException e) {
					System.out.println("Lost access to xml file read. Waiting 2 minutes, and will try again.\n");
					e.printStackTrace();
					Thread.sleep(120 /* s */ * 1000 /* ms/s */);
				}
			}
        }
        connection.close();
	}

	private void processFile( String srcFile ) throws Exception {
		try (  InputStream xmlstream = davService.getFileAsInputStream(srcFile)  ) {
			XMLInputFactory input_factory = XMLInputFactory.newInstance();
			XMLStreamReader r  = 
				input_factory.createXMLStreamReader(xmlstream);
			processRecords(r);
		}
	}
	private void setUpDatabase() throws SQLException {
		try ( Statement stmt = connection.createStatement() ) {

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
				+ "ENGINE=InnoDB DEFAULT CHARSET=utf8");

		stmt.execute("DROP TABLE IF EXISTS `note`");
		stmt.execute("CREATE TABLE `note` ( "
				+ "`heading_id` int(10) unsigned NOT NULL, "
				+ "`note` text NOT NULL, "
				+ "KEY (`heading_id`)) "
				+ "ENGINE=InnoDB DEFAULT CHARSET=utf8");

		stmt.execute("DROP TABLE IF EXISTS `ref_type`");
		stmt.execute("CREATE TABLE `ref_type` ( "
				+ "`id` tinyint(3) unsigned NOT NULL, "
				+ "`name` varchar(256) NOT NULL, "
				+ "PRIMARY KEY  (`id`)) "
				+ "ENGINE=InnoDB DEFAULT CHARSET=latin1");

		stmt.execute("DROP TABLE IF EXISTS `reference`");
		stmt.execute("CREATE TABLE `reference` ( "
				+ "`from_heading` int(10) unsigned NOT NULL, "
				+ "`to_heading` int(10) unsigned NOT NULL, "
				+ "`ref_type` tinyint(3) unsigned NOT NULL, "
				+ "`ref_desc` varchar(256) NOT NULL DEFAULT '', "
				+ " PRIMARY KEY (`from_heading`,`to_heading`,`ref_type`,`ref_desc`), "
				+ " KEY (`to_heading`) ) "
				+ "ENGINE=InnoDB DEFAULT CHARSET=latin1");

		stmt.execute("DROP TABLE IF EXISTS `type_desc`");
		stmt.execute("CREATE TABLE `type_desc` ( "
				+ "`id` tinyint(3) unsigned NOT NULL, "
				+ "`name` varchar(256) NOT NULL, "
				+ "PRIMARY KEY  (`id`)) "
				+ "ENGINE=InnoDB DEFAULT CHARSET=latin1");

		stmt.execute("DROP TABLE IF EXISTS `rda`");
		stmt.execute("CREATE TABLE `rda` ( "
				+ "`heading_id` int(10) unsigned NOT NULL, "
				+ "`rda` text NOT NULL, "
				+ "KEY `heading_id` (`heading_id`)) "
				+ "ENGINE=InnoDB DEFAULT CHARSET=utf8");
		}

		try ( PreparedStatement insertDesc = connection.prepareStatement(
				"INSERT INTO type_desc (id,name) VALUES (? , ?)") ) {
		for ( HeadTypeDesc ht : HeadTypeDesc.values()) {
			insertDesc.setInt(1, ht.ordinal());
			insertDesc.setString(2, ht.toString());
			insertDesc.executeUpdate();
		}}

		try ( PreparedStatement insertRefType = connection.prepareStatement(
				"INSERT INTO ref_type (id,name) VALUES (? , ?)") ) {
		for ( ReferenceType rt : ReferenceType.values()) {
			insertRefType.setInt(1, rt.ordinal());
			insertRefType.setString(2, rt.toString());
			insertRefType.executeUpdate();
		}}
	}

	private void processRecords (XMLStreamReader r) throws Exception {
		while (r.hasNext()) {
			if (r.next() == XMLStreamConstants.START_ELEMENT)
				if (r.getLocalName().equals("record")) {
					MarcRecord rec = processRecord(r);
					createHeadingRecordsFromAuthority(rec);
				}
		}
	}

	private void createHeadingRecordsFromAuthority( MarcRecord rec ) throws SQLException, JsonProcessingException  {
		String heading = null;
		String headingSort = null;
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
			if (f.tag.equals("010")) {
				for (Subfield sf : f.subfields) 
					if (sf.code.equals('a'))
						if (sf.value.startsWith("sj")) {
							// this is a Juvenile subject authority heading, which
							// we will not represent in the headings browse.
							System.out.println("Skipping Juvenile subject authority heading: "+rec.id);
							return;
						}
			} else if (f.tag.startsWith("1")) { // main heading
				MAIN: switch (f.tag) {
				case "100":
				case "110":
				case "111":
					for (Subfield sf : f.subfields)
						if (sf.code.equals('t') || sf.code.equals('k')) {
							htd = HeadTypeDesc.WORK;
							break MAIN;
						}
					if (f.tag.equals("100"))
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
				heading = dashedHeading(f, htd);

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
				case "375": {
					String start = null, end = null, field = null;
					List<String> values = new ArrayList<>();
					switch (f.tag) {
					case "372": field = "Field"; break;
					case "373": field = "Group/Organization"; break;
					case "374": field = "Occupation"; break;
					case "375": field = "Gender"; break;
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
/*				case "375": { // Gender
					String value = null;
					for (Subfield sf : f.subfields.values())
						switch (sf.code) {
						case 'a': value = sf.value; break;
						case 't':
							System.out.println("Blocking past gender information base on $t "+sf.value+". ("+heading+")");
							break MAIN;
						}
					if (value != null)
						rdaData.add("Gender", value);
				}
					break MAIN; */
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

	private static String dashedHeading(DataField f, HeadTypeDesc htd) {
		String dashed_terms = f.concatenateSpecificSubfields(" > ", "vxyz");
		String heading = null;
		if (htd.equals(HeadTypeDesc.WORK)) {
			if (f.tag.endsWith("30"))
				heading = f.concatenateSpecificSubfields("abcdeghjklmnopqrstu");
			else {
				FieldValues vals;
				if (f.tag.endsWith("00"))
					vals = f.getFieldValuesForNameMaybeTitleField("abcdq;tklnpmors");
				else if (f.tag.endsWith("10"))
					vals = f.getFieldValuesForNameMaybeTitleField("abd;tklnpmors");
				else // 11
					vals = f.getFieldValuesForNameMaybeTitleField("abde;tklnpmors");
				heading = vals.author+" | "+vals.title;
			}
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
		try ( PreparedStatement stmt = connection.prepareStatement(
				"INSERT INTO rda (heading_id, rda) VALUES (?, ?)") ){
			stmt.setInt(1, heading_id);
			stmt.setString(2, json);
			stmt.executeUpdate();
		}
	}

	private void insertNote(Integer heading_id, String note) throws SQLException {
		try ( PreparedStatement stmt = connection.prepareStatement(
				"INSERT INTO note (heading_id, note) VALUES (? , ?)") ){
			stmt.setInt(1, heading_id);
			stmt.setString(2, note);
			stmt.executeUpdate();
		}
	}

	private Integer getMainHeadingRecordId(String heading, String headingSort,
			HeadTypeDesc htd, Boolean isUndifferentiated) throws SQLException {

		Integer recordId = null;
		boolean undifferentiated = false;

		try ( PreparedStatement pstmt = connection.prepareStatement(
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
			try ( PreparedStatement pstmt = connection.prepareStatement(
					"UPDATE heading SET main_entry = 1, heading = ? WHERE id = ?") ){
				pstmt.setString(1, heading); 
				pstmt.setInt(2, recordId);
				pstmt.executeUpdate();
			}
			// if the record was inserted as differentiated, but this one is undifferentiated
			// the update it.
			if (isUndifferentiated && ! undifferentiated) {
				try ( PreparedStatement pstmt = connection.prepareStatement(
						"UPDATE heading SET undifferentiated = 1 WHERE id = ?") ) {
					pstmt.setInt(1, recordId);
					pstmt.executeUpdate();
				}
			}
		} else {

			// create new record
			try (PreparedStatement pstmt = connection.prepareStatement(
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
		String heading = dashedHeading(f, r.headingTypeDesc);
		r.headingOrig = heading;
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

		try ( PreparedStatement pstmt = connection.prepareStatement(
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

		try ( PreparedStatement pstmt = connection.prepareStatement(
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
			try ( PreparedStatement pstmt = connection.prepareStatement(
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

		public void add(String field, String value) {
			if ( ! data.containsKey(field))
				data.put(field, new HashSet<String>());
			data.get(field).add(value);
		}

		public String json() throws JsonProcessingException {
			if (data.isEmpty()) return null;
			return mapper.writeValueAsString(data);
		}
	}

	private static class Relation {	
		public String relationship = null;
		public String reciprocalRelationship = null;
		public String heading = null;
		public String headingOrig = null; // access to original heading before
		                                  // parenthesized main heading optionally added.
		public String headingSort = null;
		public HeadTypeDesc headingTypeDesc = null;
		public Collection<RecordSet> applicableContexts = new HashSet<>();
		public Collection<String> expectedNotes = new HashSet<>();
		boolean display = true;
	}

	// general MARC methods and classes start here

	private static MarcRecord processRecord( XMLStreamReader r ) throws Exception {

		MarcRecord rec = new MarcRecord();
		int id = 0;
		while (r.hasNext()) {
			int event = r.next();
			if (event == XMLStreamConstants.END_ELEMENT) {
				if (r.getLocalName().equals("record")) 
					return rec;
			}
			if (event == XMLStreamConstants.START_ELEMENT) {
				String element = r.getLocalName();
				switch (element) {

				case "leader":
					rec.leader = r.getElementText();
					break;


				case "controlfield":
					String tag = null;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("tag"))
							tag = r.getAttributeValue(i);
					ControlField cf = new ControlField(++id,tag,r.getElementText());
					if (cf.tag.equals("001"))
						rec.id = cf.value;
					rec.controlFields.add(cf);
					break;


				case "datafield":
					DataField df = new DataField();
					df.id = ++id;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("tag"))
							df.tag = r.getAttributeValue(i);
						else if (r.getAttributeLocalName(i).equals("ind1"))
							df.ind1 = r.getAttributeValue(i).charAt(0);
						else if (r.getAttributeLocalName(i).equals("ind2"))
							df.ind2 = r.getAttributeValue(i).charAt(0);
					df.subfields = processSubfields(r);
					rec.dataFields.add(df); 
				}
			}
		}
		return rec;
	}

	private static TreeSet<Subfield> processSubfields( XMLStreamReader r ) throws Exception {
		TreeSet<Subfield> fields = new TreeSet<>();
		int id = 0;
		while (r.hasNext()) {
			int event = r.next();
			if (event == XMLStreamConstants.END_ELEMENT)
				if (r.getLocalName().equals("datafield"))
					return fields;
			if (event == XMLStreamConstants.START_ELEMENT)
				if (r.getLocalName().equals("subfield")) {
					Subfield f = new Subfield();
					f.id = ++id;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("code"))
							f.code = r.getAttributeValue(i).charAt(0);
					f.value = r.getElementText();
					fields.add(f);
				}
		}
		return fields; // We should never reach this line.
	}

}
