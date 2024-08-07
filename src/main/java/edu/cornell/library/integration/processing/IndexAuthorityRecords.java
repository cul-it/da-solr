package edu.cornell.library.integration.processing;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.addDashesTo_YYYYMMDD_Date;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.catalog.Catalog;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.AuthorityData.AuthoritySource;
import edu.cornell.library.integration.metadata.support.AuthorityData.RecordSet;
import edu.cornell.library.integration.metadata.support.AuthorityData.ReferenceType;
import edu.cornell.library.integration.metadata.support.Heading;
import edu.cornell.library.integration.metadata.support.HeadingCategory;
import edu.cornell.library.integration.metadata.support.HeadingType;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.FieldValues;
import edu.cornell.library.integration.utilities.NameUtils;

public class IndexAuthorityRecords {

	private static EnumSet<HeadingType> authorTypes = EnumSet.of(
			HeadingType.PERSNAME, HeadingType.CORPNAME, HeadingType.EVENT);

	public static void main(String[] args)
			throws FileNotFoundException, IOException, SQLException {

		Collection<String> requiredArgs = Config.getRequiredArgsForDB("Headings");
		requiredArgs.addAll( Config.getRequiredArgsForDB("Voy"));
		requiredArgs.add("catalogClass");
		Config config = Config.loadConfig(requiredArgs);

		int maxId;
		try ( Connection voyager = config.getDatabaseConnection("Voy") ){
			maxId = getMaxAuthorityId( voyager );
		}
		config.setDatabasePoolsize("Headings", 2);
		try ( Connection headings = config.getDatabaseConnection("Headings") ) {
			
			//set up database (including populating description maps)
			setUpDatabase(headings);

			Catalog.DownloadMARC marc = Catalog.getMarcDownloader(config);
			int cursor = 0;
			int batchSize = 100;
			while (cursor <= maxId) {
				for (MarcRecord rec : marc.retrieveRecordsByIdRange(RecordType.AUTHORITY, cursor+1, cursor+batchSize))
					processAuthorityMarc( headings, rec );
				cursor += batchSize;
			}
		}
	}

	private static int getMaxAuthorityId(Connection voyager) throws SQLException {
		try ( Statement stmt = voyager.createStatement();
				ResultSet rs = stmt.executeQuery("select max(auth_id) from auth_data")) {
			while ( rs.next() )
				return rs.getInt(1);
		}
		throw new RuntimeException("Max authority record id not determined.");
	}

	private static void setUpDatabase(Connection headings) throws SQLException {
		try ( Statement stmt = headings.createStatement() ) {

			stmt.execute("CREATE TABLE `heading` ("
					+ "`id` int(10) unsigned NOT NULL auto_increment, "
					+ "`parent_id` int(10) unsigned NOT NULL default '0', "
					+ "`heading` text, "
					+ "`sort` mediumtext NOT NULL, "
					+ "`heading_type` tinyint(3) unsigned NOT NULL, "
					+ "`works_by` mediumint(8) unsigned NOT NULL default '0', "
					+ "`works_about` mediumint(8) unsigned NOT NULL default '0', "
					+ "`works` mediumint(8) unsigned NOT NULL default '0', "
					+ "PRIMARY KEY  (`id`), "
					+ "KEY `parent_id` (`parent_id`), "
					+ "KEY `uk` (`heading_type`,`sort`(100))) "
					+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

			stmt.execute("CREATE TABLE `note` ( "
					+ "`heading_id` int(10) unsigned NOT NULL, "
					+ "`authority_id` int(10) unsigned NOT NULL, "
					+ "`note` text NOT NULL, "
					+ "KEY (`heading_id`)) "
					+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

			stmt.execute("CREATE TABLE `authority_source` ("
					+ "`id` int(1) unsigned NOT NULL, "
					+ "`name` varchar(100) NOT NULL, "
					+ "KEY (`id`)) "
					+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

			stmt.execute("CREATE TABLE `authority` ("
					+ "`id` int(10) unsigned NOT NULL auto_increment, "
					+ "`source` int(1) unsigned NOT NULL, "
					+ "`nativeId` varchar(80) NOT NULL, "
					+ "`nativeHeading` text NOT NULL, "
					+ "`voyagerId` varchar(10) NOT NULL, "
					+ "`undifferentiated` tinyint(1) unsigned NOT NULL default '0', "
					+ "KEY (`id`), "
					+ "KEY (`voyagerId`), "
					+ "PRIMARY KEY(`source`,`nativeId`)) "
					+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

			stmt.execute("CREATE TABLE `authority2heading` ( "
					+ "`heading_id` int(10) unsigned NOT NULL, "
					+ "`authority_id` int(10) unsigned NOT NULL, "
					+ "`main_entry` tinyint(1) unsigned NOT NULL default '0', "
					+ "PRIMARY KEY (`heading_id`,`authority_id`), "
					+ "KEY (`authority_id`)) "
					+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

			stmt.execute("CREATE TABLE `ref_type` ( "
					+ "`id` tinyint(3) unsigned NOT NULL, "
					+ "`name` varchar(256) NOT NULL, "
					+ "PRIMARY KEY  (`id`)) "
					+ "ENGINE=MyISAM DEFAULT CHARSET=latin1");

			stmt.execute("CREATE TABLE `reference` ( "
					+ "`id` int(10) unsigned NOT NULL auto_increment, "
					+ "`from_heading` int(10) unsigned NOT NULL, "
					+ "`to_heading` int(10) unsigned NOT NULL, "
					+ "`ref_type` tinyint(3) unsigned NOT NULL, "
					+ "`ref_desc` varchar(256) NOT NULL DEFAULT '', "
					+ " PRIMARY KEY (`id`), "
					+ " UNIQUE KEY (`from_heading`,`to_heading`,`ref_type`,`ref_desc`), "
					+ " KEY (`to_heading`) ) "
					+ "ENGINE=MyISAM DEFAULT CHARSET=latin1");

			stmt.execute("CREATE TABLE `authority2reference` ( "
					+ "`reference_id` int(10) unsigned NOT NULL, "
					+ "`authority_id` int(10) unsigned NOT NULL, "
					+ "PRIMARY KEY (`reference_id`,`authority_id`), "
					+ "KEY (`authority_id`)) "
					+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

			stmt.execute("CREATE TABLE `heading_type` ( "
					+ "`id` tinyint(3) unsigned NOT NULL, "
					+ "`name` varchar(256) NOT NULL, "
					+ "PRIMARY KEY  (`id`)) "
					+ "ENGINE=MyISAM DEFAULT CHARSET=latin1");

			stmt.execute("CREATE TABLE `heading_category` ( "
					+ "`id` tinyint(1) unsigned NOT NULL, "
					+ "`name` varchar(256) NOT NULL, "
					+ "PRIMARY KEY  (`id`)) "
					+ "ENGINE=MyISAM DEFAULT CHARSET=latin1");

			stmt.execute("CREATE TABLE `rda` ( "
					+ "`heading_id` int(10) unsigned NOT NULL, "
					+ "`authority_id` int(10) unsigned NOT NULL, "
					+ "`rda` text NOT NULL, "
					+ "KEY `heading_id` (`heading_id`)) "
					+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

			stmt.execute("CREATE TABLE `bib2heading` ( "
					+ "`bib_id` int(10) unsigned NOT NULL, " 
					+ "`category` tinyint(1) unsigned NOT NULL, "
					+ "`heading_id` int(10) unsigned NOT NULL, "
					+ "`heading` text, "
					+ "UNIQUE KEY `category` (`category`,`heading_id`,`bib_id`), "
					+ "KEY `bib_id` (`bib_id`), "
					+ "KEY `heading_id` (`heading_id`)) "
					+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");
		}

		try ( PreparedStatement insertDesc = headings.prepareStatement(
				"INSERT INTO heading_type (id,name) VALUES (? , ?)") ) {
		for ( HeadingType ht : HeadingType.values()) {
			insertDesc.setInt(1, ht.ordinal());
			insertDesc.setString(2, ht.toString());
			insertDesc.executeUpdate();
		}}

		try ( PreparedStatement insertDesc = headings.prepareStatement(
				"INSERT INTO heading_category (id,name) VALUES (? , ?)") ) {
		for ( HeadingCategory hc : HeadingCategory.values()) {
			insertDesc.setInt(1, hc.ordinal());
			insertDesc.setString(2, hc.toString());
			insertDesc.executeUpdate();
		}}

		try ( PreparedStatement insertRefType = headings.prepareStatement(
				"INSERT INTO ref_type (id,name) VALUES (? , ?)") ) {
		for ( ReferenceType rt : ReferenceType.values()) {
			insertRefType.setInt(1, rt.ordinal());
			insertRefType.setString(2, rt.toString());
			insertRefType.executeUpdate();
		}}

		try ( PreparedStatement insertAuthSource = headings.prepareStatement(
				"INSERT INTO authority_source (id,name) VALUES (? , ?)") ) {
			for ( AuthoritySource rt : AuthoritySource.values()) {
				insertAuthSource.setInt(1, rt.ordinal());
				insertAuthSource.setString(2, rt.toString());
				insertAuthSource.executeUpdate();
			}
		}

	}

	private static void processAuthorityMarc( Connection headings, MarcRecord rec )
			throws SQLException, JsonProcessingException  {

		AuthorityData a = new AuthorityData();

		for (ControlField f : rec.controlFields) {
			if (f.tag.equals("001"))
				a.catalogId = f.value;
			if (f.tag.equals("008")) {
				if (f.value.length() >= 10) {
					Character recordType = f.value.charAt(9);
					if (recordType.equals('b')) 
						a.expectedNotes.add("666");
					else if (recordType.equals('c'))
						a.expectedNotes.add("664");
					if ( f.value.length() > 32 ) {
						Character undifferentiated = f.value.charAt(32);
						if (undifferentiated.equals('b'))
							a.isUndifferentiated = true;
					}
				}
			}
		}
		Collection<String> foundNotes = new HashSet<>();
		// iterate through fields. Look for main heading and alternate forms.
		for (DataField f : rec.dataFields) {
			if (f.tag.equals("010")) {
				for (Subfield sf : f.subfields) 
					if (sf.code.equals('a')) {
						if (sf.value.startsWith("sj")) {
							// this is a Juvenile subject authority heading, which
							// we will not represent in the headings browse.
							System.out.println("Skipping Juvenile subject authority heading: "+rec.id);
							return;
						}
						a.lccn = sf.value;
					}
			} else if (f.tag.startsWith("1")) { // main heading
				a.mainHead = processHeadingField(f, null);
				a.nativeHeading = nativeHeading(f);
			} else if (f.tag.equals("260") || f.tag.equals("360")) {
				a.notes.add("Search under: "+f.concatenateSubfieldsOtherThan(""));
			} else if (f.tag.startsWith("3")) {
				addToRdaData(a.rdaData,f);
			} else if (f.tag.startsWith("4")) {
				// equivalent values
				Relation r = determineRelationship(f);
				if (r != null) {
					if ( a.mainHead == null ) {
						System.out.println("Found 4xx relation while main heading is null.");
						System.out.println(rec.toString());
					} else {
						a.expectedNotes.addAll(r.expectedNotes);
						r.heading = processHeadingField(f,a.mainHead.displayForm());
						a.sees.add(r);
					}
				}
			} else if (f.tag.startsWith("5")) {
				// see alsos
				Relation r = determineRelationship(f);
				if (r != null) {
					a.expectedNotes.addAll(r.expectedNotes);
					r.heading = processHeadingField(f,null);
					a.seeAlsos.add(r);
				}
			} else if (f.tag.equals("663")) {
				foundNotes.add("663");
				if (a.expectedNotes.contains("663")) {
					a.notes.add(buildJsonNote(f));
				} else {
					System.out.println("Field 663 found, but no matching 4XX or 5XX subfield w. "+rec.id);
				}
			} else if (f.tag.equals("664")) {
				foundNotes.add("664");
				if (a.expectedNotes.contains("664")) {
					a.notes.add(buildJsonNote(f));
				} else {
					System.out.println("Field 664 found, but no matching 4XX or 5XX subfield w or matching record type: c. "+rec.id);
				}
			} else if (f.tag.equals("665")) {
				foundNotes.add("665");
				if (a.expectedNotes.contains("665")) {
					a.notes.add(buildJsonNote(f));
				} else {
					System.out.println("Field 665 found, but no matching 4XX or 5XX subfield w. "+rec.id);
				}
			} else if (f.tag.equals("666")) {
				foundNotes.add("666");
				if (a.expectedNotes.contains("666")) {
					a.notes.add(buildJsonNote(f));
				} else {
					System.out.println("Field 666 found, but no matching record type: b. "+rec.id);
				}
			}
		}
		if ( a.mainHead == null ) return;
		if ( a.lccn == null ) a.lccn = "local "+a.catalogId;

		getHeadingId(headings, a.mainHead);
		getAuthorityId(headings, a);
		if (a.id == null) return;

		for (String note : a.notes)
			insertNote(headings, a.mainHead.id(), a.id, note);

		a.expectedNotes.removeAll(foundNotes);
		if ( ! a.expectedNotes.isEmpty())
			System.out.println("Expected notes based on 4XX and/or 5XX subfield ws that didn't appear. "+rec.id);


		// Populate incoming 4XX cross references
		for (Relation r: a.sees) {
			if ( ! r.display) continue;
			if ( r.heading.sort().equals(a.mainHead.sort()) 
					&& Objects.equals(r.heading.parent(), a.mainHead.parent())) continue;
			crossRef(headings, a.mainHead.id(),a.id,r, ReferenceType.FROM4XX);
		}

		for (Relation r: a.seeAlsos) {
			// Populate incoming 5XX cross references
			if ( ! r.display) continue;
			if ( r.heading.sort().equals(a.mainHead.sort()) 
					&& Objects.equals(r.heading.parent(), a.mainHead.parent())) continue;
			crossRef(headings, a.mainHead.id(),a.id,r, ReferenceType.FROM5XX);

			// Where appropriate, populate outgoing 5XX cross references
			if (r.reciprocalRelationship != null)
				directRef(headings, a.mainHead.id(),a.id,r, ReferenceType.TO5XX);
		}

		populateRdaInfo(headings, a.mainHead.id(), a.id, a.rdaData);

		return;
	}

	private static void addToRdaData(RdaData rda, DataField f) {

		String fieldName = null;

		MAIN: switch (f.tag) {
		case "370":
			for (Subfield sf : f.subfields)
				switch (sf.code) {
				case 'a': rda.add("Birth Place", sf.value);		break;
				case 'b': rda.add("Place of Death",sf.value);	break;
				case 'c': rda.add("Country",sf.value);			break;
				}
			break MAIN;

		case "372":
		case "373":
		case "374":
		{
			String start = null, end = null, field = null;
			List<String> values = new ArrayList<>();
			switch (f.tag) {
			case "372": field = "Field"; break;
			case "373": field = "Group/Organization"; break;
			case "374": field = "Occupation"; break;
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
							rda.add(field, String.format("%s (%s)", value,start));
						else
							rda.add(field, String.format("%s (%s through %s)", value,start,end));
					} else {
						rda.add(field, String.format("%s (starting %s)", value,start));
					}
				} else {
					if (end != null)
						rda.add(field, String.format("%s (until %s)", value,end));
					else
						rda.add(field, value);
				}
			}
			break MAIN;
		case "380": fieldName = "Form of Work";		break MAIN;
		case "382": fieldName = "Instrumentation";
		} //end MAIN

		if (fieldName != null)
			for (Subfield sf : f.subfields)
				if (sf.code.equals('a'))
					rda.add(fieldName, sf.value);
		
	}

	private static Heading processHeadingField( DataField f, String mainHeading ) {

		Heading parentHeading = null;
		HeadingType ht = null;
		FieldValues fvs = NameUtils.authorAndOrTitleValues(f);
		switch (f.tag.substring(1)) {
		case "00":
		case "10":
		case "11":
			HeadingType authorType = (f.tag.endsWith("00")) ? HeadingType.PERSNAME :
				                      (f.tag.endsWith("10")) ? HeadingType.CORPNAME :
				                    	                       HeadingType.EVENT;
			if (fvs.category.equals(HeadingCategory.AUTHORTITLE)) {
				parentHeading = new Heading
						(fvs.author,getFilingForm(fvs.author),authorType);
				ht = HeadingType.WORK;
				if ( ! fvs.author.equals(NameUtils.facetValue(f)) )
					System.out.println(NameUtils.facetValue(f)+"\n"+fvs.author);
			} else
				ht = authorType;
			break;

		case "30":	ht = HeadingType.WORK;		break;
		case "48":	ht = HeadingType.CHRONTERM;	break;
		case "50":	ht = HeadingType.TOPIC;		break;
		case "51":	ht = HeadingType.GEONAME;	break;
		case "55":	ht = HeadingType.GENRE;		break;
		case "62":	ht = HeadingType.MEDIUM;	break;
		default:
			// If the entry is for a subdivision (main entry >=180), we won't do anything with it.
			return null;
		}

		String heading = dashedHeading(f, ht, fvs);

		// If mainHeading is supplied, this is a 4xx reference. Evaluate the possibility of
		// adding the mainHeading as a parenthetical. If five or fewer capital letters discounting
		// periods, heading is probably an acronym.
		ACRONYM:
			if ( mainHeading != null ) {
			int capitalCount = 0;
			for (char c : heading.toCharArray())
				if ( Character.isUpperCase(c) ) capitalCount++;
				else if ( c != '.' )
					break ACRONYM;
			if (capitalCount > 5)
				break ACRONYM;
			// It checks out as an acroymn, do adding the mainHeading to disambiguate the crossref.
			heading = heading+" ("+mainHeading+")";
		}

		return new Heading( heading, getFilingForm(heading),ht ,parentHeading);
	}

	private static String nativeHeading( DataField f ) {
		String main = f.concatenateSpecificSubfields("abcdefghijklmnopqrstu");
		String dashedTerms = f.concatenateSpecificSubfields(" > ", "vxyz");
		if ( ! main.isEmpty() && ! dashedTerms.isEmpty() )
			main += " > "+dashedTerms;
		return main;
	}

	private static String dashedHeading(DataField f, HeadingType ht, FieldValues nameFieldVals) {
		String dashed_terms = f.concatenateSpecificSubfields(" > ", "vxyz");
		String heading = null;
		if (ht.equals(HeadingType.WORK)) {
			if (f.tag.endsWith("30"))
				heading = f.concatenateSpecificSubfields("abcdeghjklmnopqrstu");
			else
				heading = nameFieldVals.author+" | "+nameFieldVals.title;
		} else if (authorTypes.contains(ht)){
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

	private static void populateRdaInfo(Connection headings, Integer headingId, Integer authorityId, RdaData data)
			throws SQLException, JsonProcessingException {
		String json = data.json();
		if (json == null) return;
		try ( PreparedStatement stmt = headings.prepareStatement(
				"INSERT INTO rda (heading_id, authority_id, rda) VALUES (?, ?, ?)") ){
			stmt.setInt(1, headingId);
			stmt.setInt(2, authorityId);
			stmt.setString(3, json);
			stmt.executeUpdate();
		}
	}

	private static void insertNote(Connection headings, Integer headingId, Integer authorityId, String note)
			throws SQLException {
		try ( PreparedStatement stmt = headings.prepareStatement(
				"INSERT INTO note (heading_id, authority_id, note) VALUES (?,?,?)") ){
			stmt.setInt(1, headingId);
			stmt.setInt(2, authorityId);
			stmt.setString(3, note);
			stmt.executeUpdate();
		}
	}

	private static void getAuthorityId(Connection headings, AuthorityData a) throws SQLException {
		if (a.lccn == null) return;
		for ( AuthoritySource source : AuthoritySource.values() )
			if (source.prefix() != null && a.lccn.startsWith(source.prefix()))
				a.source = source;
		if (a.source == null) {
			System.out.println("Not registering authority. Failed to recognize source: "+a.lccn);
			return;
		}

		Integer authorityId = null;

		// Get record id if already exists
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"SELECT id FROM authority " +
				"WHERE source = ? AND nativeId = ?") ){
			pstmt.setInt    (1, a.source.ordinal());
			pstmt.setString (2, a.lccn);
			try ( ResultSet resultSet = pstmt.executeQuery() ) {
				while (resultSet.next())
					authorityId = resultSet.getInt(1);
			}
		}
		if ( authorityId != null )
			System.out.println("Possible duplicate authority ID: "+authorityId);

		else try ( PreparedStatement stmt = headings.prepareStatement(
				"INSERT INTO authority"
				+ " (source, nativeId, nativeHeading, voyagerId, undifferentiated)"
				+ " VALUES (?,?,?,?,?)",
				Statement.RETURN_GENERATED_KEYS) ){
			stmt.setInt    (1, a.source.ordinal());
			stmt.setString (2, a.lccn);
			stmt.setString (3, a.nativeHeading);
			stmt.setString (4, a.catalogId);
			stmt.setBoolean(5, a.isUndifferentiated);
			int affectedCount = stmt.executeUpdate();
			if (affectedCount < 1) 
				throw new SQLException("Creating Authority Record Failed.");
			try ( ResultSet generatedKeys = stmt.getGeneratedKeys() ) {
				if (generatedKeys.next())
					authorityId = generatedKeys.getInt(1); }
		}
		if (authorityId == null) return;

		try (PreparedStatement pstmt = headings.prepareStatement(
				"REPLACE INTO authority2heading (heading_id, authority_id, main_entry) VALUES (?,?,1)")) {
			pstmt.setInt(1, a.mainHead.id());
			pstmt.setInt(2, authorityId);
			pstmt.executeUpdate();
		}
		a.id = authorityId;
	}

	private static void storeReferenceAuthority2Heading(Connection headings, Integer headingId, Integer authorityId)
			throws SQLException {
		try (PreparedStatement pstmt = headings.prepareStatement(
				"REPLACE INTO authority2heading (heading_id, authority_id, main_entry) VALUES (?,?,0)")) {
			pstmt.setInt(1, headingId);
			pstmt.setInt(2, authorityId);
			pstmt.executeUpdate();
		}
	}

	private static void getHeadingId(Connection headings, Heading h) throws SQLException {

		if (h.parent() != null)
			getHeadingId(headings, h.parent());

		// Get record id if already exists
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"SELECT id FROM heading " +
				"WHERE heading_type = ? AND sort = ?") ){
			pstmt.setInt(1,h.headingType().ordinal());
			pstmt.setString(2,h.sort());
			try ( ResultSet resultSet = pstmt.executeQuery() ) {
				while (resultSet.next()) {
					h.setId(resultSet.getInt(1));
					return;
				}
			}
		}

		// Create record and return id, otherwise
		try (PreparedStatement pstmt = headings.prepareStatement(
				"INSERT INTO heading (heading, sort, heading_type, parent_id) VALUES (?, ?, ?, ?)",
				Statement.RETURN_GENERATED_KEYS) ) {

			pstmt.setString(1, Normalizer.normalize(h.displayForm(), Normalizer.Form.NFC));
			pstmt.setString(2, h.sort());
			pstmt.setInt(3,    h.headingType().ordinal());
			pstmt.setInt(4,   (h.parent() == null)?0:h.parent().id());
			int affectedCount = pstmt.executeUpdate();
			if (affectedCount < 1) 
				throw new SQLException("Creating Heading Record Failed.");
			try ( ResultSet generatedKeys = pstmt.getGeneratedKeys() ) {
				if (generatedKeys.next()) {
					h.setId(generatedKeys.getInt(1));
					return;
				}
			}
		}
	}

	private static void crossRef(
			Connection headings, Integer mainHeadingId, Integer authorityId, Relation r, ReferenceType rt) throws SQLException {
		getHeadingId( headings, r.heading );
		insertRef(headings, r.heading.id(), mainHeadingId, authorityId, rt, r.relationship);
		storeReferenceAuthority2Heading( headings, r.heading.id(), authorityId );
	}

	private static void directRef(
			Connection headings, Integer mainHeadingId, Integer authorityId, Relation r, ReferenceType rt)
					throws SQLException {
		getHeadingId( headings, r.heading );
		insertRef(headings, mainHeadingId, r.heading.id(), authorityId, rt, r.reciprocalRelationship );
		storeReferenceAuthority2Heading( headings, r.heading.id(), authorityId );
	}

	private static void insertRef(Connection headings, int fromId, int toId, int authorityId,
			ReferenceType rt, String relationshipDescription) throws SQLException {

		Integer referenceId = null;

		String description = ( relationshipDescription == null )?"":relationshipDescription;
		// Get record id if already exists
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"SELECT id FROM reference " +
				"WHERE from_heading = ? AND to_heading = ? AND ref_type = ? AND ref_desc = ?") ){
			pstmt.setInt(1,fromId);
			pstmt.setInt(2,toId);
			pstmt.setInt(3,rt.ordinal());
			pstmt.setString(4,description);
			try ( ResultSet resultSet = pstmt.executeQuery() ) {
				while (resultSet.next())
					referenceId = resultSet.getInt(1);
			}
		}

		if ( referenceId == null )
		try (PreparedStatement pstmt = headings.prepareStatement(
				"INSERT INTO reference (from_heading, to_heading, ref_type, ref_desc) " +
						"VALUES (?, ?, ?, ?)",
						Statement.RETURN_GENERATED_KEYS) ) {

			pstmt.setInt(1, fromId);
			pstmt.setInt(2, toId);
			pstmt.setInt(3, rt.ordinal());
			pstmt.setString(4, description);
			int affectedCount = pstmt.executeUpdate();
			if (affectedCount < 1) 
				throw new SQLException("Creating Reference Record Failed.");
			try ( ResultSet generatedKeys = pstmt.getGeneratedKeys() ) {
				if (generatedKeys.next())
					referenceId = generatedKeys.getInt(1); }
		}

		// If there's a problem, we should already have thrown an SQL exception.
		if ( referenceId == null ) return;

		try (PreparedStatement pstmt = headings.prepareStatement(
				"REPLACE INTO authority2reference (reference_id, authority_id) VALUES (?,?)")) {
			pstmt.setInt(1, referenceId);
			pstmt.setInt(2, authorityId);
			pstmt.executeUpdate();
		}
	}

	private static Relation determineRelationship( DataField f ) {
		// Is there a subfield w? The relationship note in subfield w
		// describes the 4XX or 5XX heading, and must be reversed for the
		// from tracing.
		Relation r = new Relation();
		boolean hasW = false;

		for (Subfield sf : f.subfields) {
			if (sf.code.equals('w')) {
				hasW = true;

				switch (sf.value.charAt(0)) {
				case 'a':
					//earlier heading
					r.relationship = "Later Heading";
					r.reciprocalRelationship = "Earlier Heading"; break;
				case 'b':
					//later heading
					r.relationship = "Earlier Heading";
					r.reciprocalRelationship = "Later Heading"; break;
				case 'd':
					//acronym
					r.relationship = "Full Heading";
					r.reciprocalRelationship = "Acronym"; break;
				case 'f':
					//musical composition
					r.relationship = "Musical Composition Based on this Work";
					r.reciprocalRelationship = "Based on"; break;
				case 'g':
					//broader term
					r.relationship = "Narrower Term";
					r.reciprocalRelationship = "Broader Term"; break;
				case 'h':
					//narrower term
					r.relationship = "Broader Term";
					r.reciprocalRelationship = "Narrower Term"; break;
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
						r.reciprocalRelationship = "Earlier Form of Heading";
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
					r.relationship = "Real Identity";
					r.reciprocalRelationship = "Alternate Identity"; break;
				case "real identity":
					r.relationship = "Alternate Identity";
					r.reciprocalRelationship = "Real Identity"; break;
				case "family member":
					r.relationship = "Family";
					r.reciprocalRelationship = "Family Member"; break;
				case "family":
					r.relationship = "Family Member";
					r.reciprocalRelationship = "Family"; break;
				case "progenitor":
					r.relationship = "Descendants";
					r.reciprocalRelationship = "Progenitor"; break;
				case "descendants":
					r.relationship = "Progenitor";
					r.reciprocalRelationship = "Descendants"; break;
				case "employee": 
					r.relationship = "Employer"; break;
				case "employer":
					r.relationship = "Employee";
					r.reciprocalRelationship = "Employer"; break;

				// The reciprocal relationship to descendant family is missing
				// from the RDA spec (appendix K), so it's unlikely that
				// "Progenitive Family" will actually appear. Of the three
				// adjective forms of "Progenitor" I found in the OED (progenital,
				// progenitive, progenitorial), progenitive has the highest level
				// of actual use according to my Google NGrams search
				case "descendant family": 
					r.relationship = "Progenitive Family";
					r.reciprocalRelationship = "Descendant Family"; break;
				case "progenitive family":
					r.relationship = "Descendant Family";
					r.reciprocalRelationship = "Progenitive Family";
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
		public Heading heading = null;
		public Collection<RecordSet> applicableContexts = new HashSet<>();
		public Collection<String> expectedNotes = new HashSet<>();
		boolean display = true;
	}

	private static class AuthorityData {
		public AuthorityData() { }
		Integer id = null;
		String catalogId = null;
		Heading mainHead = null;
		String nativeHeading = null;
		String lccn = null;
		AuthoritySource source;
		Boolean isUndifferentiated = false;
		Collection<Relation> sees = new HashSet<>();
		Collection<Relation> seeAlsos = new HashSet<>();
		Collection<String> expectedNotes = new HashSet<>();
		Collection<String> notes = new HashSet<>();
		RdaData rdaData = new RdaData();

	}

}
