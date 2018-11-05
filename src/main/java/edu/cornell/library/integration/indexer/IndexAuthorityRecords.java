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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;

import org.apache.http.ConnectionClosedException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.jdbc.MysqlDataTruncation;

import edu.cornell.library.integration.indexer.utilities.BrowseUtils.AuthoritySource;
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
import edu.cornell.library.integration.webdav.DavService;
import edu.cornell.library.integration.webdav.DavServiceFactory;

public class IndexAuthorityRecords {

	private Connection connection = null;
	private DavService davService;
	private static EnumSet<HeadTypeDesc> authorTypes = EnumSet.of(
			HeadTypeDesc.PERSNAME, HeadTypeDesc.CORPNAME, HeadTypeDesc.EVENT);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// load configuration for location of index, location of authorities
		Collection<String> requiredArgs = Config.getRequiredArgsForWebdav();
		requiredArgs.add("mrcDir");

		Config config = Config.loadConfig(args,requiredArgs);
		try {
			new IndexAuthorityRecords(config);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public IndexAuthorityRecords(Config config) throws Exception {
        this.davService = DavServiceFactory.getDavService(config);

		connection = config.getDatabaseConnection("Headings");
		//set up database (including populating description maps)
		setUpDatabase();

		String mrcDir = config.getWebdavBaseUrl() + "/" + config.getMrcDir();
		System.out.println("Looking for authority MARC in directory: "+mrcDir);
        List<String> authMrcFiles = davService.getFileUrlList(mrcDir);
        System.out.println("Found: "+authMrcFiles.size()+" files.");
        Iterator<String> i = authMrcFiles.iterator();
        while (i.hasNext()) {
			String srcFile = i.next();
			System.out.println(srcFile);
			boolean processedFile = false;
			while ( ! processedFile ) {
				try {
					processFile( srcFile );
					processedFile = true;
				} catch (ConnectionClosedException e) {
					System.out.println("Lost access to mrc file read. Waiting 2 minutes, and will try again.\n");
					e.printStackTrace();
					Thread.sleep(120 /* s */ * 1000 /* ms/s */);
				}
			}
        }
        connection.close();
	}

	private void processFile( String srcFile ) throws Exception {
		try (  InputStream is = davService.getFileAsInputStream(srcFile);
				Scanner s1 = new Scanner(is);
				Scanner s2 = s1.useDelimiter("\\A")) {
			String marc21OrMarcXml = s2.hasNext() ? s2.next() : "";
			List<MarcRecord> recs = MarcRecord.getMarcRecords(RecordType.AUTHORITY, marc21OrMarcXml);
			System.out.println(recs.size() + " records found in file.");
			for (MarcRecord rec : recs)
				processAuthorityMarc(rec);
		}
	}

	private void setUpDatabase() throws SQLException {
		try ( Statement stmt = connection.createStatement() ) {

		stmt.execute("DROP TABLE IF EXISTS `heading`");
		stmt.execute("CREATE TABLE `heading` ("
				+ "`id` int(10) unsigned NOT NULL auto_increment, "
				+ "`parent_id` int(10) unsigned NOT NULL default '0', "
				+ "`heading` text, "
				+ "`sort` mediumtext NOT NULL, "
				+ "`type_desc` tinyint(3) unsigned NOT NULL, "
				+ "`works_by` mediumint(8) unsigned NOT NULL default '0', "
				+ "`works_about` mediumint(8) unsigned NOT NULL default '0', "
				+ "`works` mediumint(8) unsigned NOT NULL default '0', "
				+ "PRIMARY KEY  (`id`), "
				+ "KEY `parent_id` (`parent_id`), "
				+ "KEY `uk` (`type_desc`,`parent_id`,`sort`(100))) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

		stmt.execute("DROP TABLE IF EXISTS `note`");
		stmt.execute("CREATE TABLE `note` ( "
				+ "`heading_id` int(10) unsigned NOT NULL, "
				+ "`authority_id` int(10) unsigned NOT NULL, "
				+ "`note` text NOT NULL, "
				+ "KEY (`heading_id`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

		stmt.execute("DROP TABLE IF EXISTS `authority_source`");
		stmt.execute("CREATE TABLE `authority_source` ("
				+ "`id` int(1) unsigned NOT NULL, "
				+ "`name` varchar(100) NOT NULL, "
				+ "KEY (`id`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

		stmt.execute("DROP TABLE IF EXISTS `authority`");
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

		stmt.execute("DROP TABLE IF EXISTS `authority2heading`");
		stmt.execute("CREATE TABLE `authority2heading` ( "
				+ "`heading_id` int(10) unsigned NOT NULL, "
				+ "`authority_id` int(10) unsigned NOT NULL, "
				+ "`main_entry` tinyint(1) unsigned NOT NULL default '0', "
				+ "PRIMARY KEY (`heading_id`,`authority_id`), "
				+ "KEY (`authority_id`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

		stmt.execute("DROP TABLE IF EXISTS `ref_type`");
		stmt.execute("CREATE TABLE `ref_type` ( "
				+ "`id` tinyint(3) unsigned NOT NULL, "
				+ "`name` varchar(256) NOT NULL, "
				+ "PRIMARY KEY  (`id`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=latin1");

		stmt.execute("DROP TABLE IF EXISTS `reference`");
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

		stmt.execute("DROP TABLE IF EXISTS `authority2reference`");
		stmt.execute("CREATE TABLE `authority2reference` ( "
				+ "`reference_id` int(10) unsigned NOT NULL, "
				+ "`authority_id` int(10) unsigned NOT NULL, "
				+ "PRIMARY KEY (`reference_id`,`authority_id`), "
				+ "KEY (`authority_id`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");

		stmt.execute("DROP TABLE IF EXISTS `type_desc`");
		stmt.execute("CREATE TABLE `type_desc` ( "
				+ "`id` tinyint(3) unsigned NOT NULL, "
				+ "`name` varchar(256) NOT NULL, "
				+ "PRIMARY KEY  (`id`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=latin1");

		stmt.execute("DROP TABLE IF EXISTS `rda`");
		stmt.execute("CREATE TABLE `rda` ( "
				+ "`heading_id` int(10) unsigned NOT NULL, "
				+ "`authority_id` int(10) unsigned NOT NULL, "
				+ "`rda` text NOT NULL, "
				+ "KEY `heading_id` (`heading_id`)) "
				+ "ENGINE=MyISAM DEFAULT CHARSET=utf8");
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

		try ( PreparedStatement insertAuthSource = connection.prepareStatement(
				"INSERT INTO authority_source (id,name) VALUES (? , ?)") ) {
			for ( AuthoritySource rt : AuthoritySource.values()) {
				insertAuthSource.setInt(1, rt.ordinal());
				insertAuthSource.setString(2, rt.toString());
				insertAuthSource.executeUpdate();
			}
		}

	}

	private void processAuthorityMarc( MarcRecord rec ) throws SQLException, JsonProcessingException  {

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
					Character undifferentiated = f.value.charAt(32);
					if (undifferentiated.equals('b'))
						a.isUndifferentiated = true;
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
					a.expectedNotes.addAll(r.expectedNotes);
					r.heading = processHeadingField(f,a.mainHead.val);
					a.sees.add(r);
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

		getHeadingId(a.mainHead);
		getAuthorityId(a);
		if (a.id == null) return;

		for (String note : a.notes)
			insertNote(a.mainHead.id, a.id, note);

		a.expectedNotes.removeAll(foundNotes);
		if ( ! a.expectedNotes.isEmpty())
			System.out.println("Expected notes based on 4XX and/or 5XX subfield ws that didn't appear. "+rec.id);


		// Populate incoming 4XX cross references
		for (Relation r: a.sees) {
			if ( ! r.display) continue;
			if ( r.heading.sort.equals(a.mainHead.sort) 
					&& Objects.equals(r.heading.parent, a.mainHead.parent)) continue;
			crossRef(a.mainHead.id,a.id,r, ReferenceType.FROM4XX);
		}

		for (Relation r: a.seeAlsos) {
			// Populate incoming 5XX cross references
			if ( ! r.display) continue;
			if ( r.heading.sort.equals(a.mainHead.sort) 
					&& Objects.equals(r.heading.parent, a.mainHead.parent)) continue;
			crossRef(a.mainHead.id,a.id,r, ReferenceType.FROM5XX);

			// Where appropriate, populate outgoing 5XX cross references
			if (r.reciprocalRelationship != null)
				directRef(a.mainHead.id,a.id,r, ReferenceType.TO5XX);
		}

		populateRdaInfo(a.mainHead.id, a.id, a.rdaData);

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
		HeadTypeDesc htd = null;
		FieldValues fvs = NameUtils.authorAndOrTitleValues(f);
		switch (f.tag.substring(1)) {
		case "00":
		case "10":
		case "11":
			HeadTypeDesc authorType = (f.tag.endsWith("00")) ? HeadTypeDesc.PERSNAME :
				                      (f.tag.endsWith("10")) ? HeadTypeDesc.CORPNAME :
				                    	                       HeadTypeDesc.EVENT;
			if (fvs.type.equals(HeadType.AUTHORTITLE)) {
				parentHeading = new Heading
						(fvs.author,getFilingForm(fvs.author),authorType);
				htd = HeadTypeDesc.WORK;
				if ( ! fvs.author.equals(NameUtils.facetValue(f)) )
					System.out.println(NameUtils.facetValue(f)+"\n"+fvs.author);
			} else
				htd = authorType;
			break;

		case "30":	htd = HeadTypeDesc.WORK;		break;
		case "48":	htd = HeadTypeDesc.CHRONTERM;	break;
		case "50":	htd = HeadTypeDesc.TOPIC;		break;
		case "51":	htd = HeadTypeDesc.GEONAME;		break;
		case "55":	htd = HeadTypeDesc.GENRE;		break;
		case "62":	htd = HeadTypeDesc.MEDIUM;		break;
		default:
			// If the entry is for a subdivision (main entry >=180), we won't do anything with it.
			return null;
		}

		String heading = dashedHeading(f, htd, fvs);

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

		return new Heading( heading, getFilingForm(heading),htd ,parentHeading);
	}

	private static String nativeHeading( DataField f ) {
		String main = f.concatenateSpecificSubfields("abcdefghijklmnopqrstu");
		String dashedTerms = f.concatenateSpecificSubfields(" > ", "vxyz");
		if ( ! main.isEmpty() && ! dashedTerms.isEmpty() )
			main += " > "+dashedTerms;
		return main;
	}

	private static String dashedHeading(DataField f, HeadTypeDesc htd, FieldValues nameFieldVals) {
		String dashed_terms = f.concatenateSpecificSubfields(" > ", "vxyz");
		String heading = null;
		if (htd.equals(HeadTypeDesc.WORK)) {
			if (f.tag.endsWith("30"))
				heading = f.concatenateSpecificSubfields("abcdeghjklmnopqrstu");
			else
				heading = nameFieldVals.title;
		} else if (authorTypes.contains(htd)){
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

	private void populateRdaInfo(Integer headingId, Integer authorityId, RdaData data) throws SQLException, JsonProcessingException {
		String json = data.json();
		if (json == null) return;
		try ( PreparedStatement stmt = connection.prepareStatement(
				"INSERT INTO rda (heading_id, authority_id, rda) VALUES (?, ?, ?)") ){
			stmt.setInt(1, headingId);
			stmt.setInt(2, authorityId);
			stmt.setString(3, json);
			stmt.executeUpdate();
		}
	}

	private void insertNote(Integer headingId, Integer authorityId, String note) throws SQLException {
		try ( PreparedStatement stmt = connection.prepareStatement(
				"INSERT INTO note (heading_id, authority_id, note) VALUES (?,?,?)") ){
			stmt.setInt(1, headingId);
			stmt.setInt(2, authorityId);
			stmt.setString(3, note);
			stmt.executeUpdate();
		}
	}

	private void getAuthorityId(AuthorityData a) throws SQLException {
		if (a.lccn == null) return;
		for ( AuthoritySource source : AuthoritySource.values() )
			if (source.prefix() != null && a.lccn.startsWith(source.prefix()))
				a.source = source;
		if (a.source == null) {
			System.out.println("Not registering authority. Failed to recognize source: "+a.lccn);
			return;
		}

		Integer authorityId = null;
		try ( PreparedStatement stmt = connection.prepareStatement(
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
		} catch (MysqlDataTruncation e) {
			System.out.println(a.lccn);
			e.printStackTrace();
			System.exit(1);
		}
		if (authorityId == null) return;

		try (PreparedStatement pstmt = connection.prepareStatement(
				"INSERT INTO authority2heading (heading_id, authority_id, main_entry) VALUES (?,?,1)")) {
			pstmt.setInt(1, a.mainHead.id);
			pstmt.setInt(2, authorityId);
			pstmt.executeUpdate();
		}
		a.id = authorityId;
	}

	private void storeReferenceAuthority2Heading(Integer headingId, Integer authorityId) throws SQLException {
		try (PreparedStatement pstmt = connection.prepareStatement(
				"REPLACE INTO authority2heading (heading_id, authority_id, main_entry) VALUES (?,?,0)")) {
			pstmt.setInt(1, headingId);
			pstmt.setInt(2, authorityId);
			pstmt.executeUpdate();
		}
	}

	private void getHeadingId(Heading h) throws SQLException {

		if (h.parent != null)
			getHeadingId(h.parent);

		// Get record id if already exists
		try ( PreparedStatement pstmt = connection.prepareStatement(
				"SELECT id FROM heading " +
				"WHERE type_desc = ? AND parent_id = ? AND sort = ?") ){
			pstmt.setInt(1,h.headTypeDesc.ordinal());
			pstmt.setInt(2,(h.parent == null)?0:h.parent.id);
			pstmt.setString(3,h.sort);
			try ( ResultSet resultSet = pstmt.executeQuery() ) {
				while (resultSet.next()) {
					h.id = resultSet.getInt(1);
					return;
				}
			}
		}

		// Create record and return id, otherwise
		try (PreparedStatement pstmt = connection.prepareStatement(
				"INSERT INTO heading (heading, sort, type_desc, parent_id) VALUES (?, ?, ?, ?)",
				Statement.RETURN_GENERATED_KEYS) ) {

			pstmt.setString(1, Normalizer.normalize(h.val, Normalizer.Form.NFC));
			pstmt.setString(2, h.sort);
			pstmt.setInt(3,    h.headTypeDesc.ordinal());
			pstmt.setInt(4,   (h.parent == null)?0:h.parent.id);
			int affectedCount = pstmt.executeUpdate();
			if (affectedCount < 1) 
				throw new SQLException("Creating Heading Record Failed.");
			try ( ResultSet generatedKeys = pstmt.getGeneratedKeys() ) {
				if (generatedKeys.next()) {
					h.id = generatedKeys.getInt(1);
					return;
				}
			}
		}
	}

	private void crossRef(Integer mainHeadingId, Integer authorityId, Relation r, ReferenceType rt) throws SQLException {
		getHeadingId( r.heading );
		insertRef(r.heading.id, mainHeadingId, authorityId, rt, r.relationship);
		storeReferenceAuthority2Heading( r.heading.id, authorityId );
	}

	private void directRef(Integer mainHeadingId, Integer authorityId, Relation r, ReferenceType rt) throws SQLException {
		getHeadingId( r.heading );
		insertRef(mainHeadingId, r.heading.id, authorityId, rt, r.reciprocalRelationship );
		storeReferenceAuthority2Heading( r.heading.id, authorityId );
	}

	private void insertRef(int fromId, int toId, int authorityId,
			ReferenceType rt, String relationshipDescription) throws SQLException {

		Integer referenceId = null;

		if ( relationshipDescription == null )
			relationshipDescription = "";
		// Get record id if already exists
		try ( PreparedStatement pstmt = connection.prepareStatement(
				"SELECT id FROM reference " +
				"WHERE from_heading = ? AND to_heading = ? AND ref_type = ? AND ref_desc = ?") ){
			pstmt.setInt(1,fromId);
			pstmt.setInt(2,toId);
			pstmt.setInt(3,rt.ordinal());
			pstmt.setString(4,relationshipDescription);
			try ( ResultSet resultSet = pstmt.executeQuery() ) {
				while (resultSet.next())
					referenceId = resultSet.getInt(1);
			}
		}

		if ( referenceId == null )
		try (PreparedStatement pstmt = connection.prepareStatement(
				"INSERT INTO reference (from_heading, to_heading, ref_type, ref_desc) " +
						"VALUES (?, ?, ?, ?)",
						Statement.RETURN_GENERATED_KEYS) ) {

			pstmt.setInt(1, fromId);
			pstmt.setInt(2, toId);
			pstmt.setInt(3, rt.ordinal());
			pstmt.setString(4, relationshipDescription);
			int affectedCount = pstmt.executeUpdate();
			if (affectedCount < 1) 
				throw new SQLException("Creating Reference Record Failed.");
			try ( ResultSet generatedKeys = pstmt.getGeneratedKeys() ) {
				if (generatedKeys.next())
					referenceId = generatedKeys.getInt(1); }
		}

		// If there's a problem, we should already have thrown an SQL exception.
		if ( referenceId == null ) return;

		try (PreparedStatement pstmt = connection.prepareStatement(
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
		public Heading heading = null;
		public Collection<RecordSet> applicableContexts = new HashSet<>();
		public Collection<String> expectedNotes = new HashSet<>();
		boolean display = true;
	}

	private static class Heading implements Comparable<Heading> {
		Integer id = null;
		final String val;
		final String sort;
		final HeadTypeDesc headTypeDesc;
		Heading parent = null;
		Heading( String heading, String headingSort, HeadTypeDesc headTypeDesc ) {
			this.val = heading;
			this.sort = headingSort;
			this.headTypeDesc = headTypeDesc;
		}
		Heading( String heading, String headingSort, HeadTypeDesc headTypeDesc, Heading parent ) {
			this(heading,headingSort,headTypeDesc);
			this.parent = parent;
		}
		@Override
		public String toString () {
			return String.format("[type_desc: %s; sort: \"%s\"%s]",
					this.headTypeDesc.toString(), this.val,
					(parent != null)?String.format(" parent:[%s]",this.parent.toString()):"");
		}
		@Override
	    public int hashCode() { return Integer.hashCode( this.id ); }
		@Override
		public int compareTo(final Heading other) { return Integer.compare(this.id, other.id);	}
		@Override
		public boolean equals(final Object o) {
			if (this == o) return true;
			if (o == null) return false;
			if (! this.getClass().equals( o.getClass() )) return false;
			Heading other = (Heading) o;
			return Objects.equals(this.id, other.id);
		}
	}

	private static class AuthorityData {
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
