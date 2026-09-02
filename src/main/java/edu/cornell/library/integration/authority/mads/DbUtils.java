package edu.cornell.library.integration.authority.mads;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.authority.activitystreams.IFetcher;
import edu.cornell.library.integration.metadata.support.AuthorityData.AuthoritySource;
import edu.cornell.library.integration.metadata.support.AuthorityData.ReferenceType;
import edu.cornell.library.integration.metadata.support.HeadingCategory;

public class DbUtils {
	// authority
	public static final String MADS_UPDATE_TABLE = "madsAuthorityUpdate";
	// headings
	public static final String MADS_AUTHORITY_SOURCE_TABLE = "madsAuthoritySource";
	public static final String MADS_AUTHORITY_TABLE = "madsAuthority";
	public static final String MADS_AUTHORITY_TO_HEADING_TABLE = "madsAuthority2Heading";
	public static final String MADS_AUTHORITY_TO_REFERENCE_TABLE = "madsAuthority2Reference";
	public static final String MADS_BIB_TO_HEADING_TABLE = "madsBib2Heading";
	public static final String MADS_HEADING_CATEGORY_TABLE = "madsHeadingCategory";
	public static final String MADS_HEADING_TYPE_TABLE = "madsAuthorityHeadingType";
	public static final String MADS_HEADING_TABLE = "madsHeading";
	public static final String MADS_NOTE_TABLE = "madsNote";
	public static final String MADS_RDA_TABLE = "madsRda";
	public static final String MADS_RECORD_STATUS_TABLE = "madsAuthorityRecordStatus";
	public static final String MADS_REFERENCE_TABLE = "madsReference";
	public static final String MADS_REFERENCE_TYPE_TABLE = "madsReferenceType";
	public static final String RWO_TABLE = "rwo";
	// cursor
	public static final String HEADINGS_CURSOR_INDEX_MADS_AUTHORITY_RECORDS = "indexMadsAuthorityRecords";
	public static final String HEADINGS_UPDATE_CURSOR_TABLE = "headingsUpdateCursor";

	public static IFetcher fetcher = new IFetcher.HttpFetcher();

	protected static void setUpHeadingsDatabase(Connection headings) throws SQLException {
		try ( Statement stmt = headings.createStatement() ) {
			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`id` int(10) unsigned NOT NULL auto_increment,
	`parent_id` int(10) unsigned NOT NULL default '0',
	`heading` text,
	`sort` mediumtext NOT NULL,
	`heading_type` tinyint(3) unsigned NOT NULL,
	`works_by` mediumint(8) unsigned NOT NULL default '0',
	`works_about` mediumint(8) unsigned NOT NULL default '0',
	`works` mediumint(8) unsigned NOT NULL default '0',
	PRIMARY KEY  (`id`),
	KEY `parent_id` (`parent_id`),
	KEY `uk` (`heading_type`,`sort`(100)))
ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci""".formatted(MADS_HEADING_TABLE));

			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`heading_id` int(10) unsigned NOT NULL,
	`authority_id` int(10) unsigned NOT NULL,
	`note` text NOT NULL,
	KEY (`heading_id`),
	KEY (`authority_id`))
ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci""".formatted(MADS_NOTE_TABLE));

			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`id` int(1) unsigned NOT NULL,
	`name` varchar(100) NOT NULL,
	KEY (`id`))
ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci""".formatted(MADS_AUTHORITY_SOURCE_TABLE));

			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`id` int(10) unsigned NOT NULL auto_increment,
	`source` int(1) unsigned NOT NULL,
	`nativeId` varchar(80) NOT NULL,
	`nativeHeading` text NOT NULL,
	`localId` varchar(50) NOT NULL,
	`undifferentiated` tinyint(1) unsigned NOT NULL default '0',
	KEY (`id`),
	KEY (`localId`),
	PRIMARY KEY(`source`,`nativeId`))
ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci""".formatted(MADS_AUTHORITY_TABLE));

			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`heading_id` int(10) unsigned NOT NULL,
	`authority_id` int(10) unsigned NOT NULL,
	`main_entry` tinyint(1) unsigned NOT NULL default '0',
	PRIMARY KEY (`heading_id`,`authority_id`),
	KEY (`authority_id`))
ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci""".formatted(MADS_AUTHORITY_TO_HEADING_TABLE));

			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`id` tinyint(3) unsigned NOT NULL,
	`name` varchar(210) NOT NULL,
	PRIMARY KEY  (`id`))
ENGINE=MyISAM DEFAULT CHARSET=latin1""".formatted(MADS_REFERENCE_TYPE_TABLE));

			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`id` int(10) unsigned NOT NULL auto_increment,
	`from_heading` int(10) unsigned NOT NULL,
	`to_heading` int(10) unsigned NOT NULL,
	`ref_type` tinyint(3) unsigned NOT NULL,
	`ref_desc` varchar(256) NOT NULL DEFAULT '',
	PRIMARY KEY (`id`),
	UNIQUE KEY (`from_heading`,`to_heading`,`ref_type`,`ref_desc`),
	KEY (`to_heading`))
ENGINE=MyISAM DEFAULT CHARSET=latin1""".formatted(MADS_REFERENCE_TABLE));

			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`reference_id` int(10) unsigned NOT NULL,
	`authority_id` int(10) unsigned NOT NULL,
	PRIMARY KEY (`reference_id`,`authority_id`),
	KEY (`authority_id`))
ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci""".formatted(MADS_AUTHORITY_TO_REFERENCE_TABLE));

			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`id` tinyint(3) unsigned NOT NULL,
	`name` varchar(210) NOT NULL,
	PRIMARY KEY  (`id`))
ENGINE=MyISAM DEFAULT CHARSET=latin1""".formatted(MADS_HEADING_TYPE_TABLE));

			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`id` tinyint(1) unsigned NOT NULL,
	`name` varchar(210) NOT NULL,
	PRIMARY KEY (`id`))
ENGINE=MyISAM DEFAULT CHARSET=latin1""".formatted(MADS_HEADING_CATEGORY_TABLE));

			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`heading_id` int(10) unsigned NOT NULL,
	`authority_id` int(10) unsigned NOT NULL,
	`rda` text NOT NULL,
	KEY `heading_id` (`heading_id`),
	KEY `authority_id` (`authority_id`))
ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci""".formatted(MADS_RDA_TABLE));

			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`bib_id` int(10) unsigned NOT NULL,
	`category` tinyint(1) unsigned NOT NULL,
	`heading_id` int(10) unsigned NOT NULL,
	`heading` text,
	UNIQUE KEY `category` (`category`,`heading_id`,`bib_id`),
	KEY `bib_id` (`bib_id`),
	KEY `heading_id` (`heading_id`))
ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci""".formatted(MADS_BIB_TO_HEADING_TABLE));

			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`cursor_name` varchar(25) NOT NULL,
	`current_to_date` char(10) DEFAULT NULL,
	PRIMARY KEY (`cursor_name`))
ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci""".formatted(HEADINGS_UPDATE_CURSOR_TABLE));
		}

		// populateStaticData(headings);
	}

	protected static void populateStaticData( Connection headings ) throws SQLException {
		try ( PreparedStatement insertDesc = headings.prepareStatement(
				"INSERT INTO %s (id,name) VALUES (? , ?)".formatted(MADS_HEADING_TYPE_TABLE)) ) {
		for ( HeadingType ht : HeadingType.values()) {
			insertDesc.setInt(1, ht.ordinal());
			insertDesc.setString(2, ht.toString());
			insertDesc.executeUpdate();
		}}

		try ( PreparedStatement insertDesc = headings.prepareStatement(
				"INSERT INTO %s (id,name) VALUES (? , ?)".formatted(MADS_HEADING_CATEGORY_TABLE)) ) {
		for ( HeadingCategory hc : HeadingCategory.values()) {
			insertDesc.setInt(1, hc.ordinal());
			insertDesc.setString(2, hc.toString());
			insertDesc.executeUpdate();
		}}

		try ( PreparedStatement insertRefType = headings.prepareStatement(
				"INSERT INTO %s (id,name) VALUES (? , ?)".formatted(MADS_REFERENCE_TYPE_TABLE)) ) {
		for ( ReferenceType rt : ReferenceType.values()) {
			insertRefType.setInt(1, rt.ordinal());
			insertRefType.setString(2, rt.toString());
			insertRefType.executeUpdate();
		}}

		try ( PreparedStatement insertAuthSource = headings.prepareStatement(
				"INSERT INTO %s (id,name) VALUES (? , ?)".formatted(MADS_AUTHORITY_SOURCE_TABLE)) ) {
			for ( AuthoritySource rt : AuthoritySource.values()) {
				insertAuthSource.setInt(1, rt.ordinal());
				insertAuthSource.setString(2, rt.toString());
				insertAuthSource.executeUpdate();
			}
		}
	}

	public static void setupAuthorityDatabase(Connection authority) throws SQLException {
		try ( Statement stmt = authority.createStatement() ) {
			stmt.execute("""
CREATE TABLE IF NOT EXISTS %s (
	`id` varchar(210) NOT NULL,
	`label` varchar(210) NOT NULL,
	`associatedLocale` mediumtext DEFAULT NULL,
	`birthPlace` mediumtext DEFAULT NULL,
	`deathPlace` mediumtext DEFAULT NULL,
	`fieldOfActivities` mediumtext DEFAULT NULL,
	`hasAffiliations` mediumtext DEFAULT NULL,
	`occupation` mediumtext DEFAULT NULL,
	PRIMARY KEY (`id`))
ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci""".formatted(RWO_TABLE));
		}
	}

	public static PreparedStatement authority2HeadingRemove(Connection headings) throws SQLException {
		return headings.prepareStatement("DELETE FROM %s WHERE authority_id = ?".formatted(MADS_AUTHORITY_TO_HEADING_TABLE));
	}

	public static PreparedStatement authority2HeadingReplace(Connection headings) throws SQLException {
		return headings.prepareStatement(
				"REPLACE INTO %s (heading_id, authority_id, main_entry) VALUES (?,?,?)".formatted(MADS_AUTHORITY_TO_HEADING_TABLE));
	}

	public static int authority2HeadingReplace(Connection headings, int headingId, int authorityId) throws SQLException {
		try ( PreparedStatement pstmt = authority2HeadingReplace(headings) ) {
			pstmt.setInt(1, headingId);
			pstmt.setInt(2, authorityId);
			pstmt.setInt(3, 1);
			return pstmt.executeUpdate();
		}
	}

	public static int authority2HeadingReplace(Connection headings, int headingId, int authorityId, int mainEntry) throws SQLException {
		try ( PreparedStatement pstmt = authority2HeadingReplace(headings) ) {
			pstmt.setInt(1, headingId);
			pstmt.setInt(2, authorityId);
			pstmt.setInt(3, mainEntry);
			return pstmt.executeUpdate();
		}
	}

	public static int authority2HeadingReplace(Connection headings, int headingId, int authorityId, boolean mainEntry) throws SQLException {
		try ( PreparedStatement pstmt = authority2HeadingReplace(headings) ) {
			pstmt.setInt(1, headingId);
			pstmt.setInt(2, authorityId);
			pstmt.setInt(3, mainEntry ? 1 : 0);
			return pstmt.executeUpdate();
		}
	}

	public static PreparedStatement authority2ReferenceRemove(Connection headings) throws SQLException {
		return headings.prepareStatement("DELETE FROM %s WHERE authority_id = ?".formatted(MADS_AUTHORITY_TO_REFERENCE_TABLE));
	}

	public static int authority2ReferenceReplace(Connection headings, int referenceId, int authorityId) throws SQLException {
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"REPLACE INTO %s (reference_id, authority_id) VALUES (?,?)".formatted(MADS_AUTHORITY_TO_REFERENCE_TABLE)) ) {
			pstmt.setInt(1, referenceId);
			pstmt.setInt(2, authorityId);
			return pstmt.executeUpdate();
		}
	}

	public static Integer authorityAdd(Connection headings, AuthorityData rec) throws SQLException, URISyntaxException {
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"INSERT INTO %s (source,nativeId,nativeHeading,localId,undifferentiated) VALUES (?,?,?,?,?)".formatted(MADS_AUTHORITY_TABLE),
				Statement.RETURN_GENERATED_KEYS) ) {
			pstmt.setInt(1, rec.vocab.ordinal());
			pstmt.setString(2, rec.lccn);
			pstmt.setString(3, rec.authorativeLabel);
			pstmt.setString(4, rec.catalogId());
			pstmt.setBoolean(5, rec.undifferentiated);
			int affectedCount = pstmt.executeUpdate();
			if (affectedCount < 1) throw new SQLException("Creating authority row failed.");

			try ( ResultSet resultSet = pstmt.getGeneratedKeys() ) {
				if (resultSet.next()) return resultSet.getInt(1);
			}
		}

		return null;
	}

	public static Integer authorityId(Connection headings, String nativeId,
			AuthoritySource source) throws SQLException {
		if (nativeId == null || source == null) return null;

		try ( PreparedStatement pstmt = headings.prepareStatement(
				"SELECT id FROM %s WHERE source = ? AND nativeId = ?".formatted(MADS_AUTHORITY_TABLE)) ) {
			pstmt.setInt(1, source.ordinal());
			pstmt.setString(2, nativeId);
			try (ResultSet resultSet = pstmt.executeQuery()) {
				while (resultSet.next()) return resultSet.getInt(1);
			}
		}

		return null;
	}

	public static PreparedStatement authorityRemove(Connection headings) throws SQLException {
		return headings.prepareStatement("DELETE FROM %s WHERE id = ?".formatted(MADS_AUTHORITY_TABLE));
	}

	public static AuthorityData authorityRecord(Connection authority, String identifier) throws SQLException, JsonLdError, IOException, URISyntaxException {
		try (PreparedStatement stmt = authority.prepareStatement("""
SELECT *
FROM %s
WHERE id = ?
ORDER BY addedDate DESC
LIMIT 1""".formatted(MADS_UPDATE_TABLE))) {
			stmt.setString(1, identifier);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) return AuthorityData.fromDbRecord(rs);
			}
			System.out.println("authority record not found for identifier: " + identifier);
		}
		return null;
	}

	/**
	 * Retrieves the most recent authority record for the given ID from the database or fetches it if not present.
	 * 
	 * @param authority DB Connection
	 * @param id Authority ID (LC id string)
	 * @return MadsAuthorityData object
	 * @throws SQLException, JsonLdError, IOException, URISyntaxException, InterruptedException
	 */
	public static AuthorityData getOrFetchAuthorityRecord(Connection authority, String id) throws SQLException, JsonLdError, IOException, URISyntaxException, InterruptedException {
		AuthorityData auth = authorityRecord(authority, id);
		if (auth == null) {
			try (InputStream is = fetcher.fetch(id + ".madsrdf.json")) {
				var doc = JsonldUtils.parseFlatJsonLd(is, id);
				auth = JsonldUtils.parseAuthorityData(doc, id);
				auth.init();
				System.out.println("Authority data id " + id + " not found in DB, fetched from remote.");
			}
		}

		return auth;
	}

	public static PreparedStatement checkReferenceWithAuthId(Connection headings) throws SQLException {
		return headings.prepareStatement("SELECT reference_id FROM %s WHERE authority_id = ?".formatted(MADS_AUTHORITY_TO_REFERENCE_TABLE));
	}

	public static PreparedStatement checkReferenceWithRefId(Connection headings) throws SQLException {
		return headings.prepareStatement("SELECT reference_id FROM %s WHERE reference_id = ?".formatted(MADS_AUTHORITY_TO_REFERENCE_TABLE));
	}

	public static String cursor(Connection headings) throws SQLException {
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"SELECT current_to_date FROM %s WHERE cursor_name = ?".formatted(HEADINGS_UPDATE_CURSOR_TABLE)) ) {
			pstmt.setString(1, HEADINGS_CURSOR_INDEX_MADS_AUTHORITY_RECORDS);
			try ( ResultSet resultSet = pstmt.executeQuery() ) {
				if (resultSet.next()) return resultSet.getString(1);
			}
		}

		return null;
	}

	public static int cursorReplace(Connection headings, String cursorName) throws SQLException {
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"REPLACE INTO %s (cursor_name, current_to_date) VALUES (?,?)".formatted(HEADINGS_UPDATE_CURSOR_TABLE)) ) {
			pstmt.setString(1, cursorName);
			pstmt.setString(2, HEADINGS_CURSOR_INDEX_MADS_AUTHORITY_RECORDS);
			return pstmt.executeUpdate();
		}
	}

	public static Integer addHeading(Connection headings, Heading h) throws SQLException {
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"INSERT INTO %s (heading,sort,heading_type,parent_id) VALUES (?,?,?,?)".formatted(MADS_HEADING_TABLE),
				Statement.RETURN_GENERATED_KEYS) ) {
			pstmt.setString(1, h.heading());
			pstmt.setString(2, h.sort());
			pstmt.setInt(3, h.headingType().ordinal());
			pstmt.setInt(4, h.parentId());
			int affectedCount = pstmt.executeUpdate();
			if (affectedCount < 1) throw new SQLException("Creating heading row failed.");

			try ( var resultSet = pstmt.getGeneratedKeys() ) {
				if (resultSet.next()) {
					h.setHeadingId(resultSet.getInt(1));
					return h.headingId();
				}
			}
		}
		return null;
	}

	public static Integer headingId(Connection headings, HeadingType headingType, String sort) throws SQLException {
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"SELECT id FROM %s WHERE heading_type = ? AND sort = ?".formatted(MADS_HEADING_TABLE)) ) {
			pstmt.setInt(1, headingType.ordinal());
			pstmt.setString(2, sort);
			try ( var resultSet = pstmt.executeQuery() ) {
				if (resultSet.next()) {
					return resultSet.getInt(1);
				}
			}
		}
		return null;
	}

	public static Set<String> identifiers(Connection authority) throws SQLException {
		Set<String> identifiers = new TreeSet<>();
		try (Statement stmt = authority.createStatement()) {
			 try (ResultSet rs = stmt.executeQuery("SELECT DISTINCT id FROM %s".formatted(MADS_UPDATE_TABLE))) {
				while (rs.next()) identifiers.add(rs.getString(1));
			}
			System.out.format("%d distinct records in %s.\n".formatted(identifiers.size(), MADS_UPDATE_TABLE));
		}
		return identifiers;
	}

	public static Set<String> identifiersNewerThan(Connection authorityDB, String cursor) throws SQLException {
		Set<String> identifiers = new TreeSet<>();
		try ( PreparedStatement pstmt = authorityDB.prepareStatement("SELECT DISTINCT id FROM %s WHERE addedDate > ?".formatted(MADS_UPDATE_TABLE))) {
			pstmt.setString(1, cursor);
			try (ResultSet rs = pstmt.executeQuery()) {
				while (rs.next())
					identifiers.add(rs.getString(1));
			}
		}
		return identifiers;
	}

	public static String maxAddedDate(Connection authority) throws SQLException {
		try (PreparedStatement pstmt = authority.prepareStatement("SELECT MAX(addedDate) as maxAddedDate FROM %s".formatted(MADS_UPDATE_TABLE));
			 ResultSet rs = pstmt.executeQuery()) {
			if (rs.next()) return rs.getString(1);
		}

		throw new SQLException("No addedDate in system!");
	}

	protected static void noteAdd(Connection headings, Integer headingId, Integer authorityId, String note)
			throws SQLException {
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"INSERT INTO note (heading_id, authority_id, note) VALUES (?,?,?)") ) {
			pstmt.setInt(1, headingId);
			pstmt.setInt(2, authorityId);
			pstmt.setString(3, note);
			int affectedCount = pstmt.executeUpdate();
			if (affectedCount < 1) throw new SQLException("Creating note row failed.");
		}
	}

	public static Integer noteId(Connection headings, int authorityId, String note) throws SQLException {
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"SELECT heading_id FROM %s WHERE authority_id = ? AND note = ?".formatted(MADS_NOTE_TABLE)) ) {
			pstmt.setInt(1, authorityId);
			pstmt.setString(2, note);
			try ( ResultSet resultSet = pstmt.executeQuery() ) {
				while (resultSet.next()) return resultSet.getInt(1);
			}
		}

		return null;
	}

	public static PreparedStatement noteRemove(Connection headings) throws SQLException {
		return headings.prepareStatement("DELETE FROM %s WHERE authority_id = ?".formatted(MADS_NOTE_TABLE));
	}

	protected static void rdaAdd(Connection headings, Integer headingId, Integer authorityId, String json)
			throws SQLException {
		try ( PreparedStatement stmt = headings.prepareStatement(
				"INSERT INTO rda (heading_id, authority_id, rda) VALUES (?, ?, ?)") ) {
			stmt.setInt(1, headingId);
			stmt.setInt(2, authorityId);
			stmt.setString(3, json);
			stmt.executeUpdate();
		}
	}

	public static PreparedStatement rdaRemove(Connection headings) throws SQLException {
		return headings.prepareStatement("DELETE FROM %s WHERE authority_id = ?".formatted(MADS_RDA_TABLE));
	}

	public static Integer referenceAdd(Connection headings, int fromHeadingId, int toHeadingId,
			ReferenceType refType, String refDesc) throws SQLException {
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"INSERT INTO %s (from_heading, to_heading, ref_type, ref_desc) VALUES (?,?,?,?)".formatted(MADS_REFERENCE_TABLE),
				Statement.RETURN_GENERATED_KEYS) ) {
			pstmt.setInt(1, fromHeadingId);
			pstmt.setInt(2, toHeadingId);
			pstmt.setInt(3, refType.ordinal());
			pstmt.setString(4, refDesc);
			int affectedCount = pstmt.executeUpdate();
			if (affectedCount < 1) throw new SQLException("Creating reference row failed.");

			try ( ResultSet resultSet = pstmt.getGeneratedKeys() ) {
				if (resultSet.next()) return resultSet.getInt(1);
			}
		}

		return null;
	}

	public static Integer referenceId(Connection headings, int fromHeadingId, int toHeadingId,
			ReferenceType refType, String refDesc) throws SQLException {
		try ( PreparedStatement pstmt = headings.prepareStatement(
				"SELECT id FROM %s WHERE from_heading = ? AND to_heading = ? AND ref_type = ? AND ref_desc = ?".formatted(MADS_REFERENCE_TABLE)) ) {
			pstmt.setInt(1, fromHeadingId);
			pstmt.setInt(2, toHeadingId);
			pstmt.setInt(3, refType.ordinal());
			pstmt.setString(4, refDesc);
			try ( ResultSet resultSet = pstmt.executeQuery() ) {
				while (resultSet.next()) return resultSet.getInt(1);
			}
		}

		return null;
	}

	public static PreparedStatement referenceRemove(Connection authority) throws SQLException {
		return authority.prepareStatement("DELETE FROM %s WHERE id = ?".formatted(MADS_REFERENCE_TABLE));
	}

	public static PreparedStatement rwo(Connection authority) throws SQLException {
		return authority.prepareStatement("SELECT * FROM %s WHERE id = ?".formatted(RWO_TABLE));
	}

	public static RWO rwo(Connection authority, String rwoId) throws SQLException {
		try ( PreparedStatement pstmt = rwo(authority) ) {
			pstmt.setString(1, rwoId);
			try ( ResultSet rs = pstmt.executeQuery() ) {
				if (rs.next())
					return new RWO(
						rs.getString("id"),
						rs.getString("label"),
						Arrays.asList(rs.getString("associatedLocale").split(",")),
						Arrays.asList(rs.getString("birthPlace").split(",")),
						Arrays.asList(rs.getString("deathPlace").split(",")),
						Arrays.asList(rs.getString("fieldOfActivities").split(",")),
						Arrays.asList(rs.getString("hasAffiliations").split(",")),
						Arrays.asList(rs.getString("occupation").split(","))
					);
			}
		}
		return null;
	}

	public static PreparedStatement addRwo(Connection authority) throws SQLException {
		return authority.prepareStatement(
			"INSERT INTO %s (id, label, associatedLocale, birthPlace, deathPlace, fieldOfActivities, hasAffiliations, occupation) VALUES (?,?,?,?,?,?,?,?)".formatted(RWO_TABLE)
		);
	}

	public static void addRwo(Connection authority, RWO rwo) throws SQLException {
		try ( PreparedStatement pstmt = addRwo(authority) ) {
			pstmt.setString(1, rwo.id());
			pstmt.setString(2, rwo.label());
			pstmt.setString(3, list2Str(rwo.associatedLocale()));
			pstmt.setString(4, list2Str(rwo.birthPlace()));
			pstmt.setString(5, list2Str(rwo.deathPlace()));
			pstmt.setString(6, list2Str(rwo.fieldOfActivities()));
			pstmt.setString(7, list2Str(rwo.hasAffiliations()));
			pstmt.setString(8, list2Str(rwo.occupation()));
			pstmt.executeUpdate();
		}
	}

	/**
	 * Tries to get the RWO from the database, and if not found, fetches it from the remote source.
	 * When it fetches one from the remote source, it caches it to DB.
	 * 
	 * @param authority
	 * @param rwoId
	 * @return
	 * @throws SQLException
	 * @throws JsonLdError
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 */
	public static RWO getOrFetchRwo(Connection authority, String rwoId) throws SQLException, JsonLdError, IOException, InterruptedException, URISyntaxException {
		RWO rwo = rwo(authority, rwoId);
		if (rwo != null) return rwo;

		try (InputStream is = fetcher.fetch(rwoId + ".madsrdf.json")) {
			var doc = JsonldUtils.parseFlatJsonLd(is, rwoId);
			rwo = RWO.fromJsonObject(authority, doc, rwoId);
			addRwo(authority, rwo);
			return rwo;
		}
	}

	public static String list2Str(List<String> list) {
		return String.join(",", list);
	}
}
