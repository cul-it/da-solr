package edu.cornell.library.integration.metadata.support;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.utilities.Config;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;

public class AuthorityData {

	public Boolean authorized = false;
	public List<String> alternateForms = null;
	public int headingId = 0;
	private Boolean undifferentiated = false;

	public AuthorityData( Config config, String heading, HeadTypeDesc htd)
			throws ClassNotFoundException, SQLException {

		try ( Connection conn = config.getDatabaseConnection("Headings") ){
			try ( PreparedStatement isAuthorizedStmt = conn.prepareStatement(
						"SELECT main_entry, id, undifferentiated FROM heading WHERE type_desc = ? AND sort = ?")  ){
				isAuthorizedStmt.setInt(1, htd.ordinal());
				isAuthorizedStmt.setString(2, getFilingForm(heading));
				try ( ResultSet rs = isAuthorizedStmt.executeQuery() ){
					while (rs.next()) {
						authorized = rs.getBoolean(1);
						headingId = rs.getInt(2);
						undifferentiated = rs.getBoolean(3);
					}
				}
			}

			if ( ! authorized || undifferentiated)
				return;

			try ( PreparedStatement alternateFormsStmt = conn.prepareStatement(
					"SELECT heading FROM heading, reference "
					+ "WHERE to_heading = ? "
					+ "AND from_heading = heading.id "
					+ "AND ref_type = "+ReferenceType.FROM4XX.ordinal()
					+" ORDER BY sort") ){
				alternateFormsStmt.setInt(1, headingId);
				try ( ResultSet rs = alternateFormsStmt.executeQuery() ){
					while (rs.next()) {
						if (alternateForms == null)
							alternateForms = new ArrayList<>();
						alternateForms.add(rs.getString(1));
					}
				}
			}
		}
	}

	/**
	 * 
	 * @param authorized MUST BE FALSE
	 */
	public AuthorityData( Boolean authorized ) {
		if (authorized.equals(true))
			throw new IllegalArgumentException( "Please use default constructor if a heading may be authorized");
	}

	public static enum HeadTypeDesc {
		PERSNAME("Personal Name","pers"),
		CORPNAME("Corporate Name","corp"),
		EVENT("Event","event"),
		GENHEAD("General Heading","gen"),
		TOPIC("Topical Term","topic"),
		GEONAME("Geographic Name","geo"),
		CHRONTERM("Chronological Term","era"),
		GENRE("Genre/Form Term","genr"),
		MEDIUM("Medium of Performance","med"),
		WORK("Work","work");

		private final String string;
		private final String abbrev;

		private HeadTypeDesc(final String name,final String abbrev) {
			string = name;
			this.abbrev = abbrev;
		}

		@Override
		public String toString() { return string; }
		public String abbrev() { return abbrev; }
	}

	public static enum BlacklightField {
		AUTHOR_PERSON    (HeadType.AUTHOR,       HeadTypeDesc.PERSNAME),
		AUTHOR_CORPORATE (HeadType.AUTHOR,       HeadTypeDesc.CORPNAME),
		AUTHOR_EVENT     (HeadType.AUTHOR,       HeadTypeDesc.EVENT),
		SUBJECT_PERSON   (HeadType.SUBJECT,      HeadTypeDesc.PERSNAME),
		SUBJECT_CORPORATE(HeadType.SUBJECT,      HeadTypeDesc.CORPNAME),
		SUBJECT_EVENT    (HeadType.SUBJECT,      HeadTypeDesc.EVENT),
		AUTHORTITLE_WORK (HeadType.AUTHORTITLE,  HeadTypeDesc.WORK),
		SUBJECT_WORK     (HeadType.SUBJECT,      HeadTypeDesc.WORK),
		SUBJECT_TOPIC    (HeadType.SUBJECT,      HeadTypeDesc.TOPIC),
		SUBJECT_PLACE    (HeadType.SUBJECT,      HeadTypeDesc.GEONAME),
		SUBJECT_CHRON    (HeadType.SUBJECT,      HeadTypeDesc.CHRONTERM),
		SUBJECT_GENRE    (HeadType.SUBJECT,      HeadTypeDesc.GENRE);

		private final HeadType _ht;
		private final HeadTypeDesc _htd;

		BlacklightField(final HeadType ht, final HeadTypeDesc htd) {
			_ht = ht;
			_htd = htd;
		}


		public HeadType headingType() { return _ht; }
		public HeadTypeDesc headingTypeDesc() { return _htd; }
		public String browseCtsName() {
			final StringBuilder sb = new StringBuilder();
			sb.append(_ht.toString());
			if ( ! _ht.equals(HeadType.AUTHORTITLE) )
				sb.append('_').append(_htd.abbrev());
			sb.append("_browse");
			return sb.toString();
		}
		public String fieldName() {
			final StringBuilder sb = new StringBuilder();
			sb.append(_ht.toString());
			if ( ! _ht.equals(HeadType.AUTHORTITLE) )
				sb.append('_').append(_htd.abbrev());
			sb.append("_filing");
			return sb.toString();
		}
		public String facetField() {
			final StringBuilder sb = new StringBuilder();
			sb.append(_ht.toString());
			if ( _ht.equals(HeadType.SUBJECT) )
				sb.append('_').append(_htd.abbrev());
			sb.append("_facet");
			return sb.toString();
		}
	}

	public static enum HeadType {
		AUTHOR("author","works_by"),
		SUBJECT("subject","works_about"),
		AUTHORTITLE("authortitle","works"),
		TITLE("title","works");

		private final String string;
		private final String field;

		private HeadType(final String name, final String dbField) {
			string = name;
			field = dbField;
		}

		@Override
		public String toString() { return string; }
		public String dbField() { return field; }
	}

	public static enum RecordSet {
		NAME("name"),
		SUBJECT("subject"),
		SERIES("series"), /*Not currently implementing series header browse*/
		NAMETITLE("nametitle");
		private final String string;

		private RecordSet(final String name) {
			string = name;
		}

		@Override
		public String toString() { return string; }
	}

	public static enum ReferenceType {
		TO4XX("alternateForm"),
		FROM4XX("see"),
		TO5XX("seeAlso"),
		FROM5XX("seeAlso");

		private final String string;

		private ReferenceType(final String name) {
			string = name;
		}

		@Override
		public String toString() { return string; }

	}

	public static enum AuthoritySource {
		LCNAF ("Library of Congress Name Authority File", "n"),
		LCSH  ("Library of Congress Subject Headings",    "s"),
		LCGFT ("Library of Congress Genre/Form Terms",    "g"),
		LOCAL ("Local Authority File",                  "loc");

		private final String name;
		private final String idPrefix;

		private AuthoritySource(final String name, final String idPrefix) {
			this.name = name;
			this.idPrefix = idPrefix;
		}

		@Override
		public String toString() { return name; }

		public String prefix() { return idPrefix; }
	}

}
