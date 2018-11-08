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

	public AuthorityData( Config config, String heading, HeadingType ht)
			throws ClassNotFoundException, SQLException {

		try ( Connection conn = config.getDatabaseConnection("Headings") ){
			try ( PreparedStatement isAuthorizedStmt = conn.prepareStatement(
						"SELECT main_entry, id, undifferentiated FROM heading WHERE heading_type = ? AND sort = ?")  ){
				isAuthorizedStmt.setInt(1, ht.ordinal());
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

	public static enum BlacklightField {
		AUTHOR_PERSON    (HeadingCategory.AUTHOR,      HeadingType.PERSNAME),
		AUTHOR_CORPORATE (HeadingCategory.AUTHOR,      HeadingType.CORPNAME),
		AUTHOR_EVENT     (HeadingCategory.AUTHOR,      HeadingType.EVENT),
		SUBJECT_PERSON   (HeadingCategory.SUBJECT,     HeadingType.PERSNAME),
		SUBJECT_CORPORATE(HeadingCategory.SUBJECT,     HeadingType.CORPNAME),
		SUBJECT_EVENT    (HeadingCategory.SUBJECT,     HeadingType.EVENT),
		AUTHORTITLE_WORK (HeadingCategory.AUTHORTITLE, HeadingType.WORK),
		SUBJECT_WORK     (HeadingCategory.SUBJECT,     HeadingType.WORK),
		SUBJECT_TOPIC    (HeadingCategory.SUBJECT,     HeadingType.TOPIC),
		SUBJECT_PLACE    (HeadingCategory.SUBJECT,     HeadingType.GEONAME),
		SUBJECT_CHRON    (HeadingCategory.SUBJECT,     HeadingType.CHRONTERM),
		SUBJECT_GENRE    (HeadingCategory.SUBJECT,     HeadingType.GENRE);

		private final HeadingCategory hc;
		private final HeadingType ht;

		BlacklightField(final HeadingCategory hc, final HeadingType ht) {
			this.hc = hc;
			this.ht = ht;
		}

		public HeadingCategory headingCategory() { return hc; }
		public HeadingType headingTypeDesc() { return ht; }
		public String browseCtsName() {
			final StringBuilder sb = new StringBuilder();
			sb.append(hc.toString());
			if ( ! hc.equals(HeadingCategory.AUTHORTITLE) )
				sb.append('_').append(ht.abbrev());
			sb.append("_browse");
			return sb.toString();
		}
		public String fieldName() {
			final StringBuilder sb = new StringBuilder();
			sb.append(hc.toString());
			if ( ! hc.equals(HeadingCategory.AUTHORTITLE) )
				sb.append('_').append(ht.abbrev());
			sb.append("_filing");
			return sb.toString();
		}
		public String facetField() {
			final StringBuilder sb = new StringBuilder();
			sb.append(hc.toString());
			if ( hc.equals(HeadingCategory.SUBJECT) )
				sb.append('_').append(ht.abbrev());
			sb.append("_facet");
			return sb.toString();
		}
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
