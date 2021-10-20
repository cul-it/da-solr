package edu.cornell.library.integration.metadata.support;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.utilities.Config;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;

public class AuthorityData {

	public Boolean authorized = false;
	public List<String> alternateForms = null;
	public int headingId = 0;
	public List<String> authorityId = new ArrayList<>();
	public String replacementForm = null;

	public AuthorityData( Config config, String heading, HeadingType ht) throws SQLException {

		try ( Connection conn = config.getDatabaseConnection("Headings") ){

			String filingForm = getFilingForm(heading);

			try ( PreparedStatement isReplacedStmt = conn.prepareStatement(
					"SELECT preferred_display FROM replacement_headings WHERE orig_sort = ?")) {
				int i = filingForm.indexOf(" 0000 ");
				String filingPrefix = (i == -1) ? filingForm : filingForm.substring(0,i);
				isReplacedStmt.setString(1, filingPrefix);
				try ( ResultSet rs = isReplacedStmt.executeQuery()) {
					while ( rs.next() ) this.replacementForm = rs.getString(1);
				}
			}

			try ( PreparedStatement isAuthorizedStmt = conn.prepareStatement(
						"SELECT main_entry, heading.id, authority.id"+
						"  FROM heading, authority2heading, authority"+
						" WHERE heading_type = ? AND sort = ?"+
						"   AND heading.id = authority2heading.heading_id"+
						"   AND authority2heading.authority_id = authority.id")  ){
				isAuthorizedStmt.setInt(1, ht.ordinal());
				isAuthorizedStmt.setString(2, filingForm);
				try ( ResultSet rs = isAuthorizedStmt.executeQuery() ){
					while (rs.next()) {
						this.authorized = this.authorized || rs.getBoolean(1);
						this.headingId = rs.getInt(2);
					}
				}
			}

			if ( ! this.authorized )
				return;

			try ( PreparedStatement alternateFormsStmt = conn.prepareStatement(
					"SELECT heading FROM heading, reference, authority2reference, authority "
					+" WHERE to_heading = ? "
					+" AND from_heading = heading.id "
					+" AND ref_type = "+ReferenceType.FROM4XX.ordinal()
					+" AND reference.id = authority2reference.reference_id"
					+" AND authority2reference.authority_id = authority.id"
					+" AND authority.undifferentiated = 0"
					+" ORDER BY sort") ){
				alternateFormsStmt.setInt(1, this.headingId);
				try ( ResultSet rs = alternateFormsStmt.executeQuery() ){
					while (rs.next()) {
						if (this.alternateForms == null)
							this.alternateForms = new ArrayList<>();
						this.alternateForms.add(rs.getString(1).replaceAll(" \\(.*\\)", ""));
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



	public static enum RecordSet {
		NAME("name"),
		SUBJECT("subject"),
		SERIES("series"), /*Not currently implementing series header browse*/
		NAMETITLE("nametitle");
		private final String string;

		private RecordSet(final String name) {
			this.string = name;
		}

		@Override
		public String toString() { return this.string; }
	}

	public static enum ReferenceType {
		TO4XX("alternateForm"),
		FROM4XX("see"),
		TO5XX("seeAlso"),
		FROM5XX("seeAlso");

		private final String string;

		private ReferenceType(final String name) {
			this.string = name;
		}

		@Override
		public String toString() { return this.string; }

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
		public String toString() { return this.name; }

		public String prefix() { return this.idPrefix; }
	}

}
