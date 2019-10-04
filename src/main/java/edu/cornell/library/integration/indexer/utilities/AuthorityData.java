package edu.cornell.library.integration.indexer.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.ReferenceType;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;

public class AuthorityData {

	public Boolean authorized = false;
	public List<String> alternateForms = null;
	public int headingId = 0;
	public Boolean undifferentiated = false;

	public AuthorityData( Config config, String heading, HeadTypeDesc htd) throws SQLException {

		try ( Connection conn = config.getDatabaseConnection("Headings") ){
			try ( PreparedStatement isAuthorizedStmt = conn.prepareStatement(
						"SELECT main_entry, id, undifferentiated FROM heading WHERE type_desc = ? AND sort = ?")  ){
				isAuthorizedStmt.setInt(1, htd.ordinal());
				isAuthorizedStmt.setString(2, getFilingForm(heading));
				try ( ResultSet rs = isAuthorizedStmt.executeQuery() ){
					while (rs.next()) {
						this.authorized = rs.getBoolean(1);
						this.headingId = rs.getInt(2);
						this.undifferentiated = rs.getBoolean(3);
					}
				}
			}

			if ( ! this.authorized || this.undifferentiated)
				return;

			try ( PreparedStatement alternateFormsStmt = conn.prepareStatement(
					"SELECT heading FROM heading, reference "
					+ "WHERE to_heading = ? "
					+ "AND from_heading = heading.id "
					+ "AND ref_type = "+ReferenceType.FROM4XX.ordinal()
					+" ORDER BY sort") ){
				alternateFormsStmt.setInt(1, this.headingId);
				try ( ResultSet rs = alternateFormsStmt.executeQuery() ){
					while (rs.next()) {
						if (this.alternateForms == null)
							this.alternateForms = new ArrayList<>();
						this.alternateForms.add(rs.getString(1));
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

}
