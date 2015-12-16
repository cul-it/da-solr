package edu.cornell.library.integration.indexer.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;
import static edu.cornell.library.integration.indexer.utilities.FilingNormalization.getFilingForm;

public class AuthorityData {
	
	public Boolean authorized = false;
	public List<String> alternateForms = null;
	public int headingId = 0;
	
	public AuthorityData( SolrBuildConfig config, String heading, HeadTypeDesc htd) throws ClassNotFoundException, SQLException {

		Connection conn = config.getDatabaseConnection("Headings");
		PreparedStatement isAuthorizedStmt = conn.prepareStatement(
				"SELECT main_entry, id FROM heading WHERE type_desc = ? AND sort = ?");
		isAuthorizedStmt.setInt(1, htd.ordinal());
		isAuthorizedStmt.setString(2, getFilingForm(heading));
		ResultSet rs = isAuthorizedStmt.executeQuery();
		while (rs.next()) {
			authorized = rs.getBoolean(1);
			headingId = rs.getInt(2);
		}
		rs.close();
		isAuthorizedStmt.close();

		if ( ! authorized )
			return;
		
		PreparedStatement alternateFormsStmt = conn.prepareStatement(
				"SELECT form FROM alt_form WHERE heading_id = ?");
		alternateFormsStmt.setInt(1, headingId);
		rs = alternateFormsStmt.executeQuery();
		while (rs.next()) {
			if (alternateForms == null)
				alternateForms = new ArrayList<String>();
			alternateForms.add(rs.getString(1));
		}
		rs.close();
		alternateFormsStmt.close();
		conn.close();
	}

}
