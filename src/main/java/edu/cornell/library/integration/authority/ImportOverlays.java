package edu.cornell.library.integration.authority;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import edu.cornell.library.integration.utilities.Config;

public class ImportOverlays {

	public static void main(String[] args) throws FileNotFoundException, IOException, SQLException {

		Config config = Config.loadConfig(Config.getRequiredArgsForDB("Headings"));

		String filename = "overlay.txt";
		try(BufferedReader in = new BufferedReader(new FileReader(filename, StandardCharsets.UTF_16));
				Connection headings = config.getDatabaseConnection("Headings");
				PreparedStatement pstmt = headings.prepareStatement(
					"REPLACE INTO replacement_headings( orig_sort, preferred_display, created) VALUES (?,?,now())")) {
			String line;
			while ((line = in.readLine()) != null) {
				String[] parts = line.split("\t");
				if (parts.length < 2) continue;
				String sortOrig = getFilingForm(unescape(parts[0]));
				String wanted = unescape(parts[1]);
				if (wanted.contains("Overlay")) continue;
				System.out.printf("%s-->%s\n", sortOrig, wanted);
				pstmt.setString(1, sortOrig);
				pstmt.setString(2, wanted);
				pstmt.addBatch();
			}
			pstmt.executeBatch();
		}
		
	}

	private static String unescape( String before ) {
		if ( ! before.contains("\"") ) return before;
		return before.replaceAll("\"\"", "____").replaceAll("\"", "").replaceAll("____", "\"");
	}
}
