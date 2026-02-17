package edu.cornell.library.integration.authority;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;

public class LoadAuthorityUpdateFiles {

	public static void main(String[] args) throws SQLException, IOException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Authority");
		Config config = Config.loadConfig(requiredArgs);
		Map<String, String> env = System.getenv();
		String inputDir = env.get("input_directory");
		String inputFiles = env.get("input_files");

		if (inputDir == null || inputFiles == null) {
			System.out.println("input_directory and input_files are required environment variables.");
			System.exit(1);
		}
		String[] files = inputFiles.split(", *");
		for (String file : files) {
			File f = new File(inputDir,file);
			if ( ! f.exists() ) {
				System.out.println("File not found at "+f.getPath());
				System.exit(2);
			}
		}

		try (Connection authority = config.getDatabaseConnection("Authority");
				PreparedStatement deleteTailStmt = authority.prepareStatement(
						"DELETE FROM authorityUpdate WHERE updateFile = ? and positionInFile > ?")) {

			for (String file : files) {
				Path filePath = Paths.get(inputDir,file);
				System.out.println(filePath.toString());
				Map<MarcRecord,String> records = LCAuthorityUpdateFile.readFile(filePath);
				LCAuthorityUpdateFile.pushRecordsToDatabase(authority, records, file);
				deleteTailStmt.setString(1, file);
				deleteTailStmt.setInt(2, records.size());
				int deletedCount = deleteTailStmt.executeUpdate();
				if (deletedCount > 0)
					System.out.format("%d records deleted from authorityUpdate for %s past position %d.\n", deletedCount, file, records.size());
			}
		}
	}

}
