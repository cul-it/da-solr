package edu.cornell.library.integration.authority;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;

public class IngestChangeFiles {

	public static void main(String[] args) throws IOException, SQLException {

		Config config = Config.loadConfig(Config.getRequiredArgsForDB("Authority"));
		System.out.println(config.getAuthorityChangeFileDirectory());
		String directory = config.getAuthorityChangeFileDirectory();

		Set<String> files = Stream.of(new File(config.getAuthorityChangeFileDirectory()).listFiles())
				.filter(file -> !file.isDirectory())
				.map(File::getName)
				.collect(Collectors.toSet());

		for (String inputFile : files) {
			System.out.println(inputFile);
			Map<MarcRecord,String> records =
					LCAuthorityUpdateFile.readFile(Paths.get(directory, inputFile));
			try (Connection authority = config.getDatabaseConnection("Authority")) {
				LCAuthorityUpdateFile.pushRecordsToDatabase(authority, records, inputFile);
			}
		}
	}

}
