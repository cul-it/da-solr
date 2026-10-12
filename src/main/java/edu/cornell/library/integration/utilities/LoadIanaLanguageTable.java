package edu.cornell.library.integration.utilities;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
  * Load the language data from:
  * https://www.iana.org/assignments/language-subtag-registry/language-subtag-registry
  * into a database table, language_iana_codes. This data will be used by BuildHtmlLanguageTable.java
  * to build an HTML data block of language mappings for posting in Confluence.
  */
public class LoadIanaLanguageTable {

	public static void main(String[] args) throws IOException, SQLException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		Config config = Config.loadConfig(requiredArgs);
		ObjectMapper mapper = new ObjectMapper();
		
		List<String> lines = Files.readAllLines(Paths.get("language-subtag-registry"), StandardCharsets.UTF_8);

		Map<String,String> language = new HashMap<>();
		List<Map<String,String>> languages = new ArrayList<>();
		for (String line : lines)
			if (line.equals("%%")) {
				if (language.getOrDefault("Type", "").equals("language")) languages.add(language);
				language = new HashMap<>();
			} else {
				String[] parts = line.split(": ", 2);
				if (parts.length == 2)
					if (language.containsKey(parts[0]))
						language.put(parts[0], language.get(parts[0])+"; "+parts[1]);
					else
						language.put(parts[0], parts[1]);
			}
		if (language.getOrDefault("Type", "").equals("language")) languages.add(language);

		try (Connection connection = config.getDatabaseConnection("Current");
				PreparedStatement insert = connection.prepareStatement(
						"INSERT INTO language_iana_codes (code, description, preferred, content) VALUES (?,?,?,?)")) {
			for (Map<String,String> l : languages) {
				String json = mapper.writeValueAsString(l);
				System.out.println(json);
				insert.setString(1, l.getOrDefault("Subtag", ""));
				insert.setString(2, l.getOrDefault("Description", null));
				insert.setString(3, l.getOrDefault("Preferred-Value", null));
				insert.setString(4, json);
				insert.executeUpdate();
			}
		}
	}

}
