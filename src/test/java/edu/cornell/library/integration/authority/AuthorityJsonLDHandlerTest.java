package edu.cornell.library.integration.authority;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import com.apicatalog.jsonld.JsonLdError;

public class AuthorityJsonLDHandlerTest {
	@Test
	public void loadLcJsonLd() throws IOException, JsonLdError, URISyntaxException {
		AuthorityJsonLDHandler lbd = new AuthorityJsonLDHandler();
		Path path = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority", "names_madsrdf_1.json");
		String content = Files.readString(path);
		AuthorityParsedData authorityParsedData = lbd.parseBulkEntry(content);
		authorityParsedData.print();
		System.out.println("-----");

		path = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority", "subjects_madsrdf_1.json");
		content = Files.readString(path);
		authorityParsedData = lbd.parseBulkEntry(content);
		authorityParsedData.print();
		System.out.println("-----");

		path = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority", "names_madsrdf_subdivision_1.json");
		content = Files.readString(path);
		authorityParsedData = lbd.parseBulkEntry(content);
		authorityParsedData.print();
		System.out.println("-----");

		path = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority", "subjects_madsrdf_subdivision_1.json");
		content = Files.readString(path);
		authorityParsedData = lbd.parseBulkEntry(content);
		authorityParsedData.print();
		System.out.println("-----");

		path = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority", "subjects_madsrdf_hai_van_pass.json");
		content = Files.readString(path);
		authorityParsedData = lbd.parseBulkEntry(content);
		authorityParsedData.print();
		System.out.println("-----");

		path = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority", "names_undifferentiated_1.json");
		content = Files.readString(path);
		authorityParsedData = lbd.parseBulkEntry(content);
		authorityParsedData.print();
		System.out.println("-----");

		path = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority", "names_deprecated_1.json");
		content = Files.readString(path);
		authorityParsedData = lbd.parseBulkEntry(content);
		authorityParsedData.print();
		System.out.println("-----");

		path = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority", "names_name_title_1.json");
		content = Files.readString(path);
		authorityParsedData = lbd.parseBulkEntry(content);
		authorityParsedData.print();
		System.out.println("-----");

		path = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority", "subjects_name_title_1.json");
		content = Files.readString(path);
		authorityParsedData = lbd.parseBulkEntry(content);
		authorityParsedData.print();
		System.out.println("-----");

		path = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority", "subjects_complex_subject_1.json");
		content = Files.readString(path);
		authorityParsedData = lbd.parseBulkEntry(content);
		authorityParsedData.print();
	}
}
