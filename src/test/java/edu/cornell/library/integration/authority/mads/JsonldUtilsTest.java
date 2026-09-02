package edu.cornell.library.integration.authority.mads;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.authority.AuthoritySource;

public class JsonldUtilsTest {
	static Map<String, Path> resources = new LinkedHashMap<>();

	@BeforeClass
	public static void setup() {
		String base = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority").toString();
		for (String file : Arrays.asList("names_madsrdf_1.json", "names_madsrdf_2.json",
				"subjects_madsrdf_1.json", "names_madsrdf_subdivision_1.json",
				"subjects_madsrdf_subdivision_1.json", "subjects_madsrdf_hai_van_pass.json",
				"names_undifferentiated_1.json", "names_deprecated_1.json",
				"names_name_title_1.json", "subjects_name_title_1.json",
				"subjects_complex_subject_1.json")) {
			Path path = Path.of(base, file);
			resources.put(file, path);
		}
	}

	@Test
	public void parseBulkEntryTest() throws IOException, JsonLdError, URISyntaxException {
		String content = Files.readString(resources.get("names_madsrdf_1.json"));
		AuthorityData data = JsonldUtils.parseAuthorityData(content);
		assertEquals("http://id.loc.gov/authorities/names/n00000001", data.id);
		assertEquals("n 00000001", data.lccn);
		assertEquals(AuthoritySource.valueOf("NAF"), data.vocab);
		assertEquals(RecordStatus.valueOf("REVISED"), data.recordStatus);
		assertEquals("McQuerry, Maureen, 1955-", data.authorativeLabel);
		assertEquals(HeadingType.valueOf("PERSONAL_NAME"), data.headingType);
		assertEquals(false, data.isSubdivision);
		assertEquals(false, data.undifferentiated);
		assertEquals("2025-08-05T02:34:09", data.moddate);

		content = Files.readString(resources.get("names_madsrdf_2.json"));
		data = JsonldUtils.parseAuthorityData(content);
		assertEquals("Belʹdiĭ, A. I︠A︡.", data.authorativeLabel);

		content = Files.readString(resources.get("subjects_madsrdf_1.json"));
		data = JsonldUtils.parseAuthorityData(content);
		assertEquals(AuthoritySource.valueOf("LCSH"), data.vocab);
		assertEquals("ActionScript (Computer program language)", data.authorativeLabel);
		assertEquals(HeadingType.valueOf("TOPIC"), data.headingType);

		content = Files.readString(resources.get("names_madsrdf_subdivision_1.json"));
		data = JsonldUtils.parseAuthorityData(content);
		assertEquals(null, data.lccn);
		assertEquals("Germany > Marienstatt", data.authorativeLabel);
		assertEquals(HeadingType.valueOf("HIERARCHICAL_GEOGRAPHIC"), data.headingType);

		content = Files.readString(resources.get("subjects_madsrdf_subdivision_1.json"));
		data = JsonldUtils.parseAuthorityData(content);
		assertEquals("sh 00008433", data.lccn);
		assertEquals("Reinstatement", data.authorativeLabel);
		assertEquals(HeadingType.valueOf("TOPIC"), data.headingType);
		assertEquals(true, data.isSubdivision);

		content = Files.readString(resources.get("subjects_madsrdf_hai_van_pass.json"));
		data = JsonldUtils.parseAuthorityData(content);
		assertEquals("Hải Vân Pass (Vietnam)", data.authorativeLabel);
		assertEquals(HeadingType.valueOf("GEOGRAPHIC"), data.headingType);
		assertEquals(false, data.isSubdivision);

		content = Files.readString(resources.get("names_undifferentiated_1.json"));
		data = JsonldUtils.parseAuthorityData(content);
		assertEquals("Mason, Jack", data.authorativeLabel);
		assertEquals(true, data.undifferentiated);

		content = Files.readString(resources.get("names_deprecated_1.json"));
		data = JsonldUtils.parseAuthorityData(content);
		assertEquals(null, data.lccn);
		assertEquals(RecordStatus.valueOf("DEPRECATED"), data.recordStatus);
		assertEquals(null, data.authorativeLabel);

		content = Files.readString(resources.get("names_name_title_1.json"));
		data = JsonldUtils.parseAuthorityData(content);
		assertEquals(HeadingType.valueOf("NAME_TITLE"), data.headingType);

		// Following two have -- in authoratativeLabel but is not subdivision
		content = Files.readString(resources.get("subjects_name_title_1.json"));
		data = JsonldUtils.parseAuthorityData(content);
		assertEquals(HeadingType.valueOf("NAME_TITLE"), data.headingType);
		assertEquals("Texas. Declaration of Independence--Signers", data.authorativeLabel);

		content = Files.readString(resources.get("subjects_complex_subject_1.json"));
		data = JsonldUtils.parseAuthorityData(content);
		assertEquals(HeadingType.valueOf("COMPLEX_SUBJECT"), data.headingType);
		assertEquals("Space vehicles--Doppler tracking", data.authorativeLabel);
	}
}
