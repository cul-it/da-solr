package edu.cornell.library.integration.authority.mads;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.apicatalog.jsonld.JsonLdError;

public class Json2Db {
	static List<String> ids = new ArrayList<>();
	static final Path rootPath = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority");

	@BeforeAll
	public static void init() {
		ids.add("gf2010025067"); ids.add("gf2011026387");
		ids.add("n00000001"); ids.add("n00000054");
		ids.add("n00000203"); ids.add("n00000264");
		ids.add("n00002040"); ids.add("n00002659");
		ids.add("n00014709"); ids.add("n50000589");
		ids.add("n50051125"); ids.add("n50071530");
		ids.add("n50071531"); ids.add("n50071532");
		ids.add("n50071533"); ids.add("n78015928");
		ids.add("n78095330"); ids.add("n79039766");
		ids.add("n79059593"); ids.add("n79089613");
		ids.add("n81092336"); ids.add("n86125073");
		ids.add("no98021565");
		ids.add("sh2001001501"); ids.add("sh2010004770");
		ids.add("sh85007960"); ids.add("sh85009793");
		ids.add("sh85028378"); ids.add("sh85041065");
		ids.add("sh85043705"); ids.add("sh85132964");
		ids.add("sh86001903"); ids.add("sh88001224");
		ids.add("sh97007140");
	}

	public static String uri(String id) {
		if (id.startsWith("gf")) {
			return "http://id.loc.gov/authorities/genreForms/" + id;
		} else if (id.startsWith("n")) {
			return "http://id.loc.gov/authorities/names/" + id;
		} else if (id.startsWith("sh")) {
			return "http://id.loc.gov/authorities/subjects/" + id;
		}

		return null;
	}

	public static String id(String uri) {
		if (uri.startsWith("http://id.loc.gov/authorities/genreForms/")) {
			return uri.substring("http://id.loc.gov/authorities/genreForms/".length());
		} else if (uri.startsWith("http://id.loc.gov/authorities/names/")) {
			return uri.substring("http://id.loc.gov/authorities/names/".length());
		} else if (uri.startsWith("http://id.loc.gov/authorities/subjects/")) {
			return uri.substring("http://id.loc.gov/authorities/subjects/".length());
		}

		return null;
	}

	public static Path resourcePath(String id) {
		return rootPath.resolve(id + ".madsrdf.json");
	}

	// CREATE TABLE IF NOT EXISTS `madsAuthorityUpdate` (`id` text NOT NULL, `lccn` text DEFAULT NULL, `vocabulary` int DEFAULT NULL, `recordStatus` int DEFAULT NULL, `heading` text DEFAULT NULL, `headingType` int DEFAULT NULL, `isSubdivision` int DEFAULT NULL, `undifferentiated` int DEFAULT NULL, `moddate` text DEFAULT NULL, `addedDate` text DEFAULT NULL, `numUpdates` int DEFAULT 0, source text NOT NULL, PRIMARY KEY (`id`, `numUpdates`, `moddate`));
	@Test
	public void testJson2Db() throws IOException, SQLException, JsonLdError, URISyntaxException {
		try (PrintWriter pw = new PrintWriter(new FileWriter("output.txt"))) {
			String addedDate = edu.cornell.library.integration.authority.activitystreams.Utils.getToday();
			for (String id : ids) {
				AuthorityData data = getAuthorityData(id);
				int ri = 0;
				if (data.recordStatus != null) {
					ri = data.recordStatus.ordinal();
				}
				pw.println("insert into %s (id, lccn, vocabulary, recordStatus, heading, headingType, isSubdivision, undifferentiated, moddate, addedDate, numUpdates, source) values (\"%s\", \"%s\", %s, %s, \"%s\", %s, %s, %s, \"%s\", \"%s\", \"%s\", '%s');".formatted(
					DbUtils.MADS_UPDATE_TABLE,
					data.id,
					data.lccn,
					data.vocab.ordinal(),
					ri,
					data.authorativeLabel,
					data.headingType.ordinal(),
					data.isSubdivision,
					data.undifferentiated,
					data.moddate,
					addedDate,
					data.numUpdates,
					data.source.replaceAll("'", "''")
				));
			}
		}
	}

	public static AuthorityData getAuthorityData(String id) throws IOException, SQLException, JsonLdError, URISyntaxException {
		String content = Files.readString(resourcePath(id));
		if (content.startsWith("{")) {
			return JsonldUtils.parseAuthorityData(content);
		} else {
			String uri = uri(id);
			return JsonldUtils.parseAuthorityData(content, uri);
		}
	}
}
